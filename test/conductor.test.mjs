import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { WebSocketServer } from "ws";
import { activity, Worker } from "../dist/index.js";
import { startAgent } from "../dist/conductor/index.js";

const key = "rqk_test";
const quiet = { info() {}, warn() {} };

// fakeGateway is RunnerQ Cloud's side of an agent session.
async function fakeGateway(
  t,
  { config = {}, rejectN = 0, frame = 4 << 20 } = {},
) {
  const server = http.createServer();
  const wss = new WebSocketServer({ noServer: true });
  const g = {
    hellos: [],
    events: [],
    sockets: [],
    replies: new Map(),
    waiters: [],
    rejected: 0,
    nextId: 0,
  };
  server.on("upgrade", (req, socket, head) => {
    if (
      !new URL(req.url, "http://x").pathname.endsWith("/v1/agent") ||
      req.headers.authorization !== `Bearer ${key}` ||
      g.rejected < rejectN
    ) {
      g.rejected++;
      socket.end("HTTP/1.1 401 Unauthorized\r\n\r\n");
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      g.sockets.push(ws);
      ws.on("message", (raw) => {
        const env = JSON.parse(raw.toString());
        if (env.type === "hello") {
          g.hellos.push(env.data);
          ws.send(
            JSON.stringify({
              v: 1,
              kind: "res",
              id: env.id,
              type: "hello",
              data: {
                version: 1,
                session_id: "sess-" + g.hellos.length,
                app: { id: "app-1", name: "test-app" },
                config,
                limits: { max_frame_bytes: frame },
              },
            }),
          );
        } else if (env.kind === "res") {
          g.replies.get(env.id)?.(env);
        } else if (env.kind === "evt") {
          g.events.push(env);
        }
        for (const w of g.waiters.splice(0)) w();
      });
    });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  g.url = `http://127.0.0.1:${server.address().port}`;
  g.socket = () => g.sockets.at(-1);
  g.until = async (fn, what, ms = 5_000) => {
    const end = Date.now() + ms;
    while (!fn()) {
      if (Date.now() > end) throw new Error(`no ${what}`);
      await new Promise((r) => {
        g.waiters.push(r);
        setTimeout(r, 20);
      });
    }
  };
  g.call = (type, data, meta) =>
    new Promise((resolve, reject) => {
      const id = String(++g.nextId);
      g.replies.set(id, resolve);
      setTimeout(() => reject(new Error(`no reply to ${type}`)), 5_000);
      g.socket().send(
        JSON.stringify({ v: 1, kind: "req", id, type, data, meta }),
      );
    });
  g.send = (type, data) =>
    g.socket().send(JSON.stringify({ v: 1, kind: "evt", type, data }));
  g.eventsOf = (type) => g.events.filter((e) => e.type === type);
  t.after(async () => {
    for (const s of g.sockets) s.terminate();
    wss.close();
    await new Promise((r) => server.close(r));
  });
  return g;
}

function storage() {
  const claims = [];
  let wake;
  return {
    queue: "q1",
    push(c) {
      claims.push(c);
      wake?.();
    },
    async claim(limit) {
      return claims.splice(0, limit);
    },
    async waitForWork(signal) {
      await new Promise((r) => {
        wake = r;
        signal.addEventListener("abort", r, { once: true });
        setTimeout(r, 20);
      });
    },
    async renew() {
      return true;
    },
    async complete() {},
    async fail() {
      return "failed";
    },
    async reap() {
      return 0;
    },
    async close() {},
  };
}
async function worker(t, labels) {
  const s = storage();
  const w = new Worker({ storage: s, concurrency: 3, labels });
  let release;
  const gate = new Promise((r) => (release = r));
  w.register(activity("Echo"), async (_, input) => {
    await gate;
    return input;
  });
  await w.start();
  t.after(async () => {
    release();
    await w.stop({ graceMs: 100 });
  });
  return { worker: w, storage: s, release };
}

test("the agent introduces the worker and reports it", async (t) => {
  const g = await fakeGateway(t);
  const { worker: w, storage: s } = await worker(t, {
    region: "us",
    deploy: "v7",
  });
  const agent = startAgent(w, {
    url: g.url + "/base/",
    apiKey: key,
    labels: { region: "eu" },
    logger: quiet,
  });
  t.after(() => agent.close());

  await g.until(() => g.hellos.length === 1, "hello");
  const hello = g.hellos[0];
  assert.deepEqual(hello.protocol_versions, [1]);
  assert.equal(hello.sdk.name, "runnerq-ts");
  assert.equal(hello.executor.id, w.id);
  assert.deepEqual(hello.executor.queues, ["q1"]);
  assert.deepEqual(hello.executor.activity_types, ["Echo"]);
  assert.equal(hello.executor.max_concurrency, 3);
  assert.ok(hello.executor.started_at.endsWith("Z"));
  assert.deepEqual(hello.executor.labels, { region: "eu", deploy: "v7" });
  assert.deepEqual(Object.keys(hello.capabilities), ["executor.describe"]);
  assert.equal(hello.limits.max_frame_bytes, 4 << 20);
  await g.until(
    () => agent.connected && agent.sessionId === "sess-1",
    "session",
  );

  // A report on connect, then one soon after the worker changes.
  await g.until(
    () => g.eventsOf("executor.report").length === 1,
    "first report",
  );
  const first = g.eventsOf("executor.report")[0].data;
  assert.equal(first.id, w.id);
  assert.equal(first.in_flight, 0);
  assert.equal(first.running, undefined);
  assert.equal(first.claim_lag_ms, 0);
  assert.deepEqual(first.counters, {
    claimed: 0,
    succeeded: 0,
    retried: 0,
    failed: 0,
    timed_out: 0,
    dead_lettered: 0,
    claims_lost: 0,
  });
  const id = "11111111-1111-4111-8111-111111111111";
  s.push({
    id,
    type: "Echo",
    payload: 1,
    serialization: "json-v1",
    token: "t",
    retryCount: 1,
    timeoutMs: 30_000,
    parentId: null,
    rootId: id,
    depth: 0,
    metadata: {},
    leaseDeadlineMs: Date.now() + 60_000,
  });
  await g.until(
    () => g.eventsOf("executor.report").some((e) => e.data.in_flight === 1),
    "report after an activity started",
    3_000,
  );

  const described = await g.call("executor.describe", {});
  assert.equal(described.error, undefined);
  assert.equal(described.data.running.length, 1);
  assert.equal(described.data.running[0].activity_id, id);
  assert.equal(described.data.running[0].attempt, 2);
  assert.equal(described.data.counters.claimed, 1);
});

test("protocol errors, deadlines and the request limit", async (t) => {
  const g = await fakeGateway(t);
  const { worker: w } = await worker(t);
  const agent = startAgent(w, {
    url: g.url,
    apiKey: key,
    maxConcurrentRequests: 1,
    logger: quiet,
  });
  t.after(() => agent.close());
  await g.until(() => agent.connected, "connection");

  const unknown = await g.call("launch_missiles", {});
  assert.equal(unknown.error.code, "unsupported");
  const expired = await g.call(
    "executor.describe",
    {},
    {
      deadline: new Date(Date.now() - 1_000).toISOString(),
    },
  );
  assert.equal(expired.error.code, "deadline_exceeded");

  // A slow handler holds the only slot: the next request is refused, not queued.
  let release;
  agent.handle("block", { v: 1 }, () => new Promise((r) => (release = r)));
  const blocked = g.call("block", {});
  await g.until(() => release, "blocked request");
  const refused = await g.call("executor.describe", {});
  assert.equal(refused.error.code, "resource_exhausted");
  release({ ok: true });
  assert.deepEqual((await blocked).data, { ok: true });
  assert.equal((await g.call("executor.describe", {})).error, undefined);
});

test("oversized replies and failing handlers", async (t) => {
  const g = await fakeGateway(t, { frame: 64 << 10 });
  const { worker: w } = await worker(t);
  const agent = startAgent(w, { url: g.url, apiKey: key, logger: quiet });
  t.after(() => agent.close());
  await g.until(() => agent.connected, "connection");
  agent.handle("huge", { v: 1 }, () => ({ blob: "x".repeat(128 << 10) }));
  agent.handle("boom", { v: 1 }, () => {
    throw new Error("kaboom\nstack");
  });
  assert.equal((await g.call("huge", {})).error.code, "resource_exhausted");
  const boom = await g.call("boom", {});
  assert.equal(boom.error.code, "internal");
  assert.equal(boom.error.message, "kaboom");
});

test("config.update changes the report interval", async (t) => {
  const g = await fakeGateway(t, { config: { report_interval_ms: 60_000 } });
  const { worker: w } = await worker(t);
  const agent = startAgent(w, { url: g.url, apiKey: key, logger: quiet });
  t.after(() => agent.close());
  await g.until(
    () => g.eventsOf("executor.report").length === 1,
    "first report",
  );
  g.send("config.update", { report_interval_ms: 1_000 });
  await new Promise((r) => setTimeout(r, 50));
  // The current 60s wait still runs; a change (the drain beginning) ends it, and from
  // then on reports come every second.
  void w.stop({ graceMs: 100 });
  await g.until(
    () => g.eventsOf("executor.report").length >= 3,
    "reports every second",
    4_000,
  );
  assert.ok(g.eventsOf("executor.report").at(-1).data.draining);
});

test("reconnects after a rejection and a dropped connection, then says goodbye", async (t) => {
  const g = await fakeGateway(t, { rejectN: 2 });
  const { worker: w } = await worker(t);
  const agent = startAgent(w, {
    url: g.url,
    apiKey: key,
    minReconnectDelayMs: 20,
    maxReconnectDelayMs: 100,
    logger: quiet,
  });
  await g.until(() => agent.connected, "connection after two rejections");
  assert.equal(g.rejected, 2);
  g.socket().terminate();
  await g.until(() => g.hellos.length === 2 && agent.connected, "reconnection");
  assert.equal(g.hellos[1].executor.id, w.id);

  const socket = g.socket();
  const closed = new Promise((r) => socket.once("close", (code) => r(code)));
  await agent.close();
  assert.equal(await closed, 1000);
  await g.until(() => g.eventsOf("goodbye").length === 1, "goodbye");
  assert.deepEqual(g.eventsOf("goodbye")[0].data, { reason: "shutdown" });
  assert.equal(agent.connected, false);
  await new Promise((r) => setTimeout(r, 150));
  assert.equal(g.hellos.length, 2, "no reconnection after close");
});

test("an aborted signal says goodbye; bad config is refused", async (t) => {
  const g = await fakeGateway(t);
  const { worker: w } = await worker(t);
  const stop = new AbortController();
  const agent = startAgent(w, {
    url: g.url,
    apiKey: key,
    signal: stop.signal,
    logger: quiet,
  });
  await g.until(() => agent.connected, "connection");
  stop.abort();
  await g.until(() => g.eventsOf("goodbye").length === 1, "goodbye");

  assert.throws(() => startAgent(w, { url: g.url, apiKey: "" }));
  assert.throws(() => startAgent(w, { url: "ftp://x", apiKey: key }));
  assert.throws(() => startAgent(w, { url: "not a url", apiKey: key }));
});
