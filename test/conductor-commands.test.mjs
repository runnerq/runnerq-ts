// The agent's commands against PostgreSQL (ports of runnerq-go's conductor/commands_test.go).
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  activity,
  runner,
  ActivityFailedError,
  RunnerQClient,
  Worker,
} from "../dist/index.js";
import { startAgent } from "../dist/conductor/index.js";
import { fingerprint } from "../dist/conductor/commands.js";
import { fakeGateway, key } from "./gateway.mjs";
import { dsn, fakeStorage, quiet, setup, until } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 60_000 }, fn);
const commandTypes = [
  "activities.cancel",
  "activities.delete",
  "activities.reschedule",
  "activities.retry",
  "activities.run_now",
  "activities.set_priority",
  "activities.signal",
];
const Blocker = activity("Blocker");
const Parent = activity("Parent");
const Approval = activity("Approval");

/** A worker (not started) with a Blocker that runs until aborted, and its agent. */
async function control(t, storage, agentConfig = {}, workerConfig = {}) {
  const g = await fakeGateway(t);
  const worker = new Worker({
    storage,
    concurrency: 4,
    waitGraceMs: 20,
    ...workerConfig,
  });
  const started = [],
    stopped = [];
  worker.register(Blocker, async (ctx) => {
    started.push(ctx.activityId);
    await new Promise((resolve) =>
      ctx.signal.addEventListener("abort", resolve, { once: true }),
    );
    stopped.push(ctx.signal.reason);
    throw ctx.signal.reason;
  });
  worker.register(Parent, async (ctx) => {
    try {
      await ctx.wait(await ctx.spawn(Blocker, {}, runner.step("child")));
      return { child: "ok" };
    } catch (error) {
      if (!(error instanceof ActivityFailedError)) throw error;
      return { child_error: error.message };
    }
  });
  worker.register(Approval, (ctx) => ctx.waitForSignal("approve"));
  const agent = startAgent(worker, {
    url: g.url,
    apiKey: key,
    allowControl: true,
    logger: quiet,
    ...agentConfig,
  });
  t.after(() => agent.close());
  await g.until(() => agent.connected, "connection");
  g.ok = async (type, data) => {
    const res = await g.call(type, data);
    assert.equal(res.error, undefined, `${type}: ${JSON.stringify(res.error)}`);
    return res.data;
  };
  g.fails = async (type, data, code) => {
    const res = await g.call(type, data);
    assert.equal(
      res.error?.code,
      code,
      `${type}: got ${JSON.stringify(res.error ?? res.data)}, want ${code}`,
    );
    return res.error;
  };
  const run = async () => {
    await worker.start();
    t.after(() => worker.stop({ graceMs: 1_000 }));
  };
  return { g, agent, worker, started, stopped, run };
}
async function status(storage, id) {
  return (await storage.getActivity(id))?.status;
}

test("commands need allowControl and a CommandStorage", async (t) => {
  const g = await fakeGateway(t);
  for (const [storage, allowControl] of [
    [fakeStorage(), true],
    [fakeStorage({ applyCommand: async () => ({}) }), false],
  ]) {
    const w = new Worker({ storage });
    w.register(activity("Echo"), (_, x) => x);
    const agent = startAgent(w, {
      url: g.url,
      apiKey: key,
      allowControl,
      logger: quiet,
    });
    t.after(() => agent.close());
    await g.until(() => agent.connected, "connection");
    const caps = g.hellos.at(-1).capabilities;
    for (const type of commandTypes) assert.equal(caps[type], undefined);
    const res = await g.call("activities.cancel", {
      target: { ids: [randomUUID()] },
    });
    assert.equal(res.error.code, "unsupported");
    await agent.close();
  }
});

test("capabilities, and a command round trip through the agent", async (t) => {
  const g = await fakeGateway(t);
  const applied = [];
  const storage = fakeStorage({
    queue: "payments",
    async applyCommand(cmd) {
      applied.push(cmd);
      return {
        matched: 1,
        applied: 1,
        cascaded: 0,
        more: false,
        replayed: false,
        items: [
          { id: cmd.target.ids[0], outcome: "applied", status: "pending" },
        ],
      };
    },
  });
  const w = new Worker({ storage });
  w.register(activity("Echo"), (_, x) => x);
  const agent = startAgent(w, {
    url: g.url,
    apiKey: key,
    allowControl: true,
    logger: quiet,
  });
  t.after(() => agent.close());
  await g.until(() => agent.connected, "connection");
  const caps = g.hellos[0].capabilities;
  for (const type of commandTypes)
    assert.deepEqual(caps[type], {
      v: 1,
      targets:
        type === "activities.signal"
          ? ["filter", "idempotency_key", "ids"]
          : ["filter", "ids"],
    });

  const id = randomUUID();
  const data = {
    target: { queue: "payments", ids: [id.toUpperCase(), "nope"] },
    reset_attempts: true,
    command_id: "c-9",
    reason: "flaky",
  };
  const res = await g.call("activities.retry", data);
  assert.deepEqual(res.data, {
    matched: 1,
    applied: 1,
    more: false,
    results: [
      { id, outcome: "applied", status: "pending" },
      {
        id: "nope",
        outcome: "skipped",
        error: { code: "not_found", message: "no such activity" },
      },
    ],
  });
  assert.deepEqual(applied, [
    {
      id: "c-9",
      fingerprint: fingerprint(data),
      kind: "retry",
      target: { ids: [id] },
      dryRun: false,
      reason: "flaky",
      resetAttempts: true,
    },
  ]);
  // Each command decodes as its own request type: another command's field, even at its
  // zero value, fails the request before anything is applied.
  const wrong = await g.call("activities.cancel", {
    command_id: "c-10",
    target: data.target,
    reset_attempts: false,
  });
  assert.equal(wrong.error.code, "invalid_argument");
  assert.deepEqual(wrong.error.details, { field: "reset_attempts" });
  assert.equal(applied.length, 1);

  // Key order doesn't change the fingerprint (Go's: sha256 of re-marshalled JSON).
  assert.equal(
    fingerprint({ command_id: "c-1", target: { ids: ["a"] }, reason: "stuck" }),
    "b17a6e9612043c40b5286d4d46973dcbec9d937bca6bfe2b711c57c4f1123a06",
  );
  assert.equal(
    fingerprint(
      JSON.parse(
        String.raw`{"z":1,"a":[1.5,-0,1e21,1e-7,0.000001,123456789012345678,true,null],"é":"<a&b> ","b":"\ud800x","A":{"y":"\u0001\t\"\\"}}`,
      ),
    ),
    "0b01c33df2e5973440223a8faaba3b16acab2b87e77d18b5c7f9ba50c006ae2e",
  );
  assert.equal(
    fingerprint(JSON.parse(String.raw`{"k":"😀","ÿ":1,"￿":2,"𝄞":3}`)),
    "11c3fc19f9d30c4b2f7799952ce0d54c25e7c9271c3a8cb01da7ef8d7c83d95b",
  );
});

integration(
  "a cancel through the agent stops the handler at once",
  async (t) => {
    const { storage } = await setup(t);
    // A heartbeat far off: only the interrupt can stop the handler in time.
    const { g, worker, started, stopped, run } = await control(
      t,
      storage,
      {},
      { heartbeatMs: 30_000, leaseMs: 60_000 },
    );
    await run();
    const client = new RunnerQClient({ storage });
    const h = await client.execute(Blocker, {});
    await until(() => started.length === 1, 10_000, "blocker start");

    const req = {
      command_id: "c-1",
      reason: "stuck",
      target: { ids: [h.id] },
    };
    const res = await g.ok("activities.cancel", req);
    assert.equal(res.applied, 1);
    assert.equal(res.results[0].status, "cancelled");
    await until(() => stopped.length === 1, 2_000, "handler stop");
    assert.equal(stopped[0].code, "claim_lost");
    assert.match(stopped[0].message, /was cancelled/);
    await until(() => worker.snapshot().state.running.length === 0);
    const { counters } = worker.snapshot();
    assert.equal(counters.claimsLost, 1);
    assert.equal(counters.failed + counters.retried + counters.succeeded, 0);
    assert.equal(await status(storage, h.id), "cancelled");
    await assert.rejects(
      h.result({ signal: AbortSignal.timeout(5_000) }),
      /activity cancelled: stuck/,
    );

    const again = await g.ok("activities.cancel", req);
    assert.equal(again.replayed, true);
    assert.equal(again.applied, 1);
    await g.fails(
      "activities.cancel",
      { ...req, reason: "different" },
      "conflict",
    );
  },
);

integration("a cancelled child fails its awaiting parent", async (t) => {
  const { storage } = await setup(t);
  const { g, started, run } = await control(t, storage);
  await run();
  const client = new RunnerQClient({ storage });
  const parent = await client.execute(Parent, {});
  await until(() => started.length === 1, 10_000, "child start");
  const res = await g.ok("activities.cancel", {
    cascade: "none",
    target: { ids: [started[0]] },
  });
  assert.equal(res.applied, 1);
  const out = await parent.result({ signal: AbortSignal.timeout(10_000) });
  assert.match(out.child_error, /activity cancelled/);
});

integration("signal by idempotency key", async (t) => {
  const { storage } = await setup(t);
  const { g, run } = await control(t, storage);
  await run();
  const client = new RunnerQClient({ storage });
  const h = await client.execute(
    Approval,
    {},
    runner.idempotencyKey("order-9"),
  );
  await until(
    async () => (await status(storage, h.id)) === "waiting",
    10_000,
    "waiting",
  );
  const res = await g.ok("activities.signal", {
    name: "approve",
    payload: { by: "ops" },
    target: { idempotency_key: "order-9", type: "Approval" },
  });
  assert.equal(res.applied, 1);
  assert.deepEqual(await h.result({ signal: AbortSignal.timeout(10_000) }), {
    by: "ops",
  });
});

integration("command requests are checked", async (t) => {
  const { storage, queue } = await setup(t);
  // Not running: work stays pending. Commands work in metadata-only mode, as in Go.
  const { g } = await control(t, storage, { metadataOnly: true });
  const client = new RunnerQClient({ storage });
  const { id } = await client.execute(Blocker, {});

  const e1 = await g.fails(
    "activities.cancel",
    { priority: 3, target: { ids: [id] } },
    "invalid_argument",
  );
  assert.equal(e1.details.field, "priority");
  const e2 = await g.fails(
    "activities.cancel",
    { target: { ids: [id], queue: queue + "_other" } },
    "failed_precondition",
  );
  assert.deepEqual(e2.details, { field: "target.queue" });
  await g.fails(
    "activities.cancel",
    { cascade: "sideways", target: { ids: [id] } },
    "invalid_argument",
  );
  await g.fails(
    "activities.retry",
    { payload: null, target: { ids: [id] } },
    "invalid_argument",
  );
  await g.fails(
    "activities.signal",
    { name: "x", target: { idempotency_key: "k" } },
    "invalid_argument",
  );
  await g.fails(
    "activities.reschedule",
    { at: "tomorrow", target: { ids: [id] } },
    "invalid_argument",
  );
  const noMax = await g.fails(
    "activities.cancel",
    { target: { filter: { field: "type", op: "eq", value: "Blocker" } } },
    "invalid_argument",
  );
  assert.equal(noMax.details.field, "target.max");
  await g.fails(
    "activities.cancel",
    { target: { ids: ["x"] }, surprise: true },
    "invalid_argument",
  );

  const dry = await g.ok("activities.set_priority", {
    priority: 4,
    dry_run: true,
    target: { ids: [id, "not-an-id"] },
  });
  assert.equal(dry.results.length, 2);
  assert.equal(dry.results[0].outcome, "would_apply");
  assert.equal(dry.results[1].error.code, "not_found");
  const retry = await g.ok("activities.retry", { target: { ids: [id] } });
  assert.deepEqual(retry.results[0], {
    id,
    outcome: "skipped",
    status: "pending",
    error: {
      code: "failed_precondition",
      message:
        "only failed, dead-lettered or cancelled activities can be retried",
      details: { status: "pending" },
    },
  });
  const foreign = await g.ok("activities.cancel", {
    target: { ids: ["only-foreign"] },
  });
  assert.equal(foreign.matched, 0);
  assert.equal(foreign.results.length, 1);
  assert.equal(foreign.results[0].error.code, "not_found");
  assert.equal(await status(storage, id), "pending");
  const bulk = await g.ok("activities.cancel", {
    target: {
      filter: { field: "queue", op: "eq", value: queue },
      max: 10,
    },
  });
  assert.equal(bulk.applied, 1);
});
