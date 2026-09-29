import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  activity,
  ChangeSignal,
  NonRetryableError,
  reportExecutor,
  Worker,
} from "../dist/index.js";
import { fakeStorage } from "./helpers.mjs";

function claimFor(type, due) {
  const id = randomUUID();
  return {
    id,
    type,
    payload: { n: 1 },
    serialization: "json-v1",
    token: "t:" + id,
    retryCount: 0,
    timeoutMs: 30_000,
    parentId: null,
    rootId: id,
    depth: 0,
    metadata: {},
    leaseDeadlineMs: Date.now() + 60_000,
    dueAt: due?.toISOString(),
  };
}
const within = (promise, ms, what) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`no ${what} within ${ms}ms`)), ms),
    ),
  ]);

test("a worker's snapshot, counters, changes and observers", async () => {
  let release;
  const gate = new Promise((r) => (release = r));
  const started = [],
    stopped = [];
  const storage = fakeStorage({
    executorStarted: (source) => started.push(source),
    executorStopped: async (id) => stopped.push(id),
  });
  const extra = { started: [], stopped: [] };
  const worker = new Worker({
    storage,
    concurrency: 3,
    labels: { region: "eu" },
  });
  worker.observe({
    executorStarted: (s) => extra.started.push(s),
    executorStopped: (id) => extra.stopped.push(id),
  });
  worker.register(activity("charge"), async () => {
    await gate;
    return { ok: true };
  });
  worker.register(activity("refund"), () => {
    throw new NonRetryableError("no refunds");
  });

  const before = worker.snapshot();
  assert.equal(before.info.id, worker.id);
  assert.equal(before.info.startedAt, undefined);
  assert.equal(before.counters.claimed, 0);
  assert.deepEqual(before.info.labels, { region: "eu" });

  await worker.start();
  assert.deepEqual(started, [worker]);
  assert.deepEqual(extra.started, [worker]);

  const running = worker.changed();
  const charge = claimFor("charge", new Date(Date.now() - 2_000));
  storage.push(charge);
  await within(running, 2_000, "change when an activity started");
  const snap = worker.snapshot();
  assert.equal(snap.info.queue, "payments");
  assert.equal(snap.info.maxConcurrency, 3);
  assert.deepEqual(snap.info.activityTypes, ["charge", "refund"]);
  assert.ok(snap.info.startedAt instanceof Date);
  assert.equal(snap.info.sdk.name, "runnerq-ts");
  assert.equal(snap.info.sdk.language, "typescript");
  assert.ok(snap.info.hostname);
  assert.equal(snap.state.draining, false);
  assert.equal(snap.state.running.length, 1);
  assert.equal(snap.state.running[0].id, charge.id);
  assert.equal(snap.state.running[0].attempt, 1);
  assert.equal(snap.counters.claimed, 1);
  assert.ok(
    snap.counters.lastClaimLagMs >= 2_000 &&
      snap.counters.lastClaimLagMs < 60_000,
    `claim lag ${snap.counters.lastClaimLagMs}`,
  );

  const finished = worker.changed();
  release();
  await within(finished, 2_000, "change when an activity finished");
  storage.push(claimFor("refund"));
  await within(
    (async () => {
      while (worker.snapshot().counters.failed < 1)
        await new Promise((r) => setTimeout(r, 5));
    })(),
    2_000,
    "failed refund",
  );
  const after = worker.snapshot();
  assert.equal(after.counters.claimed, 2);
  assert.equal(after.counters.succeeded, 1);
  assert.equal(after.counters.failed, 1);
  assert.equal(after.state.running.length, 0);

  const draining = worker.changed();
  const stopping = worker.stop();
  await within(draining, 2_000, "change when the drain began");
  assert.equal(worker.snapshot().state.draining, true);
  await stopping;
  assert.deepEqual(stopped, [worker.id]);
  assert.deepEqual(extra.stopped, [worker.id]);
  assert.throws(() =>
    worker.observe({ executorStarted() {}, executorStopped() {} }),
  );
});

test("ChangeSignal wakes every waiter once", async () => {
  const s = new ChangeSignal();
  s.notify(); // nobody waiting
  const a = s.changed(),
    b = s.changed();
  assert.equal(a, b);
  s.notify();
  await within(Promise.all([a, b]), 100, "wake-up");
  let woken = false;
  s.changed().then(() => (woken = true));
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(woken, false);
});

test("reportExecutor: on the interval, soon after changes, spaced by the gap", async () => {
  const signal = new ChangeSignal();
  const source = { snapshot: () => ({}), changed: () => signal.changed() };
  const sends = [];
  const stop = new AbortController();
  const loop = reportExecutor({
    signal: stop.signal,
    source,
    intervalMs: () => 3_600_000,
    minGapMs: 100,
    send: () => {
      sends.push(Date.now());
    },
  });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(sends.length, 1, "sends at once");
  for (let i = 0; i < 5; i++) {
    signal.notify();
    await new Promise((r) => setTimeout(r, 5));
  }
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(sends.length, 2, "a burst of changes is one report");
  const gap = sends[1] - sends[0];
  assert.ok(gap >= 95 && gap < 1_000, `second report after ${gap}ms`);
  stop.abort();
  await within(loop, 500, "loop to end");

  const plain = [];
  const ctl = new AbortController();
  const plainLoop = reportExecutor({
    signal: ctl.signal,
    source: { snapshot: () => ({}) },
    intervalMs: () => 50,
    minGapMs: 1_000,
    send: () => plain.push(Date.now()),
  });
  await new Promise((r) => setTimeout(r, 180));
  ctl.abort();
  await plainLoop;
  assert.ok(
    plain.length >= 3 && plain.length <= 5,
    `${plain.length} interval reports`,
  );
});

test("a stuck or failing observer can't hold up stop()", async () => {
  const worker = new Worker({ storage: fakeStorage() });
  worker.register(activity("noop"), () => null);
  const errors = [];
  worker.on("workerError", (e) => errors.push(e.message));
  worker.observe({
    executorStarted() {},
    executorStopped: () => new Promise(() => {}), // never settles
  });
  worker.observe({
    executorStarted() {},
    executorStopped: async () => {
      throw new Error("goodbye failed");
    },
  });
  await worker.start();
  const began = Date.now();
  await within(worker.stop({ graceMs: 0 }), 3_000, "stop to finish");
  const took = Date.now() - began;
  assert.ok(took >= 900 && took < 2_500, `stop took ${took}ms`);
  assert.ok(
    errors.some((m) => m.includes("did not finish stopping within 1000ms")),
  );
  assert.ok(errors.includes("goodbye failed"));
});
