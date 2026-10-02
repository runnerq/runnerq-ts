import test from "node:test";
import assert from "node:assert/strict";
import { activity, runner, Worker } from "../dist/index.js";
import { json, checkpointId } from "../dist/codec.js";
import { executionOptions } from "../dist/options.js";
import { fakeStorage, until } from "./helpers.mjs";

test("activity options are immutable, composable and last wins without losing defaults", () => {
  assert.equal(executionOptions([]).maxAttempts, "unlimited");
  assert.equal(
    executionOptions([runner.priority("high")]).maxAttempts,
    "unlimited",
  );
  assert.equal(executionOptions([runner.maxAttempts(1)]).maxAttempts, 1);
  const base = [runner.priority("low"), runner.maxAttempts(5)];
  const o = executionOptions([...base, runner.priority("high")]);
  assert.equal(o.priority, "high");
  assert.equal(o.maxAttempts, 5);
  assert.equal(o.timeoutMs, 300000);
  assert.equal(
    executionOptions([runner.maxAttempts("unlimited")]).maxAttempts,
    "unlimited",
  );
  assert.throws(() => runner.maxAttempts(0));
  assert.throws(() => runner.timeoutMs(500));
  assert.throws(() =>
    executionOptions([runner.step("a"), runner.idempotencyKey("key")]),
  );
  assert.throws(() => executionOptions([runner.step("a"), runner.asRoot()]));
  assert.throws(() => executionOptions([{ value: { priority: "high" } }]));
  const metadata = { a: "before" };
  const option = runner.metadata(metadata);
  metadata.a = "after";
  assert.equal(executionOptions([option]).metadata.a, "before");
});
test("JSON rejects silent corruption and snapshots mutable input", () => {
  for (const bad of [
    NaN,
    Infinity,
    1n,
    Number.MAX_SAFE_INTEGER + 1,
    new Date(),
    new Map(),
    { a: undefined },
    [undefined],
    new Array(1),
    { a: () => {} },
  ])
    assert.throws(() => json(bad));
  const circular = {};
  circular.self = circular;
  assert.throws(() => json(circular));
  const original = { nested: { a: 1 } };
  const copied = json(original);
  original.nested.a = 2;
  assert.equal(copied.nested.a, 1);
  assert.equal(json(undefined), null);
  assert.equal(
    JSON.stringify(json(JSON.parse('{"__proto__":{"x":1}}'))),
    '{"__proto__":{"x":1}}',
  );
});
test("names are explicit and worker registration validates routing", async () => {
  assert.throws(() => activity(""));
  const w = new Worker({ storage: {} });
  const a = activity("A");
  w.register(a, () => null);
  assert.throws(() => w.register(a, () => null));
  assert.throws(() => new Worker({ storage: {}, concurrency: 0 }));
});

test("a signal wait past setTimeout's range parks on one timer instead of spinning", async (t) => {
  const DAY_MS = 86_400_000;
  const results = new Map();
  let waits = 0,
    deliver;
  const storage = fakeStorage({
    async checkpoint(_fence, id, result) {
      results.set(id, result);
    },
    async getResult(id) {
      return results.get(id) ?? null;
    },
    waitResult(id, signal) {
      waits++;
      return new Promise((resolve, reject) => {
        deliver = () => resolve(results.get(id));
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
      });
    },
  });
  const w = new Worker({ storage });
  w.register(activity("Approval"), (ctx) =>
    ctx.waitForSignal("go", { timeoutMs: 30 * DAY_MS }),
  );
  await w.start();
  t.after(() => w.stop({ graceMs: 100 }));
  const id = "22222222-2222-4222-8222-222222222222";
  storage.push({
    id,
    type: "Approval",
    payload: null,
    serialization: "json-v1",
    token: "t",
    retryCount: 0,
    timeoutMs: 40 * DAY_MS,
    parentId: null,
    rootId: id,
    depth: 0,
    metadata: {},
    leaseDeadlineMs: Date.now() + 60_000,
  });
  await until(() => waits > 0, 3_000, "signal wait");
  // An overflowing timeout aborts after ~1 ms and re-arms in a loop.
  await new Promise((r) => setTimeout(r, 100));
  assert.equal(waits, 1);
  results.set(checkpointId(id, "signal", "go"), {
    state: "Ok",
    serialization: "json-v1",
    data: "approved",
  });
  deliver();
  await until(() => storage.outcomes.length, 3_000, "completion");
  assert.deepEqual(storage.outcomes, [["complete", id]]);
});
