import test from "node:test";
import assert from "node:assert/strict";
import { activity, runner, Worker } from "../dist/index.js";
import { json, businessKey, checkpointId } from "../dist/codec.js";
import { executionOptions } from "../dist/options.js";

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
test("identities use RFC UUIDv5 and UTF-8 length with standard unpadded base64", () => {
  // Golden values independently calculated with Python uuid.uuid5 and base64.
  assert.equal(
    checkpointId("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "run", "charge"),
    "020e4a37-85b6-5ee4-a6c6-ce49b8c3fad7",
  );
  assert.equal(businessKey("é", "Charge"), "rq:key:v2:MjrDqUNoYXJnZQ");
  assert.notEqual(businessKey("a-b", "c"), businessKey("a", "b-c"));
  assert.ok(!businessKey("foo", "Bar").endsWith("="));
});
test("names are explicit and worker registration validates routing", async () => {
  assert.throws(() => activity(""));
  const w = new Worker({ storage: {} });
  const a = activity("A");
  w.register(a, () => null);
  assert.throws(() => w.register(a, () => null));
  assert.throws(() => new Worker({ storage: {}, concurrency: 0 }));
});
