import test from "node:test";
import assert from "node:assert/strict";
import {
  encode,
  decode,
  registerSerialization,
} from "../dist/serialization.js";

class Money {
  constructor(cents) {
    this.cents = cents;
  }
}
registerSerialization({
  name: "test.Money.v1",
  isApplicable: (value) => value instanceof Money,
  serialize: (value) => value.cents.toString(),
  deserialize: (value) => new Money(BigInt(value)),
});
const roundtrip = (value, format) =>
  decode(JSON.parse(JSON.stringify(encode(value, format))));

test("native values survive a JSON database round trip with their types and references", () => {
  const shared = { amount: 9007199254740993n };
  const value = {
    date: new Date("2026-01-02T03:04:05Z"),
    bytes: Buffer.from([0, 255]),
    map: new Map([["entry", shared]]),
    set: new Set([shared]),
    shared,
    again: shared,
    missing: undefined,
    regexp: /order/gi,
    url: new URL("https://example.com"),
    money: new Money(12500n),
    nan: NaN,
    infinity: Infinity,
    negativeZero: -0,
  };
  value.self = value;
  const restored = roundtrip(value);
  assert.deepEqual(restored, value);
  assert.equal(restored.self, restored);
  assert.equal(restored.shared, restored.again);
  assert.equal(restored.map.get("entry"), restored.shared);
  assert.equal([...restored.set][0], restored.shared);
  assert.equal(restored.money instanceof Money, true);
  assert.equal(Buffer.isBuffer(restored.bytes), true);
  assert.equal(roundtrip(undefined), undefined);
  assert.equal(roundtrip(null), null);
  const error = roundtrip(new Error("failure", { cause: "provider" }));
  assert.equal(error instanceof Error, true);
  assert.equal(error.message, "failure");
  assert.equal(error.cause, "provider");
});

test("portable values remain plain JSON and user fields never select a decoder", () => {
  const value = {
    serialization: "superjson-v1",
    json: "user data",
    meta: { values: "user data" },
  };
  const stored = encode(value, "json-v1");
  assert.deepEqual(stored.data, value);
  assert.deepEqual(roundtrip(value, "json-v1"), value);
  assert.equal(roundtrip(undefined, "json-v1"), null);
  for (const unsupported of [
    new Date(),
    1n,
    new Map(),
    new Set(),
    { absent: undefined },
  ])
    assert.throws(() => encode(unsupported, "json-v1"), {
      code: "serialization",
    });
});

test("unsupported values and unknown encodings fail instead of silently losing data", () => {
  for (const value of [
    () => {},
    { fn() {} },
    Symbol("x"),
    { [Symbol("x")]: 1 },
    new WeakMap(),
    new (class Unregistered {})(),
    Number.MAX_SAFE_INTEGER + 1,
  ])
    assert.throws(() => encode(value), { code: "serialization" });
  for (const stored of [
    { data: {} },
    { serialization: "future-v2", data: {} },
    { serialization: "superjson-v1", data: {} },
    {
      serialization: "superjson-v1",
      data: { json: "x", meta: { values: ["custom", "missing.v1"] } },
    },
  ])
    assert.throws(() => decode(stored), { code: "serialization" });
  assert.throws(
    () =>
      registerSerialization({
        name: "late",
        isApplicable: () => false,
        serialize: () => null,
        deserialize: () => null,
      }),
    { code: "configuration" },
  );
});
