// Vectors from runnerq-spec (the spec/ submodule), shared with the Go SDK.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { businessKey, checkpointId, stepKey } from "../dist/codec.js";
import { applicationIdempotencyKey } from "../dist/postgres/query.js";
import { plainJson } from "../dist/conductor/queries.js";

function cases(path) {
  const url = new URL(`../spec/${path}`, import.meta.url);
  let raw;
  try {
    raw = readFileSync(url, "utf8");
  } catch (error) {
    throw new Error(
      `${path}: is the spec submodule checked out? git submodule update --init`,
      { cause: error },
    );
  }
  const { cases } = JSON.parse(raw);
  assert.ok(cases.length, `${path}: no cases`);
  return cases;
}

test("spec: checkpoint IDs", () => {
  for (const { name, input, output } of cases("vectors/checkpoint_id.json"))
    assert.equal(
      checkpointId(input.activity_id, input.kind, input.name),
      output,
      name,
    );
});

test("spec: business keys", () => {
  for (const { name, input, output } of cases("vectors/business_key.json"))
    assert.equal(businessKey(input.key, input.activity_type), output, name);
});

test("spec: application keys", () => {
  for (const { name, input, output } of cases("vectors/application_key.json"))
    assert.equal(
      applicationIdempotencyKey(input.stored, input.activity_type),
      output,
      name,
    );
});

test("spec: step keys", () => {
  for (const { name, input, output } of cases("vectors/step_key.json"))
    assert.equal(
      stepKey(input.root_id, input.parent_id, input.step),
      output,
      name,
    );
});

test("spec: plain JSON of stored values", () => {
  for (const { name, input, output } of cases(
    "serialization/vectors/plain_json.json",
  ))
    assert.deepEqual(plainJson(input.serialization, input.data), output, name);
});
