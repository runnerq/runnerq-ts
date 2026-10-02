// Vectors from runnerq-spec (the spec/ submodule), shared with the Go SDK.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { businessKey, checkpointId, stepKey } from "../dist/codec.js";
import {
  applicationIdempotencyKey,
  canonicalEvent,
  canonicalStatus,
  internalEvents,
} from "../dist/postgres/query.js";
import { attemptsRemain, retryDelaySeconds } from "../dist/retry.js";
import { normalizeDefault, normalizeIndex } from "../dist/postgres/schema.js";
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

test("spec: attempts remain after a failure", () => {
  for (const { name, input, output } of cases("vectors/attempts_remain.json"))
    assert.equal(
      attemptsRemain(input.retry_count, input.max_retries),
      output,
      name,
    );
});

test("spec: retry delay", () => {
  for (const { name, input, output } of cases("vectors/retry_delay.json"))
    assert.equal(
      retryDelaySeconds(
        input.retry_count,
        input.retry_delay_seconds,
        input.max_retry_delay_seconds,
      ),
      output,
      name,
    );
});

test("spec: canonical statuses", () => {
  for (const { name, input, output } of cases("vectors/canonical_status.json"))
    assert.equal(canonicalStatus(input.status), output, name);
});

test("spec: canonical events", () => {
  for (const { name, input, output } of cases("vectors/canonical_event.json"))
    assert.equal(canonicalEvent(input.event_type), output, name);
});

test("spec: internal events for a canonical type", () => {
  for (const { name, input, output } of cases("vectors/internal_events.json"))
    assert.deepEqual(internalEvents(input.type), output, name);
});

test("spec: index definitions normalize", () => {
  for (const { name, input, output } of cases("vectors/index_definition.json"))
    assert.equal(normalizeIndex(input.definition), output, name);
});

test("spec: column defaults normalize", () => {
  for (const { name, input, output } of cases("vectors/column_default.json"))
    assert.equal(normalizeDefault(input.default), output, name);
});
