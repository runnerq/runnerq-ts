// The agent against the conductor protocol's schema and examples (spec/protocol/conductor).
import test from "node:test";
import assert from "node:assert/strict";
import { decodeRequest } from "../dist/conductor/decode.js";
import { Commands } from "../dist/conductor/commands.js";
import { Queries } from "../dist/conductor/queries.js";
import { specs } from "../dist/conductor/specs.js";
import { queryCapabilities } from "../dist/postgres/query.js";
import { WireError } from "../dist/conductor/index.js";
import {
  examples,
  frameViolations,
  messages,
  violations,
} from "./conductor-schema.mjs";

const fromCloud = Object.values(messages).filter((m) => m.from === "cloud");

test("conductor: every message has an example, and the examples conform", () => {
  assert.deepEqual(
    examples.map((e) => e.type).sort(),
    Object.keys(messages).sort(),
  );
  for (const e of examples) {
    const m = messages[e.type];
    assert.equal(violations(m.data, e.data), "", `${e.type} data`);
    if (m.response)
      assert.equal(violations(m.response, e.response), "", `${e.type} reply`);
  }
});

test("conductor: the agent decodes every Cloud message's example", () => {
  for (const e of examples) {
    const m = messages[e.type];
    if (m.from !== "cloud") continue;
    assert.ok(specs[m.data], `${e.type}: no decode spec for ${m.data}`);
    assert.deepEqual(decodeRequest(specs[m.data], e.data), e.data, e.type);
  }
});

test("conductor: the agent serves exactly the Cloud's requests", () => {
  const qs = { queryCapabilities };
  const served = [
    "executor.describe",
    ...Object.keys(new Queries(qs, {}, () => false).routes()),
    ...Object.keys(new Commands({}, "q").routes()),
  ];
  assert.deepEqual(
    served.sort(),
    fromCloud
      .filter((m) => m.kind === "req")
      .map((m) => m.type)
      .sort(),
  );
});

test("conductor: each command accepts only its own fields", () => {
  const commands = examples.filter(
    (e) => messages[e.type].response === "CommandResult",
  );
  assert.equal(commands.length, 7);
  // Zero values too: they were tolerated when every command shared one request type.
  const extra = {
    cascade: "none",
    reset_attempts: false,
    at: "",
    priority: 0,
    name: "",
    payload: null,
  };
  for (const e of commands) {
    const spec = specs[messages[e.type].data];
    for (const [field, value] of Object.entries(extra)) {
      if (field in spec.object) continue;
      assert.throws(
        () => decodeRequest(spec, { ...e.data, [field]: value }),
        (err) =>
          err instanceof WireError &&
          err.code === "invalid_argument" &&
          err.details?.field === field,
        `${e.type} with ${field}`,
      );
    }
  }
});

test("conductor: the frame check catches what the schema forbids", () => {
  const report = examples.find((e) => e.type === "executor.report").data;
  const frame = (data) => ({
    v: 1,
    kind: "evt",
    type: "executor.report",
    data,
  });
  assert.equal(frameViolations(frame(report)), "");
  assert.notEqual(frameViolations(frame({ ...report, running: null })), "");
  const { counters: _, ...noCounters } = report;
  assert.notEqual(frameViolations(frame(noCounters)), "");
  assert.notEqual(
    frameViolations({
      v: 1,
      kind: "res",
      id: "1",
      type: "events.unsubscribe",
      data: { cursor: "1" },
    }),
    "",
  );
});
