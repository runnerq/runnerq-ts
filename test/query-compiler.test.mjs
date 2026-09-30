// The query layer without a database: the filter compiler, cursors, request decoding
// and the wire views.
import test from "node:test";
import assert from "node:assert/strict";
import {
  SqlBuilder,
  PostgresQueries,
  applicationIdempotencyKey,
  canonicalEvent,
  internalEvents,
  encodeCursor,
  parseWait,
  queryCapabilities,
} from "../dist/postgres/query.js";
import { QueryError } from "../dist/storage.js";
import { businessKey, isTimestamp, parseUuid } from "../dist/codec.js";
import { encode } from "../dist/serialization.js";
import { decodeRequest } from "../dist/conductor/decode.js";
import {
  filterSpec,
  plainJson,
  toResult,
  toActivity,
  Queries,
} from "../dist/conductor/queries.js";
import { WireError } from "../dist/conductor/index.js";

const id = "0b6b1a51-8f3a-4d5e-9c3b-2f1e0d9c8b7a";

function compile(filter, on = "activities") {
  const sb = new SqlBuilder();
  return { sql: sb.where(filter, on), args: sb.args };
}
function rejects(fn, kind, field) {
  assert.throws(fn, (e) => {
    assert.ok(e instanceof QueryError, `not a QueryError: ${e}`);
    assert.equal(e.kind, kind, e.message);
    if (field !== undefined) assert.equal(e.field, field);
    return true;
  });
}

test("filters compile to parameterised SQL", () => {
  const { sql, args } = compile({
    and: [
      { field: "status", op: "in", value: ["scheduled", "running"] },
      { field: "type", op: "eq", value: "charge_card" },
      { field: "created_at", op: "gte", value: "2026-09-01T00:00:00Z" },
      { field: "metadata.tenant", op: "eq", value: "acme" },
      { not: { field: "parent_id", op: "exists" } },
      {
        or: [
          { field: "priority", op: "gt", value: 2 },
          { field: "queue", op: "prefix", value: "bill" },
        ],
      },
    ],
  });
  assert.equal(
    sql,
    "(a.status = ANY($1::text[]) AND a.activity_type = ANY($2::text[]) AND a.created_at >= $3::timestamptz" +
      " AND (a.metadata->>$4::text) = ANY($5::text[]) AND (NOT a.parent_activity_id IS NOT NULL)" +
      " AND (a.priority > $6::bigint OR left(a.queue_name, char_length($7::text)) = $7::text))",
  );
  assert.deepEqual(args, [
    ["scheduled", "retrying", "processing"],
    ["charge_card"],
    "2026-09-01T00:00:00Z",
    "tenant",
    ["acme"],
    2,
    "bill",
  ]);
  assert.equal(compile(undefined).sql, "TRUE");
  // Negations stay correct over NULLs.
  assert.equal(
    compile({ field: "executor_id", op: "nin", value: ["x"] }).sql,
    "(NOT COALESCE(NULLIF(split_part(a.current_worker_id, ':', 1), '') = ANY($1::text[]), false))",
  );
});

test("the compiler rejects what it cannot evaluate, naming the field", () => {
  const cases = [
    [{ field: "colour", op: "eq", value: "red" }, "unsupported", "colour"],
    [{ field: "type", op: "contains", value: "x" }, "unsupported", "type"],
    [{ field: "type", op: "", value: "x" }, "unsupported", "type"],
    [{ field: "type", op: "lt", value: "x" }, "unsupported", "type"],
    [
      { field: "priority", op: "prefix", value: "x" },
      "unsupported",
      "priority",
    ],
    [
      { field: "status", op: "eq", value: "exploded" },
      "invalid_argument",
      "status",
    ],
    [{ field: "type", op: "eq", value: 1 }, "invalid_argument", "type"],
    [{ field: "type", op: "in", value: "x" }, "invalid_argument", "type"],
    [
      { field: "priority", op: "eq", value: 1.5 },
      "invalid_argument",
      "priority",
    ],
    [
      { field: "priority", op: "eq", value: 2 ** 54 },
      "invalid_argument",
      "priority",
    ],
    [
      { field: "created_at", op: "gt", value: "yesterday" },
      "invalid_argument",
      "created_at",
    ],
    [
      { field: "created_at", op: "gt", value: "2026-02-30T00:00:00Z" },
      "invalid_argument",
      "created_at",
    ],
    [{ field: "id", op: "eq", value: 7 }, "invalid_argument", "id"],
    [
      { field: "parent_id", op: "exists", value: "yes" },
      "invalid_argument",
      "parent_id",
    ],
    [
      { field: "metadata.", op: "eq", value: "x" },
      "invalid_argument",
      "metadata.",
    ],
    [
      { field: "idempotency_key", op: "prefix", value: "o" },
      "unsupported",
      "idempotency_key",
    ],
    [
      { field: "idempotency_key", op: "eq", value: 3 },
      "invalid_argument",
      "idempotency_key",
    ],
    [
      { field: "type", op: "in", value: Array(1001).fill("x") },
      "invalid_argument",
      "type",
    ],
    [{}, "invalid_argument", "filter"],
    [
      {
        field: "type",
        op: "eq",
        value: "x",
        not: { field: "type", op: "eq", value: "y" },
      },
      "invalid_argument",
      "filter",
    ],
    [{ and: [] }, "invalid_argument", "filter"],
  ];
  for (const [filter, kind, field] of cases)
    rejects(() => compile(filter), kind, field);
  // Events have their own fields: no metadata, and root_id only by eq and in.
  rejects(
    () => compile({ field: "metadata.a", op: "eq", value: "x" }, "events"),
    "unsupported",
  );
  rejects(
    () => compile({ field: "status", op: "eq", value: "pending" }, "events"),
    "unsupported",
    "status",
  );
  rejects(
    () => compile({ field: "root_id", op: "ne", value: id }, "events"),
    "unsupported",
    "root_id",
  );
  // Bounded nesting and size.
  let deep = { field: "type", op: "eq", value: "x" };
  for (let i = 0; i < 9; i++) deep = { not: deep };
  rejects(() => compile(deep), "invalid_argument", "filter");
  const wide = {
    or: Array.from({ length: 64 }, () => ({
      field: "type",
      op: "eq",
      value: "x",
    })),
  };
  rejects(() => compile(wide), "invalid_argument", "filter");
});

test("values that can't match compile to constants", () => {
  assert.equal(
    compile({ field: "id", op: "eq", value: "not-an-id" }).sql,
    "false",
  );
  assert.equal(
    compile({ field: "id", op: "ne", value: "not-an-id" }).sql,
    "true",
  );
  assert.equal(compile({ field: "depth", op: "exists" }).sql, "true");
  assert.equal(
    compile({ field: "depth", op: "exists", value: false }).sql,
    "false",
  );
  assert.equal(
    compile({ field: "root_id", op: "in", value: ["x"] }, "events").sql,
    "false",
  );
  assert.equal(
    compile({ field: "type", op: "eq", value: "made.up" }, "events").sql,
    "false",
  );
  // UUIDs are lowercased; ones this backend could not have issued are dropped.
  const { args } = compile({
    field: "id",
    op: "in",
    value: [id.toUpperCase(), "{" + id + "}", "nope"],
  });
  assert.deepEqual(args, [[id]]);
});

test("event filters: canonical types, roots and seq", () => {
  const { sql, args } = compile(
    {
      and: [
        { field: "type", op: "in", value: ["attempt.failed", "other.custom"] },
        { field: "root_id", op: "eq", value: id },
        { field: "seq", op: "gt", value: 10n },
      ],
    },
    "events",
  );
  assert.equal(
    sql,
    "(e.event_type = ANY($1::text[]) AND e.activity_id IN (SELECT x.id FROM runnerq_activities x" +
      " WHERE x.id = ANY($2::uuid[]) OR x.root_activity_id = ANY($2::uuid[])) AND e.id > $3::bigint)",
  );
  assert.deepEqual(args, [["Failed", "Retrying", "custom"], [id], "10"]);
});

test("every event the TypeScript storage writes has a canonical type", () => {
  const written = [
    "Enqueued",
    "Scheduled",
    "Dequeued",
    "Completed",
    "Retrying",
    "DeadLetter",
    "Failed",
    "ResultStored",
    "Yielded",
    "Signaled",
    "SpawnLinked",
    "Requeued",
    // Commands.
    "Cancelled",
    "Retried",
    "Redriven",
    "RunNow",
    "Rescheduled",
    "PriorityChanged",
  ];
  for (const name of written) {
    const canonical = canonicalEvent(name);
    assert.ok(!canonical.startsWith("other."), `${name} has no canonical type`);
    assert.ok(internalEvents(canonical).includes(name));
  }
  assert.equal(canonicalEvent("Enqueued"), "activity.created");
  assert.equal(canonicalEvent("Dequeued"), "attempt.started");
  assert.equal(canonicalEvent("Requeued"), "attempt.lease_expired");
  assert.equal(canonicalEvent("Brand New"), "other.brand new");
  assert.equal(canonicalEvent("constructor"), "other.constructor");
});

test("capabilities match the Go agent's", () => {
  assert.deepEqual(queryCapabilities(), {
    activityFilters: [
      "attempt",
      "completed_at",
      "created_at",
      "depth",
      "executor_id",
      "id",
      "idempotency_key",
      "max_attempts",
      "metadata",
      "parent_id",
      "priority",
      "queue",
      "root_id",
      "scheduled_for",
      "started_at",
      "status",
      "type",
      "updated_at",
    ],
    activitySorts: ["completed_at", "created_at", "priority"],
    eventFilters: [
      "activity_id",
      "at",
      "executor_id",
      "queue",
      "root_id",
      "seq",
      "type",
    ],
    groupBy: ["queue", "root", "status", "type"],
    buckets: ["completed_at", "created_at"],
    durations: ["queue", "run", "total"],
  });
});

test("ids, timestamps and idempotency keys", () => {
  for (const form of [id, id.toUpperCase()])
    assert.equal(parseUuid(form), id, form);
  for (const bad of ["", "x", id + "0", `{${id}}`, id.replaceAll("-", "")])
    assert.equal(parseUuid(bad), undefined, bad);
  for (const ok of [
    "2026-09-29T10:00:00Z",
    "2026-09-29T10:00:00.123456789+02:00",
    "2024-02-29t00:00:00z",
  ])
    assert.ok(isTimestamp(ok), ok);
  for (const bad of [
    "2026-09-29",
    "2026-09-29T24:00:00Z",
    "2025-02-29T00:00:00Z",
    "2026-09-29T10:00:00",
    5,
  ])
    assert.ok(!isTimestamp(bad), String(bad));

  assert.equal(
    applicationIdempotencyKey(businessKey("order-42", "Charge"), "Charge"),
    "order-42",
  );
  const other = businessKey("order-42", "Refund");
  assert.equal(applicationIdempotencyKey(other, "Charge"), other);
  assert.equal(applicationIdempotencyKey("rq:step:a:b:c", "Charge"), "");
  assert.equal(applicationIdempotencyKey("raw", "Charge"), "raw");
  assert.equal(
    applicationIdempotencyKey("rq:key:v2:!!", "Charge"),
    "rq:key:v2:!!",
  );
});

test("park reasons become waits", () => {
  assert.deepEqual(
    parseWait({
      kind: "sleep",
      step: "nap",
      wake_at: "2026-09-29T10:00:00.000Z",
    }),
    {
      kind: "sleep",
      name: "nap",
      until: new Date("2026-09-29T10:00:00.000Z"),
    },
  );
  // Go records "<kind>:<name>".
  assert.equal(
    parseWait({ kind: "signal", step: "signal:approve" }).name,
    "approve",
  );
  assert.equal(parseWait({ kind: "signal", step: "a:b" }).name, "a:b");
  assert.deepEqual(parseWait({ kind: "await", step: `await:${id}` }), {
    kind: "children",
    name: `await:${id}`,
  });
  assert.deepEqual(parseWait(null), { kind: "other", name: "" });
});

/** A PostgresQueries whose database returns `rows` and records what it was asked. */
function fake(...results) {
  const calls = [];
  const q = new PostgresQueries(async (sql, values) => {
    calls.push({ sql, values });
    return results.shift() ?? [];
  });
  return { q, calls };
}
function row(over = {}) {
  return {
    id,
    activity_type: "Echo",
    queue_name: "q",
    status: "pending",
    priority: 2,
    root_id: id,
    parent_activity_id: null,
    depth: 0,
    idempotency_key: null,
    retry_count: 0,
    max_retries: 0,
    created_at: new Date("2026-09-29T10:00:00.123Z"),
    scheduled_at: null,
    started_at: null,
    completed_at: null,
    updated_at: new Date("2026-09-29T10:00:00.123Z"),
    timeout_seconds: "300",
    lease_deadline_ms: null,
    current_worker_id: null,
    metadata: {},
    yield_detail: null,
    sort_key: "2026-09-29T10:00:00.123400Z",
    ...over,
  };
}

test("activity cursors keep the database's microseconds and their sort", async () => {
  const other = "1b6b1a51-8f3a-4d5e-9c3b-2f1e0d9c8b7a";
  const { q, calls } = fake([row(), row({ id: other })], []);
  const page = await q.activities({ limit: 1 });
  assert.equal(page.items.length, 1);
  const cursor = JSON.parse(
    Buffer.from(page.nextCursor, "base64url").toString(),
  );
  assert.deepEqual(cursor, {
    s: "created_at",
    d: true,
    t: "2026-09-29T10:00:00.1234Z",
    i: id,
  });
  assert.match(calls[0].sql, /ORDER BY a\.created_at DESC, a\.id DESC LIMIT 2/);

  await q.activities({ limit: 1, cursor: page.nextCursor });
  assert.match(
    calls[1].sql,
    /a\.created_at < \$1::timestamptz OR \(a\.created_at = \$1::timestamptz AND a\.id < \$2::uuid\)/,
  );
  assert.deepEqual(calls[1].values, ["2026-09-29T10:00:00.1234Z", id]);

  const bad = async (query, message) =>
    assert.rejects(q.activities(query), (e) => {
      assert.ok(e instanceof QueryError);
      assert.equal(e.kind, "invalid_argument");
      assert.equal(e.field, "cursor");
      if (message) assert.equal(e.message, message);
      return true;
    });
  await bad(
    { cursor: page.nextCursor, sort: { field: "created_at", desc: false } },
    "cursor was issued for a different sort",
  );
  await bad(
    { cursor: page.nextCursor, sort: { field: "priority", desc: true } },
    "cursor was issued for a different sort",
  );
  await bad({ cursor: "!!" }, "invalid cursor");
  await bad({ cursor: "abc=" }, "invalid cursor");
  await bad({
    cursor: encodeCursor({ s: "created_at", d: true, t: "noon", i: id }),
  });
  await bad({ cursor: encodeCursor({ s: "created_at", d: true, i: id }) });
  await bad({
    cursor: encodeCursor({
      s: "created_at",
      d: true,
      t: "2026-09-29T10:00:00Z",
      i: "x",
    }),
  });
  await bad({ cursor: encodeCursor([1]) });
  await assert.rejects(
    q.activities({ sort: { field: "colour", desc: true } }),
    (e) => e.kind === "unsupported" && e.field === "colour",
  );

  // Priority cursors carry a number; completed_at's nulls sort as the zero time.
  const p = fake([row({ sort_key: 3 }), row({ id: other })]);
  const pp = await p.q.activities({
    limit: 1,
    sort: { field: "priority", desc: false },
  });
  assert.deepEqual(JSON.parse(Buffer.from(pp.nextCursor, "base64url")), {
    s: "priority",
    d: false,
    n: 3,
    i: id,
  });
  const c = fake([
    row({ sort_key: "0001-01-01T00:00:00.000000Z" }),
    row({ id: other }),
  ]);
  const cp = await c.q.activities({
    limit: 1,
    sort: { field: "completed_at", desc: true },
  });
  assert.equal(
    JSON.parse(Buffer.from(cp.nextCursor, "base64url")).t,
    "0001-01-01T00:00:00Z",
  );
});

test("limits, counts, events, steps and aggregates", async () => {
  let f = fake();
  await f.q.activities({ limit: 5000 });
  assert.match(f.calls[0].sql, /LIMIT 1001$/);
  await f.q.activities({});
  assert.match(f.calls[1].sql, /LIMIT 51$/);

  f = fake([{ n: "11" }], [{ n: "4" }]);
  assert.deepEqual(await f.q.count(undefined, 10), { count: 10, exact: false });
  assert.deepEqual(await f.q.count(undefined, 10), { count: 4, exact: true });
  assert.match(f.calls[0].sql, /LIMIT 11\) s$/);

  f = fake();
  await assert.rejects(
    f.q.events({ cursor: "abc" }),
    (e) => e.field === "cursor",
  );
  await assert.rejects(
    f.q.events({ cursor: "99999999999999999999" }),
    (e) => e.field === "cursor",
  );
  await f.q.events({ cursor: "12", desc: true });
  assert.match(f.calls[0].sql, /e\.id < \$1::bigint ORDER BY e\.id DESC/);

  f = fake();
  assert.deepEqual(await f.q.steps("nope", false, 0, ""), {
    items: [],
    nextCursor: "",
  });
  assert.equal(f.calls.length, 0);
  await assert.rejects(
    f.q.steps(id, false, 0, "%%"),
    (e) => e.field === "cursor",
  );

  f = fake();
  const agg = (query) => f.q.aggregate(query);
  await assert.rejects(
    agg({}),
    (e) => e.kind === "invalid_argument" && e.field === "metrics",
  );
  await assert.rejects(
    agg({ count: true, groupBy: ["colour"] }),
    (e) => e.kind === "unsupported" && e.field === "colour",
  );
  await assert.rejects(
    agg({ count: true, bucket: { field: "at", intervalMs: 60_000 } }),
    (e) => e.field === "at",
  );
  await assert.rejects(
    agg({ count: true, bucket: { field: "created_at", intervalMs: 500 } }),
    (e) => e.field === "bucket.interval_ms",
  );
  await assert.rejects(
    agg({ durations: [{ field: "sleep" }] }),
    (e) => e.kind === "unsupported" && e.field === "sleep",
  );
  await assert.rejects(
    agg({ durations: [{ field: "run", percentiles: [100] }] }),
    (e) => e.field === "percentiles",
  );

  f = fake([
    {
      c1: "Echo",
      c2: new Date("2026-09-29T10:00:00Z"),
      c3: "7",
      c4: [12.5, null],
    },
    { c1: "Other", c2: new Date("2026-09-29T10:01:00Z"), c3: "1", c4: null },
  ]);
  const rows = await f.q.aggregate({
    groupBy: ["type"],
    count: true,
    bucket: { field: "created_at", intervalMs: 60_000 },
    durations: [{ field: "run", percentiles: [50, 99.9] }],
    limit: 1,
  });
  assert.deepEqual(rows, {
    rows: [
      {
        key: { type: "Echo" },
        bucket: new Date("2026-09-29T10:00:00Z"),
        count: 7,
        durations: { run: { p50: 12.5 } },
      },
    ],
    truncated: true,
  });
  assert.match(f.calls[0].sql, /GROUP BY 1, 2 ORDER BY 2 ASC, 3 DESC LIMIT 2$/);
  assert.deepEqual(f.calls[0].values.at(-1), [0.5, 99.9 / 100]);
});

test("requests decode strictly", () => {
  const spec = {
    object: { filter: filterSpec, include: { array: "string" }, limit: "int" },
  };
  const fails = (data, pattern) =>
    assert.throws(
      () => decodeRequest(spec, data),
      (e) => {
        assert.ok(e instanceof WireError);
        assert.equal(e.code, "invalid_argument");
        assert.match(e.message, /^decode request: /);
        if (pattern) assert.match(e.message, pattern);
        return true;
      },
    );
  fails({ surprise: 1 }, /unknown field "surprise"/);
  fails(
    { filter: { field: "type", op: "eq", value: 1, colour: "red" } },
    /unknown field "colour"/,
  );
  fails({ filter: { and: [{ nope: 1 }] } }, /unknown field "nope"/);
  fails({ limit: "5" }, /limit/);
  fails({ limit: 1.5 });
  fails({ include: "payload" });
  fails([]);
  assert.deepEqual(decodeRequest(spec, undefined), {});
  assert.deepEqual(
    decodeRequest(spec, { filter: null, include: [null, "x"] }),
    { include: ["", "x"] },
  );
  assert.deepEqual(
    decodeRequest(spec, { filter: { field: "a", op: "eq", value: null } }),
    {
      filter: { field: "a", op: "eq", value: null },
    },
  );
});

test("stored values reach the wire as plain JSON", () => {
  const native = encode(
    { at: new Date(0), n: 1n, tags: new Set(["a"]) },
    "superjson-v1",
  );
  assert.deepEqual(plainJson(native.serialization, native.data), {
    at: "1970-01-01T00:00:00.000Z",
    n: "1",
    tags: ["a"],
  });
  const plain = encode({ a: [1, "x"] }, "superjson-v1");
  assert.deepEqual(plainJson(plain.serialization, plain.data), { a: [1, "x"] });
  assert.deepEqual(plainJson("json-v1", { json: 1 }), { json: 1 });
  // A value that won't decode shows as stored rather than failing the query.
  assert.deepEqual(
    plainJson("superjson-v1", {
      json: { a: 1 },
      meta: { values: { a: ["bogus"] } },
    }),
    { a: 1 },
  );

  assert.deepEqual(
    toResult({ state: "Ok", serialization: "json-v1", data: null }),
    { state: "ok", data: null },
  );
  assert.deepEqual(toResult({ state: "Ok", serialization: "json-v1" }), {
    state: "ok",
  });
  assert.deepEqual(
    toResult({
      state: "Err",
      serialization: "json-v1",
      data: { error: "boom", type: "dead_letter", failed_at: "x" },
    }),
    { state: "error", error: { message: "boom", kind: "dead_letter" } },
  );
  assert.deepEqual(
    toResult({
      state: "Err",
      serialization: "json-v1",
      data: { error: "boom", failure: {} },
    }),
    {
      state: "error",
      error: { message: "boom" },
    },
  );
  assert.deepEqual(
    toResult({ state: "Err", serialization: "json-v1", data: { error: 5 } }),
    {
      state: "error",
      data: { error: 5 },
    },
  );

  // Empty fields are omitted, as the protocol asks; unlimited attempts have no max.
  const view = toActivity({
    id,
    type: "Echo",
    queue: "q",
    status: "pending",
    priority: 2,
    rootId: id,
    depth: 0,
    idempotencyKey: "",
    attempt: 1,
    createdAt: new Date(0),
    updatedAt: new Date(0),
    timeoutMs: 0,
    executorId: "",
    metadata: {},
  });
  assert.deepEqual(view, {
    id,
    type: "Echo",
    queue: "q",
    status: "pending",
    priority: 2,
    root_id: id,
    depth: 0,
    attempt: 1,
    created_at: "1970-01-01T00:00:00.000Z",
    updated_at: "1970-01-01T00:00:00.000Z",
  });
});

test("the agent's query handlers check requests and advertise capabilities", async () => {
  const empty = async () => ({ items: [], nextCursor: "" });
  const qs = {
    queryCapabilities,
    queryActivities: empty,
    queryEvents: empty,
    aggregateActivities: async () => ({ rows: [], truncated: false }),
  };
  const routes = new Queries(qs, {}, () => false).routes();
  const fails = (type, data, code, field) =>
    assert.rejects(
      async () => routes[type].handler(data),
      (e) => {
        assert.ok(e instanceof WireError, `not a WireError: ${e}`);
        assert.equal(e.code, code, e.message);
        assert.equal(e.details?.field, field);
        return true;
      },
    );
  await fails(
    "activities.list",
    { include: ["secrets"] },
    "unsupported",
    "include",
  );
  await fails(
    "activities.list",
    { sort: [{ field: "created_at" }, { field: "priority" }] },
    "unsupported",
    "sort",
  );
  await fails(
    "activities.list",
    { sort: [{ field: "created_at", order: "sideways" }] },
    "invalid_argument",
    "sort",
  );
  await fails(
    "events.list",
    { sort: [{ field: "type" }] },
    "unsupported",
    "sort",
  );
  await fails(
    "activities.aggregate",
    { metrics: [{ name: "vibes" }] },
    "unsupported",
    "metrics",
  );
  await fails(
    "activities.list",
    { filter: null, surprise: 1 },
    "invalid_argument",
  );

  const caps = Object.fromEntries(
    Object.entries(routes).map(([type, r]) => [type, r.capability]),
  );
  assert.deepEqual(caps["activities.list"].include, [
    "last_error",
    "payload",
    "result",
  ]);
  assert.ok(caps["activities.list"].filters.includes("metadata"));
  assert.deepEqual(caps["activities.aggregate"].metrics, [
    "count",
    "duration.queue",
    "duration.run",
    "duration.total",
  ]);
  // Stream requests are advertised here but served by the session.
  assert.equal(routes["events.subscribe"].handler, undefined);
  assert.deepEqual(
    caps["events.subscribe"].filters,
    queryCapabilities().eventFilters,
  );
});
