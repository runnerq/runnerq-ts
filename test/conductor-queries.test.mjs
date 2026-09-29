// The agent's queries and event streams against PostgreSQL (ports of runnerq-go's
// conductor/agent_test.go query tests and conductor/stream_test.go).
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  activity,
  NonRetryableError,
  RunnerQClient,
  Worker,
} from "../dist/index.js";
import { startAgent } from "../dist/conductor/index.js";
import { fakeGateway, key } from "./gateway.mjs";
import { dsn, setup, submission, until } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 60_000 }, fn);
const quiet = { info() {}, warn() {} };
const created = "activity.created";

/** An agent for a worker on `storage`, connected to a fake gateway. */
async function connect(t, storage, { config, frame, metadataOnly } = {}) {
  const g = await fakeGateway(t, { config, frame });
  const worker = new Worker({ storage, concurrency: 2 });
  worker.register(activity("Echo"), (_, input) => input);
  const agent = startAgent(worker, {
    url: g.url,
    apiKey: key,
    metadataOnly,
    minReconnectDelayMs: 10,
    maxReconnectDelayMs: 50,
    logger: quiet,
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
  return { g, agent, worker };
}

/** Submits a root activity (or a child, with `parent`) straight through storage. */
async function enqueue(storage, { type = "Echo", payload = {}, parent } = {}) {
  const a = submission();
  a.type = type;
  a.payload = payload;
  if (parent) {
    a.parentId = parent;
    a.rootId = parent;
    a.depth = 1;
  }
  await storage.submit(a);
  return a.id;
}

/** Scopes a filter to the test's queue (queries span every queue). */
function inQueue(queue, ...terms) {
  return { and: [{ field: "queue", op: "eq", value: queue }, ...terms] };
}
const queueFilter = (queue) => ({ field: "queue", op: "eq", value: queue });

/**
 * Waits for `want` ("<activity id>/<type>" or event ids) on a subscription's frames;
 * returns everything that subscription delivered and its last cursor.
 */
async function collect(g, sub, want, ms = 15_000) {
  let seen, cursor, frames;
  const scan = () => {
    seen = new Map();
    frames = g
      .eventsOf("stream.events")
      .filter((e) => e.data.subscription_id === sub);
    for (const f of frames) {
      for (const ev of f.data.items) {
        seen.set(ev.id, (seen.get(ev.id) ?? 0) + 1);
        const k = `${ev.activity_id}/${ev.type}`;
        seen.set(k, (seen.get(k) ?? 0) + 1);
      }
      cursor = f.data.cursor;
    }
    return want.every((w) => seen.has(w));
  };
  await g.until(scan, `stream events ${want}`, ms);
  return { seen, cursor, frames };
}

integration("queries are answered from storage", async (t) => {
  const { storage, queue } = await setup(t);
  const { g } = await connect(t, storage);
  const hello = g.hellos[0];
  for (const type of [
    "activities.list",
    "activities.get",
    "activities.count",
    "activities.aggregate",
    "steps.list",
    "events.list",
    "results.get",
    "trees.get",
    "events.subscribe",
    "events.unsubscribe",
  ])
    assert.ok(hello.capabilities[type], `capability ${type}`);
  const listCap = hello.capabilities["activities.list"];
  assert.ok(listCap.filters.includes("metadata") && listCap.sorts.length);
  assert.deepEqual(listCap.include, ["last_error", "payload", "result"]);
  assert.deepEqual(hello.capabilities["activities.aggregate"].metrics, [
    "count",
    "duration.queue",
    "duration.run",
    "duration.total",
  ]);

  const id = await enqueue(storage, { payload: { n: 1 } });
  const child = await enqueue(storage, { payload: { n: 2 }, parent: id });
  for (let i = 0; i < 3; i++) await enqueue(storage, { type: "Other" });

  const echo = inQueue(queue, { field: "type", op: "eq", value: "Echo" });
  const first = await g.ok("activities.list", { filter: echo, limit: 1 });
  assert.equal(first.items.length, 1);
  assert.ok(first.next_cursor);
  assert.equal(first.items[0].payload, undefined);
  const second = await g.ok("activities.list", {
    filter: echo,
    limit: 1,
    cursor: first.next_cursor,
  });
  assert.equal(second.items.length, 1);
  assert.equal(second.next_cursor, undefined);
  assert.notEqual(second.items[0].id, first.items[0].id);

  const act = await g.ok("activities.get", {
    id,
    include: ["payload", "events", "steps"],
  });
  assert.equal(act.id, id);
  assert.equal(act.status, "pending");
  assert.equal(act.type, "Echo");
  assert.equal(act.queue, queue);
  assert.equal(act.root_id, id);
  assert.equal(act.attempt, 1);
  assert.equal(act.max_attempts, undefined, "unlimited attempts");
  assert.equal(act.timeout_ms, 300_000);
  assert.deepEqual(act.payload, { n: 1 });
  assert.equal(act.events[0].type, created);
  assert.deepEqual(act.steps, []);
  await g.fails("activities.get", { id: randomUUID() }, "not_found");
  await g.fails("activities.get", { id: "not-an-id" }, "not_found");

  assert.deepEqual(await g.ok("activities.count", { filter: inQueue(queue) }), {
    count: 5,
    exact: true,
  });
  const agg = await g.ok("activities.aggregate", {
    filter: inQueue(queue),
    group_by: ["type"],
    metrics: [{ name: "count" }],
  });
  assert.equal(agg.truncated, false);
  assert.deepEqual(
    Object.fromEntries(agg.groups.map((gr) => [gr.key.type, gr.count])),
    { Echo: 2, Other: 3 },
  );

  const tree = await g.ok("trees.get", { id: child });
  assert.equal(tree.root_id, id);
  assert.deepEqual(
    tree.items.map((a) => a.id),
    [id, child],
  );
  assert.equal(tree.items[1].parent_id, id);
  assert.equal(tree.truncated, false);
  const events = await g.ok("events.list", {
    filter: { field: "root_id", op: "eq", value: id },
    sort: [{ field: "at", order: "desc" }],
  });
  assert.equal(events.items.length, 2);
  assert.equal(events.items[0].activity_id, child);
  assert.equal(events.items[0].id, events.items[0].cursor);
  await g.fails("results.get", { activity_id: id }, "not_found");
  await g.fails("results.get", { activity_id: "nope" }, "not_found");
  assert.deepEqual(await g.ok("steps.list", { activity_id: "nope" }), {
    items: [],
  });
  await g.fails("trees.get", { id: "nope" }, "not_found");

  // Errors carry the offending field.
  const colour = await g.fails(
    "activities.list",
    { filter: { field: "colour", op: "eq", value: "red" } },
    "unsupported",
  );
  assert.deepEqual(colour.details, { field: "colour" });
  const status = await g.fails(
    "activities.list",
    { filter: { field: "status", op: "eq", value: "exploded" } },
    "invalid_argument",
  );
  assert.deepEqual(status.details, { field: "status" });
  const secrets = await g.fails(
    "activities.list",
    { include: ["secrets"] },
    "unsupported",
  );
  assert.deepEqual(secrets.details, { field: "include" });
  await g.fails(
    "activities.list",
    { sort: [{ field: "created_at" }, { field: "priority" }] },
    "unsupported",
  );
  await g.fails(
    "activities.list",
    { sort: [{ field: "created_at", order: "sideways" }] },
    "invalid_argument",
  );
  const surprise = await g.fails(
    "activities.list",
    { filter: null, surprise: 1 },
    "invalid_argument",
  );
  assert.match(surprise.message, /^decode request: .*surprise/);
  await g.fails(
    "activities.aggregate",
    { metrics: [{ name: "vibes" }] },
    "unsupported",
  );
  await g.fails("events.list", { sort: [{ field: "type" }] }, "unsupported");
  await g.fails("activities.list", { cursor: "garbage!" }, "invalid_argument");
});

integration(
  "keyset pages are stable over sub-millisecond ties and nulls",
  async (t) => {
    const { storage, queue, pool } = await setup(t);
    const { g } = await connect(t, storage);
    const ids = [];
    for (let i = 0; i < 5; i++) ids.push(await enqueue(storage));
    // Five activities created within one millisecond; two of them completed.
    for (const [i, id] of ids.entries())
      await pool.query(
        `UPDATE runnerq_activities SET created_at='2026-01-01T00:00:00.123Z'::timestamptz + $2 * INTERVAL '1 microsecond',
      priority=$3, completed_at=CASE WHEN $2 < 2 THEN now() END WHERE id=$1`,
        [id, i, i % 2],
      );
    async function all(sort) {
      const out = [];
      let cursor;
      do {
        const page = await g.ok("activities.list", {
          filter: inQueue(queue),
          sort: [sort],
          limit: 2,
          cursor,
        });
        out.push(...page.items.map((a) => a.id));
        cursor = page.next_cursor;
      } while (cursor);
      return out;
    }
    assert.deepEqual(await all({ field: "created_at" }), [...ids].reverse());
    assert.deepEqual(await all({ field: "created_at", order: "asc" }), ids);
    const byCompletion = await all({ field: "completed_at", order: "desc" });
    assert.deepEqual(new Set(byCompletion), new Set(ids));
    assert.deepEqual(
      new Set(byCompletion.slice(0, 2)),
      new Set(ids.slice(0, 2)),
    );
    const byPriority = await all({ field: "priority", order: "asc" });
    assert.deepEqual(new Set(byPriority), new Set(ids));
    assert.equal(byPriority.length, 5);
  },
);

integration("a running worker's activities, steps and results", async (t) => {
  const { storage, queue } = await setup(t);
  const { g, worker } = await connect(t, storage);
  const Charge = activity("Charge");
  const Refuse = activity("Refuse");
  let release;
  const gate = new Promise((r) => (release = r));
  worker.register(Charge, async (ctx, input) => {
    const at = await ctx.run("stamp", () => new Date(0));
    await gate;
    return { at, input };
  });
  worker.register(Refuse, async () => {
    throw new NonRetryableError("card declined");
  });
  await worker.start();
  t.after(async () => {
    release();
    await worker.stop({ graceMs: 1_000 });
  });
  const client = new RunnerQClient({ storage });
  const h = await client.execute(Charge, { when: new Date(1_000) });

  // Running: claimed by this worker (its id is the executor id), with a lease.
  const running = await until(async () => {
    const steps = await g.ok("steps.list", {
      activity_id: h.id,
      include: ["result"],
    });
    return steps.items.length === 1 && steps;
  });
  assert.equal(running.items[0].name, "stamp");
  assert.equal(running.items[0].kind, "run");
  assert.equal(running.items[0].status, "completed");
  assert.deepEqual(running.items[0].result, {
    state: "ok",
    data: "1970-01-01T00:00:00.000Z",
  });
  const act = await g.ok("activities.get", {
    id: h.id,
    include: ["payload", "result", "steps"],
  });
  assert.equal(act.status, "running");
  assert.equal(act.executor_id, worker.id);
  assert.ok(act.lease_expires_at && act.started_at);
  assert.deepEqual(act.payload, { when: "1970-01-01T00:00:01.000Z" });
  assert.equal(act.result, undefined);
  assert.equal(act.steps.length, 1);
  assert.equal(act.steps[0].result, undefined, "embedded steps carry no data");
  assert.deepEqual(
    await g.ok("activities.count", {
      filter: inQueue(queue, {
        field: "executor_id",
        op: "eq",
        value: worker.id,
      }),
    }),
    { count: 1, exact: true },
  );
  const started = await g.ok("events.list", {
    filter: {
      and: [
        { field: "activity_id", op: "eq", value: h.id },
        { field: "type", op: "eq", value: "attempt.started" },
      ],
    },
  });
  assert.equal(started.items[0].executor_id, worker.id);

  release();
  await h.result({ signal: AbortSignal.timeout(10_000) });
  const done = await g.ok("activities.get", { id: h.id, include: ["result"] });
  assert.equal(done.status, "completed");
  assert.equal(done.executor_id, undefined);
  const expected = {
    at: "1970-01-01T00:00:00.000Z",
    input: { when: "1970-01-01T00:00:01.000Z" },
  };
  assert.deepEqual(done.result, { state: "ok", data: expected });
  assert.deepEqual(await g.ok("results.get", { activity_id: h.id }), {
    state: "ok",
    data: expected,
  });

  const refused = await client.execute(Refuse, {});
  await assert.rejects(refused.result({ signal: AbortSignal.timeout(10_000) }));
  const failed = await g.ok("activities.get", {
    id: refused.id,
    include: ["last_error", "result"],
  });
  assert.equal(failed.status, "failed");
  assert.equal(failed.last_error.message, "card declined");
  assert.equal(failed.last_error.kind, "non_retryable");
  assert.deepEqual(failed.result, {
    state: "error",
    error: { message: "card declined", kind: "non_retryable" },
  });
  assert.deepEqual(
    await g.ok("activities.count", {
      filter: inQueue(queue, { field: "status", op: "in", value: ["failed"] }),
    }),
    { count: 1, exact: true },
  );
});

integration("aggregates bucket and measure durations", async (t) => {
  const { storage, queue, pool } = await setup(t);
  const { g } = await connect(t, storage);
  for (const [i, ms] of [100, 200, 300, 400].entries()) {
    const id = await enqueue(storage);
    await pool.query(
      `UPDATE runnerq_activities SET status='completed',
      created_at='2026-01-01T00:00:10Z'::timestamptz + $2 * INTERVAL '1 minute',
      started_at='2026-01-01T00:00:10Z'::timestamptz + $2 * INTERVAL '1 minute' + INTERVAL '1 second',
      completed_at='2026-01-01T00:00:11Z'::timestamptz + $2 * INTERVAL '1 minute' + $3 * INTERVAL '1 millisecond'
      WHERE id=$1`,
      [id, i < 3 ? 0 : 1, ms],
    );
  }
  await enqueue(storage); // pending: no durations
  const overall = await g.ok("activities.aggregate", {
    filter: inQueue(queue),
    group_by: ["status", "root"],
    metrics: [
      { name: "count" },
      { name: "duration", field: "run", percentiles: [50] },
    ],
  });
  const completed = overall.groups.find((gr) => gr.key.status === "completed");
  assert.deepEqual(completed, {
    key: { status: "completed", root: "true" },
    count: 4,
    durations: { run: { p50: 250 } },
  });
  const pending = overall.groups.find((gr) => gr.key.status === "pending");
  assert.deepEqual(pending.durations, { run: {} });

  const buckets = await g.ok("activities.aggregate", {
    filter: inQueue(queue),
    bucket: {
      field: "created_at",
      interval_ms: 60_000,
      from: "2026-01-01T00:00:00Z",
      to: "2026-01-01T01:00:00Z",
    },
    metrics: [{ name: "duration", field: "total" }],
  });
  assert.deepEqual(buckets, {
    groups: [
      {
        bucket: "2026-01-01T00:00:00.000Z",
        durations: { total: { p50: 1200, p95: 1290, p99: 1298 } },
      },
      {
        bucket: "2026-01-01T00:01:00.000Z",
        durations: { total: { p50: 1400, p95: 1400, p99: 1400 } },
      },
    ],
    truncated: false,
  });
  await g.fails(
    "activities.aggregate",
    {
      bucket: { field: "created_at", interval_ms: 60_000, from: "soon" },
      metrics: [{ name: "count" }],
    },
    "invalid_argument",
  );
});

for (const [name, options] of [
  ["the Cloud asks", { config: { data_mode: "metadata_only" } }],
  ["the agent forces", { config: { data_mode: "full" }, metadataOnly: true }],
])
  integration(`metadata-only mode when ${name}`, async (t) => {
    const { storage, queue } = await setup(t);
    const { g } = await connect(t, storage, options);
    const id = await enqueue(storage, { payload: { secret: "pii" } });
    for (const [type, data] of [
      ["activities.get", { id, include: ["payload"] }],
      ["activities.list", { include: ["last_error"] }],
      ["activities.list", { include: ["result"] }],
      ["trees.get", { id, include: ["payload"] }],
      ["steps.list", { activity_id: id, include: ["result"] }],
      ["events.list", { include: ["detail"] }],
      ["results.get", { activity_id: id }],
    ])
      await g.fails(type, data, "forbidden");
    const act = await g.ok("activities.get", { id, include: ["events"] });
    assert.doesNotMatch(JSON.stringify(act), /pii|detail/);

    // Streams leave details out too.
    const sub = await g.ok("events.subscribe", {
      filter: queueFilter(queue),
      max_delay_ms: 50,
    });
    const later = await enqueue(storage, { payload: { secret: "pii" } });
    const { frames } = await collect(g, sub.subscription_id, [
      `${later}/${created}`,
    ]);
    assert.doesNotMatch(JSON.stringify(frames.map((f) => f.data)), /detail/);
  });

integration("config.update changes the data mode", async (t) => {
  const { storage } = await setup(t);
  const { g } = await connect(t, storage, { config: { data_mode: "full" } });
  const id = await enqueue(storage);
  await g.ok("activities.get", { id, include: ["payload"] });
  g.send("config.update", { data_mode: "metadata_only" });
  await until(
    async () =>
      (await g.call("activities.get", { id, include: ["payload"] })).error
        ?.code === "forbidden",
  );
});

integration(
  "a stream delivers new events and resumes after its cursor",
  async (t) => {
    const { storage, queue } = await setup(t);
    const { g } = await connect(t, storage);
    const before = await enqueue(storage); // before the subscription: not streamed
    const sub = await g.ok("events.subscribe", {
      filter: queueFilter(queue),
      max_delay_ms: 50,
    });
    assert.match(sub.subscription_id, /^sub_[0-9a-f-]{36}$/);
    assert.match(sub.cursor, /^\d+$/);
    const first = await enqueue(storage);
    const { seen, cursor } = await collect(g, sub.subscription_id, [
      `${first}/${created}`,
    ]);
    assert.ok(!seen.has(`${before}/${created}`), "an older event was streamed");
    assert.deepEqual(
      await g.ok("events.unsubscribe", {
        subscription_id: sub.subscription_id,
      }),
      {},
    );
    await g.fails(
      "events.unsubscribe",
      { subscription_id: sub.subscription_id },
      "not_found",
    );

    // Events while nobody is subscribed reach a subscription resuming after the last
    // cursor; nothing already delivered is repeated.
    const missed = await enqueue(storage);
    const again = await g.ok("events.subscribe", {
      filter: queueFilter(queue),
      after_cursor: cursor,
      max_delay_ms: 50,
    });
    assert.equal(again.cursor, cursor);
    const resumed = await collect(g, again.subscription_id, [
      `${missed}/${created}`,
    ]);
    assert.ok(
      !resumed.seen.has(`${first}/${created}`),
      "a resumed stream repeated an event before its cursor",
    );
  },
);

integration(
  "a stream catches events whose transaction commits late",
  async (t) => {
    const { storage, queue, pool } = await setup(t);
    const { g } = await connect(t, storage);
    const sub = await g.ok("events.subscribe", {
      filter: queueFilter(queue),
      max_delay_ms: 50,
    });
    // A transaction takes an event id, then commits after later events have already been
    // streamed past it.
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const late = randomUUID();
      await client.query(
        `INSERT INTO runnerq_events (activity_id, queue_name, event_type) VALUES ($1, $2, 'Enqueued')`,
        [late, queue],
      );
      const after = await enqueue(storage);
      await collect(g, sub.subscription_id, [`${after}/${created}`]);
      await client.query("COMMIT");
      const { seen } = await collect(g, sub.subscription_id, [
        `${late}/${created}`,
      ]);
      for (const [k, n] of seen)
        assert.equal(n, 1, `${k} delivered ${n} times`);
    } finally {
      client.release();
    }
  },
);

integration("stream gaps, limits and filters", async (t) => {
  const { storage, queue } = await setup(t);
  const { g } = await connect(t, storage);
  const sub = await g.ok("events.subscribe", {
    filter: queueFilter(queue),
    after_cursor: "not-a-cursor",
    max_delay_ms: 50,
  });
  await g.until(
    () =>
      g
        .eventsOf("stream.gap")
        .some(
          (e) =>
            e.data.subscription_id === sub.subscription_id &&
            e.data.since_cursor === "not-a-cursor",
        ),
    "stream.gap",
  );
  assert.match(sub.cursor, /^\d+$/);

  const colour = await g.fails(
    "events.subscribe",
    { filter: { field: "colour", op: "eq", value: "x" } },
    "unsupported",
  );
  assert.deepEqual(colour.details, { field: "colour" });
  await g.fails(
    "events.subscribe",
    { filter: queueFilter(queue), surprise: true },
    "invalid_argument",
  );
  for (let i = 0; i < 3; i++)
    await g.ok("events.subscribe", { filter: queueFilter(queue) });
  await g.fails(
    "events.subscribe",
    { filter: queueFilter(queue) },
    "resource_exhausted",
  );

  // Filters apply to the stream: only this queue's "created" events, read from the start
  // of the log.
  await g.ok("events.unsubscribe", { subscription_id: sub.subscription_id });
  const id = await enqueue(storage);
  const filtered = await g.ok("events.subscribe", {
    after_cursor: "0",
    max_delay_ms: 50,
    filter: inQueue(queue, { field: "type", op: "eq", value: created }),
  });
  assert.equal(filtered.cursor, "0");
  const { frames } = await collect(g, filtered.subscription_id, [
    `${id}/${created}`,
  ]);
  for (const f of frames)
    for (const ev of f.data.items) assert.equal(ev.type, created);
});

// A catch-up batch bigger than the Cloud's frame limit goes out as several frames, each
// under the limit, with cursors that only move forward; an event too large for any frame
// goes without its detail rather than jamming the stream.
integration("a stream splits batches to the frame limit", async (t) => {
  const { storage, queue, pool } = await setup(t);
  const frame = 16 << 10;
  const { g } = await connect(t, storage, { frame });
  const sub = await g.ok("events.subscribe", {
    filter: queueFilter(queue),
    max_delay_ms: 50,
  });
  const want = new Map(); // activity id -> detail bytes
  for (let i = 0; i < 13; i++) {
    const id = randomUUID();
    const size = i === 12 ? 40 << 10 : 3000;
    want.set(id, size);
    await pool.query(
      `INSERT INTO runnerq_events (activity_id, queue_name, event_type, detail)
      VALUES ($1, $2, 'Enqueued', jsonb_build_object('blob', repeat('x', $3::int)))`,
      [id, queue, size],
    );
  }
  const { frames } = await collect(
    g,
    sub.subscription_id,
    [...want.keys()].map((id) => `${id}/${created}`),
  );
  let last = 0n;
  const count = new Map();
  for (const f of frames) {
    assert.ok(f.bytes <= frame, `a ${f.bytes}-byte frame is over ${frame}`);
    const cursor = BigInt(f.data.cursor);
    assert.ok(cursor >= last, "the cursor went backwards");
    last = cursor;
    for (const ev of f.data.items) {
      if (!want.has(ev.activity_id)) continue;
      count.set(ev.activity_id, (count.get(ev.activity_id) ?? 0) + 1);
      if (want.get(ev.activity_id) > frame)
        assert.equal(
          ev.detail,
          undefined,
          "the oversized event kept its detail",
        );
      else assert.ok(ev.detail, "an event that fits lost its detail");
    }
  }
  for (const [id, n] of count) assert.equal(n, 1, `${id} delivered ${n} times`);
  assert.ok(frames.length >= 3, `36KB of events in ${frames.length} frames`);
});

integration(
  "subscriptions end without taking the session down, and end with it",
  async (t) => {
    const { storage, queue, pool } = await setup(t);
    const { g, agent } = await connect(t, storage);
    // Unsubscribing while large frames are being pushed leaves the session serving.
    const sub = await g.ok("events.subscribe", {
      filter: queueFilter(queue),
      max_delay_ms: 50,
    });
    for (let i = 0; i < 3; i++)
      await pool.query(
        `INSERT INTO runnerq_events (activity_id, queue_name, event_type, detail)
      VALUES ($1, $2, 'Enqueued', jsonb_build_object('blob', repeat('x', 1000000)))`,
        [randomUUID(), queue],
      );
    await g.until(
      () =>
        g
          .eventsOf("stream.events")
          .some((e) => e.data.subscription_id === sub.subscription_id),
      "a frame",
    );
    await g.ok("events.unsubscribe", { subscription_id: sub.subscription_id });
    await g.ok("executor.describe", {});
    assert.equal(g.hellos.length, 1, "the session survived");

    // A dropped connection ends its subscriptions; the next session starts afresh.
    const old = await g.ok("events.subscribe", {
      filter: queueFilter(queue),
      max_delay_ms: 50,
    });
    g.socket().terminate();
    await g.until(
      () => g.hellos.length === 2 && agent.connected,
      "reconnection",
    );
    const mark = g.events.length;
    const subs = [];
    for (let i = 0; i < 4; i++)
      subs.push(
        await g.ok("events.subscribe", {
          filter: queueFilter(queue),
          max_delay_ms: 50,
        }),
      );
    const id = await enqueue(storage);
    for (const s of subs)
      await collect(g, s.subscription_id, [`${id}/${created}`]);
    assert.ok(
      !g.events
        .slice(mark)
        .some((e) => e.data?.subscription_id === old.subscription_id),
    );
    await g.fails(
      "events.unsubscribe",
      { subscription_id: old.subscription_id },
      "not_found",
    );
  },
);
