// PostgresStorage.applyCommand: a port of runnerq-go's storage/storagetest/commands.go
// conformance cases, plus the ledger it shares with Go, the schema migration, and a worker
// stopping an activity cancelled elsewhere.
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { activity, runner, RunnerQClient, Worker } from "../dist/index.js";
import { PostgresStorage } from "../dist/postgres/index.js";
import { isCommandStorage } from "../dist/storage.js";
import { businessKey, checkpointId } from "../dist/codec.js";
import { dsn, setup, submission, until } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30_000 }, fn);
const json = (data) => ({ serialization: "json-v1", data });
const hourFromNow = () => new Date(Date.now() + 3600_000).toISOString();

/** The Go suite's harness over one test queue. */
async function suite(t) {
  const { storage, pool, queue } = await setup(t);
  const types = new Set(["test"]);
  const s = {
    storage,
    pool,
    queue,
    /** Submits an activity (options as runner.* values; `parent` a submitted one). */
    async enqueue({ options = [], parent, type = "test", key } = {}) {
      const a = submission(...options);
      a.type = type;
      types.add(type);
      if (parent) {
        a.parentId = parent.id;
        a.rootId = parent.rootId;
        a.depth = parent.depth + 1;
      }
      if (key) a.key = businessKey(key, type);
      await storage.submit(a);
      return a;
    },
    /** Submits and claims an activity; returns it with its fence. */
    async enqueueClaimed(opts = {}) {
      const a = await s.enqueue({ ...opts, type: "claimed_" + randomUUID() });
      return { ...a, fence: await s.claim(a) };
    },
    async claim(a) {
      const [c] = await storage.claim(1, [a.type], 60_000);
      assert.equal(c?.id, a.id, "claimed the wrong activity");
      return { ownerId: c.id, token: c.token };
    },
    async status(id) {
      const r = await pool.query(
        "SELECT status FROM runnerq_activities WHERE queue_name=$1 AND id=$2",
        [queue, id],
      );
      return r.rows[0]?.status;
    },
    async claimNothing() {
      assert.deepEqual(await storage.claim(10, [...types], 60_000), []);
    },
    apply: (cmd) => storage.applyCommand(cmd),
  };
  return s;
}
const onIds = (kind, ...ids) => ({ kind, target: { ids } });
function itemFor(res, id) {
  const it = res.items.find((x) => x.id === id);
  assert.ok(it, `no result item for ${id} in ${JSON.stringify(res.items)}`);
  return it;
}
function wantApplied(res, id, status) {
  const it = itemFor(res, id);
  assert.equal(it.outcome, "applied", JSON.stringify(it));
  if (status !== undefined) assert.equal(it.status, status);
}
function wantSkipped(res, id, kind) {
  const it = itemFor(res, id);
  assert.equal(it.outcome, "skipped", JSON.stringify(it));
  assert.equal(it.error?.kind, kind);
  assert.ok(it.error.message);
}
function invalid(field) {
  return (e) => {
    assert.equal(e.name, "QueryError");
    assert.equal(e.kind, "invalid_argument");
    assert.equal(e.field, field);
    return true;
  };
}

integration("PostgresStorage is a CommandStorage", async (t) => {
  const { storage } = await setup(t);
  assert.ok(isCommandStorage(storage));
  assert.ok(!isCommandStorage({}));
});

integration("cancel finishes non-terminal work", async (t) => {
  const s = await suite(t);
  const waiting = await s.enqueueClaimed();
  await s.storage.park(waiting.fence, {
    kind: "sleep",
    step: "sleep:nap",
    wakeAt: hourFromNow(),
  });
  const done = await s.enqueueClaimed();
  await s.storage.complete(done.fence, json(null));
  const pending = await s.enqueue();
  const scheduled = await s.enqueue({ options: [runner.delayMs(3600_000)] });
  const missing = randomUUID();

  const res = await s.apply({
    kind: "cancel",
    reason: "bad deploy",
    target: {
      ids: [pending.id, scheduled.id, waiting.id, done.id, missing],
    },
  });
  assert.equal(res.matched, 4);
  assert.equal(res.applied, 3);
  assert.equal(res.items.length, 5);
  assert.equal(res.replayed, false);
  for (const id of [pending.id, scheduled.id, waiting.id]) {
    wantApplied(res, id, "cancelled");
    assert.equal(await s.status(id), "cancelled");
    const r = await s.storage.getResult(id);
    assert.equal(r?.state, "Err");
    assert.equal(r.data.type, "cancelled");
    assert.equal(r.data.error, "activity cancelled: bad deploy");
    assert.match(r.data.failed_at, /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
  }
  wantSkipped(res, done.id, "conflict");
  assert.equal(itemFor(res, done.id).status, "completed");
  wantSkipped(res, missing, "not_found");
  const events = await s.storage.events(pending.id);
  const cancelled = events.find((e) => e.type === "Cancelled");
  assert.deepEqual(cancelled.detail, {
    command_id: "",
    from: "pending",
    reason: "bad deploy",
  });
  await s.claimNothing(); // cancelled work is never claimed
  const snap = await s.storage.getActivity(pending.id);
  assert.equal(snap.status, "cancelled");
  assert.ok(snap.completedAt);
  assert.equal(snap.lastError, "activity cancelled: bad deploy");
});

integration("cancel fences the running claim", async (t) => {
  const s = await suite(t);
  const running = await s.enqueueClaimed();
  wantApplied(
    await s.apply(onIds("cancel", running.id)),
    running.id,
    "cancelled",
  );
  assert.equal(await s.storage.renew(running.fence, 60_000), false);
  await assert.rejects(
    s.storage.complete(running.fence, json({})),
    /no longer owns/,
  );
  await assert.rejects(
    s.storage.fail(running.fence, "late", true),
    /no longer owns/,
  );
  assert.equal(await s.status(running.id), "cancelled");
  const snap = await s.storage.getActivity(running.id);
  assert.equal(snap.currentWorkerId, null);
  assert.equal(snap.lastWorkerId, running.fence.token);
  await s.storage.reap(100);
  await s.claimNothing();
});

integration("cancel cascades and wakes an awaiting parent", async (t) => {
  const s = await suite(t);
  const parent = await s.enqueueClaimed();
  const child = await s.enqueue({ parent });
  const grandchild = await s.enqueue({ parent: child });
  // The parent parks awaiting its child's result.
  await s.storage.park(parent.fence, {
    kind: "await",
    step: "await:child",
    wakeAt: hourFromNow(),
    resultId: child.id,
    producerId: child.id,
  });
  assert.equal(await s.status(parent.id), "waiting");

  const res = await s.apply({
    kind: "cancel",
    cascadeChildren: true,
    target: { ids: [child.id] },
  });
  assert.equal(res.applied, 1);
  assert.equal(res.cascaded, 1);
  assert.equal(await s.status(grandchild.id), "cancelled");
  assert.equal(await s.status(parent.id), "pending");

  // Without cascade, children keep running.
  const other = await s.enqueue();
  const kid = await s.enqueue({ parent: other });
  assert.equal((await s.apply(onIds("cancel", other.id))).cascaded, 0);
  assert.equal(await s.status(kid.id), "pending");
});

integration("retry and redrive replay from checkpoints", async (t) => {
  const s = await suite(t);
  const failed = await s.enqueueClaimed();
  await s.storage.checkpoint(
    failed.fence,
    randomUUID(),
    { state: "Ok", ...json(1) },
    "run:step-1",
  );
  await s.storage.fail(failed.fence, "boom", false);
  const dead = await s.enqueueClaimed({ options: [runner.maxAttempts(1)] });
  assert.equal(await s.storage.fail(dead.fence, "again", true), "dead_letter");
  const pending = await s.enqueue();

  const res = await s.apply({
    kind: "retry",
    resetAttempts: true,
    target: { ids: [failed.id, dead.id, pending.id] },
  });
  wantApplied(res, failed.id, "pending");
  wantApplied(res, dead.id, "pending");
  wantSkipped(res, pending.id, "conflict");
  assert.equal(await s.storage.getResult(failed.id), null);
  const steps = await s.storage.listStepEntries(failed.id, false, 0, "");
  assert.equal(steps.items.length, 1, "checkpoints must survive a retry");
  assert.equal((await s.storage.getActivity(dead.id)).retryCount, 0);
  const events = await s.storage.events(dead.id);
  assert.deepEqual(events.find((e) => e.type === "Redriven")?.detail, {
    command_id: "",
    from: "dead_letter",
    reset_attempts: true,
  });
  assert.ok(
    (await s.storage.events(failed.id)).some((e) => e.type === "Retried"),
  );
  const claimed = await s.storage.claim(10, [failed.type, dead.type], 60_000);
  assert.deepEqual(
    claimed.map((c) => c.id).sort(),
    [failed.id, dead.id].sort(),
  );
});

integration("run now, reschedule and set priority", async (t) => {
  const s = await suite(t);
  const later = await s.enqueue({ options: [runner.delayMs(3600_000)] });
  const pending = await s.enqueue();

  const res = await s.apply(onIds("run_now", later.id, pending.id));
  wantApplied(res, later.id, "scheduled");
  wantSkipped(res, pending.id, "conflict");
  const claimed = await s.storage.claim(2, ["test"], 60_000);
  assert.deepEqual(
    claimed.map((c) => c.id).sort(),
    [later.id, pending.id].sort(),
  );

  const moved = await s.enqueue({ options: [runner.delayMs(60_000)] });
  // Nanoseconds and an offset: the event keeps them, in UTC, as Go's does.
  const at = new Date(Date.now() + 24 * 3600_000);
  const utc = at.toISOString().slice(0, 19) + ".12345678Z";
  const east =
    new Date(at.getTime() + 2 * 3600_000).toISOString().slice(0, 19) +
    ".123456780+02:00";
  wantApplied(
    await s.apply({
      kind: "reschedule",
      at: east,
      target: { ids: [moved.id] },
    }),
    moved.id,
    "scheduled",
  );
  const snap = await s.storage.getActivity(moved.id);
  assert.ok(Date.parse(snap.scheduledAt) - Date.now() > 23 * 3600_000);
  const rescheduled = (await s.storage.events(moved.id)).find(
    (e) => e.type === "Rescheduled",
  );
  assert.equal(rescheduled.detail.at, utc);
  assert.equal(snap.scheduledAt, at.toISOString().slice(0, 19) + ".123Z");

  const low = await s.enqueue({ options: [runner.priority("low")] });
  await s.apply({
    kind: "set_priority",
    priority: 4,
    target: { ids: [low.id] },
  });
  assert.equal((await s.storage.getActivity(low.id)).priority, 4);
  await assert.rejects(
    s.apply({ kind: "set_priority", priority: 9, target: { ids: [low.id] } }),
    invalid("priority"),
  );
  await assert.rejects(
    s.apply({ kind: "reschedule", target: { ids: [low.id] } }),
    invalid("at"),
  );
});

integration("delete removes finished trees", async (t) => {
  const s = await suite(t);
  const root = await s.enqueueClaimed();
  const child = await s.enqueue({ parent: root, type: "child" });
  const childFence = await s.claim(child);
  await s.storage.complete(childFence, json(null));

  const res = await s.apply(onIds("delete", root.id, child.id));
  wantSkipped(res, root.id, "conflict"); // still running
  wantSkipped(res, child.id, "conflict"); // not a root

  await s.storage.complete(root.fence, json(null));
  const dry = await s.apply({
    kind: "delete",
    dryRun: true,
    target: { ids: [root.id] },
  });
  assert.equal(itemFor(dry, root.id).outcome, "would_apply");
  assert.ok(await s.storage.getActivity(root.id), "dry run deleted the tree");
  const done = await s.apply(onIds("delete", root.id));
  wantApplied(done, root.id);
  assert.equal(itemFor(done, root.id).status, undefined);
  for (const id of [root.id, child.id])
    assert.equal(await s.storage.getActivity(id), null);
  const leftovers = await s.pool.query(
    `SELECT (SELECT count(*) FROM runnerq_events WHERE activity_id=ANY($1::uuid[]))
    + (SELECT count(*) FROM runnerq_inputs WHERE activity_id=ANY($1::uuid[]))
    + (SELECT count(*) FROM runnerq_results WHERE activity_id=ANY($1::uuid[])) AS n`,
    [[root.id, child.id]],
  );
  assert.equal(Number(leftovers.rows[0].n), 0);
});

integration("signal delivers and wakes", async (t) => {
  const s = await suite(t);
  const waiter = await s.enqueue({ key: "order-7", type: "approval" });
  const fence = await s.claim(waiter);
  await s.storage.park(fence, {
    kind: "signal",
    step: "signal:approve",
    wakeAt: hourFromNow(),
  });
  const res = await s.apply({
    kind: "signal",
    signalName: "approve",
    signalPayload: { ok: true },
    target: { idempotencyKey: businessKey("order-7", "approval") },
  });
  wantApplied(res, waiter.id, "pending");
  const r = await s.storage.getResult(
    checkpointId(waiter.id, "signal", "approve"),
  );
  assert.equal(r?.state, "Ok");
  assert.deepEqual(r.data, { ok: true });
  const none = await s.apply({
    kind: "signal",
    signalName: "approve",
    target: { idempotencyKey: "nobody" },
  });
  assert.equal(none.matched, 0);
  // Without a payload, nothing is stored (SQL NULL, as Go stores it).
  await s.apply({
    kind: "signal",
    signalName: "empty",
    target: { ids: [waiter.id] },
  });
  const empty = await s.pool.query(
    "SELECT data IS NULL AS none FROM runnerq_results WHERE activity_id=$1",
    [checkpointId(waiter.id, "signal", "empty")],
  );
  assert.equal(empty.rows[0].none, true);
  await assert.rejects(
    s.apply({ kind: "cancel", target: { idempotencyKey: "k" } }),
    invalid("target.idempotency_key"),
  );
});

integration("filter targets are bounded", async (t) => {
  const s = await suite(t);
  for (let i = 0; i < 5; i++) await s.enqueue({ type: "batch" });
  const keep = await s.enqueue({ type: "keep" });
  const filter = { field: "type", op: "eq", value: "batch" };

  let res = await s.apply({ kind: "cancel", target: { filter, max: 3 } });
  assert.deepEqual([res.matched, res.applied, res.more], [3, 3, true]);
  // Filter targets select only what the command can act on, so repeating it works
  // through the rest.
  res = await s.apply({ kind: "cancel", target: { filter, max: 3 } });
  assert.deepEqual([res.matched, res.applied, res.more], [2, 2, false]);
  assert.equal(await s.status(keep.id), "pending");
  await assert.rejects(
    s.apply({ kind: "cancel", target: { filter } }),
    invalid("target.max"),
  );
  await assert.rejects(
    s.apply({
      kind: "cancel",
      target: { filter: { field: "nope", op: "eq", value: 1 }, max: 1 },
    }),
    (e) => e.kind === "unsupported" && e.field === "nope",
  );
  await assert.rejects(
    s.apply({ kind: "cancel", target: {} }),
    invalid("target"),
  );
  await assert.rejects(
    s.apply({ kind: "launch", target: { ids: [keep.id] } }),
    (e) => e.kind === "unsupported" && e.field === "kind",
  );
});

integration("the ledger replays and rejects reuse", async (t) => {
  const s = await suite(t);
  const a = await s.enqueue();
  const cmd = {
    id: "cmd-" + randomUUID(),
    fingerprint: "fp-1",
    kind: "set_priority",
    priority: 3,
    target: { ids: [a.id] },
  };
  const dry = await s.apply({ ...cmd, dryRun: true });
  assert.equal(dry.replayed, false);
  assert.equal(itemFor(dry, a.id).outcome, "would_apply");
  assert.equal((await s.storage.getActivity(a.id)).priority, 2);

  const first = await s.apply(cmd);
  assert.equal(first.replayed, false);
  assert.equal(first.applied, 1);
  // An operator changes the priority again; replaying the old command must not undo it.
  await s.apply({ kind: "set_priority", priority: 1, target: { ids: [a.id] } });
  const again = await s.apply(cmd);
  assert.equal(again.replayed, true);
  assert.equal(again.applied, 1);
  assert.deepEqual(
    again.items.map((x) => x.id),
    [a.id],
  );
  assert.equal((await s.storage.getActivity(a.id)).priority, 1);
  await assert.rejects(s.apply({ ...cmd, fingerprint: "fp-2" }), (e) => {
    assert.equal(e.code, "conflict");
    return true;
  });
});

integration("the ledger's JSON is Go's, both ways", async (t) => {
  const s = await suite(t);
  const a = await s.enqueue();
  const missing = randomUUID();
  const id = "cmd-" + randomUUID();
  await s.apply({
    id,
    fingerprint: "fp",
    kind: "cancel",
    target: { ids: [a.id, missing] },
  });
  const row = (
    await s.pool.query(
      "SELECT kind,result FROM runnerq_commands WHERE queue_name=$1 AND command_id=$2",
      [s.queue, id],
    )
  ).rows[0];
  assert.equal(row.kind, "cancel");
  assert.deepEqual(row.result, {
    matched: 1,
    applied: 1,
    cascaded: 0,
    more: false,
    items: [
      { id: a.id, outcome: "applied", status: "cancelled" },
      {
        id: missing,
        outcome: "skipped",
        err_kind: 2,
        err_message: "no such activity in this queue",
      },
    ],
  });

  // A result Go recorded replays here.
  const goId = "go-" + randomUUID();
  await s.pool.query(
    "INSERT INTO runnerq_commands(queue_name,command_id,fingerprint,kind,result) VALUES($1,$2,'gofp','retry',$3)",
    [
      s.queue,
      goId,
      {
        matched: 1,
        applied: 0,
        cascaded: 0,
        more: true,
        items: [
          {
            id: a.id,
            outcome: "skipped",
            status: "pending",
            err_kind: 1,
            err_message:
              "only failed, dead-lettered or cancelled activities can be retried",
          },
        ],
      },
    ],
  );
  const replay = await s.apply({
    id: goId,
    fingerprint: "gofp",
    kind: "retry",
    target: { ids: [a.id] },
  });
  assert.deepEqual(replay, {
    matched: 1,
    applied: 0,
    cascaded: 0,
    more: true,
    replayed: true,
    items: [
      {
        id: a.id,
        outcome: "skipped",
        status: "pending",
        error: {
          kind: "conflict",
          message:
            "only failed, dead-lettered or cancelled activities can be retried",
        },
      },
    ],
  });
});

integration("cancelled trees are retained, then swept", async (t) => {
  const s = await suite(t);
  const root = await s.enqueue();
  await s.enqueue({ parent: root });
  const res = await s.apply({
    kind: "cancel",
    cascadeChildren: true,
    target: { ids: [root.id] },
  });
  assert.equal(res.cascaded, 1);
  assert.equal(await s.storage.cleanup({ completedMs: 1 }), 0);
  await new Promise((r) => setTimeout(r, 1100));
  assert.equal(await s.storage.cleanup({ failedMs: 1000 }), 1);
  assert.equal(await s.storage.getActivity(root.id), null);
});

integration(
  "initialize adds the ledger to an existing database; connect works without it",
  async (t) => {
    const { pool } = await setup(t);
    const name = "rq_cmd_" + randomUUID().replaceAll("-", "");
    await pool.query(`CREATE SCHEMA ${name}`);
    const url = new URL(dsn);
    url.searchParams.set("options", `-c search_path=${name}`);
    const connectionString = url.toString();
    const local = new Pool({ connectionString });
    try {
      await PostgresStorage.initialize({ connectionString });
      // A database from before commands.
      await local.query("DROP TABLE runnerq_commands");
      const storage = await PostgresStorage.connect({
        connectionString,
        queue: "old",
      });
      try {
        const a = submission();
        await storage.submit(a);
        // Without an id there is no ledger; with one the missing table is named.
        assert.equal(
          (await storage.applyCommand(onIds("cancel", a.id))).applied,
          1,
        );
        await assert.rejects(
          storage.applyCommand({ id: "c", ...onIds("cancel", a.id) }),
          /runnerq_commands table is missing.*initialize/,
        );
        await PostgresStorage.initialize({ connectionString });
        const res = await storage.applyCommand({
          id: "c",
          ...onIds("retry", a.id),
        });
        assert.equal(res.applied, 1);
      } finally {
        await storage.close();
      }
      // Present but incompatible is refused.
      await local.query(
        "ALTER TABLE runnerq_commands ALTER COLUMN kind DROP NOT NULL",
      );
      await assert.rejects(
        PostgresStorage.connect({ connectionString, queue: "bad" }),
        /runnerq_commands\.kind/,
      );
    } finally {
      await local.end();
      await pool.query(`DROP SCHEMA ${name} CASCADE`);
    }
  },
);

integration(
  "a worker stops an activity cancelled elsewhere at its next heartbeat",
  async (t) => {
    const { storage } = await setup(t);
    const worker = new Worker({ storage, heartbeatMs: 50, leaseMs: 5_000 });
    let reason;
    const started = [];
    const Blocker = activity("Blocker");
    worker.register(Blocker, async (ctx) => {
      started.push(ctx.activityId);
      await new Promise((resolve) =>
        ctx.signal.addEventListener("abort", resolve, { once: true }),
      );
      reason = ctx.signal.reason;
      throw reason;
    });
    const lost = [];
    worker.on("claimLost", (e) => lost.push(e.activityId));
    await worker.start();
    try {
      const { id } = await new RunnerQClient({ storage }).execute(Blocker, {});
      await until(() => started.length === 1, 10_000, "start");
      const res = await storage.applyCommand({
        kind: "cancel",
        target: { ids: [id] },
      });
      assert.equal(res.applied, 1);
      await until(() => lost.length === 1, 3_000, "heartbeat stop");
      assert.equal(reason.code, "claim_lost");
      assert.deepEqual(lost, [id]);
      const { counters } = worker.snapshot();
      assert.equal(counters.claimsLost, 1);
      assert.equal(counters.failed + counters.retried, 0);
      assert.equal((await storage.getActivity(id)).status, "cancelled");
    } finally {
      await worker.stop({ graceMs: 1_000 });
    }
  },
);
