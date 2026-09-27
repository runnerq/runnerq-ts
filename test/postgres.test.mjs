import test from "node:test";
import assert from "node:assert/strict";
import { runner } from "../dist/index.js";
import { checkpointId, businessKey } from "../dist/codec.js";
import { dsn, setup, submission, claim } from "./helpers.mjs";
const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30000 }, fn);

for (const expiry of [false, true]) {
  integration(
    `attempt budgets apply to ${expiry ? "lease expiry" : "retryable failure"}: default unlimited, explicit one and three`,
    async (t) => {
      const { storage, pool, queue } = await setup(t);
      for (const limit of [undefined, "unlimited", 1, 3]) {
        const a = submission(
          ...(limit === undefined ? [] : [runner.maxAttempts(limit)]),
        );
        let fence = await claim(storage, a);
        const unlimited = limit === undefined || limit === "unlimited";
        const attempts = unlimited ? 5 : limit;
        assert.equal(
          (await storage.getActivity(a.id)).maxAttempts,
          unlimited ? "unlimited" : limit,
        );
        for (let attempt = 1; attempt <= attempts; attempt++) {
          const terminal = !unlimited && attempt === limit;
          if (expiry) {
            await pool.query(
              "UPDATE runnerq_activities SET lease_deadline_ms=0 WHERE queue_name=$1 AND id=$2",
              [queue, a.id],
            );
            assert.equal(await storage.reap(1), 1);
          } else {
            assert.equal(
              await storage.fail(fence, "retryable error", true),
              terminal ? "dead_letter" : "retrying",
            );
          }
          assert.equal(
            (await storage.getActivity(a.id)).status,
            terminal ? "dead_letter" : expiry ? "pending" : "retrying",
          );
          if (terminal) {
            assert.equal((await storage.getResult(a.id)).state, "Err");
            assert.equal((await storage.claim(1, [a.type], 60000)).length, 0);
          } else {
            assert.equal(await storage.getResult(a.id), null);
            // Advance only the retry schedule so the test exercises multiple real claims without sleeping.
            await pool.query(
              "UPDATE runnerq_activities SET scheduled_at=NOW() WHERE queue_name=$1 AND id=$2",
              [queue, a.id],
            );
            const [next] = await storage.claim(1, [a.type], 60000);
            assert.equal(next.id, a.id);
            fence = { ownerId: next.id, token: next.token };
          }
        }
        if (unlimited) await storage.complete(fence, "eventually succeeds");
      }
    },
  );
}

integration(
  "schema initialization is concurrent, inputs are separate, and connect does not need DDL",
  async (t) => {
    const { storage, pool, queue } = await setup(t);
    const a = submission();
    await storage.submit(a);
    assert.deepEqual(await storage.getInput(a.id), a.payload);
    const row = (
      await pool.query("SELECT * FROM runnerq_activities WHERE queue_name=$1", [
        queue,
      ])
    ).rows[0];
    assert.equal("payload" in row, false);
    assert.equal(row.max_retries, 0);
    const defaults = await pool.query(
      "SELECT column_default FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='runnerq_activities' AND column_name='max_retries'",
    );
    assert.equal(defaults.rows[0].column_default, "0");
    const { PostgresStorage } = await import("../dist/postgres/index.js");
    await Promise.all(
      Array.from({ length: 4 }, () =>
        PostgresStorage.initialize({ connectionString: dsn }),
      ),
    );
  },
);
integration(
  "concurrent fleets never claim the same activity and claims obey priority/type routing",
  async (t) => {
    const { storage, another } = await setup(t);
    const other = await another();
    const activities = Array.from({ length: 30 }, () => submission());
    await Promise.all(activities.map((a) => storage.submit(a)));
    const [one, two] = await Promise.all([
      storage.claim(20, ["test"], 60000),
      other.claim(20, ["test"], 60000),
    ]);
    assert.equal(one.length + two.length, 30);
    assert.equal(new Set([...one, ...two].map((c) => c.id)).size, 30);
    assert.equal(new Set([...one, ...two].map((c) => c.token)).size, 30);
    const high = submission(runner.priority("critical")),
      low = submission(runner.priority("low"));
    await storage.submit(low);
    await storage.submit(high);
    assert.deepEqual(
      (await storage.claim(2, ["test"], 60000)).map((c) => c.id),
      [high.id, low.id],
    );
    const unsupported = submission();
    unsupported.type = "other";
    await storage.submit(unsupported);
    assert.equal((await storage.claim(1, ["test"], 60000)).length, 0);
  },
);
integration(
  "atomic idempotency deduplicates concurrent submissions without replacing inputs",
  async (t) => {
    const { storage, pool, queue } = await setup(t);
    const as = Array.from({ length: 25 }, (_, i) => {
      const a = submission(runner.idempotencyKey("key"));
      a.key = businessKey("key", a.type);
      a.payload = { i };
      return a;
    });
    const ids = await Promise.all(as.map((a) => storage.submit(a)));
    assert.equal(new Set(ids).size, 1);
    assert.equal(
      (
        await pool.query(
          "SELECT count(*) FROM runnerq_inputs WHERE queue_name=$1",
          [queue],
        )
      ).rows[0].count,
      "1",
    );
    const original = await storage.getInput(ids[0]);
    await storage.submit(as[1]);
    assert.deepEqual(await storage.getInput(ids[0]), original);
  },
);
integration(
  "completion and failure reconcile lost replies and reject conflicting or stale writes",
  async (t) => {
    const { storage } = await setup(t);
    const a = submission(),
      f = await claim(storage, a);
    await storage.complete(f, null);
    await storage.complete(f, null);
    assert.deepEqual(await storage.getResult(a.id), {
      state: "Ok",
      data: null,
    });
    await assert.rejects(storage.complete(f, { changed: true }), {
      code: "checkpoint_conflict",
    });
    assert.equal(
      (await storage.events(a.id)).filter((e) => e.type === "Completed").length,
      1,
    );
    const b = submission(),
      bf = await claim(storage, b);
    assert.equal(await storage.fail(bf, "oops", true), "retrying");
    assert.equal(await storage.fail(bf, "oops", true), "retrying");
    assert.equal((await storage.getActivity(b.id)).retryCount, 1);
    await assert.rejects(storage.complete({ ...bf, token: "stale" }, null), {
      code: "claim_lost",
    });
  },
);
integration(
  "checkpoint writes are immutable and stale attempts cannot checkpoint or spawn",
  async (t) => {
    const { storage, pool, queue } = await setup(t);
    const a = submission(),
      f = await claim(storage, a),
      id = checkpointId(a.id, "run", "charge");
    const result = { state: "Ok", data: { paid: true } };
    await storage.checkpoint(f, id, result, "run:charge");
    await storage.checkpoint(f, id, result, "run:charge");
    await assert.rejects(
      storage.checkpoint(f, id, { state: "Ok", data: false }, "run:charge"),
      { code: "checkpoint_conflict" },
    );
    await pool.query(
      "UPDATE runnerq_activities SET lease_deadline_ms=0 WHERE queue_name=$1 AND id=$2",
      [queue, a.id],
    );
    assert.equal(await storage.reap(10), 1);
    await storage.claim(1, ["test"], 60000);
    await assert.rejects(
      storage.checkpoint(
        f,
        checkpointId(a.id, "run", "other"),
        result,
        "run:other",
      ),
      { code: "claim_lost" },
    );
    const child = submission();
    child.fence = f;
    child.parentId = a.id;
    child.rootId = a.id;
    child.depth = 1;
    await assert.rejects(storage.submit(child), { code: "claim_lost" });
    assert.equal(await storage.getActivity(child.id), null);
    assert.equal(await storage.renew(f, 60000), false);
  },
);
integration(
  "result publication races with durable parking without stranding consumers",
  async (t) => {
    const { storage, another } = await setup(t);
    const other = await another();
    for (let i = 0; i < 8; i++) {
      const child = submission(),
        cf = await claim(storage, child),
        parent = submission(),
        pf = await claim(storage, parent);
      const wait = {
        kind: "await",
        step: `await:${child.id}`,
        resultId: child.id,
        producerId: child.id,
        wakeAt: new Date(Date.now() + 60000).toISOString(),
      };
      await Promise.all([
        storage.park(pf, wait),
        other.complete(cf, { ok: true }),
      ]);
      assert.equal((await storage.getActivity(parent.id)).status, "pending");
      await storage.park(pf, wait); // lost park reply reconciles after wake
      const [p] = await storage.claim(1, ["test"], 60000);
      await storage.complete({ ownerId: p.id, token: p.token }, null);
    }
  },
);
integration(
  "signals are buffered, overwrite, wake waiting but preserve delayed schedules",
  async (t) => {
    const { storage } = await setup(t);
    const a = submission(),
      f = await claim(storage, a);
    await storage.signal(a.id, "approve", { yes: false });
    await storage.signal(a.id, "approve", { yes: true });
    assert.deepEqual(
      (await storage.getResult(checkpointId(a.id, "signal", "approve"))).data,
      { yes: true },
    );
    await storage.park(f, {
      kind: "signal",
      step: "approve",
      resultId: checkpointId(a.id, "signal", "approve"),
      wakeAt: new Date(Date.now() + 60000).toISOString(),
    });
    assert.equal((await storage.getActivity(a.id)).status, "pending");
    const delayed = submission(runner.delayMs(60000));
    await storage.submit(delayed);
    await storage.signal(delayed.id, "hello", null);
    assert.equal((await storage.getActivity(delayed.id)).status, "scheduled");
  },
);
integration(
  "lease expiry counts toward the attempt budget and always stores terminal results",
  async (t) => {
    const { storage, pool, queue } = await setup(t);
    const a = submission(runner.maxAttempts(1));
    await claim(storage, a);
    await pool.query(
      "UPDATE runnerq_activities SET lease_deadline_ms=0 WHERE queue_name=$1",
      [queue],
    );
    await storage.reap(10);
    assert.equal((await storage.getActivity(a.id)).status, "dead_letter");
    assert.equal((await storage.getResult(a.id)).state, "Err");
  },
);
integration(
  "retention pins a shared producer, then removes every table including separate inputs",
  async (t) => {
    const { storage, pool, queue } = await setup(t);
    const producer = submission(),
      pf = await claim(storage, producer),
      consumer = submission(),
      cf = await claim(storage, consumer);
    await storage.registerDependency(cf, producer.id);
    await storage.complete(pf, { value: 1 });
    await pool.query(
      "UPDATE runnerq_activities SET completed_at=NOW()-INTERVAL '1 day' WHERE queue_name=$1 AND id=$2",
      [queue, producer.id],
    );
    assert.equal(await storage.cleanup({ completedMs: 1 }), 0);
    await storage.complete(cf, null);
    await pool.query(
      "UPDATE runnerq_activities SET completed_at=NOW()-INTERVAL '1 day' WHERE queue_name=$1",
      [queue],
    );
    assert.equal(await storage.cleanup({ completedMs: 1 }), 2);
    for (const table of [
      "runnerq_inputs",
      "runnerq_results",
      "runnerq_events",
      "runnerq_dependencies",
      "runnerq_activities",
    ])
      assert.equal(
        (
          await pool.query(
            `SELECT count(*) FROM ${table} WHERE queue_name=$1`,
            [queue],
          )
        ).rows[0].count,
        "0",
      );
  },
);
