import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import {
  activity,
  runner,
  RunnerQClient,
  Worker,
  RunnerQError,
} from "../dist/index.js";
import { PostgresStorage } from "../dist/postgres/index.js";
import { dsn, setup, until, submission, claim } from "./helpers.mjs";
const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30000 }, fn);

integration(
  "storage error observers contain throws and rejected promises",
  async (t) => {
    const { storage } = await setup(t);
    let observed = 0;
    storage.on("storageError", () => {
      throw new Error("sync observer");
    });
    storage.on("storageError", async () => {
      throw new Error("async observer");
    });
    storage.once("storageError", () => {
      observed++;
    });
    storage.pool.emit("error", new Error("simulated idle connection failure"));
    storage.pool.emit("error", new Error("second failure"));
    await delay(10);
    assert.equal(observed, 1);
  },
);

integration(
  "a transient in-process result wait does not consume an activity attempt",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage });
    const Parent = activity("WaitRecovery"),
      Child = activity("SlowChild");
    const worker = new Worker({ storage, concurrency: 2, waitGraceMs: 1000 });
    let failures = 0;
    const waitResult = storage.waitResult.bind(storage);
    storage.waitResult = async (id, signal) => {
      const snapshot = await storage.getActivity(id);
      if (snapshot?.type === "SlowChild" && failures++ === 0)
        throw new RunnerQError("unavailable", "temporary read outage");
      return waitResult(id, signal);
    };
    worker.register(Child, async () => {
      await delay(200);
      return 7;
    });
    worker.register(Parent, async (ctx) =>
      ctx.wait(await ctx.spawn(Child, null, runner.step("child"))),
    );
    await worker.start();
    try {
      const h = await client.execute(Parent, null, runner.maxAttempts(1));
      assert.equal(await h.result({ signal: AbortSignal.timeout(7000) }), 7);
      assert.ok(failures >= 1);
      assert.equal((await storage.getActivity(h.id)).retryCount, 0);
    } finally {
      await worker.stop();
    }
  },
);
integration(
  "a signal timeout survives parked replay and becomes a permanent failure",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      worker = new Worker({ storage });
    const Task = activity("SignalDeadline");
    worker.register(Task, (ctx) =>
      ctx.waitForSignal("never", { timeoutMs: 1500 }),
    );
    await worker.start();
    try {
      const h = await client.execute(
        Task,
        null,
        runner.timeoutMs(1000),
        runner.maxAttempts(1),
      );
      await until(
        async () => (await storage.getActivity(h.id))?.status === "waiting",
      );
      await assert.rejects(h.result({ signal: AbortSignal.timeout(7000) }), {
        code: "activity_failed",
      });
      assert.equal((await storage.getActivity(h.id)).status, "failed");
      assert.equal((await storage.getActivity(h.id)).retryCount, 0);
    } finally {
      await worker.stop();
    }
  },
);
integration(
  "forced shutdown returns within its grace and stops renewal of an uncooperative handler",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage });
    const Task = activity("Uncooperative");
    let entered = false,
      release;
    const pending = new Promise((resolve) => {
      release = resolve;
    });
    let renewals = 0;
    const renew = storage.renew.bind(storage);
    storage.renew = async (...args) => {
      renewals++;
      return renew(...args);
    };
    const worker = new Worker({ storage, heartbeatMs: 20, leaseMs: 1000 });
    worker.register(Task, async () => {
      entered = true;
      await pending;
      return "late";
    });
    await worker.start();
    const h = await client.execute(Task, null);
    await until(() => entered);
    await until(() => renewals > 0);
    try {
      const started = performance.now();
      const result = await worker.stop({ graceMs: 30 });
      assert.equal(result.drained, false);
      assert.equal(result.remaining, 1);
      assert.ok(performance.now() - started < 500);
      await delay(40);
      const count = renewals;
      await delay(80);
      assert.equal(renewals, count);
    } finally {
      release();
      await delay(30);
      await worker.stop();
    }
    assert.equal(await storage.getResult(h.id), null);
  },
);
integration(
  "overlapping durable waits fail explicitly instead of parking indefinitely",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      worker = new Worker({ storage });
    const Task = activity("BadParallelWait");
    worker.register(Task, async (ctx) => {
      await Promise.all([ctx.waitForSignal("a"), ctx.waitForSignal("b")]);
      return null;
    });
    await worker.start();
    try {
      const h = await client.execute(Task, null);
      await assert.rejects(h.result({ signal: AbortSignal.timeout(5000) }), {
        code: "activity_failed",
      });
      assert.equal((await storage.getActivity(h.id)).status, "failed");
    } finally {
      await worker.stop();
    }
  },
);
integration(
  "ordinary input insertion failure rolls back activity and idempotency ownership together",
  async (t) => {
    const { storage, pool, queue } = await setup(t);
    const a = submission(runner.idempotencyKey("atomic"));
    a.key = "test-atomic";
    a.payload = undefined;
    await assert.rejects(storage.submit(a));
    assert.equal(
      (
        await pool.query(
          "SELECT count(*) FROM runnerq_idempotency WHERE queue_name=$1",
          [queue],
        )
      ).rows[0].count,
      "0",
    );
    assert.equal(await storage.getActivity(a.id), null);
  },
);
integration(
  "listener reconnect and a completely missed notification both recover from database state",
  async (t) => {
    const { storage, pool, another } = await setup(t),
      other = await another();
    const a = submission(),
      f = await claim(storage, a);
    const waiting = other.waitResult(a.id, AbortSignal.timeout(10000));
    // Fault injection targets only this instance's dedicated LISTEN connection.
    await until(() => other.notifications.listener?.processID);
    await pool.query("SELECT pg_terminate_backend($1)", [
      other.notifications.listener.processID,
    ]);
    const hint = storage.notifications.hint.bind(storage.notifications);
    storage.notifications.hint = () => {};
    try {
      await storage.complete(f, { recovered: true });
      assert.deepEqual((await waiting).data, { recovered: true });
    } finally {
      storage.notifications.hint = hint;
    }
  },
);
integration(
  "schema validation works read-only and rejects old inline inputs or altered defaults",
  async (t) => {
    const { pool } = await setup(t);
    const name = "rq_schema_" + randomUUID().replaceAll("-", "");
    await pool.query(`CREATE SCHEMA ${name}`);
    const url = new URL(dsn);
    url.searchParams.set("options", `-c search_path=${name}`);
    const connectionString = url.toString();
    try {
      await PostgresStorage.initialize({ connectionString });
      const readonly = new URL(connectionString);
      readonly.searchParams.set(
        "options",
        `-c search_path=${name} -c default_transaction_read_only=on`,
      );
      const connected = await PostgresStorage.connect({
        connectionString: readonly.toString(),
        queue: "readonly",
      });
      await connected.close();
      await pool.query(
        `ALTER TABLE ${name}.runnerq_activities ADD COLUMN payload JSONB`,
      );
      await assert.rejects(
        PostgresStorage.connect({ connectionString, queue: "old" }),
        /Inline-payload/,
      );
      await pool.query(
        `ALTER TABLE ${name}.runnerq_activities DROP COLUMN payload`,
      );
      await pool.query(
        `ALTER TABLE ${name}.runnerq_activities ALTER COLUMN retry_count SET DEFAULT 100`,
      );
      await assert.rejects(
        PostgresStorage.connect({ connectionString, queue: "bad_default" }),
        /retry_count/,
      );
    } finally {
      await pool.query(`DROP SCHEMA ${name} CASCADE`);
    }
  },
);
