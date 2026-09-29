import { decode } from "../dist/serialization.js";
import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import {
  activity,
  runner,
  RunnerQClient,
  Worker,
  NonRetryableError,
  RunnerQError,
} from "../dist/index.js";
import { dsn, setup, until } from "./helpers.mjs";
const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30000 }, fn);
function worker(t, storage, config = {}) {
  const w = new Worker({ storage, waitGraceMs: 20, ...config });
  // Each test stops it in try/finally, before setup's cleanup.
  return w;
}
integration(
  "hello workflow returns typed JSON; emitter exceptions never corrupt committed execution",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage);
    const Signup = activity("SignupWorkflow");
    let completed = 0,
      listenerErrors = 0;
    w.on("activityCompleted", () => {
      throw Error("bad observer");
    });
    w.on("activityCompleted", async () => {
      throw Error("bad async observer");
    });
    w.on("activityCompleted", () => completed++);
    w.on("listenerError", () => listenerErrors++);
    w.register(Signup, async (ctx, input) => {
      const account = await ctx.run("create-account", () => ({
        user_id: "u_1001",
        email: input.email,
      }));
      await ctx.run("send-welcome", () => true);
      return account;
    });
    await w.start();
    try {
      const h = await client.execute(Signup, { email: "ada@example.com" });
      assert.equal(typeof h.then, "undefined");
      assert.deepEqual(await h.result({ signal: AbortSignal.timeout(5000) }), {
        user_id: "u_1001",
        email: "ada@example.com",
      });
      await until(() => listenerErrors === 2);
      assert.equal(completed, 1);
      assert.deepEqual(
        (await storage.steps(h.id)).map((s) => s.name),
        ["create-account", "send-welcome"],
      );
    } finally {
      assert.equal((await w.stop()).drained, true);
    }
  },
);
integration(
  "retry replays checkpoints and does not repeat recorded external work",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage);
    const Task = activity("Retry");
    let calls = 0,
      invocations = 0;
    w.register(Task, async (ctx) => {
      invocations++;
      const value = await ctx.run("effect", () => {
        calls++;
        return { ok: true };
      });
      if (ctx.retryCount === 0) throw Error("temporary");
      return value;
    });
    await w.start();
    try {
      const h = await client.execute(Task, null, runner.maxRetryDelayMs(1000));
      assert.deepEqual(await h.result({ signal: AbortSignal.timeout(10000) }), {
        ok: true,
      });
      assert.equal(calls, 1);
      assert.equal(invocations, 2);
    } finally {
      await w.stop();
    }
  },
);
integration(
  "checkpoint persistence and lost completion replies recover without rerunning handler",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage);
    const Task = activity("Recovery");
    let effects = 0,
      handlers = 0,
      checkpoints = 0,
      completions = 0;
    const checkpoint = storage.checkpoint.bind(storage),
      complete = storage.complete.bind(storage);
    storage.checkpoint = async (...args) => {
      await checkpoint(...args);
      if (checkpoints++ === 0)
        throw new RunnerQError("unavailable", "lost checkpoint reply");
    };
    storage.complete = async (...args) => {
      await complete(...args);
      if (completions++ === 0)
        throw new RunnerQError("unavailable", "lost completion reply");
    };
    w.register(Task, async (ctx) => {
      handlers++;
      return ctx.run("effect", () => {
        effects++;
        return 42;
      });
    });
    await w.start();
    try {
      const h = await client.execute(Task, null);
      assert.equal(await h.result({ signal: AbortSignal.timeout(5000) }), 42);
      await until(() => completions === 2);
      assert.equal(effects, 1);
      assert.equal(handlers, 1);
    } finally {
      await w.stop();
    }
  },
);
integration(
  "concurrency one parent parks and resumes children without exhausting attempt budget",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage, { concurrency: 1 });
    const Parent = activity("Parent"),
      Child = activity("Child");
    let effects = 0,
      yields = 0;
    w.on("activityYielded", () => yields++);
    w.register(Child, async (_ctx, input) => input * 2);
    w.register(Parent, async (ctx) => {
      await ctx.run("once", () => {
        effects++;
        return true;
      });
      const children = [];
      for (let n = 1; n <= 3; n++)
        children.push(await ctx.spawn(Child, n, runner.step(`child-${n}`)));
      return ctx.waitAll(children);
    });
    await w.start();
    try {
      const h = await client.execute(Parent, null, runner.maxAttempts(1));
      assert.deepEqual(
        await h.result({ signal: AbortSignal.timeout(12000) }),
        [2, 4, 6],
      );
      assert.equal(effects, 1);
      assert.ok(yields > 0);
      assert.equal((await storage.getActivity(h.id)).retryCount, 0);
      assert.equal((await storage.list({ parentId: h.id })).length, 3);
    } finally {
      await w.stop();
    }
  },
);
integration(
  "signal waits survive parking and external wait cancellation does not cancel activity",
  async (t) => {
    const { storage, another } = await setup(t),
      other = await another(),
      client = new RunnerQClient({ storage }),
      external = new RunnerQClient({ storage: other }),
      w = worker(t, storage);
    const Task = activity("Approval");
    w.register(Task, (ctx) => ctx.waitForSignal("approval"));
    await w.start();
    try {
      const h = await client.execute(
        Task,
        null,
        runner.idempotencyKey("order"),
      );
      await until(
        async () => (await storage.getActivity(h.id))?.status === "waiting",
      );
      await assert.rejects(
        external.handle(h.id).result({ signal: AbortSignal.timeout(30) }),
      );
      assert.equal((await storage.getActivity(h.id)).status, "waiting");
      await external.signalByKey(Task, "order", "approval", { approved: true });
      assert.deepEqual(
        await external
          .handle(h.id)
          .result({ signal: AbortSignal.timeout(5000) }),
        { approved: true },
      );
    } finally {
      await w.stop();
    }
  },
);
integration(
  "sleep checkpoint retains original deadline across a worker restart",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage });
    const Task = activity("Sleep");
    let runs = 0;
    const handler = async (ctx) => {
      await ctx.run("once", () => ++runs);
      await ctx.sleep("nap", 1500);
      return "awake";
    };
    const first = worker(t, storage, { concurrency: 1 });
    first.register(Task, handler);
    await first.start();
    const h = await client.execute(
      Task,
      null,
      runner.timeoutMs(1000),
      runner.maxAttempts(1),
    );
    await until(
      async () => (await storage.getActivity(h.id))?.status === "waiting",
    );
    await first.stop();
    const checkpoint = (await storage.steps(h.id)).find((s) => s.name === "nap")
      .data.wake_at;
    await delay(1600);
    const next = worker(t, storage);
    next.register(Task, handler);
    await next.start();
    try {
      assert.equal(
        await h.result({ signal: AbortSignal.timeout(5000) }),
        "awake",
      );
      assert.equal(runs, 1);
      assert.equal(
        (await storage.steps(h.id)).find((s) => s.name === "nap").data.wake_at,
        checkpoint,
      );
    } finally {
      await next.stop();
    }
  },
);
integration(
  "swallowing suspension cannot falsely complete or perform further SDK mutations",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage);
    const Task = activity("Swallow");
    w.register(Task, async (ctx) => {
      try {
        await ctx.waitForSignal("go");
      } catch {}
      return "incorrect";
    });
    await w.start();
    try {
      const h = await client.execute(Task, null);
      await until(
        async () => (await storage.getActivity(h.id))?.status === "waiting",
      );
      assert.equal(await storage.getResult(h.id), null);
    } finally {
      await w.stop();
    }
  },
);
integration(
  "nonretryable errors and unserializable values are terminal, and failed steps stay checkpointed",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage);
    const Task = activity("Permanent"),
      Bad = activity("BadJSON");
    w.register(Task, (ctx) =>
      ctx.run("fail", () => {
        throw new NonRetryableError("declined");
      }),
    );
    w.register(Bad, (ctx) => ctx.run("bad", () => ({ value: () => {} })));
    await w.start();
    try {
      for (const definition of [Task, Bad]) {
        const h = await client.execute(definition, null);
        await assert.rejects(h.result({ signal: AbortSignal.timeout(5000) }), {
          code: "activity_failed",
        });
        assert.equal((await storage.getActivity(h.id)).status, "failed");
        assert.equal((await storage.steps(h.id))[0].state, "Err");
      }
    } finally {
      await w.stop();
    }
  },
);
integration(
  "graceful shutdown persists an in-flight outcome and stop is idempotent",
  async (t) => {
    const { storage } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage);
    const Task = activity("Drain");
    let entered = false;
    w.register(Task, async () => {
      entered = true;
      await delay(100);
      return "done";
    });
    await w.start();
    const h = await client.execute(Task, null);
    await until(() => entered);
    const stop = w.stop();
    assert.equal(w.stop(), stop);
    assert.equal((await stop).drained, true);
    assert.equal(decode(await storage.getResult(h.id)), "done");
  },
);
integration(
  "claim loss aborts handler signal and prevents stale child creation",
  async (t) => {
    const { storage, pool, queue } = await setup(t),
      client = new RunnerQClient({ storage }),
      w = worker(t, storage, { heartbeatMs: 30 });
    const Task = activity("LoseClaim"),
      Child = activity("Child");
    let entered = false,
      aborted = false;
    w.register(Task, async (ctx) => {
      entered = true;
      try {
        await delay(10000, undefined, { signal: ctx.signal });
      } catch {
        aborted = true;
      }
      await ctx.spawn(Child, null, runner.step("illegal"));
    });
    await w.start();
    try {
      const h = await client.execute(Task, null);
      await until(() => entered);
      await pool.query(
        "UPDATE runnerq_activities SET current_worker_id='replacement' WHERE queue_name=$1 AND id=$2",
        [queue, h.id],
      );
      await until(() => aborted);
      assert.equal((await storage.list({ parentId: h.id })).length, 0);
      assert.equal(await storage.getResult(h.id), null);
    } finally {
      await w.stop();
    }
  },
);
