import test from "node:test";
import assert from "node:assert/strict";
import {
  activity,
  runner,
  RunnerQClient,
  Worker,
  Inspector,
} from "../dist/index.js";
import { encode, decode } from "../dist/serialization.js";
import { checkpointId } from "../dist/codec.js";
import { dsn, setup, claim, submission, until } from "./helpers.mjs";
const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30000 }, fn);

integration(
  "native void results remain present and unknown input formats never reach handlers",
  async (t) => {
    const { storage, pool, queue } = await setup(t),
      client = new RunnerQClient({ storage });
    const Void = activity("Void"),
      Unknown = activity("UnknownFormat");
    const worker = new Worker({ storage });
    let calls = 0;
    worker.register(Void, (_ctx, input) => {
      assert.equal(input, undefined);
      return undefined;
    });
    worker.register(Unknown, () => {
      calls++;
      return "wrong";
    });
    const good = await client.execute(Void, undefined);
    const bad = await client.execute(Unknown, null);
    await pool.query(
      "UPDATE runnerq_inputs SET serialization='future-v2' WHERE queue_name=$1 AND activity_id=$2",
      [queue, bad.id],
    );
    await worker.start();
    try {
      assert.equal(
        await good.result({ signal: AbortSignal.timeout(5000) }),
        undefined,
      );
      assert.equal((await storage.getResult(good.id)).state, "Ok");
      assert.equal((await storage.getActivity(good.id)).status, "completed");
      await assert.rejects(bad.result({ signal: AbortSignal.timeout(5000) }), {
        code: "activity_failed",
      });
      assert.equal((await storage.getActivity(bad.id)).status, "failed");
      assert.equal(calls, 0);
    } finally {
      await worker.stop();
    }
  },
);

integration(
  "native inputs, children, signals, checkpoints and reconstructed results retain their types",
  async (t) => {
    const { storage, another } = await setup(t);
    const other = await another(),
      client = new RunnerQClient({ storage }),
      sender = new RunnerQClient({ storage: other });
    const Parent = activity("NativeParent"),
      Child = activity("NativeChild");
    const worker = new Worker({ storage, concurrency: 1, waitGraceMs: 0 });
    let effects = 0,
      invocations = 0;
    const date = new Date("2026-09-27T00:00:00Z");
    worker.register(Child, (_ctx, input) => {
      assert.equal(input.when instanceof Date, true);
      return {
        when: input.when,
        amount: 9007199254740993n,
        bytes: Buffer.from("ok"),
      };
    });
    worker.register(Parent, async (ctx, input) => {
      invocations++;
      assert.equal(input.when instanceof Date, true);
      const step = await ctx.run(
        "native-step",
        () => {
          effects++;
          return { date, set: new Set([1n]), absent: undefined };
        },
        {
          parse: (value) => {
            assert.equal(value.date instanceof Date, true);
            assert.equal(value.set instanceof Set, true);
            assert.equal("absent" in value, true);
            return value;
          },
        },
      );
      assert.equal(await ctx.run("void", () => undefined), undefined);
      const child = await ctx.spawn(Child, input, runner.step("child"));
      const result = await child.result();
      const signal = await ctx.waitForSignal("approval");
      assert.equal(signal instanceof Map, true);
      return { result, step, signal };
    });
    await worker.start();
    try {
      const handle = await client.execute(Parent, { when: date });
      await until(async () =>
        (await storage.events(handle.id)).some((e) => e.type === "Yielded"),
      );
      await sender.signal(handle.id, "approval", new Map([["yes", 1n]]));
      const result = await sender
        .handle(Parent, handle.id)
        .result({ signal: AbortSignal.timeout(10000) });
      assert.equal(result.result.when instanceof Date, true);
      assert.equal(result.result.amount, 9007199254740993n);
      assert.equal(Buffer.isBuffer(result.result.bytes), true);
      assert.equal(result.signal.get("yes"), 1n);
      assert.equal(effects, 1);
      assert.ok(invocations >= 2);
      const inspector = new Inspector({ storage });
      assert.equal(
        (await inspector.input(handle.id)).data.when instanceof Date,
        true,
      );
      assert.equal(
        (await inspector.result(handle.id)).data.result.amount,
        9007199254740993n,
      );
      assert.equal(
        (await inspector.steps(handle.id)).find((s) => s.name === "native-step")
          .data.date instanceof Date,
        true,
      );
      await inspector.close();
      assert.equal(
        (await storage.getInput(handle.id)).serialization,
        "superjson-v1",
      );
      assert.equal(
        (await storage.getResult(handle.id)).serialization,
        "superjson-v1",
      );
    } finally {
      await worker.stop();
    }
  },
);

integration(
  "portable boundaries use plain JSON while internal checkpoints remain native across changed definitions",
  async (t) => {
    const { storage, pool, queue } = await setup(t),
      client = new RunnerQClient({ storage });
    const Portable = activity("PortableBoundary", {
      serialization: "portable",
    });
    await assert.rejects(client.execute(Portable, { date: new Date() }), {
      code: "serialization",
    });
    const handle = await client.execute(
      Portable,
      { orderId: "order-1" },
      runner.idempotencyKey("order-1"),
    );
    await client.signalByKey(Portable, "order-1", "decision", {
      accepted: true,
    });
    // The persisted input format, not the worker's current definition default, determines output format.
    const worker = new Worker({ storage });
    worker.register(activity("PortableBoundary"), async (ctx, input) => {
      const step = await ctx.run(
        "date",
        () => new Date("2026-01-01T00:00:00Z"),
      );
      const signal = await ctx.waitForSignal("decision");
      return { ...input, date: step.toISOString(), accepted: signal.accepted };
    });
    await worker.start();
    try {
      assert.equal(
        (await handle.result({ signal: AbortSignal.timeout(5000) })).accepted,
        true,
      );
      const rows = (
        await pool.query(
          "SELECT data,serialization FROM runnerq_results WHERE queue_name=$1 AND activity_id=$2",
          [queue, handle.id],
        )
      ).rows;
      assert.equal(rows[0].serialization, "json-v1");
      assert.equal(rows[0].data.orderId, "order-1");
      assert.equal(
        (await storage.getResult(checkpointId(handle.id, "signal", "decision")))
          .serialization,
        "json-v1",
      );
      assert.equal(
        (await storage.steps(handle.id)).find(
          (step) => step.kind === "run" && step.name === "date",
        ).serialization,
        "superjson-v1",
      );
    } finally {
      await worker.stop();
    }
  },
);

integration(
  "checkpoint and completion reconciliation compare serialization metadata",
  async (t) => {
    const { storage } = await setup(t),
      a = submission(),
      fence = await claim(storage, a);
    const id = checkpointId(a.id, "run", "format");
    const native = encode(new Date("2026-01-01T00:00:00Z"));
    await storage.checkpoint(
      fence,
      id,
      { state: "Ok", ...native },
      "run:format",
    );
    await assert.rejects(
      storage.checkpoint(
        fence,
        id,
        { state: "Ok", ...native, serialization: "json-v1" },
        "run:format",
      ),
      { code: "checkpoint_conflict" },
    );
    await storage.complete(fence, native);
    await storage.complete(fence, native);
    await assert.rejects(
      storage.complete(fence, { ...native, serialization: "json-v1" }),
      { code: "checkpoint_conflict" },
    );
    assert.equal(decode(await storage.getResult(a.id)) instanceof Date, true);
  },
);
