import test from "node:test";
import assert from "node:assert/strict";
import {
  activity,
  runner,
  Worker,
  RunnerQClient,
  NonRetryableError,
  RecordedError,
} from "../dist/index.js";
import { captureFailure, retryable } from "../dist/errors.js";
import { encode } from "../dist/serialization.js";
import { checkpointId } from "../dist/codec.js";
import { dsn, setup, claim, submission } from "./helpers.mjs";

test("failure snapshots tolerate circular causes, hostile properties and non-JSON data", () => {
  const err = new Error("provider failed");
  err.code = "PROVIDER";
  err.data = { amount: 1n };
  err.cause = err;
  const captured = captureFailure(err);
  assert.equal(captured.cause.name, "TruncatedCause");
  assert.match(captured.data, /not portable JSON/);
  assert.equal(retryable(err), true);
  assert.doesNotThrow(() => JSON.stringify(captured));
  const hostile = {
    get name() {
      throw new Error();
    },
    get message() {
      throw new Error();
    },
    toString() {
      throw new Error();
    },
  };
  assert.equal(captureFailure(hostile).message, "Unknown error");
  assert.equal(captureFailure("rejected").message, "rejected");
});

test(
  "terminal failures and replayed step failures retain portable diagnostics",
  { skip: !dsn, timeout: 15000 },
  async (t) => {
    const { storage, another } = await setup(t);
    const client = new RunnerQClient({ storage });
    const Fail = activity("DetailedFailure"),
      Replay = activity("FailureReplay");
    const worker = new Worker({ storage });
    const error = new NonRetryableError("payment rejected", {
      cause: Object.assign(new Error("provider unavailable"), {
        code: "E_PROVIDER",
        data: { requestId: "req-1" },
      }),
    });
    let calls = 0,
      attempts = 0;
    worker.register(Fail, () => {
      throw error;
    });
    worker.register(Replay, async (ctx) => {
      attempts++;
      try {
        await ctx.run("charge", () => {
          calls++;
          throw error;
        });
      } catch (e) {
        if (attempts === 1) throw new Error("retry handler");
        assert.ok(e.cause instanceof RecordedError);
        assert.equal(e.cause.cause.code, "E_PROVIDER");
        throw e;
      }
    });
    await worker.start();
    try {
      const handle = await client.execute(Fail, null);
      const other = new RunnerQClient({ storage: await another() });
      await assert.rejects(
        other
          .handle(Fail, handle.id)
          .result({ signal: AbortSignal.timeout(5000) }),
        (e) => {
          assert.equal(e.code, "activity_failed");
          assert.equal(e.message, error.message);
          assert.ok(e.cause instanceof RecordedError);
          assert.equal(e.cause.name, "NonRetryableError");
          assert.equal(e.cause.stack, error.stack);
          assert.equal(e.cause.cause.code, "E_PROVIDER");
          assert.deepEqual(e.cause.cause.data, { requestId: "req-1" });
          return true;
        },
      );
      const replay = await client.execute(Replay, null, runner.maxAttempts(2));
      await assert.rejects(
        replay.result({ signal: AbortSignal.timeout(10000) }),
        { code: "activity_failed" },
      );
      assert.equal(calls, 1);
      assert.equal(attempts, 2);
      const step = await storage.getResult(
        checkpointId(replay.id, "run", "charge"),
      );
      assert.equal(step.serialization, "json-v1");
      assert.equal(step.data.failure.cause.code, "E_PROVIDER");
      assert.equal(
        (await storage.events(replay.id)).find((e) => e.type === "Retrying")
          .detail.failure.message,
        "retry handler",
      );
    } finally {
      await worker.stop();
    }
  },
);

test(
  "failure write reconciliation compares the captured diagnostics",
  { skip: !dsn, timeout: 15000 },
  async (t) => {
    const { storage } = await setup(t),
      a = submission(),
      fence = await claim(storage, a);
    const failure = captureFailure(
      Object.assign(new Error("same message"), { code: "FIRST" }),
    );
    await storage.fail(fence, failure.message, false, failure);
    assert.equal(
      await storage.fail(fence, failure.message, false, failure),
      "failed",
    );
    await assert.rejects(
      storage.fail(fence, failure.message, false, {
        ...failure,
        code: "SECOND",
      }),
      { code: "claim_lost" },
    );
    assert.equal((await storage.getResult(a.id)).data.failure.code, "FIRST");
  },
);

test(
  "results that can't be decoded fail with a serialization error",
  { skip: !dsn, timeout: 15000 },
  async (t) => {
    const { storage, pool, queue } = await setup(t),
      a = submission(),
      fence = await claim(storage, a);
    const goodId = checkpointId(a.id, "run", "good"),
      badId = checkpointId(a.id, "run", "bad");
    await storage.checkpoint(
      fence,
      goodId,
      { state: "Ok", ...encode(new Date("2026-01-01")) },
      "run:good",
    );
    await storage.checkpoint(
      fence,
      badId,
      { state: "Ok", ...encode(42) },
      "run:bad",
    );
    await storage.complete(fence, encode("done"));
    await pool.query(
      "UPDATE runnerq_inputs SET serialization='future-v9' WHERE queue_name=$1 AND activity_id=$2",
      [queue, a.id],
    );
    await pool.query(
      "UPDATE runnerq_results SET serialization='future-v9' WHERE queue_name=$1 AND activity_id=$2",
      [queue, badId],
    );
    await pool.query(
      "UPDATE runnerq_results SET data=$3::jsonb WHERE queue_name=$1 AND activity_id=$2",
      [
        queue,
        a.id,
        JSON.stringify({
          json: "x",
          meta: { values: ["custom", "missing-recipe"] },
        }),
      ],
    );
    const client = new RunnerQClient({ storage });
    await assert.rejects(client.handle(activity("test"), a.id).result(), {
      code: "serialization",
    });
  },
);
