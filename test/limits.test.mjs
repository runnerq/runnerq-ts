import test from "node:test";
import assert from "node:assert/strict";
import { activity, RunnerQClient, Worker } from "../dist/index.js";
import { dsn, setup, quiet } from "./helpers.mjs";
import { limitStorageContract } from "./limit-contract.mjs";

limitStorageContract(
  "PostgresStorage limits",
  async (t) => {
    const s = await setup(t);
    return {
      ...s,
      running: async (type) =>
        Number(
          (
            await s.pool.query(
              "SELECT count(*) AS n FROM runnerq_activities WHERE queue_name=$1 AND activity_type=$2 AND status='processing'",
              [s.queue, type],
            )
          ).rows[0].n,
        ),
    };
  },
  { skip: !dsn },
);

const send = activity("send", { serialization: "portable" });
const slow = activity("slow", { serialization: "portable" });
const fast = activity("fast", { serialization: "portable" });

test("register validates limits", async (t) => {
  if (!dsn) return t.skip();
  const { storage } = await setup(t);
  const worker = new Worker({ storage, logger: quiet });
  for (const bad of [
    { maxConcurrent: 0 },
    { rateLimit: 5 },
    { rateLimit: 5, ratePeriodMs: 0 },
    { maxConcurrentPerWorker: 0 },
  ])
    assert.throws(() => worker.register(send, () => null, bad), /./);
});

test(
  "two workers share a declared concurrency limit",
  { skip: !dsn, timeout: 60_000 },
  async (t) => {
    const { storage, another } = await setup(t);
    let running = 0,
      peak = 0;
    const handler = async () => {
      peak = Math.max(peak, ++running);
      await new Promise((r) => setTimeout(r, 40));
      running--;
      return null;
    };
    const client = new RunnerQClient({ storage });
    const handles = [];
    for (let i = 0; i < 12; i++) handles.push(await client.execute(send, null));
    const workers = [storage, await another()].map((s) =>
      new Worker({
        storage: s,
        concurrency: 5,
        shutdownGraceMs: 2000,
        logger: quiet,
      }).register(send, handler, {
        maxConcurrent: 2,
        rateLimit: 100,
        ratePeriodMs: 1000,
      }),
    );
    await Promise.all(workers.map((w) => w.start()));
    try {
      for (const h of handles) await h.result();
    } finally {
      await Promise.all(workers.map((w) => w.stop()));
    }
    assert.ok(peak <= 2, `${peak} ran at once`);
    const stored = await client.limits.get("send");
    assert.equal(stored.maxConcurrent, 2);
    assert.equal(stored.rateLimit, 100);
    assert.equal(stored.ratePeriodMs, 1000);
  },
);

test(
  "maxConcurrentPerWorker caps one type and leaves room for others",
  { skip: !dsn, timeout: 60_000 },
  async (t) => {
    const { storage } = await setup(t);
    const running = new Map(),
      peak = new Map();
    const handler = (type) => async () => {
      running.set(type, (running.get(type) ?? 0) + 1);
      peak.set(type, Math.max(peak.get(type) ?? 0, running.get(type)));
      await new Promise((r) => setTimeout(r, 60));
      running.set(type, running.get(type) - 1);
      return null;
    };
    const client = new RunnerQClient({ storage });
    const handles = [];
    for (let i = 0; i < 16; i++)
      handles.push(await client.execute(i % 2 ? fast : slow, null));
    const worker = new Worker({
      storage,
      concurrency: 6,
      shutdownGraceMs: 2000,
      logger: quiet,
    })
      .register(slow, handler("slow"), { maxConcurrentPerWorker: 2 })
      .register(fast, handler("fast"));
    await worker.start();
    try {
      for (const h of handles) await h.result();
    } finally {
      await worker.stop();
    }
    assert.ok(peak.get("slow") <= 2, `slow peaked at ${peak.get("slow")}`);
    assert.ok(peak.get("fast") >= 3, `fast peaked at ${peak.get("fast")}`);
    await assert.rejects(
      client.limits.get("slow"),
      (e) => e.code === "not_found",
    );
  },
);

test("the client manages limits", async (t) => {
  if (!dsn) return t.skip();
  const { storage } = await setup(t);
  const client = new RunnerQClient({ storage });
  await client.limits.set({ type: "a", maxConcurrent: 3 });
  await client.limits.set({ type: "b", rateLimit: 2, ratePeriodMs: 60_000 });
  const list = await client.limits.list();
  assert.deepEqual(
    list.map((l) => [l.type, l.maxConcurrent, l.ratePeriodMs]),
    [
      ["a", 3, undefined],
      ["b", undefined, 60_000],
    ],
  );
  await client.limits.delete("a");
  await assert.rejects(client.limits.get("a"), (e) => e.code === "not_found");
});
