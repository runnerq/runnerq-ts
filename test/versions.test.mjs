// Version pinning (runnerq-spec versions.md): the storage contract on PostgresStorage, and
// workers and clients stamping, serving and repinning.
import test from "node:test";
import assert from "node:assert/strict";
import { activity, runner, RunnerQClient, Worker } from "../dist/index.js";
import { dsn, setup, until } from "./helpers.mjs";

const quiet = { debug() {}, info() {}, warn() {}, error() {} };
import { versionStorageContract } from "./version-contract.mjs";

versionStorageContract(
  "PostgresStorage versions",
  async (t) => {
    const s = await setup(t);
    const set = (sql) => (id) =>
      s.pool.query(
        `UPDATE runnerq_activities SET ${sql} WHERE queue_name=$1 AND id=$2`,
        [s.queue, id],
      );
    return {
      ...s,
      expireLease: set(
        "lease_deadline_ms=(EXTRACT(EPOCH FROM NOW())*1000)::bigint-10000",
      ),
      makeDue: set("scheduled_at=NOW()"),
    };
  },
  { skip: !dsn },
);

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30_000 }, fn);

test("worker version configuration is validated", () => {
  const storage = { queue: "q" };
  assert.throws(
    () => new Worker({ storage, serves: ["A"] }),
    /serves needs a version/,
  );
  assert.throws(
    () => new Worker({ storage, version: "v".repeat(257) }),
    /1 to 256 bytes/,
  );
  assert.throws(
    () => new Worker({ storage, version: "B", serves: [""] }),
    /1 to 256 bytes/,
  );
  assert.throws(
    () =>
      new Worker({
        storage,
        version: "B",
        serves: Array.from({ length: 64 }, (_, i) => `v${i}`),
      }),
    /at most 64/,
  );
  assert.throws(
    () => new RunnerQClient({ storage, version: "" }),
    /1 to 256 bytes/,
  );
  const w = new Worker({
    storage,
    version: "B",
    labels: { version: "label", region: "eu" },
  });
  const info = w.snapshot().info;
  assert.deepEqual(info.labels, { version: "B", region: "eu" });
  assert.equal(info.version, "B");
  assert.equal(w.client.version, "B");
});

integration(
  "a worker pins its roots and serves only its version",
  async (t) => {
    const { storage } = await setup(t);
    const work = activity("work");
    const parent = activity("parent");
    const child = activity("child");
    const runs = new Map();
    const start = (version) => {
      const w = new Worker({ storage, version, logger: quiet });
      w.register(work, (ctx) => void runs.set(ctx.activityId, version));
      w.register(child, (ctx) => void runs.set(ctx.activityId, version));
      w.register(parent, async (ctx) => {
        runs.set(ctx.activityId, version);
        const h = await ctx.spawn(child, null, runner.step("c"));
        return h.result();
      });
      t.after(() => w.stop());
      return w;
    };
    const clientA = new RunnerQClient({ storage, version: "A" });
    const a = await clientA.execute(work, null);
    const plain = await new RunnerQClient({ storage }).execute(work, null);
    assert.equal((await storage.getActivity(a.id)).codeVersion, "A");
    assert.equal((await storage.getActivity(plain.id)).codeVersion, null);

    const wB = start("B");
    await wB.start();
    await plain.result();
    const p = await wB.client.execute(parent, null);
    assert.equal((await storage.getActivity(p.id)).codeVersion, "B");
    await p.result();
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(runs.has(a.id), false, "a B worker ran A work");

    await start("A").start();
    await a.result();
    assert.equal(runs.get(a.id), "A");
    assert.equal(runs.get(plain.id), "B");
    for (const [id, version] of runs) {
      const snap = await storage.getActivity(id);
      if (snap.codeVersion) assert.equal(snap.codeVersion, version);
      if (snap.parentId) assert.equal(snap.codeVersion, "B");
    }
  },
);

integration("a worker's schedules carry its version", async (t) => {
  const { storage } = await setup(t);
  const tick = activity("tick");
  const w = new Worker({ storage, version: "v7", logger: quiet });
  w.register(tick, () => {});
  w.registerSchedule({ name: "s", cron: "@daily", activity: tick });
  await w.start();
  t.after(() => w.stop());
  assert.equal((await storage.getSchedule("s")).options.version, "v7");
  await new RunnerQClient({ storage }).schedules.upsert({
    name: "s",
    cron: "@daily",
    activity: tick,
  });
  assert.equal((await storage.getSchedule("s")).options.version, undefined);
});

integration("client.repin goes through the command ledger", async (t) => {
  const { storage } = await setup(t);
  const client = new RunnerQClient({ storage, version: "A" });
  const h = await client.execute(activity("t"), null);
  await assert.rejects(client.repin(h.id, ""), /1 to 256 bytes/);
  const res = await client.repin(h.id, "B", { commandId: "r1" });
  assert.equal(res.items[0].outcome, "applied");
  assert.equal(
    (await client.repin(h.id, "B", { commandId: "r1" })).replayed,
    true,
  );
  await assert.rejects(
    client.repin(h.id, "C", { commandId: "r1" }),
    (e) => e.code === "conflict",
  );
  assert.equal((await client.get(h.id)).version, "B");
  const page = await client.list({
    filter: { field: "version", op: "eq", value: "B" },
  });
  assert.equal(page.items.length, 1);
});

integration("version gauges count by version and unserved work", async (t) => {
  const { storage } = await setup(t);
  const gauges = new Map();
  const metrics = {
    addCounter() {},
    recordHistogram() {},
    setGauge(name, value, labels) {
      gauges.set(`${name}/${labels.version ?? ""}`, value);
    },
  };
  const old = new RunnerQClient({ storage, version: "A" });
  const work = activity("work");
  const h = await old.execute(work, null);
  const w = new Worker({
    storage,
    version: "B",
    metrics,
    gaugeIntervalMs: 50,
    logger: quiet,
  });
  w.register(work, () => {});
  await w.start();
  t.after(() => w.stop());
  await until(
    () =>
      gauges.get("activities_by_version/A") === 1 &&
      gauges.get("activities_unserved/A") === 1 &&
      gauges.get("activities_unserved/B") === 0,
  );
  await old.repin(h.id, "B");
  await h.result();
  await until(
    () =>
      gauges.get("activities_by_version/A") === 0 &&
      gauges.get("activities_unserved/A") === 0,
  );
});
