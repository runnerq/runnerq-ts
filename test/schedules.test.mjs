import test from "node:test";
import assert from "node:assert/strict";
import { activity, runner, RunnerQClient, Worker } from "../dist/index.js";
import { applicationIdempotencyKey } from "../dist/postgres/query.js";
import { dsn, setup } from "./helpers.mjs";
import { scheduleStorageContract } from "./schedule-contract.mjs";

scheduleStorageContract(
  "PostgresStorage schedules",
  async (t) => {
    const s = await setup(t);
    return {
      ...s,
      setNextFireAt: (name, at) =>
        s.pool.query(
          "UPDATE runnerq_schedules SET next_fire_at=$3 WHERE queue_name=$1 AND name=$2",
          [s.queue, name, at],
        ),
    };
  },
  { skip: !dsn },
);

const tick = activity("tick", { serialization: "portable" });

test("registerSchedule validates the declaration", async (t) => {
  if (!dsn) return t.skip();
  const { storage } = await setup(t);
  const worker = new Worker({ storage });
  for (const bad of [
    { name: "x", cron: "61 * * * *", activity: tick },
    { name: "x", cron: "@daily", timezone: "Nowhere/Else", activity: tick },
    { name: "x", cron: "@daily", activity: tick, catchUp: "maybe" },
    { name: "", cron: "@daily", activity: tick },
  ])
    assert.throws(() => worker.registerSchedule(bad), /./);
  assert.throws(() =>
    worker.registerSchedule({
      name: "x",
      cron: "@daily",
      activity: tick,
      options: [runner.delayMs(1000)],
    }),
  );
  worker.registerSchedule({ name: "x", cron: "@daily", activity: tick });
  assert.throws(
    () =>
      worker.registerSchedule({ name: "x", cron: "@hourly", activity: tick }),
    /Duplicate schedule/,
  );
});

test(
  "two workers fire each tick of a declared schedule once",
  { skip: !dsn, timeout: 30_000 },
  async (t) => {
    const { storage, another, queue, pool } = await setup(t);
    const runs = new Map();
    const workers = [storage, await another()].map((s) => {
      const w = new Worker({ storage: s, shutdownGraceMs: 2000 });
      w.register(tick, (ctx) => {
        runs.set(ctx.activityId, (runs.get(ctx.activityId) ?? 0) + 1);
      });
      w.registerSchedule({
        name: "tick",
        cron: "* * * * * *",
        activity: tick,
        payload: { a: 1 },
        catchUp: "all",
      });
      return w;
    });
    await Promise.all(workers.map((w) => w.start()));
    await new Promise((r) => setTimeout(r, 2500));
    await workers[0].stop();
    await new Promise((r) => setTimeout(r, 2000));
    await new RunnerQClient({ storage }).schedules.pause("tick");
    await new Promise((r) => setTimeout(r, 1500));
    await workers[1].stop();
    const { rows } = await pool.query(
      "SELECT id, idempotency_key, status FROM runnerq_activities WHERE queue_name=$1",
      [queue],
    );
    const fires = rows
      .map((r) => {
        assert.equal(r.status, "completed");
        assert.equal(runs.get(r.id), 1, `${r.id} ran ${runs.get(r.id)} times`);
        return Date.parse(
          applicationIdempotencyKey(r.idempotency_key, "tick").replace(
            "schedule:tick:",
            "",
          ),
        );
      })
      .sort((a, b) => a - b);
    assert.ok(fires.length >= 3, `only ${fires.length} fires`);
    for (let i = 1; i < fires.length; i++)
      assert.equal(
        fires[i] - fires[i - 1],
        1000,
        "a tick was missed or doubled",
      );
  },
);

test("client.schedules manages schedules", { skip: !dsn }, async (t) => {
  const { storage } = await setup(t);
  const client = new RunnerQClient({ storage });
  await client.schedules.upsert({
    name: "report",
    cron: "30 2 * * *",
    timezone: "Europe/London",
    activity: tick,
    payload: { format: "pdf" },
    options: [runner.priority("high"), runner.maxAttempts(2)],
  });
  const s = await client.schedules.get("report");
  assert.equal(s.timezone, "Europe/London");
  assert.equal(s.type, "tick");
  assert.equal(s.options.priority, "high");
  assert.deepEqual(s.payload, { format: "pdf" });
  const handle = await client.schedules.trigger("report");
  assert.equal((await storage.getActivity(handle.id)).type, "tick");
  await client.schedules.pause("report");
  assert.equal((await client.schedules.list())[0].paused, true);
  await client.schedules.resume("report");
  await client.schedules.delete("report");
  assert.deepEqual(await client.schedules.list(), []);
});
