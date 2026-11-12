// The ScheduleStorage contract, for any backend (the Go SDK's storagetest Schedules group).
// `setup(t)` returns { storage, another(), setNextFireAt(name, date) }.
import test from "node:test";
import assert from "node:assert/strict";
import { businessKey } from "../dist/codec.js";
import { scheduleKey } from "../dist/schedule.js";

const definition = (name, cron, extra = {}) => ({
  name,
  cron,
  timezone: "UTC",
  type: "scheduled",
  payload: { n: 1 },
  serialization: "json-v1",
  options: {
    priority: "high",
    maxAttempts: 4,
    timeoutMs: 30_000,
    maxRetryDelayMs: 0,
    metadata: { k: "v" },
  },
  catchUp: "one",
  catchUpLimit: 100,
  ...extra,
});
const rejects = (promise, kind) =>
  assert.rejects(promise, (e) => (e.code ?? e.kind) === kind);

export function scheduleStorageContract(label, setup, options = {}) {
  const it = (name, fn) =>
    test(`${label}: ${name}`, { skip: options.skip, timeout: 30_000 }, fn);

  it("upsert, get, list and delete", async (t) => {
    const { storage } = await setup(t);
    await storage.upsertSchedule(definition("b", "@hourly"));
    await storage.upsertSchedule(definition("a", "0 9 * * 1-5"));
    const a = await storage.getSchedule("a");
    assert.equal(a.paused, false);
    assert.equal(a.lastFireAt, null);
    assert.ok(new Date(a.nextFireAt) > new Date(Date.now() - 1000));
    assert.deepEqual(a.payload, { n: 1 });
    assert.equal(a.options.priority, "high");
    assert.equal(a.options.maxAttempts, 4);
    assert.deepEqual(a.options.metadata, { k: "v" });
    assert.deepEqual(
      (await storage.listSchedules()).map((s) => s.name),
      ["a", "b"],
    );
    for (const bad of [
      definition("x", "61 * * * *"),
      definition("x", "@daily", { timezone: "Mars/Olympus_Mons" }),
      definition("x", "@daily", { catchUp: "some" }),
      definition("x", "@daily", { catchUpLimit: 0 }),
      definition("", "@daily"),
    ])
      await rejects(storage.upsertSchedule(bad), "invalid_argument");
    await storage.deleteSchedule("b");
    await rejects(storage.getSchedule("b"), "not_found");
    await rejects(storage.deleteSchedule("b"), "not_found");
    await rejects(storage.pauseSchedule("b"), "not_found");
    await rejects(storage.resumeSchedule("b"), "not_found");
    await rejects(storage.triggerSchedule("b"), "not_found");
  });

  it("upsert keeps paused and the next fire time unless the cron changes", async (t) => {
    const { storage, setNextFireAt } = await setup(t);
    await storage.upsertSchedule(definition("s", "@hourly"));
    await storage.pauseSchedule("s");
    const past = new Date(
      Math.floor(Date.now() / 3_600_000) * 3_600_000 - 3_600_000,
    );
    await setNextFireAt("s", past);
    await storage.upsertSchedule(
      definition("s", "@hourly", { payload: { n: 2 } }),
    );
    let s = await storage.getSchedule("s");
    assert.equal(s.paused, true);
    assert.equal(s.nextFireAt, past.toISOString());
    await storage.upsertSchedule(definition("s", "30 * * * *"));
    s = await storage.getSchedule("s");
    assert.equal(s.paused, true);
    assert.ok(new Date(s.nextFireAt) > new Date());
    assert.equal(new Date(s.nextFireAt).getUTCMinutes(), 30);
  });

  it("pause, trigger and resume", async (t) => {
    const { storage, setNextFireAt } = await setup(t);
    await storage.upsertSchedule(definition("s", "@hourly"));
    await storage.pauseSchedule("s");
    await setNextFireAt("s", new Date(Date.now() - 3 * 3_600_000));
    assert.equal(await storage.fireSchedules(100), 0);
    const id = await storage.triggerSchedule("s");
    const snap = await storage.getActivity(id);
    assert.equal(snap.type, "scheduled");
    assert.equal(snap.priority, 3);
    await storage.resumeSchedule("s");
    const s = await storage.getSchedule("s");
    assert.equal(s.paused, false);
    assert.ok(new Date(s.nextFireAt) > new Date());
    assert.equal(await storage.fireSchedules(100), 0);
  });

  it("a due tick fires once across concurrent handles", async (t) => {
    const { storage, another, setNextFireAt } = await setup(t);
    await storage.upsertSchedule(definition("s", "@hourly"));
    const tick = new Date(Math.floor(Date.now() / 3_600_000) * 3_600_000);
    await setNextFireAt("s", tick);
    const handles = [storage, await another(), await another()];
    const counts = await Promise.all(
      handles.flatMap((h) => [1, 2, 3].map(() => h.fireSchedules(100))),
    );
    assert.equal(
      counts.reduce((a, b) => a + b, 0),
      1,
    );
    const id = await storage.lookupKey(
      businessKey(scheduleKey("s", tick), "scheduled"),
    );
    const snap = await storage.getActivity(id);
    assert.equal(snap.status, "pending");
    assert.equal(snap.parentId, null);
    assert.deepEqual(snap.metadata, { k: "v" });
    const s = await storage.getSchedule("s");
    assert.equal(s.lastFireAt, tick.toISOString());
    assert.equal(
      s.nextFireAt,
      new Date(tick.getTime() + 3_600_000).toISOString(),
    );
    await setNextFireAt("s", tick);
    assert.equal(
      await storage.fireSchedules(100),
      0,
      "a lost advance re-fired",
    );
  });

  it("catch-up modes", async (t) => {
    const { storage, setNextFireAt } = await setup(t);
    const year = new Date(Date.UTC(new Date().getUTCFullYear(), 0, 1));
    for (const [catchUp, catchUpLimit, want] of [
      ["skip", 100, 0],
      ["one", 100, 1],
      ["all", 100, 6],
      ["all", 2, 2],
    ]) {
      const name = `${catchUp}${catchUpLimit}`;
      await storage.upsertSchedule(
        definition(name, "@yearly", { catchUp, catchUpLimit }),
      );
      await setNextFireAt(
        name,
        new Date(Date.UTC(year.getUTCFullYear() - 5, 0, 1)),
      );
      assert.equal(await storage.fireSchedules(100), want, name);
      const s = await storage.getSchedule(name);
      assert.equal(
        s.nextFireAt,
        new Date(Date.UTC(year.getUTCFullYear() + 1, 0, 1)).toISOString(),
      );
      if (want) assert.equal(s.lastFireAt, year.toISOString());
    }
  });
}
