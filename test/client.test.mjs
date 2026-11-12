// RunnerQClient's management API over storage commands and queries, with no worker running,
// and a cancel reaching a handler that a worker on other storage (another pool and
// notification connection, as another process has) is running.
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  activity,
  isCancelled,
  runner,
  RunnerQClient,
  RunnerQError,
  Worker,
} from "../dist/index.js";
import { dsn, setup, until } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30_000 }, fn);

function item(res, id) {
  const it = res.items.find((i) => i.id === id);
  assert.ok(it, `no item for ${id} in ${JSON.stringify(res.items)}`);
  return it;
}

integration("the client manages work without a worker", async (t) => {
  const { storage } = await setup(t);
  const client = new RunnerQClient({ storage });
  const Report = activity("report");
  const { id } = await client.execute(Report, { n: 1 });
  const later = await client.execute(Report, {}, runner.delayMs(3600_000));

  const rec = await client.get(id);
  assert.equal(rec.status, "pending");
  assert.equal(rec.type, "report");
  const page = await client.list({ statuses: ["scheduled"], type: "report" });
  assert.deepEqual(
    page.items.map((r) => r.id),
    [later.id],
  );

  let res = await client.setPriority(id, "critical");
  assert.deepEqual(item(res, id), {
    id,
    outcome: "applied",
    status: "pending",
  });
  assert.equal((await client.get(id)).priority, 4);

  res = await client.reschedule(
    [later.id, id],
    new Date(Date.now() + 7200_000),
  );
  assert.equal(item(res, later.id).outcome, "applied");
  assert.equal(item(res, id).outcome, "not_eligible");
  assert.equal(item(res, id).status, "pending");
  assert.ok(item(res, id).reason);
  res = await client.runNow(later.id);
  assert.deepEqual(item(res, later.id), {
    id: later.id,
    outcome: "applied",
    status: "scheduled",
  });

  const ghost = randomUUID();
  res = await client.cancel([id, ghost], {
    commandId: "cancel-report",
    reason: "not needed",
  });
  assert.equal(res.commandId, "cancel-report");
  assert.equal(res.replayed, false);
  assert.equal(res.applied, 1);
  assert.deepEqual(item(res, id), {
    id,
    outcome: "applied",
    status: "cancelled",
  });
  assert.equal(item(res, ghost).outcome, "not_found");
  await assert.rejects(client.handle(id).result(), (e) => isCancelled(e));

  // The same command id replays; with other input it conflicts.
  res = await client.cancel([id, ghost], {
    commandId: "cancel-report",
    reason: "not needed",
  });
  assert.equal(res.replayed, true);
  assert.equal(item(res, id).outcome, "applied");
  await assert.rejects(
    client.cancel(id, { commandId: "cancel-report" }),
    (e) => e instanceof RunnerQError && e.code === "conflict",
  );

  res = await client.delete(id, { dryRun: true });
  assert.equal(item(res, id).outcome, "would_apply");
  await client.get(id);

  res = await client.retry(id, { resetAttempts: true });
  assert.deepEqual(item(res, id), {
    id,
    outcome: "applied",
    status: "pending",
  });
  res = await client.delete(id);
  assert.equal(item(res, id).outcome, "not_eligible");

  res = await client.cancel({
    filter: { field: "type", op: "eq", value: "report" },
    max: 10,
  });
  assert.equal(res.applied, 2);
  assert.equal(res.more, false);
  res = await client.delete(id);
  assert.deepEqual(item(res, id), { id, outcome: "applied" });
  await assert.rejects(
    client.get(id),
    (e) => e instanceof RunnerQError && e.code === "not_found",
  );

  await assert.rejects(
    client.cancel("not-a-uuid"),
    (e) => e instanceof RunnerQError && e.code === "configuration",
  );
  await assert.rejects(
    client.cancel({ filter: { field: "type", op: "eq", value: "x" }, max: 0 }),
    (e) => e instanceof RunnerQError && e.code === "configuration",
  );
});

integration(
  "a cancel from another process aborts the running handler's signal promptly",
  async (t) => {
    const { storage, another } = await setup(t);
    // A minute between heartbeats: only the cancel's notification can stop it in time.
    const worker = new Worker({
      storage,
      heartbeatMs: 60_000,
      leaseMs: 120_000,
    });
    const Long = activity("Long");
    let runs = 0,
      reason,
      stoppedAt;
    worker.register(Long, async (ctx) => {
      runs++;
      await new Promise((resolve) =>
        ctx.signal.addEventListener("abort", resolve, { once: true }),
      );
      stoppedAt = Date.now();
      reason = ctx.signal.reason;
      throw reason;
    });
    await worker.start();
    t.after(() => worker.stop({ graceMs: 1_000 }));

    const client = new RunnerQClient({ storage: await another() });
    const { id } = await client.execute(Long, {});
    await until(() => runs === 1, 10_000, "start");
    assert.equal((await client.get(id)).status, "running");

    const sent = Date.now();
    const res = await client.cancel(id, { reason: "user abort" });
    assert.deepEqual(item(res, id), {
      id,
      outcome: "applied",
      status: "cancelled",
    });
    await until(() => stoppedAt !== undefined, 10_000, "handler abort");
    assert.ok(stoppedAt - sent < 5_000, `stopped ${stoppedAt - sent}ms later`);
    assert.equal(reason.code, "claim_lost");
    await assert.rejects(client.handle(id).result(), (e) => isCancelled(e));
    await new Promise((resolve) => setTimeout(resolve, 300));
    const rec = await client.get(id);
    assert.equal(rec.status, "cancelled");
    assert.equal(rec.lastError?.message, "activity cancelled: user abort");
    assert.equal(runs, 1);
  },
);

integration("cancel reaches the tree", async (t) => {
  const { storage, another } = await setup(t);
  const worker = new Worker({ storage, heartbeatMs: 60_000, leaseMs: 120_000 });
  const Child = activity("Child");
  const Parent = activity("Parent");
  const children = new Map();
  const parentErrors = new Map();
  let childAborts = 0;
  worker.register(Child, async (ctx) => {
    await new Promise((resolve) =>
      ctx.signal.addEventListener("abort", resolve, { once: true }),
    );
    childAborts++;
    throw ctx.signal.reason;
  });
  worker.register(Parent, async (ctx) => {
    const child = await ctx.spawn(Child, {}, runner.step("child"));
    children.set(ctx.activityId, child.id);
    try {
      await child.result();
    } catch (error) {
      if (!isCancelled(error)) throw error;
      parentErrors.set(ctx.activityId, error);
      throw new RunnerQError("non_retryable", "child cancelled");
    }
  });
  await worker.start();
  t.after(() => worker.stop({ graceMs: 1_000 }));
  const client = new RunnerQClient({ storage: await another() });
  const running = async (id) =>
    until(
      async () => (await client.get(id)).status === "running",
      10_000,
      "running",
    );

  const first = await client.execute(Parent, {});
  await until(() => children.has(first.id), 10_000, "spawn");
  const kid = children.get(first.id);
  await running(kid);
  let res = await client.cancel(kid, { cascade: false });
  assert.equal(item(res, kid).outcome, "applied");
  await until(() => childAborts === 1, 10_000, "child abort");
  await until(() => parentErrors.has(first.id), 20_000, "parent sees cancel");

  const second = await client.execute(Parent, {});
  await until(() => children.has(second.id), 10_000, "spawn");
  const kid2 = children.get(second.id);
  await running(kid2);
  res = await client.cancel(second.id);
  assert.equal(res.cascaded, 1);
  await until(() => childAborts === 2, 10_000, "cascaded abort");
  assert.equal((await client.get(kid2)).status, "cancelled");
});
