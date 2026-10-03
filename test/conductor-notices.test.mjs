// activity.notices (runnerq-go's conductor/notices_test.go).
import test from "node:test";
import assert from "node:assert/strict";
import { activity, RunnerQClient, runner, Worker } from "../dist/index.js";
import { startAgent } from "../dist/conductor/index.js";
import { Notices, noticeBuffer } from "../dist/conductor/notices.js";
import { fakeGateway, key } from "./gateway.mjs";
import { dsn, quiet, setup, until } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 60_000 }, fn);

/** Every notice the gateway has, keyed "<activity id>/<type>". */
function notices(g) {
  const seen = new Map();
  for (const e of g.events)
    if (e.type === "activity.notices")
      for (const n of e.data.items) seen.set(`${n.activity_id}/${n.type}`, n);
  return seen;
}

integration(
  "while the Cloud asks, the agent announces what its worker submits, claims and completes",
  async (t) => {
    const { storage, queue } = await setup(t);
    const g = await fakeGateway(t, { config: { notices: true } });
    const Child = activity("Child");
    const Parent = activity("Parent");
    const worker = new Worker({ storage, concurrency: 2 });
    worker.register(Child, () => "done");
    worker.register(Parent, async (ctx) =>
      ctx.wait(await ctx.spawn(Child, {}, runner.step("child"))),
    );
    const agent = startAgent(worker, {
      url: g.url,
      apiKey: key,
      minReconnectDelayMs: 10,
      maxReconnectDelayMs: 50,
      logger: quiet,
    });
    t.after(() => agent.close());
    await worker.start();
    t.after(() => worker.stop({ graceMs: 1_000 }));
    await g.until(() => agent.connected, "connection");
    assert.ok(g.hellos[0].capabilities["activity.notices"]);

    const parent = await new RunnerQClient({ storage }).execute(Parent, {});
    await parent.result({ signal: AbortSignal.timeout(10_000) });
    const id = parent.id;
    const seen = await until(() => {
      const s = notices(g);
      const child = [...s.values()].find((n) => n.activity_type === "Child");
      return (
        child &&
        s.has(`${id}/attempt.succeeded`) &&
        s.has(`${child.activity_id}/activity.created`) &&
        s.has(`${child.activity_id}/attempt.succeeded`) &&
        s
      );
    });
    const started = seen.get(`${id}/attempt.started`);
    assert.equal(started.queue, queue);
    assert.equal(started.activity_type, "Parent");
    assert.equal(started.root_id, id);
    assert.equal(started.attempt, 1);
    assert.equal(started.executor_id, worker.id);
    const child = [...seen.values()].find((n) => n.activity_type === "Child");
    assert.equal(child.root_id, id, "a child's root is its parent's");
    // A submission from outside a handler has no worker to announce it.
    assert.ok(!seen.has(`${id}/activity.created`));

    g.send("config.update", { notices: false });
    await new Promise((r) => setTimeout(r, 100));
    const mark = g.events.length;
    const later = await new RunnerQClient({ storage }).execute(Child, {});
    await later.result({ signal: AbortSignal.timeout(10_000) });
    await new Promise((r) => setTimeout(r, 600));
    assert.ok(
      !g.events.slice(mark).some((e) => e.type === "activity.notices"),
      "notices after the Cloud turned them off",
    );
  },
);

test("past its buffer the oldest notices go, and the next batch says how many", () => {
  const n = new Notices("q", "exec-1");
  const at = new Date();
  n.announce({
    type: "activity.created",
    activityId: "first",
    activityType: "T",
    rootId: "first",
    at,
  });
  for (let i = 0; i < noticeBuffer; i++)
    n.announce({
      type: "attempt.started",
      activityId: `a${i}`,
      activityType: "T",
      rootId: `a${i}`,
      attempt: 1,
      at,
    });
  const { items, dropped } = n.take();
  assert.equal(items.length, noticeBuffer);
  assert.equal(dropped, 1);
  assert.notEqual(items[0].activity_id, "first");
  assert.deepEqual(n.take(), { items: [], dropped: 0 });
});
