// The version pinning storage contract (runnerq-spec versions.md), for any backend: the Go
// SDK's storagetest Versions group. `setup(t)` returns { storage, expireLease(id),
// makeDue(id) }; the
// storage also answers getActivity (and events, applyCommand and the schedule methods, whose
// cases are skipped when absent).
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { executionOptions } from "../dist/options.js";
import { checkpointId, commandFingerprint } from "../dist/codec.js";

const A = ["A"];
const B = ["B"];
const hour = () => new Date(Date.now() + 3600_000).toISOString();

function submission(type, version, parent) {
  const id = randomUUID();
  return {
    id,
    type,
    payload: null,
    serialization: "json-v1",
    options: executionOptions([]),
    parentId: parent?.id ?? null,
    rootId: parent?.rootId ?? id,
    depth: parent ? parent.depth + 1 : 0,
    ...(version && { version }),
  };
}

export function versionStorageContract(label, setup, options = {}) {
  const it = (name, fn) =>
    test(`${label}: ${name}`, { skip: options.skip, timeout: 30_000 }, fn);

  async function harness(t) {
    const h = await setup(t);
    const { storage } = h;
    const s = {
      ...h,
      async enqueue(type, version, parent) {
        const a = submission(type, version, parent);
        if (parent?.fence) a.fence = parent.fence;
        await storage.submit(a);
        return a;
      },
      async claimAs(versions, type, limit = 10) {
        return storage.claim(limit, [type], 60_000, undefined, versions);
      },
      async wantClaimAs(versions, a) {
        const [c] = await s.claimAs(versions, a.type, 1);
        assert.equal(c?.id, a.id, `serving ${versions} claimed ${c?.id}`);
        return { ...a, fence: { ownerId: c.id, token: c.token }, claim: c };
      },
      async wantNothingFor(versions, type) {
        assert.deepEqual(await s.claimAs(versions, type), []);
      },
      async version(id) {
        return (await storage.getActivity(id)).codeVersion;
      },
    };
    return s;
  }

  it("claims serve only their versions; no versions claims all", async (t) => {
    const s = await harness(t);
    const a1 = await s.enqueue("t", "A");
    const b1 = await s.enqueue("t", "B");
    const u = await s.enqueue("t");
    assert.equal(await s.version(a1.id), "A");
    assert.equal(await s.version(u.id), null);
    const claims = await s.claimAs(["A", "C"], "t");
    assert.deepEqual(
      claims.map((c) => c.id),
      [a1.id, u.id],
    );
    assert.equal(claims[0].version, "A");
    assert.equal(claims[1].version, undefined);
    await s.wantNothingFor(A, "t");
    await s.wantClaimAs(B, b1);
    const x = await s.enqueue("t", "X");
    await s.wantClaimAs(undefined, x);
  });

  it("children take the root's version", async (t) => {
    const s = await harness(t);
    const root = await s.wantClaimAs(A, await s.enqueue("root", "A"));
    const child = await s.enqueue("child", "B", root);
    assert.equal(await s.version(child.id), "A");
    await s.wantNothingFor(B, "child");
    const claimed = await s.wantClaimAs(A, child);
    const grandchild = await s.enqueue("grandchild", "C", claimed);
    assert.equal(await s.version(grandchild.id), "A");
    const plain = await s.wantClaimAs(B, await s.enqueue("root"));
    const kid = await s.enqueue("child", "B", plain);
    assert.equal(await s.version(kid.id), null);
  });

  it("parked work wakes only for its version", async (t) => {
    const s = await harness(t);
    const { storage } = s;
    const sleeper = await s.wantClaimAs(A, await s.enqueue("t", "A"));
    await storage.park(sleeper.fence, {
      kind: "sleep",
      step: "sleep:nap",
      wakeAt: new Date(Date.now() - 1000).toISOString(),
    });
    await s.wantNothingFor(B, "t");
    await s.wantClaimAs(A, sleeper);

    const waiter = await s.wantClaimAs(A, await s.enqueue("waiter", "A"));
    await storage.park(waiter.fence, {
      kind: "signal",
      step: "signal:go",
      wakeAt: hour(),
      resultId: checkpointId(waiter.id, "signal", "go"),
    });
    await storage.signal(waiter.id, "go", {
      serialization: "json-v1",
      data: true,
    });
    await s.wantNothingFor(B, "waiter");
    await s.wantClaimAs(A, waiter);
  });

  it("the version survives retries and lease recovery", async (t) => {
    const s = await harness(t);
    const a = submission("t", "A");
    a.options = executionOptions([]);
    a.options.maxAttempts = 5;
    a.options.maxRetryDelayMs = 1000;
    await s.storage.submit(a);
    const c1 = await s.wantClaimAs(A, a);
    await s.storage.fail(c1.fence, "again", true);
    await s.makeDue(a.id);
    await s.wantNothingFor(B, "t");
    await s.wantClaimAs(A, a);
    await s.expireLease(a.id);
    await s.storage.reap(10);
    assert.equal(await s.version(a.id), "A");
    await s.wantNothingFor(B, "t");
    await s.wantClaimAs(A, a);
  });

  it("repin moves work through the ledger", async (t) => {
    const s = await harness(t);
    const { storage } = s;
    if (typeof storage.applyCommand !== "function") return t.skip();
    const a = await s.enqueue("t", "A");
    const b = await s.enqueue("t", "A");
    const cmd = {
      id: "repin-1",
      kind: "repin",
      target: { ids: [a.id] },
      version: "B",
    };
    cmd.fingerprint = commandFingerprint(cmd);
    const res = await storage.applyCommand(cmd);
    assert.equal(res.applied, 1);
    assert.equal(await s.version(a.id), "B");
    if (typeof storage.events === "function") {
      const events = await storage.events(a.id);
      assert.deepEqual(
        events.map((e) => [e.type, e.detail.from, e.detail.to]),
        [["Repinned", "A", "B"]],
      );
    }
    assert.equal((await storage.applyCommand(cmd)).replayed, true);
    const same = await storage.applyCommand({
      kind: "repin",
      target: { ids: [a.id] },
      version: "B",
    });
    assert.equal(same.items[0].outcome, "skipped");
    assert.equal(same.items[0].error.kind, "conflict");
    const byVersion = await storage.applyCommand({
      kind: "repin",
      target: {
        filter: { field: "version", op: "eq", value: "A" },
        max: 10,
      },
      version: "B",
    });
    assert.deepEqual(
      byVersion.items.map((i) => i.id),
      [b.id],
    );
    await s.wantNothingFor(A, "t");
    assert.equal((await s.claimAs(B, "t")).length, 2);
    await assert.rejects(
      storage.applyCommand({ kind: "repin", target: { ids: [a.id] } }),
      (e) => (e.code ?? e.kind) === "invalid_argument",
    );
  });

  it("a schedule's version pins its fires", async (t) => {
    const s = await harness(t);
    const { storage } = s;
    if (typeof storage.upsertSchedule !== "function") return t.skip();
    const definition = (name, version) => ({
      name,
      cron: "@hourly",
      timezone: "UTC",
      type: "scheduled",
      payload: null,
      serialization: "json-v1",
      options: {
        priority: "normal",
        maxAttempts: 3,
        timeoutMs: 30_000,
        maxRetryDelayMs: 0,
        metadata: {},
        ...(version && { version }),
      },
      catchUp: "one",
      catchUpLimit: 100,
    });
    await storage.upsertSchedule(definition("pinned", "A"));
    assert.equal((await storage.getSchedule("pinned")).options.version, "A");
    assert.equal(await s.version(await storage.triggerSchedule("pinned")), "A");
    await storage.upsertSchedule(definition("plain"));
    assert.equal(await s.version(await storage.triggerSchedule("plain")), null);
  });
}
