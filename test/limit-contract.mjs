// The LimitStorage contract, for any backend (the Go SDK's storagetest Limits group): stored
// limits, and the claim's promise under them. `setup(t)` returns { storage, another(),
// running(type) }, running counting the type's running activities in the store.
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { executionOptions } from "../dist/options.js";
import { runner } from "../dist/index.js";

const rejects = (promise, kind) =>
  assert.rejects(promise, (e) => (e.code ?? e.kind) === kind);

function submission(type) {
  const id = randomUUID();
  return {
    id,
    type,
    payload: null,
    serialization: "json-v1",
    options: executionOptions([]),
    parentId: null,
    rootId: id,
    depth: 0,
  };
}
async function submitN(storage, n, type) {
  const ids = [];
  for (let i = 0; i < n; i++) {
    const a = submission(type);
    await storage.submit(a);
    ids.push(a.id);
  }
  return ids;
}
const json = { serialization: "json-v1", data: null };
const fence = (c) => ({ ownerId: c.id, token: c.token });
const ids = (claims) => claims.map((c) => c.id);

export function limitStorageContract(label, setup, options = {}) {
  const it = (name, fn) =>
    test(`${label}: ${name}`, { skip: options.skip, timeout: 120_000 }, fn);

  it("set, get, list and delete", async (t) => {
    const { storage, another } = await setup(t);
    await storage.setLimit({ type: "b", maxConcurrent: 3 });
    await storage.setLimit({ type: "a", rateLimit: 5, ratePeriodMs: 1500 });
    const a = await storage.getLimit("a");
    assert.deepEqual(
      { ...a, updatedAt: undefined },
      { type: "a", rateLimit: 5, ratePeriodMs: 1500, updatedAt: undefined },
    );
    assert.deepEqual(
      (await storage.listLimits()).map((l) => l.type),
      ["a", "b"],
    );
    await storage.setLimit({ type: "a", rateLimit: 5, ratePeriodMs: 1500 });
    assert.equal(
      (await storage.getLimit("a")).updatedAt,
      a.updatedAt,
      "an unchanged definition moved updatedAt",
    );
    await storage.setLimit({ type: "a", maxConcurrent: 1 });
    assert.deepEqual(
      { ...(await storage.getLimit("a")), updatedAt: undefined },
      { type: "a", maxConcurrent: 1, updatedAt: undefined },
    );
    for (const bad of [
      { type: "", maxConcurrent: 1 },
      { type: "x" },
      { type: "x", maxConcurrent: 0 },
      { type: "x", maxConcurrent: 1.5 },
      { type: "x", rateLimit: 1 },
      { type: "x", ratePeriodMs: 1000 },
      { type: "x", rateLimit: 1, ratePeriodMs: 0 },
    ])
      await rejects(storage.setLimit(bad), "invalid_argument");
    await storage.deleteLimit("b");
    await rejects(storage.getLimit("b"), "not_found");
    await rejects(storage.deleteLimit("b"), "not_found");
    assert.deepEqual(
      (await (await another()).listLimits()).map((l) => l.type),
      ["a"],
    );
  });

  it("maxConcurrent caps running activities across handles", async (t) => {
    const { storage, another } = await setup(t);
    await storage.setLimit({ type: "t", maxConcurrent: 2 });
    const as = await submitN(storage, 4, "t");
    const first = await storage.claim(10, ["t"], 60_000);
    assert.deepEqual(ids(first), as.slice(0, 2));
    assert.deepEqual(await (await another()).claim(10, ["t"], 60_000), []);
    await storage.complete(fence(first[0]), json);
    assert.deepEqual(ids(await storage.claim(10, ["t"], 60_000)), [as[2]]);
    assert.deepEqual(await storage.claim(10, ["t"], 60_000), []);
  });

  it("waiting and retrying activities don't count", async (t) => {
    const { storage } = await setup(t);
    await storage.setLimit({ type: "t", maxConcurrent: 1 });
    const as = await submitN(storage, 3, "t");
    const [a] = await storage.claim(10, ["t"], 60_000);
    assert.equal(a.id, as[0]);
    await storage.park(fence(a), {
      kind: "sleep",
      step: "sleep:nap",
      wakeAt: new Date(Date.now() + 3_600_000).toISOString(),
    });
    const [b] = await storage.claim(10, ["t"], 60_000);
    assert.equal(b.id, as[1]);
    assert.equal(await storage.fail(fence(b), "again", true), "retrying");
    assert.deepEqual(ids(await storage.claim(10, ["t"], 60_000)), [as[2]]);
    assert.deepEqual(await storage.claim(10, ["t"], 60_000), []);
  });

  it("a rate spends a token per claim", async (t) => {
    const { storage } = await setup(t);
    await storage.setLimit({
      type: "t",
      rateLimit: 3,
      ratePeriodMs: 3_600_000,
    });
    const as = await submitN(storage, 5, "t");
    assert.deepEqual(ids(await storage.claim(1, ["t"], 60_000)), [as[0]]);
    const got = await storage.claim(10, ["t"], 60_000);
    assert.deepEqual(ids(got), as.slice(1, 3));
    await storage.complete(fence(got[0]), json);
    await storage.setLimit({
      type: "t",
      rateLimit: 3,
      ratePeriodMs: 3_600_000,
    });
    assert.deepEqual(await storage.claim(10, ["t"], 60_000), []);
    await storage.setLimit({
      type: "t",
      maxConcurrent: 10,
      rateLimit: 3,
      ratePeriodMs: 3_600_000,
    });
    assert.deepEqual(await storage.claim(10, ["t"], 60_000), []);
  });

  it("a claim mixes limited and unlimited types in claim order", async (t) => {
    const { storage } = await setup(t);
    await storage.setLimit({ type: "x", maxConcurrent: 1 });
    const xs = await submitN(storage, 3, "x");
    const ys = await submitN(storage, 3, "y");
    const urgent = submission("y");
    urgent.options = executionOptions([runner.priority("critical")]);
    await storage.submit(urgent);
    const got = await storage.claim(10, ["x", "y"], 60_000);
    assert.deepEqual(ids(got), [urgent.id, xs[0], ...ys]);
    assert.deepEqual(await storage.claim(10, ["x", "y"], 60_000), []);
    await storage.complete(fence(got[1]), json);
    await submitN(storage, 2, "y");
    const two = await storage.claim(2, ["x", "y"], 60_000);
    assert.equal(two.length, 2);
    assert.equal(two[0].id, xs[1]);
  });

  it("lowering a limit preempts nothing", async (t) => {
    const { storage } = await setup(t);
    await storage.setLimit({ type: "t", maxConcurrent: 3 });
    const as = await submitN(storage, 4, "t");
    const running = await storage.claim(10, ["t"], 60_000);
    assert.equal(running.length, 3);
    await storage.setLimit({ type: "t", maxConcurrent: 1 });
    for (const c of running) {
      assert.deepEqual(await storage.claim(10, ["t"], 60_000), []);
      assert.ok(await storage.renew(fence(c), 60_000));
      await storage.complete(fence(c), json);
    }
    assert.deepEqual(ids(await storage.claim(10, ["t"], 60_000)), [as[3]]);
  });

  it("deleting a limit frees the type", async (t) => {
    const { storage, another } = await setup(t);
    await storage.setLimit({ type: "t", maxConcurrent: 1 });
    await submitN(storage, 4, "t");
    assert.equal((await storage.claim(10, ["t"], 60_000)).length, 1);
    assert.deepEqual(await storage.claim(10, ["t"], 60_000), []);
    await (await another()).deleteLimit("t");
    assert.equal((await storage.claim(10, ["t"], 60_000)).length, 3);
  });

  it("fifty claimers never run more than ten", async (t) => {
    const { storage, another, running } = await setup(t);
    const limit = 10,
      total = 400;
    await storage.setLimit({ type: "t", maxConcurrent: limit });
    await submitN(storage, total, "t");
    const handles = [storage];
    for (let i = 0; i < 4; i++) handles.push(await another());
    let done = 0,
      held = 0,
      heldPeak = 0,
      dbPeak = 0,
      samples = 0,
      stop = false;
    const seen = new Set();
    const sampler = (async () => {
      while (!stop) {
        dbPeak = Math.max(dbPeak, await running("t"));
        samples++;
      }
    })();
    const claimer = async (i) => {
      const h = handles[i % handles.length];
      while (done < total) {
        const claims = await h.claim(1 + (i % 3), ["t"], 60_000);
        if (!claims.length) {
          await new Promise((r) => setTimeout(r, 2));
          continue;
        }
        held += claims.length;
        heldPeak = Math.max(heldPeak, held);
        await new Promise((r) => setTimeout(r, Math.random() * 3));
        for (const c of claims) {
          assert.ok(!seen.has(c.id), `${c.id} claimed twice`);
          seen.add(c.id);
          held--;
          await h.complete(fence(c), json);
          done++;
        }
      }
    };
    try {
      await Promise.all(Array.from({ length: 50 }, (_, i) => claimer(i)));
    } finally {
      stop = true;
      await sampler;
    }
    assert.equal(done, total);
    assert.ok(heldPeak <= limit, `claimers held ${heldPeak} at once`);
    assert.ok(
      dbPeak <= limit,
      `${dbPeak} running at once over ${samples} samples`,
    );
  });
}
