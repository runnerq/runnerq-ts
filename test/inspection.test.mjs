import test from "node:test";
import assert from "node:assert/strict";
import { Inspector } from "../dist/index.js";
import { dsn, setup, submission, claim } from "./helpers.mjs";
test(
  "inspection lists avoid payloads and detail loads inputs",
  { skip: !dsn, timeout: 15000 },
  async (t) => {
    const { storage } = await setup(t),
      inspector = new Inspector({ storage });
    const a = submission(),
      f = await claim(storage, a);
    await storage.complete(f, { ok: true });
    try {
      assert.equal("payload" in (await inspector.list())[0], false);
      assert.deepEqual(await inspector.input(a.id), a.payload);
      assert.equal((await inspector.subtree(a.id))[0].id, a.id);
      assert.equal((await inspector.stats()).counts.completed, 1);
    } finally {
      await inspector.close();
    }
  },
);
test(
  "slow event consumers receive an explicit overflow instead of unbounded buffering",
  { skip: !dsn, timeout: 15000 },
  async (t) => {
    const { storage } = await setup(t),
      inspector = new Inspector({ storage });
    const controller = new AbortController();
    try {
      const iterator = inspector.events({
        signal: controller.signal,
        bufferSize: 1,
      });
      const first = iterator.next();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await storage.submit(submission());
      await first;
      await Promise.all(
        Array.from({ length: 3 }, () => storage.submit(submission())),
      );
      await new Promise((resolve) => setTimeout(resolve, 1200));
      await assert.rejects(iterator.next(), /overflow/);
    } finally {
      controller.abort();
      await inspector.close();
    }
  },
);
