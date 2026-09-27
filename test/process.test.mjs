import test from "node:test";
import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { activity, RunnerQClient } from "../dist/index.js";
import { dsn, setup, until } from "./helpers.mjs";
test(
  "SIGKILL and replacement process replay a committed checkpoint without repeating the effect",
  { skip: !dsn, timeout: 30000 },
  async (t) => {
    const { storage, pool, queue } = await setup(t),
      client = new RunnerQClient({ storage });
    const messages = [];
    const children = [];
    const killChildren = () => {
      for (const child of children)
        if (child.exitCode === null && child.signalCode === null)
          child.kill("SIGKILL");
    };
    t.signal.addEventListener("abort", killChildren, { once: true });
    const exitSignal = () =>
      AbortSignal.any([t.signal, AbortSignal.timeout(5000)]);
    const spawn = (mode) => {
      const child = fork(
        new URL("./process-worker.mjs", import.meta.url),
        [queue, mode],
        { stdio: ["ignore", "ignore", "inherit", "ipc"] },
      );
      child.on("message", (m) => messages.push({ pid: child.pid, ...m }));
      children.push(child);
      return child;
    };
    try {
      const first = spawn("stall");
      await until(() =>
        messages.some((m) => m.pid === first.pid && m.type === "ready"),
      );
      const h = await client.execute(activity("ProcessCrash"), null);
      await until(() =>
        messages.some((m) => m.pid === first.pid && m.type === "checkpointed"),
      );
      const died = once(first, "exit", { signal: exitSignal() });
      first.kill("SIGKILL");
      await died;
      await pool.query(
        "UPDATE runnerq_activities SET lease_deadline_ms=0 WHERE queue_name=$1 AND id=$2",
        [queue, h.id],
      );
      await storage.reap(10);
      const second = spawn("resume");
      await until(() =>
        messages.some((m) => m.pid === second.pid && m.type === "ready"),
      );
      assert.deepEqual(await h.result({ signal: AbortSignal.timeout(10000) }), {
        receipt: "recorded",
        at: new Date("2026-01-01T00:00:00Z"),
        amount: 9007199254740993n,
        bytes: Buffer.from("receipt"),
      });
      assert.equal(messages.filter((m) => m.type === "effect").length, 1);
      const stopped = once(second, "exit", { signal: exitSignal() });
      second.send("stop");
      try {
        const [code, signal] = await stopped;
        assert.equal(code, 0);
        assert.equal(signal, null);
      } catch (cause) {
        throw new Error(
          `Replacement worker did not stop cleanly: ${JSON.stringify(messages.filter((m) => m.pid === second.pid))}`,
          { cause },
        );
      }
    } finally {
      for (const child of children)
        if (child.exitCode === null && child.signalCode === null) {
          const exited = once(child, "exit", {
            signal: AbortSignal.timeout(5000),
          });
          child.kill("SIGKILL");
          await exited;
        }
      t.signal.removeEventListener("abort", killChildren);
    }
  },
);
