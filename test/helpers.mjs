import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { PostgresStorage } from "../dist/postgres/index.js";
import { executionOptions } from "../dist/options.js";
export const dsn = process.env.RUNNERQ_TEST_DSN;
export async function setup(t) {
  await PostgresStorage.initialize({ connectionString: dsn });
  const queue = "ts_" + randomUUID().replaceAll("-", "");
  const pool = new Pool({ connectionString: dsn });
  const storage = await PostgresStorage.connect({
    connectionString: dsn,
    queue,
  });
  const others = [];
  t.after(async () => {
    await Promise.all([storage.close(), ...others.map((s) => s.close())]);
    for (const table of [
      "runnerq_inputs",
      "runnerq_results",
      "runnerq_events",
      "runnerq_dependencies",
      "runnerq_idempotency",
      "runnerq_worker_pools",
      "runnerq_commands",
      "runnerq_activities",
    ]) {
      await pool.query(`DELETE FROM ${table} WHERE queue_name=$1`, [queue]);
    }
    await pool.end();
  });
  return {
    storage,
    queue,
    pool,
    async another() {
      const s = await PostgresStorage.connect({ connectionString: dsn, queue });
      others.push(s);
      return s;
    },
  };
}
export function submission(...options) {
  const id = randomUUID();
  return {
    id,
    type: "test",
    payload: { hello: "world" },
    serialization: "json-v1",
    options: executionOptions(options),
    parentId: null,
    rootId: id,
    depth: 0,
  };
}
export async function claim(storage, a, token) {
  await storage.submit(a);
  const claims = await storage.claim(1, [a.type], 60_000);
  if (!claims.length) throw new Error("Expected a claim");
  return { ownerId: claims[0].id, token: claims[0].token };
}
export async function until(fn, timeout = 10_000, what = "Condition") {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await fn();
    if (result) return result;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`${what} timed out`);
}
export const quiet = { info() {}, warn() {} };
// fakeStorage hands out queued claims and records outcomes; nothing is durable.
export function fakeStorage(extra = {}) {
  const queue = [];
  const outcomes = [];
  let wake;
  return {
    queue: "payments",
    queued: queue,
    outcomes,
    push(claim) {
      queue.push(claim);
      wake?.();
    },
    async claim(limit) {
      return queue.splice(0, limit);
    },
    async waitForWork(signal) {
      await new Promise((resolve) => {
        wake = resolve;
        signal.addEventListener("abort", resolve, { once: true });
        setTimeout(resolve, 20);
      });
    },
    async renew() {
      return true;
    },
    async complete(fence) {
      outcomes.push(["complete", fence.ownerId]);
    },
    async fail(fence, reason, retryable) {
      outcomes.push(["fail", fence.ownerId]);
      return retryable ? "dead_letter" : "failed";
    },
    async reap() {
      return 0;
    },
    async close() {},
    ...extra,
  };
}
