// runnerq-spec's chaos worker (spec/tools/chaos/README.md). "work" runs a
// Worker serving <prefix>.flow and <prefix>.limited; "client" enqueues roots and delivers signals
// for the harness, one JSON request per stdin line.
import { createInterface } from "node:readline";
import { randomUUID } from "node:crypto";
import pg from "pg";
import {
  activity,
  runner,
  NonRetryableError,
  RunnerQClient,
  Worker,
} from "../dist/index.js";
import { PostgresStorage } from "../dist/postgres/index.js";

const env = process.env;
const dsn = env.RUNNERQ_CHAOS_DSN;
const queue = env.RUNNERQ_CHAOS_QUEUE;
const prefix = env.RUNNERQ_CHAOS_PREFIX;
const workerName = env.RUNNERQ_CHAOS_WORKER ?? "";
const positive = (s, d) => (Number(s) > 0 ? Number(s) : d);
if (!dsn || !queue || !prefix) {
  console.error(
    "chaos-worker: RUNNERQ_CHAOS_DSN, RUNNERQ_CHAOS_QUEUE and RUNNERQ_CHAOS_PREFIX are required",
  );
  process.exit(1);
}

const definitions = new Map();
const definition = (name) => {
  if (!definitions.has(name))
    definitions.set(name, activity(name, { serialization: "portable" }));
  return definitions.get(name);
};
const options = (timeoutS, maxRetryDelayS) => [
  runner.maxAttempts("unlimited"),
  runner.timeoutMs(timeoutS * 1000),
  runner.maxRetryDelayMs(Math.max(1, maxRetryDelayS) * 1000),
];
const sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const t = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        reject(signal.reason);
      },
      { once: true },
    );
  });

async function work() {
  await PostgresStorage.initialize({ connectionString: dsn });
  const storage = await PostgresStorage.connect({
    connectionString: dsn,
    queue,
  });
  const ledger = new pg.Pool({ connectionString: dsn, max: 4 });
  ledger.on("error", () => {});
  const worker = new Worker({
    storage,
    concurrency: positive(env.RUNNERQ_CHAOS_CONCURRENCY, 8),
    leaseMs: positive(env.RUNNERQ_CHAOS_LEASE_MS, 60_000),
    heartbeatMs: Math.min(
      10_000,
      Math.floor(positive(env.RUNNERQ_CHAOS_LEASE_MS, 60_000) / 3),
    ),
    shutdownGraceMs: 10_000,
  });
  worker.on("workerError", (error) =>
    console.error("workerError:", error?.message ?? error),
  );

  // A step's side effect: a ledger row, with a pause on either side so kills
  // land before, during and after it.
  const effect = async (ctx, step, stepMs, signal) => {
    const {
      rows: [{ begun }],
    } = await ledger.query("SELECT clock_timestamp() AS begun");
    await sleep(stepMs / 2, signal);
    const nonce = randomUUID();
    await ledger.query(
      `INSERT INTO runnerq_chaos_ledger (queue_name, activity_id, kind, step, nonce, worker, begun_at)
       VALUES ($1, $2, 'effect', $3, $4, $5, $6)`,
      [queue, ctx.activityId, step, nonce, workerName, begun],
    );
    await sleep(stepMs / 2, signal);
    return nonce;
  };

  const flow = async (ctx, input) => {
    if (!input || !Array.isArray(input.steps))
      throw new NonRetryableError("bad payload");
    await ledger.query(
      `INSERT INTO runnerq_chaos_ledger (queue_name, activity_id, kind, worker) VALUES ($1, $2, 'invoke', $3)`,
      [queue, ctx.activityId, workerName],
    );
    const out = { label: input.label, steps: {} };
    const runSteps = async (names = []) => {
      for (const name of names)
        out.steps[name] = await ctx.run(name, ({ signal }) =>
          effect(ctx, name, input.step_ms ?? 0, signal),
        );
    };
    await runSteps(input.steps);
    if (input.sleep_ms > 0) {
      await ctx.sleep("nap", input.sleep_ms);
    } else if (input.signal) {
      out.signal = await ctx.waitForSignal(input.signal);
    } else if (input.children?.length) {
      const handles = [];
      for (const [i, c] of input.children.entries())
        handles.push(
          await ctx.spawn(
            definition(c.type),
            c.payload,
            runner.step(`child-${i}`),
            ...options(c.timeout_s, c.max_retry_delay_s),
          ),
        );
      out.children = await ctx.waitAll(handles);
    }
    await runSteps(input.after);
    return out;
  };
  worker.register(definition(`${prefix}.flow`), flow);
  worker.register(definition(`${prefix}.limited`), flow, {
    maxConcurrent: positive(env.RUNNERQ_CHAOS_LIMIT, 2),
  });

  await worker.start();
  console.log(JSON.stringify({ ready: true }));
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    await worker.stop();
    await storage.close();
    await ledger.end();
    process.exit(0);
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
}

async function client() {
  await PostgresStorage.initialize({ connectionString: dsn });
  const storage = await PostgresStorage.connect({
    connectionString: dsn,
    queue,
  });
  const c = new RunnerQClient({ storage });
  const reply = (v) => process.stdout.write(JSON.stringify(v) + "\n");
  reply({ ready: true });
  for await (const line of createInterface({ input: process.stdin })) {
    if (!line.trim()) continue;
    const req = JSON.parse(line);
    try {
      if (req.op === "enqueue") {
        const handle = await c.execute(
          definition(req.type),
          req.payload,
          runner.idempotencyKey(req.key, "returnExisting"),
          ...options(req.timeout_s, req.max_retry_delay_s),
        );
        reply({ ok: { id: handle.id } });
      } else if (req.op === "signal") {
        await c.signal(req.id, req.name, req.payload, {
          serialization: "portable",
        });
        reply({ ok: {} });
      } else {
        reply({ error: `unknown op ${req.op}` });
      }
    } catch (error) {
      reply({ error: String(error?.message ?? error) });
    }
  }
  await storage.close();
}

const mode = process.argv[2];
if (mode === "work") await work();
else if (mode === "client") await client();
else {
  console.error("usage: chaos-worker.mjs work|client");
  process.exit(1);
}
