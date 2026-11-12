// One side of the upgrade test, run by test/upgrade/run.mjs:
//   node phase.mjs previous <sdk dir> <state file>   create the schema, park work, stop
//   node phase.mjs current  <sdk dir> <state file>   migrate, finish all of it, check
// RUNNERQ_UPGRADE_DSN points at the test's own Postgres schema.
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import pg from "pg";
import { delayMs, previousTypes, queue, workload } from "./workload.mjs";

const [phase, sdkDir, stateFile] = process.argv.slice(2);
assert.ok(phase === "previous" || phase === "current", `phase: ${phase}`);
const connectionString = process.env.RUNNERQ_UPGRADE_DSN;
assert.ok(connectionString, "RUNNERQ_UPGRADE_DSN is required");

const load = (path) => import(pathToFileURL(join(sdkDir, "dist", path)).href);
const sdk = await load("index.js");
const { PostgresStorage } = await load("postgres/index.js");

const db = new pg.Pool({ connectionString, max: 2 });
const record = (activity, step, by) =>
  db.query(
    "INSERT INTO upgrade_ledger (activity_id, step, phase) VALUES ($1, $2, $3)",
    [activity, step, by],
  );
const statuses = async () => {
  const { rows } = await db.query(
    "SELECT id::text, activity_type, status FROM runnerq_activities WHERE queue_name = $1",
    [queue],
  );
  return new Map(rows.map((r) => [r.id, r]));
};
const log = (...args) => console.log(`[${phase}]`, ...args);

const { defs, handlers, runner } = workload(sdk, record, phase);
await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({ connectionString, queue });
const client = new sdk.RunnerQClient({ storage });
const worker = new sdk.Worker({ storage, concurrency: 4, waitGraceMs: 10 });
const types = phase === "previous" ? previousTypes : Object.keys(handlers);
for (const type of types) worker.register(defs[type], handlers[type]);

try {
  await worker.start();
  if (phase === "previous") await park();
  else await finish();
} finally {
  try {
    await worker.stop();
  } finally {
    await storage.close();
    await db.end();
  }
}

async function park() {
  const ids = {};
  // A sleep that fits in the attempt's timeout stays in memory; a 1s timeout
  // makes the 5s sleep park.
  ids.Sleeper = (
    await client.execute(defs.Sleeper, null, runner.timeoutMs(1_000))
  ).id;
  for (const type of ["Waiter", "Parent"])
    ids[type] = (await client.execute(defs[type], null)).id;
  ids.Delayed = (
    await client.execute(defs.Delayed, 5, runner.delayMs(delayMs))
  ).id;

  // Wait until everything is parked: the three handlers in a durable wait,
  // the child and the delayed activity not yet claimed.
  const deadline = Date.now() + 30_000;
  for (;;) {
    const rows = await statuses();
    const child = [...rows.values()].find(
      (r) => r.activity_type === defs.Child.name,
    );
    if (child) ids.Child = child.id;
    const parked =
      ["Sleeper", "Waiter", "Parent"].every(
        (t) => rows.get(ids[t])?.status === "waiting",
      ) &&
      child !== undefined &&
      ["pending", "scheduled"].includes(child.status) &&
      ["pending", "scheduled"].includes(rows.get(ids.Delayed)?.status);
    if (parked) {
      log("parked:", [...rows.values()].map(describe).join(", "));
      break;
    }
    if (Date.now() > deadline)
      throw new Error(
        `work did not park: ${[...rows.values()].map(describe).join(", ")}`,
      );
    await new Promise((r) => setTimeout(r, 50));
  }
  writeFileSync(stateFile, JSON.stringify(ids));
}

async function finish() {
  const ids = JSON.parse(readFileSync(stateFile, "utf8"));
  await client.signal(ids.Waiter, "go", { from: phase });
  const result = (type) =>
    client
      .handle(defs[type], ids[type])
      .result({ signal: AbortSignal.timeout(60_000) });
  const [sleeper, waiter, parent, child, delayed] = await Promise.all(
    ["Sleeper", "Waiter", "Parent", "Child", "Delayed"].map(result),
  );
  log("finished:", [...(await statuses()).values()].map(describe).join(", "));

  // Steps the previous SDK checkpointed replay its value; the rest ran now.
  assert.deepEqual(sleeper, { before: "previous", after: "current" });
  assert.deepEqual(waiter, {
    before: "previous",
    after: "current",
    signal: { from: "current" },
  });
  assert.deepEqual(child, { work: "current", value: 42 });
  assert.deepEqual(parent, { before: "previous", after: "current", child });
  assert.deepEqual(delayed, { work: "current", value: 10 });

  // Every step ran exactly once, in the phase that should have run it.
  const { rows } = await db.query(
    "SELECT activity_id, step, phase FROM upgrade_ledger ORDER BY activity_id, step, phase",
  );
  const ran = rows.map((r) => `${r.activity_id} ${r.step} ${r.phase}`).sort();
  const expected = [
    `${ids.Sleeper} before previous`,
    `${ids.Sleeper} after current`,
    `${ids.Waiter} before previous`,
    `${ids.Waiter} after current`,
    `${ids.Parent} before previous`,
    `${ids.Parent} after current`,
    `${ids.Child} work current`,
    `${ids.Delayed} work current`,
  ].sort();
  assert.deepEqual(ran, expected);

  const rows2 = await statuses();
  for (const [id, row] of rows2)
    assert.equal(row.status, "completed", `${describe(row)} (${id})`);
  log(
    `ok: ${rows2.size} activities completed, ${ran.length} steps each ran once`,
  );
}

function describe(row) {
  return `${row.activity_type}=${row.status}`;
}
