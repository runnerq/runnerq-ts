import { activity, runner, RunnerQClient, Worker } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:runnerq@localhost:5432/runnerq";
await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "fan_out",
});
const client = new RunnerQClient({ storage });
const Item = activity<number, number>("ProcessItem");
const Batch = activity<number[], number[]>("ProcessBatch");
// Even at concurrency 1, parents park and free capacity for their children.
const worker = new Worker({ storage, concurrency: 1, waitGraceMs: 20 });
worker.register(Item, async (_ctx, n) => n * 2);
worker.register(Batch, async (ctx, values) => {
  const children = [];
  for (const [i, value] of values.entries())
    children.push(await ctx.spawn(Item, value, runner.step(`item-${i}`)));
  return ctx.waitAll(children);
});
try {
  await worker.start();
  const handle = await client.execute(Batch, [1, 2, 3], runner.maxAttempts(1));
  console.log(await handle.result());
} finally {
  await worker.stop();
  await storage.close();
}
