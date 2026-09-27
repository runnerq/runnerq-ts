import { activity, runner, RunnerQClient, Worker } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
import { setTimeout } from "node:timers/promises";
const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:runnerq@localhost:5432/runnerq";
await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "crash_resume",
});
const client = new RunnerQClient({ storage });
const Checkout = activity<{ orderId: string }, string>("Checkout");
const worker = new Worker({ storage });
worker.register(Checkout, async (ctx) => {
  await ctx.run("charge", () => {
    console.log("Charging once per recorded success");
    return "receipt";
  });
  console.log(
    "Checkpoint committed. Kill this process now; then run this example again.",
  );
  await setTimeout(10_000, undefined, { signal: ctx.signal });
  return ctx.run("ship", () => {
    console.log("Shipping");
    return "shipment";
  });
});
try {
  await worker.start();
  const handle = await client.execute(
    Checkout,
    { orderId: "demo-order" },
    runner.idempotencyKey("demo-order"),
    runner.timeoutMs(15_000),
  );
  console.log(await handle.result());
} finally {
  await worker.stop();
  await storage.close();
}
