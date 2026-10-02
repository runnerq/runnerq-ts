import {
  activity,
  runner,
  RunnerQClient,
  Worker,
  NonRetryableError,
  type ActivityContext,
} from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
import { startAgent } from "runnerq/conductor";

const apiKey = process.env.RUNNERQ_CONDUCTOR_KEY;
if (!apiKey) {
  console.error(
    "Set RUNNERQ_CONDUCTOR_KEY to an agent key (rqk_...) from the console's Connect page.",
  );
  process.exit(1);
}
const url = process.env.RUNNERQ_CLOUD_URL ?? "http://localhost:8088";
const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:runnerq@localhost:5432/runnerq";

type Order = { id: number; amount: number };
const Charge = activity<Order, { receipt: string }>("Charge");
const PlaceOrder = activity<Order, { receipt: string; shipped: boolean }>(
  "PlaceOrder",
);

await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "orders",
});
const client = new RunnerQClient({ storage });
const worker = new Worker({
  storage,
  concurrency: 4,
  labels: { example: "05-cloud" },
});

async function handleCharge(ctx: ActivityContext, order: Order) {
  // Every 5th charge fails once, so the console shows a retry.
  if (order.id % 5 === 0 && ctx.retryCount === 0)
    throw new Error("card network timeout");
  // Every 7th is declined for good: a failed activity.
  if (order.id % 7 === 0) throw new NonRetryableError("card declined");
  return { receipt: `r_${order.id}` };
}

async function handlePlaceOrder(ctx: ActivityContext, order: Order) {
  await ctx.run("reserve-stock", async () => `reserved ${order.id}`);
  const charge = await ctx.spawn(
    Charge,
    order,
    runner.step("charge"),
    runner.maxAttempts(3),
    runner.delayMs(1_000),
  );
  const { receipt } = await charge.result();
  await ctx.sleep("pack", 2_000); // a durable timer: the order shows as waiting
  await ctx.run("ship", async () => true);
  return { receipt, shipped: true };
}

worker.register(Charge, handleCharge);
worker.register(PlaceOrder, handlePlaceOrder);

await worker.start();
// Dials out over a WebSocket: the worker appears in Fleet, and the console reads
// activities from this database through it. allowControl lets the console cancel,
// retry, reschedule and signal them too.
const agent = startAgent(worker, { url, apiKey, allowControl: true });
console.log(
  `worker ${worker.id} connected to ${url}; placing an order every 3s`,
);
console.log("open the console's Activities and Fleet pages; Ctrl+C to stop");

let next = 1;
const placing = setInterval(async () => {
  const id = next++;
  const order = { id, amount: 10 + (id % 90) };
  try {
    await client.execute(PlaceOrder, order, runner.maxAttempts(3));
    console.log(`  ▶ placed order ${order.id}`);
  } catch (error) {
    console.error(`  ✗ order ${order.id}:`, error);
  }
}, 3_000);

process.once("SIGINT", async () => {
  clearInterval(placing);
  console.log("\nstopping...");
  await agent.close(); // first, so the Cloud records a clean shutdown
  await worker.stop();
  await storage.close();
});
