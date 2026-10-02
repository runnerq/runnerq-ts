import {
  activity,
  runner,
  RunnerQClient,
  Worker,
  type ActivityContext,
} from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:runnerq@localhost:5432/runnerq";
await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "approval",
});
const client = new RunnerQClient({ storage });
const Approval = activity<{ orderId: string }, boolean>("Approval");

async function handleApproval(ctx: ActivityContext) {
  const decision = await ctx.waitForSignal<{ approved: boolean }>("decision", {
    timeoutMs: 60_000,
  });
  await ctx.sleep("cooling-off", 2_000);
  return decision.approved;
}

const worker = new Worker({ storage });
worker.register(Approval, handleApproval);
try {
  await worker.start();
  const handle = await client.execute(
    Approval,
    { orderId: "123" },
    runner.timeoutMs(1_000),
  );
  // Another process can deliver this signal using only the activity ID and database.
  await client.signal(handle.id, "decision", { approved: true });
  console.log(await handle.result());
} finally {
  await worker.stop();
  await storage.close();
}
