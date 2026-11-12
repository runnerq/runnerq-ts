// A schedule enqueues an activity on a cron expression. Declare it on the worker and
// every worker of the queue fires it; each tick runs once however many workers you
// start. Run this in two terminals: each heartbeat is printed by exactly one of them.
import { activity, RunnerQClient, Worker } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";

const Heartbeat = activity<{ source: string }, void>("Heartbeat", {
  serialization: "portable",
});

async function main() {
  const connectionString =
    process.env.DATABASE_URL ??
    "postgres://postgres:runnerq@localhost:5432/runnerq";
  await PostgresStorage.initialize({ connectionString });
  const storage = await PostgresStorage.connect({
    connectionString,
    queue: "schedules_demo",
  });
  const worker = new Worker({ storage, concurrency: 2 });
  worker.register(Heartbeat, (ctx, input) => {
    console.log(`heartbeat ${ctx.activityId} from ${input.source}`);
  });
  // Every 10 seconds (six fields: seconds first). Upserted when the worker starts; a
  // missed tick fires once when a worker comes back.
  worker.registerSchedule({
    name: "heartbeat",
    cron: "*/10 * * * * *",
    timezone: "UTC",
    activity: Heartbeat,
    payload: { source: "cron" },
    catchUp: "one",
  });
  await worker.start();

  // The schedule outlives the process; manage it from anywhere.
  const client = new RunnerQClient({ storage });
  const s = await client.schedules.get("heartbeat");
  console.log(`next heartbeat at ${s.nextFireAt}; Ctrl-C to stop`);

  await new Promise((resolve) => process.once("SIGINT", resolve));
  await worker.stop();
  await storage.close();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
