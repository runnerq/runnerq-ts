// Runs the handlers. Start it first and leave it running.
import { Worker } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
import { SendWelcome, Signup } from "./activities.ts";
import { handleSendWelcome, handleSignup } from "./handlers.ts";

const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:runnerq@localhost:5432/runnerq";
await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "multiple_files",
});

const worker = new Worker({ storage });
worker.register(Signup, handleSignup);
worker.register(SendWelcome, handleSendWelcome);
await worker.start();
console.log(
  "worker running; start producer.ts in another terminal (Ctrl+C to stop)",
);

process.once("SIGINT", async () => {
  console.log("\nstopping...");
  await worker.stop();
  await storage.close();
});
