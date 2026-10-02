import { RunnerQClient, Worker } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
import { SignupWorkflow } from "./activities.ts";
import { handleSignupWorkflow } from "./handlers.ts";

async function main() {
  const connectionString =
    process.env.DATABASE_URL ??
    "postgres://postgres:runnerq@localhost:5432/runnerq";
  await PostgresStorage.initialize({ connectionString });
  const storage = await PostgresStorage.connect({
    connectionString,
    queue: "hello_workflow",
  });
  const client = new RunnerQClient({ storage });
  const worker = new Worker({ storage, concurrency: 4 });
  try {
    worker.register(SignupWorkflow, handleSignupWorkflow);
    await worker.start();
    console.log("starting signup workflow...");
    const handle = await client.execute(SignupWorkflow, {
      email: "ada@example.com",
    });
    const account = await handle.result({
      signal: AbortSignal.timeout(30_000),
    });
    console.log(`✓ signup complete: ${JSON.stringify(account)}`);
  } finally {
    try {
      await worker.stop();
    } finally {
      await storage.close();
    }
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
