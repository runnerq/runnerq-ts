// Submits work and waits for the result. It imports only the definitions, so it
// could live in a different service from the worker.
import { RunnerQClient } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";
import { Signup } from "./activities.ts";

const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:runnerq@localhost:5432/runnerq";
await PostgresStorage.initialize({ connectionString });
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "multiple_files",
});

try {
  const client = new RunnerQClient({ storage });
  const handle = await client.execute(Signup, { email: "ada@example.com" });
  const account = await handle.result({ signal: AbortSignal.timeout(30_000) });
  console.log(`✓ signup complete: ${JSON.stringify(account)}`);
} finally {
  await storage.close();
}
