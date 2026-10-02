import {
  activity,
  RunnerQClient,
  Worker,
  NonRetryableError,
  type ActivityContext,
} from "runnerq";
import { PostgresStorage } from "runnerq/postgres";

type Account = { user_id: string; email: string };
const SignupWorkflow = activity<{ email: string }, Account>("SignupWorkflow");

async function handleSignupWorkflow(
  ctx: ActivityContext,
  input: { email: string },
): Promise<Account> {
  if (typeof input?.email !== "string")
    throw new NonRetryableError("Invalid payload");
  const user = await ctx.run("create-account", async () => {
    console.log(`  ▶ creating account for ${input.email}`);
    return { user_id: "u_1001", email: input.email };
  });
  await ctx.run("send-welcome", async () => {
    console.log(`  ▶ sending welcome email to ${user.email}`);
    return true;
  });
  return user;
}

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
