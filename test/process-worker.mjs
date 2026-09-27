import { activity, Worker } from "../dist/index.js";
import { PostgresStorage } from "../dist/postgres/index.js";
const storage = await PostgresStorage.connect({
  connectionString: process.env.RUNNERQ_TEST_DSN,
  queue: process.argv[2],
});
const w = new Worker({ storage, concurrency: 1 });
w.register(activity("ProcessCrash"), async (ctx) => {
  const result = await ctx.run("effect", () => {
    process.send?.({ type: "effect" });
    return { receipt: "recorded" };
  });
  process.send?.({ type: "checkpointed" });
  if (process.argv[3] === "stall") await new Promise(() => {});
  return result;
});
w.on("workerError", (error) =>
  process.send?.({ type: "error", message: error.message }),
);
process.on("message", async (message) => {
  if (message === "stop") {
    await w.stop();
    await storage.close();
    process.disconnect();
  }
});
await w.start();
process.send?.({ type: "ready" });
