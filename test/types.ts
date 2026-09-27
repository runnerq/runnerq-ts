import {
  activity,
  runner,
  RunnerQClient,
  Worker,
  type ActivityHandle,
  type ActivityContext,
  type ChildActivityHandle,
} from "../src/index.js";
import type { Storage } from "../src/storage.js";
declare const storage: Storage;
const signup = activity<{ email: string }, { user_id: string }>(
  "SignupWorkflow",
);
const client = new RunnerQClient({ storage });
const result: Promise<ActivityHandle<{ user_id: string }>> = client.execute(
  signup,
  { email: "a@example.com" },
  runner.maxAttempts(3),
);
// @ts-expect-error payload is inferred from the activity
client.execute(signup, { email: 123 });
// @ts-expect-error options must be created by runner helpers
client.execute(signup, { email: "a" }, { priority: "high" });
const worker = new Worker({ storage });
worker.register(signup, async (ctx, input) => {
  const step = await ctx.run("create", () => ({ user_id: input.email }));
  const id: string = step.user_id;
  return { user_id: id };
});
// @ts-expect-error output contract is enforced
new Worker({ storage }).register(signup, async () => ({ wrong: true }));
void result;

async function handleTypes(ctx: ActivityContext) {
  const child = await ctx.spawn(signup, { email: "a" }, runner.step("signup"));
  const typed: ChildActivityHandle<{ user_id: string }> = child;
  const output: { user_id: string } = await child.result();
  // @ts-expect-error child result waits cannot use a caller-specific signal
  child.result({ signal: AbortSignal.timeout(30_000) });
  // @ts-expect-error child result waits accept no options at all
  child.result({});
  // @ts-expect-error a child cannot be widened to a cancellable client handle
  const external: ActivityHandle<{ user_id: string }> = child;
  // @ts-expect-error child output remains inferred from its definition
  const wrong: number = await child.result();
  const joined: { user_id: string } = await ctx.wait(child);
  const count = await ctx.spawn(
    activity<null, number>("Count"),
    null,
    runner.step("count"),
  );
  const tuple: readonly [{ user_id: string }, number] = await ctx.waitAll([
    child,
    count,
  ] as const);
  const array: { user_id: string }[] = await ctx.waitAll([child]);

  const submitted = await client.execute(signup, { email: "a" });
  const restored = client.handle(signup, child.id);
  const options = { signal: AbortSignal.timeout(30_000) };
  const submittedResult: { user_id: string } = await submitted.result(options);
  const restoredResult: { user_id: string } = await restored.result(options);
  const rehydratedWait: { user_id: string } = await ctx.wait(restored);
  const mixed: readonly [{ user_id: string }, { user_id: string }] =
    await ctx.waitAll([child, restored] as const);
  void [
    typed,
    output,
    external,
    wrong,
    joined,
    tuple,
    array,
    submittedResult,
    restoredResult,
    rehydratedWait,
    mixed,
  ];
}
void handleTypes;
