// The implementation. Only workers import this file.
import { runner, type ActivityContext } from "runnerq";
import { SendWelcome, type Account, type SignupInput } from "./activities.ts";

export async function handleSignup(
  ctx: ActivityContext,
  input: SignupInput,
): Promise<Account> {
  const account = await ctx.run("create-account", async () => {
    console.log(`  ▶ creating account for ${input.email}`);
    return { user_id: "u_1001", email: input.email };
  });
  const welcome = await ctx.spawn(SendWelcome, account, runner.step("welcome"));
  await ctx.wait(welcome);
  return account;
}

export async function handleSendWelcome(
  ctx: ActivityContext,
  account: Account,
): Promise<boolean> {
  return ctx.run("send-email", async () => {
    console.log(`  ▶ sending welcome email to ${account.email}`);
    return true;
  });
}
