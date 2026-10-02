// The implementation. Only workers import it.
import { NonRetryableError, type ActivityContext } from "runnerq";
import type { Account, SignupInput } from "./activities.ts";

export async function handleSignupWorkflow(
  ctx: ActivityContext,
  input: SignupInput,
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
