// The contract: persisted names and types. Producers and workers both import it.
import { activity } from "runnerq";

export type SignupInput = { email: string };
export type Account = { user_id: string; email: string };

export const SignupWorkflow = activity<SignupInput, Account>("SignupWorkflow");
