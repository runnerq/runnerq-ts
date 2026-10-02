// The contract: persisted names and types. Producers and workers both import
// this file; it pulls in no handler code.
import { activity } from "runnerq";

export type SignupInput = { email: string };
export type Account = { user_id: string; email: string };

export const Signup = activity<SignupInput, Account>("Signup");
export const SendWelcome = activity<Account, boolean>("SendWelcome");
