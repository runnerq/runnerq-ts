import { AsyncLocalStorage } from "node:async_hooks";
import type { Claim, Fence, Park, Storage, StoredResult } from "./storage.js";
import { RunnerQError } from "./errors.js";
export interface AttemptScope {
  storage: Storage;
  claim: Claim;
  fence: Fence;
  signal: AbortSignal;
  persistence: AbortSignal;
  deadline: number;
  maxDepth: number;
  waitGraceMs: number;
  closed: boolean;
  inStep?: boolean;
  suspension?: Park;
  pending: Set<Promise<unknown>>;
  names: Set<string>;
  waitActive: boolean;
  activeEffects: number;
  violation?: RunnerQError;
  wait(id: string): Promise<StoredResult>;
  recover<T>(
    fn: () => Promise<T>,
    persistence?: boolean,
    signal?: AbortSignal,
  ): Promise<T>;
}
export const executionScope = new AsyncLocalStorage<AttemptScope>();
export class Suspension extends Error {
  constructor() {
    super("Activity suspended for a durable wait");
    this.name = "Suspension";
  }
}
export function isControlFlow(error: unknown): boolean {
  return (
    error instanceof Suspension ||
    (error instanceof RunnerQError && error.code === "claim_lost")
  );
}
export function guard(scope: AttemptScope, orchestration = true): void {
  if (scope.closed)
    throw new RunnerQError(
      "claim_lost",
      "Activity invocation is no longer active",
    );
  if (scope.violation) throw scope.violation;
  if (scope.suspension) throw new Suspension();
  scope.signal.throwIfAborted();
  if (orchestration && executionScope.getStore()?.inStep)
    throw new RunnerQError(
      "configuration",
      "Durable orchestration cannot be nested inside ctx.run",
    );
}
export function track<T>(
  scope: AttemptScope,
  fn: () => Promise<T>,
): Promise<T> {
  const promise = Promise.resolve().then(() => {
    guard(scope);
    return fn();
  });
  const forget = () => scope.pending.delete(promise);
  scope.pending.add(promise);
  void promise.then(forget, forget);
  return promise;
}
export function suspend(scope: AttemptScope, park: Park): never {
  scope.suspension ??= park;
  throw new Suspension();
}
