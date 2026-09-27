import { setTimeout as delay } from "node:timers/promises";
import { transient } from "./errors.js";
export async function pause(ms: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  await delay(Math.max(1, Math.min(ms, 2_147_483_647)), undefined, { signal });
}
export async function recover<T>(
  operation: () => Promise<T>,
  signal: AbortSignal,
  onRetry?: (error: unknown) => Promise<void> | void,
): Promise<T> {
  let backoff = 100;
  for (;;) {
    signal.throwIfAborted();
    try {
      return await operation();
    } catch (error) {
      if (!transient(error)) throw error;
      await onRetry?.(error);
      await pause(backoff * (0.95 + Math.random() * 0.1), signal);
      backoff = Math.min(backoff * 2, 30_000);
    }
  }
}
