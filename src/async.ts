import { setTimeout as delay } from "node:timers/promises";
import { transient } from "./errors.js";
/** setTimeout's longest delay. */
export const maxTimerMs = 2_147_483_647;
export async function pause(ms: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  await delay(Math.max(1, Math.min(ms, maxTimerMs)), undefined, { signal });
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
/**
 * `AbortSignal.any(sources)`, plus `AbortSignal.timeout(timeoutMs)` when given, with the
 * same reasons; `done()` detaches it from the sources and clears its timer at once, where
 * those stay registered until a source aborts or the deadline passes.
 */
export function linkSignal(
  sources: readonly AbortSignal[],
  timeoutMs?: number,
): { signal: AbortSignal; done: () => void } {
  const link = new AbortController();
  const aborted = sources.find((source) => source.aborted);
  if (aborted) {
    link.abort(aborted.reason);
    return { signal: link.signal, done: () => {} };
  }
  const follow = (event: Event) =>
    link.abort((event.target as AbortSignal).reason);
  for (const source of sources)
    source.addEventListener("abort", follow, { once: true });
  let timer: NodeJS.Timeout | undefined;
  // Waits past setTimeout's range re-arm in chunks instead of overflowing to 1 ms.
  const arm = (ms: number) => {
    timer = setTimeout(
      () =>
        ms > maxTimerMs
          ? arm(ms - maxTimerMs)
          : link.abort(
              new DOMException(
                "The operation was aborted due to timeout",
                "TimeoutError",
              ),
            ),
      Math.min(ms, maxTimerMs),
    ).unref();
  };
  if (timeoutMs !== undefined) arm(timeoutMs);
  return {
    signal: link.signal,
    done: () => {
      clearTimeout(timer);
      for (const source of sources) source.removeEventListener("abort", follow);
    },
  };
}
