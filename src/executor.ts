import { createRequire } from "node:module";
import { hostname as osHostname } from "node:os";
import { maxTimerMs, pause } from "./async.js";

/** Who an executor (a started worker) is. */
export interface ExecutorInfo {
  /** Fixed for the worker's lifetime; RunnerQ Cloud's executor id. */
  id: string;
  queue: string;
  activityTypes: string[];
  maxConcurrency: number;
  /** When `start()` was called; absent before. */
  startedAt?: Date;
  hostname: string;
  sdk: ExecutorSdk;
  labels: Record<string, string>;
}
export interface ExecutorSdk {
  name: string;
  version: string;
  language: string;
}
export interface RunningActivity {
  id: string;
  type: string;
  attempt: number;
  startedAt: Date;
}
export interface ExecutorState {
  running: RunningActivity[];
  /** A stop has begun: intake has stopped and running activities finish. */
  draining: boolean;
}
/** What the executor has done since it was built. */
export interface ExecutorCounters {
  /** Activities that started executing here. */
  claimed: number;
  succeeded: number;
  /** Asked for another attempt (including ones that then ran out). */
  retried: number;
  /** Failed without retrying. */
  failed: number;
  timedOut: number;
  /** Ran out of attempts. */
  deadLettered: number;
  claimsLost: number;
  /** Lease renewals that failed and will be retried. */
  heartbeatFailures: number;
  /** How long the latest activity waited, from when it was due, to start. */
  lastClaimLagMs: number;
}
export interface ExecutorSnapshot {
  info: ExecutorInfo;
  state: ExecutorState;
  counters: ExecutorCounters;
  at: Date;
}
/** Anything that can describe an executor, such as a Worker. */
export interface ExecutorSource {
  snapshot(): ExecutorSnapshot;
  /** Resolves at the next change: an activity starting or finishing, or a drain beginning. */
  changed?(): Promise<void>;
}
/**
 * Hears a worker start and stop and reads its snapshots on its own schedule: every observer
 * given to `observe()`, and the storage when it is one (RunnerQ Cloud's hosted adapter).
 * Both calls must return promptly; the worker awaits `executorStopped`'s promise (for a final
 * report) up to the stop's grace period, at least a second, then reports a `workerError`.
 */
export interface ExecutorObserver {
  executorStarted(source: ExecutorSource): void;
  executorStopped(id: string): void | Promise<void>;
}
export function isExecutorObserver(value: unknown): value is ExecutorObserver {
  return (
    typeof (value as ExecutorObserver | null)?.executorStarted === "function" &&
    typeof (value as ExecutorObserver | null)?.executorStopped === "function"
  );
}

/** Tells any number of waiters about changes. */
export class ChangeSignal {
  private next?: { promise: Promise<void>; resolve: () => void };
  /** A promise resolved by the next `notify()`. */
  changed(): Promise<void> {
    if (!this.next) {
      let resolve!: () => void;
      const promise = new Promise<void>((r) => (resolve = r));
      this.next = { promise, resolve };
    }
    return this.next.promise;
  }
  notify(): void {
    const next = this.next;
    this.next = undefined;
    next?.resolve();
  }
}

export interface ReportOptions {
  signal: AbortSignal;
  source: ExecutorSource;
  /** Read before each wait, so it may change. */
  intervalMs: () => number;
  /** The least time between reports that changes trigger. */
  minGapMs: number;
  send: () => void | Promise<void>;
}
/**
 * Calls `send` now, then every interval and soon after each change, but never sooner than
 * `minGapMs` after the previous send, until the signal aborts. Errors from `send` are the
 * caller's to handle; they don't stop the loop.
 */
export async function reportExecutor(options: ReportOptions): Promise<void> {
  const { signal, source } = options;
  // One reaction per change promise: racing it on every wait instead would pile reactions
  // onto an unchanged one for as long as the source stays idle.
  let watched: Promise<void> | undefined,
    changed = false,
    wake: (() => void) | undefined;
  while (!signal.aborted) {
    // Taken before the send, so a change during it isn't missed.
    const next = source.changed?.();
    if (next !== watched) {
      watched = next;
      changed = false;
      void next?.then(() => {
        if (watched !== next) return;
        changed = true;
        wake?.();
      });
    }
    try {
      await options.send();
    } catch {
      /* the next report retries */
    }
    const sent = Date.now();
    if (!changed && !signal.aborted)
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          signal.removeEventListener("abort", done);
          wake = undefined;
          resolve();
        };
        const timer = setTimeout(
          done,
          Math.min(options.intervalMs(), maxTimerMs),
        );
        wake = done;
        signal.addEventListener("abort", done, { once: true });
      });
    if (changed && !signal.aborted)
      await pause(sent + options.minGapMs - Date.now(), signal).catch(() => {});
  }
}

let sdk: ExecutorSdk | undefined;
export function thisSdk(): ExecutorSdk {
  if (!sdk) {
    let version = "unknown";
    try {
      version = createRequire(import.meta.url)("../package.json").version;
    } catch {
      /* no package.json next to the build */
    }
    sdk = { name: "runnerq-ts", version, language: "typescript" };
  }
  return { ...sdk };
}
export function thisHostname(): string {
  try {
    return osHostname();
  } catch {
    return "";
  }
}
