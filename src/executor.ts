import { createRequire } from "node:module";
import { hostname as osHostname } from "node:os";
import { pause } from "./async.js";

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
/** An activity the executor is running. */
export interface RunningActivity {
  id: string;
  type: string;
  attempt: number;
  startedAt: Date;
}
/** What the executor is doing now. */
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
/** Anything that can describe an executor: the Worker is one. */
export interface ExecutorSource {
  snapshot(): ExecutorSnapshot;
  /** Resolves at the next change: an activity starting or finishing, or a drain beginning. */
  changed?(): Promise<void>;
}
/**
 * Hears a worker start and stop, and reads its snapshots from the source on its own
 * schedule. A worker calls every observer given to `observe()`, and its storage when the
 * storage is an observer (RunnerQ Cloud's storage adapter reports hosted workers this way).
 * Both calls must return promptly; `executorStopped` may return a promise the worker awaits,
 * so a final report can be sent, for up to the stop's grace period (at least a second).
 * Past that, or if it fails, the worker reports a `workerError` and finishes stopping.
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
 * Calls `send` now, then every interval until the signal aborts, and soon after each change
 * when the source signals changes, but never sooner than `minGapMs` after the previous
 * send: changes in the meantime go out together in the next report. Errors from `send` are
 * the caller's to handle; they don't stop the loop.
 */
export async function reportExecutor(options: ReportOptions): Promise<void> {
  const { signal, source } = options;
  while (!signal.aborted) {
    // Taken before the send, so a change during it isn't missed.
    const changed = source.changed?.();
    try {
      await options.send();
    } catch {
      /* the next report retries */
    }
    const sent = Date.now();
    // Each wait cancels its own timer, so a change doesn't leave one behind.
    const wait = new AbortController();
    const waiting = AbortSignal.any([signal, wait.signal]);
    let byChange = false;
    await Promise.race([
      pause(options.intervalMs(), waiting).catch(() => {}),
      changed?.then(() => {
        byChange = true;
      }) ?? new Promise<never>(() => {}),
    ]);
    wait.abort();
    if (byChange && !signal.aborted)
      await pause(sent + options.minGapMs - Date.now(), signal).catch(() => {});
  }
}

let sdk: ExecutorSdk | undefined;
/** Names the runnerq SDK in this process. */
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
