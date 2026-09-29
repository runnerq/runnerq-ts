import { randomUUID } from "node:crypto";
import { EventEmitter, captureRejectionSymbol } from "node:events";
import type { ActivityDefinition } from "./activity.js";
import { ActivityContext } from "./context.js";
import { encode, decode } from "./serialization.js";
import { message, retryable, RunnerQError, captureFailure } from "./errors.js";
import { integer } from "./options.js";
import { maxTimerMs, pause, recover } from "./async.js";
import { executionScope, type AttemptScope } from "./scope.js";
import type { Storage, Claim, Fence, Retention } from "./storage.js";
import {
  ChangeSignal,
  isExecutorObserver,
  thisHostname,
  thisSdk,
  type ExecutorCounters,
  type ExecutorObserver,
  type ExecutorSnapshot,
  type ExecutorSource,
  type RunningActivity,
} from "./executor.js";

export interface StopSummary {
  drained: boolean;
  remaining: number;
}
export interface ExecutionEvent {
  activityId: string;
  activityType: string;
  retryCount: number;
}
export interface WorkerEvents {
  started: [];
  stopped: [summary: StopSummary];
  workerError: [error: Error];
  listenerError: [error: Error];
  activityStarted: [event: ExecutionEvent];
  activityCompleted: [event: ExecutionEvent];
  activityRetrying: [event: ExecutionEvent];
  activityFailed: [event: ExecutionEvent];
  activityDeadLetter: [event: ExecutionEvent];
  activityYielded: [event: ExecutionEvent];
  claimLost: [event: ExecutionEvent];
}
export interface Metrics {
  increment(name: string, value: number): void;
  duration(name: string, milliseconds: number): void;
}
export interface WorkerConfig {
  storage: Storage;
  concurrency?: number;
  activityTypes?: readonly string[];
  leaseMs?: number;
  heartbeatMs?: number;
  reaperIntervalMs?: number;
  reaperBatchSize?: number;
  waitGraceMs?: number;
  shutdownGraceMs?: number;
  maxActivityDepth?: number;
  retention?: Retention;
  metrics?: Metrics;
  /** Free-form tags (region, deploy version) that RunnerQ Cloud shows in Fleet. */
  labels?: Readonly<Record<string, string>>;
}
export type ActivityHandler<I, O> = (
  context: ActivityContext,
  input: I,
) => O | Promise<O>;
interface Registration {
  input?: (value: unknown) => unknown;
  output?: (value: unknown) => unknown;
  handler: ActivityHandler<unknown, unknown>;
}

export class Worker
  extends EventEmitter<WorkerEvents>
  implements ExecutorSource
{
  /** Random, fixed at construction; RunnerQ Cloud's executor id. */
  readonly id = randomUUID();
  private readonly config: Required<
    Pick<
      WorkerConfig,
      | "concurrency"
      | "leaseMs"
      | "heartbeatMs"
      | "reaperIntervalMs"
      | "reaperBatchSize"
      | "waitGraceMs"
      | "shutdownGraceMs"
      | "maxActivityDepth"
    >
  > &
    WorkerConfig;
  private readonly handlers = new Map<string, Registration>();
  private readonly inFlight = new Set<Promise<void>>();
  private readonly slotFreed = new ChangeSignal();
  /** Running attempts' controllers, aborted when the shutdown budget runs out. */
  private readonly attempts = new Set<AbortController>();
  private intake = new AbortController();
  private lifetime = new AbortController();
  private maintenance: Promise<void>[] = [];
  private dispatch?: Promise<void>;
  private state: "idle" | "starting" | "running" | "stopping" | "stopped" =
    "idle";
  private startPromise?: Promise<void>;
  private stopPromise?: Promise<StopSummary>;
  private resolveClosed!: () => void;
  readonly closed: Promise<void>;
  private readonly running = new Map<string, RunningActivity>();
  private readonly counters: ExecutorCounters = {
    claimed: 0,
    succeeded: 0,
    retried: 0,
    failed: 0,
    timedOut: 0,
    deadLettered: 0,
    claimsLost: 0,
    heartbeatFailures: 0,
    lastClaimLagMs: 0,
  };
  private readonly changes = new ChangeSignal();
  private readonly observers: ExecutorObserver[] = [];
  private startedAt?: Date;
  private servedTypes?: string[];
  private readonly hostname = thisHostname();
  constructor(config: WorkerConfig) {
    super({ captureRejections: true });
    this.config = {
      ...config,
      concurrency: integer(config.concurrency ?? 10, "concurrency", 1),
      leaseMs: integer(config.leaseMs ?? 60_000, "leaseMs", 100),
      heartbeatMs: integer(config.heartbeatMs ?? 10_000, "heartbeatMs", 10),
      reaperIntervalMs: integer(
        config.reaperIntervalMs ?? 5_000,
        "reaperIntervalMs",
        10,
      ),
      reaperBatchSize: integer(
        config.reaperBatchSize ?? 100,
        "reaperBatchSize",
        1,
      ),
      waitGraceMs: integer(
        config.waitGraceMs ?? 2_000,
        "waitGraceMs",
        0,
        60_000,
      ),
      shutdownGraceMs: integer(
        config.shutdownGraceMs ?? 30_000,
        "shutdownGraceMs",
        0,
      ),
      maxActivityDepth: integer(
        config.maxActivityDepth ?? 32,
        "maxActivityDepth",
        1,
        32767,
      ),
      activityTypes: config.activityTypes
        ? [...config.activityTypes]
        : undefined,
      retention: config.retention ? { ...config.retention } : undefined,
      labels: { ...config.labels },
    };
    if (this.config.heartbeatMs >= this.config.leaseMs)
      throw new RunnerQError(
        "configuration",
        "heartbeatMs must be less than leaseMs",
      );
    if (config.retention) {
      integer(config.retention.completedMs ?? 0, "completedMs");
      integer(config.retention.failedMs ?? 0, "failedMs");
      integer(config.retention.intervalMs ?? 600_000, "retention interval", 1);
      integer(config.retention.batchSize ?? 100, "retention batch", 1);
    }
    this.closed = new Promise((resolve) => {
      this.resolveClosed = resolve;
    });
  }
  register<I, O>(
    definition: ActivityDefinition<I, O>,
    handler: ActivityHandler<I, O>,
  ): this {
    if (this.state !== "idle")
      throw new RunnerQError(
        "configuration",
        "Register handlers before starting the worker",
      );
    if (this.handlers.has(definition.name))
      throw new RunnerQError(
        "configuration",
        `Duplicate handler: ${definition.name}`,
      );
    this.handlers.set(definition.name, {
      input: definition.input,
      output: definition.output,
      handler: handler as ActivityHandler<unknown, unknown>,
    });
    return this;
  }
  get storage(): Storage {
    return this.config.storage;
  }
  snapshot(): ExecutorSnapshot {
    return {
      info: {
        id: this.id,
        queue: this.config.storage.queue,
        activityTypes: this.servedTypes
          ? [...this.servedTypes]
          : [...this.handlers.keys()].sort(),
        maxConcurrency: this.config.concurrency,
        startedAt: this.startedAt,
        hostname: this.hostname,
        sdk: thisSdk(),
        labels: { ...this.config.labels },
      },
      state: {
        running: Array.from(this.running.values(), (a) => ({ ...a })).sort(
          (a, b) => a.startedAt.getTime() - b.startedAt.getTime(),
        ),
        draining: this.state === "stopping" || this.state === "stopped",
      },
      counters: { ...this.counters },
      at: new Date(),
    };
  }
  changed(): Promise<void> {
    return this.changes.changed();
  }
  /** Tells `observer` of start and stop (call before `start()`); observer storages join unasked. */
  observe(observer: ExecutorObserver): this {
    if (this.state !== "idle")
      throw new RunnerQError(
        "configuration",
        "Add observers before starting the worker",
      );
    this.observers.push(observer);
    return this;
  }
  start(): Promise<void> {
    if (this.state !== "idle")
      return Promise.reject(
        new RunnerQError(
          "configuration",
          "Worker is already started or stopped; create another worker to restart",
        ),
      );
    this.state = "starting";
    return (this.startPromise = this.startInternal());
  }
  private async startInternal(): Promise<void> {
    try {
      const types = this.config.activityTypes ?? [...this.handlers.keys()];
      if (!types.length || types.some((t) => !this.handlers.has(t)))
        throw new RunnerQError(
          "configuration",
          "At least one registered handler is required and all filtered types must be registered",
        );
      this.state = "running";
      this.startedAt = new Date();
      this.servedTypes = [...types].sort();
      if (isExecutorObserver(this.config.storage))
        this.observers.push(this.config.storage);
      this.maintenance = [
        this.loop(this.config.reaperIntervalMs, () =>
          this.config.storage.reap(this.config.reaperBatchSize),
        ),
      ];
      if (this.config.retention)
        this.maintenance.push(
          this.loop(this.config.retention.intervalMs ?? 600_000, () =>
            this.config.storage.cleanup(this.config.retention!),
          ),
        );
      this.dispatch = this.dispatcher(types);
      for (const observer of this.observers)
        try {
          observer.executorStarted(this);
        } catch (error) {
          this.report(error);
        }
      this.publish("started");
    } catch (error) {
      this.state = "stopped";
      this.resolveClosed();
      throw error;
    }
  }
  private async loop(ms: number, fn: () => Promise<unknown>): Promise<void> {
    while (!this.intake.signal.aborted) {
      try {
        await pause(ms, this.intake.signal);
        await fn();
      } catch (error) {
        if (!this.intake.signal.aborted) this.report(error);
      }
    }
  }
  private async dispatcher(types: readonly string[]): Promise<void> {
    while (!this.intake.signal.aborted) {
      try {
        // Not Promise.race(inFlight): each race would leave a reaction on every
        // long-running activity's promise until it settles.
        if (this.inFlight.size >= this.config.concurrency) {
          await this.slotFreed.changed();
          continue;
        }
        const claims = await this.config.storage.claim(
          this.config.concurrency - this.inFlight.size,
          types,
          this.config.leaseMs,
          this.id,
        );
        // A claim already committed during shutdown still executes under the drain budget.
        for (const claim of claims) {
          const promise: Promise<void> = this.process(claim)
            .catch((error) => this.report(error))
            .finally(() => {
              this.inFlight.delete(promise);
              this.slotFreed.notify();
            });
          this.inFlight.add(promise);
        }
        if (!claims.length)
          await this.config.storage.waitForWork(this.intake.signal);
      } catch (error) {
        if (!this.intake.signal.aborted) {
          this.report(error);
          await pause(1_000, this.intake.signal).catch(() => {});
        }
      }
    }
  }
  private async process(claim: Claim): Promise<void> {
    const config = this.config,
      storage = config.storage,
      fence: Fence = { ownerId: claim.id, token: claim.token };
    // Aborted by the timeout, a lost claim, or stop() when its budget runs out; any abort
    // also stops the attempt's two timers.
    const attempt = new AbortController(),
      signal = attempt.signal;
    let beat: NodeJS.Timeout | undefined,
      expiry: NodeJS.Timeout | undefined,
      renewing: Promise<void> | undefined,
      beating = true,
      timedOut = false;
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(beat);
        clearTimeout(expiry);
      },
      { once: true },
    );
    this.attempts.add(attempt);
    if (this.lifetime.signal.aborted)
      attempt.abort(this.lifetime.signal.reason);
    const deadline = Date.now() + claim.timeoutMs;
    // Timeouts past setTimeout's range re-arm until the deadline.
    const expire = () => {
      const remaining = deadline - Date.now();
      if (remaining > 0)
        expiry = setTimeout(expire, Math.min(remaining, maxTimerMs));
      else {
        timedOut = true;
        attempt.abort(
          new RunnerQError("timeout", "Activity execution timed out"),
        );
      }
    };
    const beatMs = Math.min(config.heartbeatMs, maxTimerMs);
    const heartbeat = () => {
      renewing = storage.renew(fence, config.leaseMs).then(
        (owned) => {
          if (!owned) attempt.abort(reclaimed());
          else if (beating && !signal.aborted)
            beat = setTimeout(heartbeat, beatMs);
        },
        (error) => {
          if (!beating || signal.aborted) return;
          this.counters.heartbeatFailures++;
          this.report(error);
          beat = setTimeout(heartbeat, beatMs);
        },
      );
    };
    if (!signal.aborted) {
      beat = setTimeout(heartbeat, beatMs);
      expire();
    }
    const event: ExecutionEvent = {
      activityId: claim.id,
      activityType: claim.type,
      retryCount: claim.retryCount,
    };
    const metrics = config.metrics;
    const scope: AttemptScope = {
      storage,
      claim,
      fence,
      signal,
      persistence: this.lifetime.signal,
      deadline,
      maxDepth: config.maxActivityDepth,
      waitGraceMs: config.waitGraceMs,
      closed: false,
      pending: new Set(),
      names: new Set(),
      waitActive: false,
      activeEffects: 0,
      wait: (id) => context.waitStored(id),
      recover: <T>(
        fn: () => Promise<T>,
        persistence = false,
        within?: AbortSignal,
      ) =>
        recover(
          fn,
          within ?? (persistence ? this.lifetime.signal : signal),
          async () => {
            this.metric(() => metrics?.increment("storage_retry", 1));
            // An uncertain write may have landed: only a failed renew means ownership is lost.
            try {
              const owned = await storage.renew(fence, config.leaseMs);
              if (!owned && !persistence) attempt.abort(reclaimed());
            } catch {
              /* next storage retry classifies the outcome */
            }
          },
        ),
    };
    const context = new ActivityContext(scope);
    const startedAt = new Date();
    this.running.set(claim.id, {
      id: claim.id,
      type: claim.type,
      attempt: claim.retryCount + 1,
      startedAt,
    });
    this.counters.claimed++;
    if (claim.dueAt)
      this.counters.lastClaimLagMs = Math.max(
        0,
        startedAt.getTime() - new Date(claim.dueAt).getTime(),
      );
    this.changes.notify();
    this.publish("activityStarted", event);
    const started = performance.now();
    let output: unknown,
      error: unknown,
      failed = false;
    try {
      signal.throwIfAborted();
      const registration = this.handlers.get(claim.type)!;
      let input: unknown;
      try {
        const payload = decode({
          data: claim.payload,
          serialization: claim.serialization,
        });
        input = registration.input ? registration.input(payload) : payload;
      } catch (cause) {
        throw new RunnerQError("serialization", "Invalid activity input", {
          cause,
        });
      }
      output = await executionScope.run(scope, () =>
        registration.handler(context, input),
      );
      if (registration.output) {
        try {
          output = registration.output(output);
        } catch (cause) {
          throw new RunnerQError("serialization", "Invalid activity output", {
            cause,
          });
        }
      }
    } catch (cause) {
      error = cause;
      failed = true;
    }
    // Settle SDK calls the handler started but didn't await before giving up ownership.
    const outstanding = await Promise.allSettled([...scope.pending]);
    if (!failed) {
      const rejection = outstanding.find((x) => x.status === "rejected");
      if (rejection?.status === "rejected") {
        error = rejection.reason;
        failed = true;
      }
    }
    scope.closed = true;
    beating = false;
    clearTimeout(beat);
    clearTimeout(expiry);
    await renewing;
    try {
      const reason = signal.reason;
      if (reason instanceof RunnerQError && reason.code === "claim_lost") {
        this.counters.claimsLost++;
        this.publish("claimLost", event);
        return;
      }
      if (this.lifetime.signal.aborted) return;
      if (scope.violation) {
        error = scope.violation;
        failed = true;
      } else if (scope.suspension) {
        await scope.recover(() => storage.park(fence, scope.suspension!), true);
        this.publish("activityYielded", event);
        return;
      }
      // Checkpoint recovery may outlive the deadline; a captured result still commits under
      // the fence, as in Go.
      if (!failed) {
        let data;
        try {
          data = encode(output, claim.serialization);
        } catch (cause) {
          error = cause;
          failed = true;
        }
        if (!failed) {
          await scope.recover(() => storage.complete(fence, data!), true);
          this.counters.succeeded++;
          this.publish("activityCompleted", event);
          return;
        }
      }
      if (error instanceof RunnerQError && error.code === "claim_lost") {
        this.counters.claimsLost++;
        this.publish("claimLost", event);
        return;
      }
      const failure = captureFailure(error);
      const status = await scope.recover(
        () => storage.fail(fence, failure.message, retryable(error), failure),
        true,
      );
      // As Go counts them: a timeout is a timeout, not also a retry.
      if (timedOut) this.counters.timedOut++;
      else if (status === "failed") this.counters.failed++;
      else this.counters.retried++;
      if (status === "dead_letter") this.counters.deadLettered++;
      this.publish(
        status === "retrying"
          ? "activityRetrying"
          : status === "failed"
            ? "activityFailed"
            : "activityDeadLetter",
        event,
      );
    } finally {
      this.attempts.delete(attempt);
      this.running.delete(claim.id);
      this.changes.notify();
      this.metric(() =>
        metrics?.duration("activity_execution", performance.now() - started),
      );
    }
  }
  stop(options: { graceMs?: number } = {}): Promise<StopSummary> {
    return (this.stopPromise ??= this.stopInternal(
      integer(options.graceMs ?? this.config.shutdownGraceMs, "graceMs"),
    ));
  }
  private async stopInternal(graceMs: number): Promise<StopSummary> {
    if (this.state === "starting") await this.startPromise?.catch(() => {});
    this.state = "stopping";
    this.intake.abort();
    this.changes.notify();
    const drain = (async () => {
      await this.dispatch;
      await Promise.allSettled([...this.inFlight, ...this.maintenance]);
    })();
    let timer: NodeJS.Timeout | undefined;
    const drained = await Promise.race([
      drain.then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), graceMs);
      }),
    ]);
    clearTimeout(timer);
    const expired = new RunnerQError(
      "timeout",
      "Worker shutdown budget expired",
    );
    this.lifetime.abort(expired);
    for (const attempt of this.attempts) attempt.abort(expired);
    this.state = "stopped";
    // Observers may send a goodbye: wait up to the grace period, at least a second so a
    // zero-grace stop can still say goodbye.
    const observerBudget = Math.max(graceMs, 1_000);
    await Promise.all(
      this.observers.map(async (observer) => {
        let timer: NodeJS.Timeout | undefined;
        try {
          await Promise.race([
            observer.executorStopped(this.id),
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () =>
                  reject(
                    new RunnerQError(
                      "timeout",
                      `An executor observer did not finish stopping within ${observerBudget}ms`,
                    ),
                  ),
                observerBudget,
              );
            }),
          ]);
        } catch (error) {
          this.report(error);
        } finally {
          clearTimeout(timer);
        }
      }),
    );
    const summary = { drained, remaining: this.inFlight.size };
    this.publish("stopped", summary);
    this.resolveClosed();
    return summary;
  }
  private report(error: unknown): void {
    this.publish("workerError", asError(error));
  }
  private metric(fn: () => void): void {
    try {
      fn();
    } catch (error) {
      this.report(error);
    }
  }
  private publish<K extends keyof WorkerEvents>(
    name: K,
    ...args: WorkerEvents[K]
  ): void {
    if (name.startsWith("activity") || name === "claimLost")
      this.metric(() => this.config.metrics?.increment(name, 1));
    // A copy of the raw listeners keeps once() wrappers; each is isolated so a throw cannot
    // skip later listeners or change a committed outcome.
    for (const listener of this.rawListeners(name)) {
      try {
        const result = (
          listener as (...args: WorkerEvents[K]) => unknown
        ).apply(this, args);
        if (
          result &&
          typeof (result as PromiseLike<unknown>).then === "function"
        )
          void Promise.resolve(result).catch((error) =>
            this.observerError(error, name),
          );
      } catch (error) {
        this.observerError(error, name);
      }
    }
  }
  private observerError(error: unknown, name: keyof WorkerEvents): void {
    if (name !== "listenerError") this.publish("listenerError", asError(error));
  }
  override [captureRejectionSymbol](error: Error, ..._args: unknown[]): void {
    this.observerError(error, "workerError");
  }
}
const asError = (error: unknown): Error =>
  error instanceof Error ? error : new Error(message(error));
const reclaimed = () =>
  new RunnerQError("claim_lost", "Execution was reclaimed");
