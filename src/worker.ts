import { randomUUID } from "node:crypto";
import { EventEmitter, captureRejectionSymbol } from "node:events";
import type { ActivityDefinition } from "./activity.js";
import { ActivityContext } from "./context.js";
import { encode, decode } from "./serialization.js";
import { message, retryable, RunnerQError, captureFailure } from "./errors.js";
import { integer } from "./options.js";
import { maxTimerMs, pause, recover } from "./async.js";
import { executionScope, type AttemptScope } from "./scope.js";
import {
  isResultNotifier,
  type Storage,
  type Claim,
  type Fence,
  type Retention,
} from "./storage.js";
import {
  isScheduleStorage,
  scheduleDefinition,
  type Schedule,
  type ScheduleDefinition,
} from "./schedule.js";
import {
  ChangeSignal,
  thisHostname,
  thisSdk,
  type ExecutorCounters,
  type ExecutorObserver,
  type ExecutorSnapshot,
  type ExecutorSource,
  type RunningActivity,
} from "./executor.js";
import {
  isLabeledMetrics,
  jsonLogger,
  startSpan,
  unlabeledMetrics,
  type LabeledMetrics,
  type Logger,
  type Metrics,
  type MetricLabels,
  type Tracer,
  type TraceSpan,
} from "./observability.js";
import {
  metricNames as m,
  spanAttributes as attr,
  spanNames,
  spanOutcomes as outcomes,
  traceparentKey,
} from "./observability-spec.js";
import { isQueryStorage } from "./query.js";
import { isLimitStorage, validateLimit, type Limit } from "./limit.js";

/** Internal: the agent's handle on `Worker`'s interrupt (not exported from the package). */
export const interruptActivity = Symbol("runnerq.interruptActivity");
/** Internal: the agent's handle on `Worker`'s lifecycle announcements. */
export const announceTo = Symbol("runnerq.announceTo");

/** Internal: a lifecycle change no event is stored for, as this worker made it. */
export interface Change {
  type:
    | "activity.created"
    | "activity.scheduled"
    | "attempt.started"
    | "attempt.succeeded";
  activityId: string;
  activityType: string;
  rootId: string;
  attempt?: number;
  at: Date;
}

/** How often a worker fires its queue's due schedules. */
const scheduleIntervalMs = 1_000;

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
export interface WorkerConfig {
  storage: Storage;
  concurrency?: number;
  /**
   * Claims only these registered activities, by definition or by persisted name.
   * Defaults to every registered activity.
   */
  activityTypes?: readonly (
    string | Pick<ActivityDefinition<unknown, unknown>, "name">
  )[];
  leaseMs?: number;
  heartbeatMs?: number;
  reaperIntervalMs?: number;
  reaperBatchSize?: number;
  waitGraceMs?: number;
  shutdownGraceMs?: number;
  maxActivityDepth?: number;
  retention?: Retention;
  /**
   * Receives every metric of `metricCatalog`. A `Metrics` (names only) is adapted with
   * `unlabeledMetrics`. `runnerq/otel` provides one for OpenTelemetry.
   */
  metrics?: Metrics | LabeledMetrics;
  /** Traces this worker's attempts and the activities its handlers enqueue (see `Tracer`). */
  tracer?: Tracer;
  /**
   * Where the worker logs. Defaults to `jsonLogger()`: JSON lines on stderr at info and above,
   * with the messages and fields the Go SDK logs through slog.
   */
  logger?: Logger;
  /**
   * Samples the gauges (queue depth by status, oldest pending age, in-flight count) about this
   * often, jittered by up to a fifth. Queue depth and age need a storage that answers queries.
   * 0, the default, leaves them off.
   */
  gaugeIntervalMs?: number;
  /** Free-form tags (region, deploy version), carried in the worker's snapshots. */
  labels?: Readonly<Record<string, string>>;
}
/**
 * Limits on a registered activity type. `maxConcurrent` and the rate cap the type across every
 * worker of the queue: `start()` stores them, replacing the type's stored limit, and the
 * client changes them at runtime (`RunnerQClient.limits`). `maxConcurrentPerWorker` caps it on
 * this worker only and is not stored.
 */
export interface RegisterOptions {
  /** At most this many running at once in the queue; waiting and retrying ones don't count. */
  maxConcurrent?: number;
  /** At most this many claims per `ratePeriodMs` across the queue, in bursts of up to it. */
  rateLimit?: number;
  ratePeriodMs?: number;
  /** At most this many running at once on this worker, within `concurrency`. */
  maxConcurrentPerWorker?: number;
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
  /** Random, fixed at construction: the executor id in its snapshots. */
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
    Omit<WorkerConfig, "activityTypes"> & {
      activityTypes?: readonly string[];
    };
  private readonly handlers = new Map<string, Registration>();
  /** Limits to store at start, by type. */
  private readonly limits = new Map<string, Limit>();
  /** maxConcurrentPerWorker by type, and how many of each type run here. */
  private readonly caps = new Map<string, number>();
  private readonly capped = new Map<string, number>();
  private readonly schedules = new Map<string, ScheduleDefinition>();
  private readonly inFlight = new Set<Promise<void>>();
  private readonly slotFreed = new ChangeSignal();
  /** Running attempts' controllers by activity id, aborted when the shutdown budget runs out. */
  private readonly attempts = new Map<string, AbortController>();
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
  private announcer?: (change: Change) => void;
  private readonly metrics?: LabeledMetrics;
  private readonly log: Logger;
  private readonly gaugeIntervalMs: number;
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
      activityTypes: config.activityTypes?.map((type) =>
        typeof type === "string" ? type : type.name,
      ),
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
      integer(config.retention.eventsMs ?? 0, "eventsMs");
      integer(config.retention.intervalMs ?? 600_000, "retention interval", 1);
      integer(config.retention.batchSize ?? 100, "retention batch", 1);
    }
    this.closed = new Promise((resolve) => {
      this.resolveClosed = resolve;
    });
    this.metrics = config.metrics
      ? isLabeledMetrics(config.metrics)
        ? config.metrics
        : unlabeledMetrics(config.metrics)
      : undefined;
    this.log = config.logger ?? jsonLogger();
    this.gaugeIntervalMs = integer(
      config.gaugeIntervalMs ?? 0,
      "gaugeIntervalMs",
      0,
      maxTimerMs,
    );
  }
  /**
   * Registers the handler of an activity, with optional limits:
   *
   * ```ts
   * worker.register(sendEmail, handler, { maxConcurrent: 10, rateLimit: 5, ratePeriodMs: 1000 });
   * ```
   */
  register<I, O>(
    definition: ActivityDefinition<I, O>,
    handler: ActivityHandler<I, O>,
    options: RegisterOptions = {},
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
    const { maxConcurrentPerWorker, ...limit } = options;
    if (maxConcurrentPerWorker !== undefined)
      integer(maxConcurrentPerWorker, "maxConcurrentPerWorker", 1);
    if (
      limit.maxConcurrent !== undefined ||
      limit.rateLimit !== undefined ||
      limit.ratePeriodMs !== undefined
    ) {
      const stored = { type: definition.name, ...limit };
      try {
        validateLimit(stored);
      } catch (error) {
        throw new RunnerQError("configuration", (error as Error).message);
      }
      if (!isLimitStorage(this.config.storage))
        throw new RunnerQError(
          "configuration",
          "This storage does not support limits",
        );
      this.limits.set(definition.name, stored);
    }
    if (maxConcurrentPerWorker !== undefined)
      this.caps.set(definition.name, maxConcurrentPerWorker);
    this.handlers.set(definition.name, {
      input: definition.input,
      output: definition.output,
      handler: handler as ActivityHandler<unknown, unknown>,
    });
    return this;
  }
  /**
   * Declares a cron schedule that `start()` upserts, so the schedules a deployment's code
   * declares are the ones stored. Every worker of the queue fires the queue's schedules,
   * each tick once.
   */
  registerSchedule<I, O>(schedule: Schedule<I, O>): this {
    if (this.state !== "idle")
      throw new RunnerQError(
        "configuration",
        "Register schedules before starting the worker",
      );
    if (!isScheduleStorage(this.config.storage))
      throw new RunnerQError(
        "configuration",
        "This storage does not support schedules",
      );
    const definition = scheduleDefinition(schedule);
    if (this.schedules.has(definition.name))
      throw new RunnerQError(
        "configuration",
        `Duplicate schedule: ${definition.name}`,
      );
    this.schedules.set(definition.name, definition);
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
  /** Tells `observer` of start and stop (call before `start()`). */
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
      const store = this.config.storage;
      if (isScheduleStorage(store))
        for (const definition of this.schedules.values())
          await store.upsertSchedule(definition);
      if (isLimitStorage(store))
        for (const limit of this.limits.values()) await store.setLimit(limit);
      this.state = "running";
      this.startedAt = new Date();
      this.servedTypes = [...types].sort();
      this.maintenance = [
        this.loop(this.config.reaperIntervalMs, () =>
          this.config.storage.reap(this.config.reaperBatchSize),
        ),
      ];
      if (isScheduleStorage(store))
        this.maintenance.push(
          this.loop(scheduleIntervalMs, async () => {
            const n = await store.fireSchedules(100);
            if (n) this.count("schedule_fires", { queue: this.queue }, n);
          }),
        );
      if (this.config.retention)
        this.maintenance.push(
          this.loop(this.config.retention.intervalMs ?? 600_000, async () => {
            const swept = await this.config.storage.cleanup(
              this.config.retention!,
            );
            if (swept > 0)
              this.count(m.activityTreesSwept, { queue: this.queue }, swept);
          }),
        );
      if (this.gaugeIntervalMs > 0)
        this.maintenance.push(this.sampleGauges(this.gaugeIntervalMs, types));
      this.log.info("Starting worker engine", {
        queue: this.queue,
        max_concurrent_activities: this.config.concurrency,
      });
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
        const { claimTypes, limit, narrowed } = this.planClaim(
          types,
          this.config.concurrency - this.inFlight.size,
        );
        if (!claimTypes.length) {
          // Every type is at its per-worker cap.
          await this.slotFreed.changed();
          continue;
        }
        const claims = await this.config.storage.claim(
          limit,
          claimTypes,
          this.config.leaseMs,
          this.id,
        );
        // A claim already committed during shutdown still executes under the drain budget.
        for (const claim of claims) {
          const cap = this.caps.has(claim.type);
          if (cap)
            this.capped.set(claim.type, (this.capped.get(claim.type) ?? 0) + 1);
          const promise: Promise<void> = this.process(claim)
            .catch((error) => this.report(error))
            .finally(() => {
              if (cap)
                this.capped.set(claim.type, this.capped.get(claim.type)! - 1);
              this.inFlight.delete(promise);
              this.slotFreed.notify();
            });
          this.inFlight.add(promise);
        }
        if (!claims.length) await this.waitForWork(narrowed);
      } catch (error) {
        if (!this.intake.signal.aborted) {
          this.report(error);
          await pause(1_000, this.intake.signal).catch(() => {});
        }
      }
    }
  }
  /**
   * Narrows a claim of up to `free` activities to what `maxConcurrentPerWorker` allows: types
   * at their cap are left out, and the claim asks for no more than a capped type has room for.
   */
  private planClaim(
    types: readonly string[],
    free: number,
  ): { claimTypes: string[]; limit: number; narrowed: boolean } {
    if (!this.caps.size)
      return { claimTypes: [...types], limit: free, narrowed: false };
    const claimTypes: string[] = [];
    let limit = free,
      narrowed = false;
    for (const t of types) {
      const cap = this.caps.get(t);
      if (cap === undefined) {
        claimTypes.push(t);
        continue;
      }
      const room = cap - (this.capped.get(t) ?? 0);
      if (room <= 0) {
        narrowed = true;
        continue;
      }
      if (room < limit) {
        limit = room;
        narrowed = true;
      }
      claimTypes.push(t);
    }
    return { claimTypes, limit, narrowed };
  }
  /** Waits for new work; also for a freed slot when a per-worker cap narrowed the claim. */
  private async waitForWork(narrowed: boolean): Promise<void> {
    if (!narrowed) return this.config.storage.waitForWork(this.intake.signal);
    const wait = new AbortController();
    const stop = () => wait.abort();
    this.intake.signal.addEventListener("abort", stop, { once: true });
    void this.slotFreed.changed().then(stop);
    try {
      await this.config.storage.waitForWork(wait.signal);
    } catch (error) {
      if (this.intake.signal.aborted) throw error;
    } finally {
      this.intake.signal.removeEventListener("abort", stop);
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
    // A reclaim of an activity still running here (a wedged attempt) supersedes it.
    this.attempts.get(claim.id)?.abort(reclaimed());
    this.attempts.set(claim.id, attempt);
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
    // A hint during a renewal beats again once it settles: the renewal may predate the cancel.
    let inFlight = false,
      hinted = false;
    const heartbeat = () => {
      inFlight = true;
      hinted = false;
      renewing = storage.renew(fence, config.leaseMs).then(
        (owned) => {
          inFlight = false;
          if (!owned) attempt.abort(reclaimed());
          else if (beating && !signal.aborted)
            beat = setTimeout(heartbeat, hinted ? 0 : beatMs);
        },
        (error) => {
          inFlight = false;
          if (!beating || signal.aborted) return;
          this.counters.heartbeatFailures++;
          this.count(m.activityHeartbeatFailed, labels);
          this.report(error);
          beat = setTimeout(heartbeat, beatMs);
        },
      );
    };
    // A cancel stores the activity's result: renew now to find out whether the claim is gone.
    const unwatch = isResultNotifier(storage)
      ? storage.notifyResult(claim.id, () => {
          if (!beating || signal.aborted) return;
          if (inFlight) hinted = true;
          else {
            clearTimeout(beat);
            heartbeat();
          }
        })
      : undefined;
    if (!signal.aborted) {
      beat = setTimeout(heartbeat, beatMs);
      expire();
    }
    const event: ExecutionEvent = {
      activityId: claim.id,
      activityType: claim.type,
      retryCount: claim.retryCount,
    };
    const labels: MetricLabels = {
      queue: storage.queue,
      activityType: claim.type,
    };
    const fields = { activity_id: claim.id, activity_type: claim.type };
    const stored = claim.metadata[traceparentKey];
    const span = startSpan(
      config.tracer,
      spanNames.attempt,
      stored || undefined,
      () => {
        const a: Record<string, string | number> = {
          [attr.queue]: storage.queue,
          [attr.activityType]: claim.type,
          [attr.activityId]: claim.id,
          [attr.rootId]: claim.rootId,
          [attr.attempt]: claim.retryCount + 1,
        };
        if (claim.parentId) a[attr.parentId] = claim.parentId;
        return a;
      },
    );
    let outcome: string = outcomes.error,
      outcomeError: unknown;
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
      announce: (change) => this.announcer?.(change),
      tracer: config.tracer,
      span,
      metrics: this.metrics,
      recover: <T>(
        fn: () => Promise<T>,
        persistence = false,
        within?: AbortSignal,
      ) =>
        recover(
          fn,
          within ?? (persistence ? this.lifetime.signal : signal),
          async () => {
            this.count(m.storageRetry, labels);
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
    this.count(m.activityStarted, labels);
    if (claim.dueAt) {
      this.counters.lastClaimLagMs = Math.max(
        0,
        startedAt.getTime() - new Date(claim.dueAt).getTime(),
      );
      this.histogram(
        m.activityClaimLag,
        labels,
        this.counters.lastClaimLagMs / 1000,
      );
    }
    this.changes.notify();
    this.publish("activityStarted", event);
    this.announcer?.({
      type: "attempt.started",
      activityId: claim.id,
      activityType: claim.type,
      rootId: claim.rootId,
      attempt: claim.retryCount + 1,
      at: startedAt,
    });
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
      const run = () =>
        executionScope.run(scope, () => registration.handler(context, input));
      output = await (span ? span.run(run) : run());
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
    unwatch?.();
    clearTimeout(beat);
    clearTimeout(expiry);
    await renewing;
    let caught: unknown;
    try {
      const reason = signal.reason;
      if (reason instanceof RunnerQError && reason.code === "claim_lost") {
        this.claimLost(event, labels, fields);
        outcome = outcomes.claimLost;
        return;
      }
      if (this.lifetime.signal.aborted) return;
      if (scope.violation) {
        error = scope.violation;
        failed = true;
      } else if (scope.suspension) {
        const park = scope.suspension;
        await scope.recover(() => storage.park(fence, park), true);
        this.count(m.activityYielded, labels);
        this.log.debug("Activity yielded for durable wait", {
          ...fields,
          step: park.step,
          wake_at: park.wakeAt,
        });
        this.publish("activityYielded", event);
        outcome = outcomes.yielded;
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
          const persisting = performance.now();
          this.count(m.activityCompletionPendingStarted, labels);
          try {
            await scope.recover(() => storage.complete(fence, data!), true);
          } catch (error) {
            if (error instanceof RunnerQError && error.code === "claim_lost")
              throw error;
            this.count(m.activityCompletionError, labels);
            this.log.error("Failed to confirm activity completion", {
              ...fields,
              error: message(error),
            });
            throw error;
          } finally {
            this.count(m.activityCompletionPendingFinished, labels);
            this.histogram(
              m.activityCompletionPersistence,
              labels,
              (performance.now() - persisting) / 1000,
            );
          }
          this.counters.succeeded++;
          this.count(m.activityCompleted, labels);
          this.log.info("Activity completed successfully", fields);
          outcome = outcomes.completed;
          this.publish("activityCompleted", event);
          this.announcer?.({
            type: "attempt.succeeded",
            activityId: claim.id,
            activityType: claim.type,
            rootId: claim.rootId,
            attempt: claim.retryCount + 1,
            at: new Date(),
          });
          return;
        }
      }
      if (error instanceof RunnerQError && error.code === "claim_lost") {
        this.claimLost(event, labels, fields);
        outcome = outcomes.claimLost;
        return;
      }
      const failure = captureFailure(error);
      const status = await scope.recover(
        () => storage.fail(fence, failure.message, retryable(error), failure),
        true,
      );
      // As Go counts them: a timeout is a timeout, not also a retry.
      if (timedOut) {
        this.counters.timedOut++;
        this.count(m.activityTimeout, labels);
        this.log.error("Activity timed out", {
          ...fields,
          timeout: `${claim.timeoutMs / 1000}s`,
        });
        outcome = outcomes.timeout;
      } else if (status === "failed") {
        this.counters.failed++;
        this.count(m.activityFailedNonRetry, labels);
        this.log.error("Activity failed", {
          ...fields,
          reason: failure.message,
        });
        outcome = outcomes.failed;
      } else {
        this.counters.retried++;
        this.count(m.activityRetry, labels);
        this.log.warn("Activity requesting retry", {
          ...fields,
          reason: failure.message,
        });
        outcome = outcomes.retry;
      }
      if (status === "dead_letter") {
        this.counters.deadLettered++;
        this.count(m.activityDeadLettered, labels);
        outcome = outcomes.deadLetter;
      }
      outcomeError = error;
      this.publish(
        status === "retrying"
          ? "activityRetrying"
          : status === "failed"
            ? "activityFailed"
            : "activityDeadLetter",
        event,
      );
    } catch (error) {
      caught = error;
      throw error;
    } finally {
      if (this.attempts.get(claim.id) === attempt) {
        this.attempts.delete(claim.id);
        this.running.delete(claim.id);
      }
      this.changes.notify();
      this.histogram(
        m.activityExecution,
        labels,
        (performance.now() - started) / 1000,
      );
      span?.end(outcome, outcome === outcomes.error ? caught : outcomeError);
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
    for (const attempt of this.attempts.values()) attempt.abort(expired);
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
    this.log.info("Worker engine stopped", { queue: this.queue });
    this.publish("stopped", summary);
    this.resolveClosed();
    return summary;
  }
  /**
   * Sends every lifecycle change this worker makes from now on to `fn`: claims, successes and
   * submissions from its handlers. undefined stops. For this worker's own agent.
   */
  [announceTo](fn?: (change: Change) => void): void {
    this.announcer = fn;
  }
  /**
   * Stops the attempt running `activityId` here as if its claim were lost, and reports
   * whether one was running. For a cancel from this worker's own agent: the heartbeat would
   * notice within an interval; this makes it immediate.
   */
  [interruptActivity](activityId: string): boolean {
    const attempt = this.attempts.get(activityId);
    if (!attempt || attempt.signal.aborted) return false;
    attempt.abort(
      new RunnerQError("claim_lost", `Activity ${activityId} was cancelled`),
    );
    return true;
  }
  private report(error: unknown): void {
    this.log.error("Worker error", { error: message(error) });
    this.publish("workerError", asError(error));
  }
  private get queue(): string {
    return this.config.storage.queue;
  }
  private count(name: string, labels: MetricLabels, value = 1): void {
    if (!this.metrics) return;
    try {
      this.metrics.addCounter(name, value, labels);
    } catch (error) {
      this.report(error);
    }
  }
  private histogram(name: string, labels: MetricLabels, value: number): void {
    if (!this.metrics) return;
    try {
      this.metrics.recordHistogram(name, value, labels);
    } catch (error) {
      this.report(error);
    }
  }
  private gauge(name: string, labels: MetricLabels, value: number): void {
    if (!this.metrics) return;
    try {
      this.metrics.setGauge(name, value, labels);
    } catch (error) {
      this.report(error);
    }
  }
  private claimLost(
    event: ExecutionEvent,
    labels: MetricLabels,
    fields: Record<string, string>,
  ): void {
    this.counters.claimsLost++;
    this.count(m.activityClaimLost, labels);
    this.log.warn("Activity execution lost its claim", fields);
    this.publish("claimLost", event);
  }
  /** Sets the gauges every `ms`, jittered by up to a fifth either way, until intake stops. */
  private async sampleGauges(
    ms: number,
    types: readonly string[],
  ): Promise<void> {
    const reported = new Map<string, MetricLabels>();
    while (!this.intake.signal.aborted) {
      try {
        await pause(
          Math.round(ms * (0.8 + 0.4 * Math.random())),
          this.intake.signal,
        );
        await this.sampleGaugesOnce(types, reported);
      } catch (error) {
        if (!this.intake.signal.aborted) this.report(error);
      }
    }
  }
  private async sampleGaugesOnce(
    types: readonly string[],
    reported: Map<string, MetricLabels>,
  ): Promise<void> {
    const queue = this.queue;
    const inFlight = new Map<string, number>();
    for (const a of this.running.values())
      inFlight.set(a.type, (inFlight.get(a.type) ?? 0) + 1);
    for (const type of types)
      this.gauge(
        m.activitiesInFlight,
        { queue, activityType: type },
        inFlight.get(type) ?? 0,
      );
    const storage = this.config.storage;
    if (!isQueryStorage(storage)) return;
    const rows = await storage.aggregateActivities({
      filter: {
        and: [
          { field: "queue", op: "eq", value: queue },
          { field: "status", op: "in", value: [...gaugeStatuses] },
        ],
      },
      groupBy: ["type", "status"],
      count: true,
      limit: 1000,
    });
    const seen = new Map<string, MetricLabels>();
    for (const row of rows.rows) {
      const labels = {
        queue,
        activityType: row.key?.type ?? "",
        status: row.key?.status ?? "",
      };
      const key = `${labels.activityType}\u0000${labels.status}`;
      seen.set(key, labels);
      this.gauge(m.queueDepth, labels, row.count);
    }
    for (const [key, labels] of reported)
      if (!seen.has(key)) {
        this.gauge(m.queueDepth, labels, 0);
        reported.delete(key);
      }
    for (const [key, labels] of seen) reported.set(key, labels);
    const page = await storage.queryActivities({
      filter: {
        and: [
          { field: "queue", op: "eq", value: queue },
          { field: "status", op: "eq", value: "pending" },
        ],
      },
      sort: { field: "created_at", desc: false },
      limit: 1,
    });
    const oldest = page.items[0];
    let age = 0;
    if (oldest) {
      const due = Math.max(
        oldest.createdAt.getTime(),
        oldest.scheduledFor?.getTime() ?? 0,
      );
      age = Math.max(0, (Date.now() - due) / 1000);
    }
    this.gauge(m.queueOldestPendingAge, { queue }, age);
  }
  private publish<K extends keyof WorkerEvents>(
    name: K,
    ...args: WorkerEvents[K]
  ): void {
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
/** The statuses `queue_depth` reports. */
const gaugeStatuses = [
  "pending",
  "scheduled",
  "running",
  "waiting",
  "dead_letter",
] as const;
const asError = (error: unknown): Error =>
  error instanceof Error ? error : new Error(message(error));
const reclaimed = () =>
  new RunnerQError("claim_lost", "Execution was reclaimed");
