import { randomUUID } from "node:crypto";
import { EventEmitter, captureRejectionSymbol } from "node:events";
import type { ActivityDefinition } from "./activity.js";
import { ActivityContext } from "./context.js";
import { encode, decode } from "./serialization.js";
import { message, retryable, RunnerQError, captureFailure } from "./errors.js";
import { integer } from "./options.js";
import { pause, recover } from "./async.js";
import { executionScope, type AttemptScope } from "./scope.js";
import type { Storage, Claim, Fence, Retention } from "./storage.js";

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

export class Worker extends EventEmitter<WorkerEvents> {
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
  private intake = new AbortController();
  private lifetime = new AbortController();
  private maintenance: Promise<void>[] = [];
  private dispatch?: Promise<void>;
  private poolId = randomUUID();
  private state: "idle" | "starting" | "running" | "stopping" | "stopped" =
    "idle";
  private startPromise?: Promise<void>;
  private stopPromise?: Promise<StopSummary>;
  private resolveClosed!: () => void;
  readonly closed: Promise<void>;
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
      await this.config.storage.registerPool(
        this.poolId,
        this.config.concurrency,
        types,
      );
      this.state = "running";
      this.maintenance = [
        this.loop(10_000, () => this.config.storage.heartbeatPool(this.poolId)),
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
        if (this.inFlight.size >= this.config.concurrency) {
          await Promise.race(this.inFlight);
          continue;
        }
        const claims = await this.config.storage.claim(
          this.config.concurrency - this.inFlight.size,
          types,
          this.config.leaseMs,
        );
        // A claim already committed during shutdown still executes under the drain budget.
        for (const claim of claims) {
          const promise = this.process(claim).catch((error) =>
            this.report(error),
          );
          this.inFlight.add(promise);
          void promise.finally(() => this.inFlight.delete(promise));
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
    const handlerAbort = new AbortController(),
      heartbeatStop = new AbortController();
    const handlerSignal = AbortSignal.any([
      handlerAbort.signal,
      this.lifetime.signal,
    ]);
    const deadline = Date.now() + claim.timeoutMs;
    const timerSignal = AbortSignal.any([
      heartbeatStop.signal,
      this.lifetime.signal,
    ]);
    // Large activity timeouts are checked with bounded timer chunks.
    const timeoutTask = (async () => {
      while (!timerSignal.aborted) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          handlerAbort.abort(
            new RunnerQError("timeout", "Activity execution timed out"),
          );
          return;
        }
        await pause(remaining, timerSignal);
      }
    })().catch(() => {});
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
      signal: handlerSignal,
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
        signal?: AbortSignal,
      ) =>
        recover(
          fn,
          signal ?? (persistence ? this.lifetime.signal : handlerSignal),
          async () => {
            this.metric(() => metrics?.increment("storage_retry", 1));
            // For uncertain writes, let the write reconcile before acting on lost ownership.
            try {
              const owned = await storage.renew(fence, config.leaseMs);
              if (!owned && !persistence)
                handlerAbort.abort(
                  new RunnerQError("claim_lost", "Execution was reclaimed"),
                );
            } catch {
              /* next storage retry classifies the outcome */
            }
          },
        ),
    };
    const context = new ActivityContext(scope);
    const beatSignal = AbortSignal.any([heartbeatStop.signal, handlerSignal]);
    const heartbeat = (async () => {
      while (!beatSignal.aborted) {
        try {
          await pause(config.heartbeatMs, beatSignal);
          if (!(await storage.renew(fence, config.leaseMs))) {
            handlerAbort.abort(
              new RunnerQError("claim_lost", "Execution was reclaimed"),
            );
            return;
          }
        } catch (error) {
          if (!beatSignal.aborted) this.report(error);
        }
      }
    })();
    this.publish("activityStarted", event);
    const started = performance.now();
    let output: unknown,
      error: unknown,
      failed = false;
    try {
      handlerSignal.throwIfAborted();
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
    // Account for SDK calls started but not awaited by user code before transitioning ownership.
    const outstanding = await Promise.allSettled([...scope.pending]);
    if (!failed) {
      const rejection = outstanding.find((x) => x.status === "rejected");
      if (rejection?.status === "rejected") {
        error = rejection.reason;
        failed = true;
      }
    }
    scope.closed = true;
    heartbeatStop.abort();
    await heartbeat;
    await timeoutTask;
    try {
      const reason = handlerSignal.reason;
      if (reason instanceof RunnerQError && reason.code === "claim_lost") {
        this.publish("claimLost", event);
        return;
      }
      if (this.lifetime.signal.aborted) return;
      if (scope.violation) {
        error = scope.violation;
        failed = true;
      }
      if (scope.suspension && !scope.violation) {
        await scope.recover(() => storage.park(fence, scope.suspension!), true);
        this.publish("activityYielded", event);
        return;
      }
      // Checkpoint recovery may have outlived the handler deadline. A successfully captured
      // handler result is still worth committing under the ownership fence, matching Go.
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
          this.publish("activityCompleted", event);
          return;
        }
      }
      if (error instanceof RunnerQError && error.code === "claim_lost") {
        this.publish("claimLost", event);
        return;
      }
      const failure = captureFailure(error);
      const status = await scope.recover(
        () => storage.fail(fence, failure.message, retryable(error), failure),
        true,
      );
      this.publish(
        status === "retrying"
          ? "activityRetrying"
          : status === "failed"
            ? "activityFailed"
            : "activityDeadLetter",
        event,
      );
    } finally {
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
    const deregister = this.config.storage
      .deregisterPool(this.poolId)
      .catch((error) => this.report(error));
    const drain = (async () => {
      await this.dispatch;
      await Promise.allSettled([
        ...this.inFlight,
        ...this.maintenance,
        deregister,
      ]);
    })();
    let timer: NodeJS.Timeout | undefined;
    const drained = await Promise.race([
      drain.then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), graceMs);
      }),
    ]);
    clearTimeout(timer);
    this.lifetime.abort(
      new RunnerQError("timeout", "Worker shutdown budget expired"),
    );
    this.state = "stopped";
    const summary = { drained, remaining: this.inFlight.size };
    this.publish("stopped", summary);
    this.resolveClosed();
    return summary;
  }
  private report(error: unknown): void {
    this.publish(
      "workerError",
      error instanceof Error ? error : new Error(message(error)),
    );
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
    // Invoke a snapshot of native listeners, preserving once() wrappers. Each observer is
    // isolated, so one throw cannot prevent later observers or change committed outcomes.
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
    if (name !== "listenerError")
      this.publish(
        "listenerError",
        error instanceof Error ? error : new Error(message(error)),
      );
  }
  override [captureRejectionSymbol](error: Error, ..._args: unknown[]): void {
    this.observerError(error, "workerError");
  }
}
