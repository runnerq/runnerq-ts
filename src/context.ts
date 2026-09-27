import type { ActivityDefinition, Parser } from "./activity.js";
import { checkpointId, nonempty, type JsonValue } from "./codec.js";
import { submit, type ChildActivityHandle } from "./client.js";
import {
  message,
  NonRetryableError,
  retryable,
  RunnerQError,
  SignalTimeoutError,
} from "./errors.js";
import { integer, type ActivityOption } from "./options.js";
import {
  executionScope,
  guard,
  suspend,
  track,
  type AttemptScope,
} from "./scope.js";
import { encode, decode, type SerializedValue } from "./serialization.js";
import { pause } from "./async.js";
import type { StoredResult } from "./storage.js";

export interface StepContext {
  readonly signal: AbortSignal;
}
export class ActivityContext {
  constructor(private readonly scope: AttemptScope) {}
  get activityId(): string {
    return this.scope.claim.id;
  }
  get activityType(): string {
    return this.scope.claim.type;
  }
  get retryCount(): number {
    return this.scope.claim.retryCount;
  }
  get parentActivityId(): string | null {
    return this.scope.claim.parentId;
  }
  get rootActivityId(): string {
    return this.scope.claim.rootId;
  }
  get depth(): number {
    return this.scope.claim.depth;
  }
  get metadata(): Readonly<Record<string, string>> {
    return this.scope.claim.metadata;
  }
  get signal(): AbortSignal {
    return this.scope.signal;
  }
  private named(kind: string, name: string): string {
    nonempty(name, "Step name");
    const identity = `${kind}:${name}`;
    if (this.scope.names.has(identity))
      throw new RunnerQError(
        "configuration",
        `Step ${identity} was used more than once in this invocation`,
      );
    this.scope.names.add(identity);
    return checkpointId(this.activityId, kind, name);
  }
  run<T>(
    name: string,
    fn: (ctx: StepContext) => T | Promise<T>,
    options: { parse?: Parser<T> } = {},
  ): Promise<T> {
    return track(this.scope, async () => {
      const s = this.scope,
        id = this.named("run", name);
      if (s.waitActive)
        throw (s.violation = new RunnerQError(
          "configuration",
          "A checkpointed operation cannot overlap a durable wait",
        ));
      s.activeEffects++;
      try {
        const stored = await s.recover(() => s.storage.getResult(id));
        if (stored) {
          if (stored.state === "Err")
            throw new NonRetryableError(messageFromData(decode(stored)));
          return parse(decode(stored), options.parse, `step ${name}`);
        }
        guard(s);
        let data: SerializedValue;
        let decoded: T;
        try {
          const value = await executionScope.run({ ...s, inStep: true }, () =>
            fn({ signal: s.signal }),
          );
          guard(s, false);
          data = encode(value);
          decoded = parse(decode(data), options.parse, `step ${name}`);
        } catch (error) {
          if (
            !retryable(error) &&
            !s.signal.aborted &&
            !s.suspension &&
            !(
              error instanceof RunnerQError &&
              ["claim_lost", "configuration"].includes(error.code)
            )
          ) {
            await s.recover(
              () =>
                s.storage.checkpoint(
                  s.fence,
                  id,
                  {
                    state: "Err",
                    ...encode({ error: message(error) }, "json-v1"),
                  },
                  `run:${name}`,
                ),
              true,
            );
          }
          throw error;
        }
        // Keep the captured JSON value while retrying persistence; never rerun the callback here.
        await s.recover(
          () =>
            s.storage.checkpoint(
              s.fence,
              id,
              { state: "Ok", ...data },
              `run:${name}`,
            ),
          true,
        );
        return decoded;
      } finally {
        s.activeEffects--;
      }
    });
  }
  sleep(name: string, durationMs: number): Promise<void> {
    return track(this.scope, () =>
      this.exclusiveWait(async () => {
        integer(durationMs, "sleep duration", 0, 8_000_000_000_000);
        const s = this.scope,
          id = this.named("sleep", name);
        let stored = await s.recover(() => s.storage.getResult(id));
        if (!stored) {
          stored = {
            state: "Ok",
            serialization: "json-v1",
            data: { wake_at: new Date(Date.now() + durationMs).toISOString() },
          };
          const value = stored;
          await s.recover(
            () => s.storage.checkpoint(s.fence, id, value, `sleep:${name}`),
            true,
          );
        }
        const wake = timestamp(decode(stored), "wake_at");
        if (wake <= Date.now()) return;
        if (wake > this.budget())
          suspend(s, {
            kind: "sleep",
            step: name,
            wakeAt: new Date(wake).toISOString(),
          });
        await pause(wake - Date.now(), s.signal);
      }),
    );
  }
  waitForSignal<T = unknown>(
    name: string,
    options: { timeoutMs?: number; parse?: Parser<T> } = {},
  ): Promise<T> {
    return track(this.scope, () =>
      this.exclusiveWait(async () => {
        const timeout = integer(
          options.timeoutMs ?? 0,
          "signal timeout",
          0,
          8_000_000_000_000,
        );
        const s = this.scope,
          waitId = this.named("signalwait", name),
          resultId = checkpointId(this.activityId, "signal", name);
        let checkpoint = await s.recover(() => s.storage.getResult(waitId));
        if (!checkpoint) {
          checkpoint = {
            state: "Ok",
            serialization: "json-v1",
            data: {
              deadline: timeout
                ? new Date(Date.now() + timeout).toISOString()
                : null,
            },
          };
          const value = checkpoint;
          await s.recover(
            () => s.storage.checkpoint(s.fence, waitId, value, ""),
            true,
          );
        }
        const deadline = timestamp(decode(checkpoint), "deadline", true);
        for (;;) {
          const ready = await s.recover(() => s.storage.getResult(resultId));
          if (ready)
            return parse(decode(ready), options.parse, `signal ${name}`);
          if (deadline <= Date.now()) {
            const last = await s.recover(() => s.storage.getResult(resultId));
            if (last)
              return parse(decode(last), options.parse, `signal ${name}`);
            throw new SignalTimeoutError(name);
          }
          if (deadline > this.budget())
            suspend(s, {
              kind: "signal",
              step: name,
              resultId,
              wakeAt: new Date(
                Math.min(deadline, Date.now() + 60_000),
              ).toISOString(),
            });
          const signal = AbortSignal.any([
            s.signal,
            AbortSignal.timeout(Math.max(1, Math.ceil(deadline - Date.now()))),
          ]);
          try {
            const result = await s.recover(
              () => s.storage.waitResult(resultId, signal),
              false,
              signal,
            );
            return parse(decode(result), options.parse, `signal ${name}`);
          } catch (error) {
            if (!signal.aborted || s.signal.aborted) throw error;
          }
        }
      }),
    );
  }
  spawn<I, O>(
    definition: ActivityDefinition<I, O>,
    payload: I,
    ...options: ActivityOption[]
  ): Promise<ChildActivityHandle<O>> {
    return track(this.scope, () =>
      submit(
        this.scope.storage,
        definition,
        payload,
        options,
        this.scope,
        true,
      ),
    );
  }
  wait<O>(handle: ChildActivityHandle<O>): Promise<O> {
    return handle.result();
  }
  waitAll<T extends readonly ChildActivityHandle<unknown>[]>(
    handles: T,
  ): Promise<{
    [K in keyof T]: T[K] extends ChildActivityHandle<infer O> ? O : never;
  }> {
    return track(this.scope, async () => {
      for (const handle of handles) {
        this.sameStorage(handle);
        await this.scope.recover(() =>
          this.scope.storage.registerDependency(this.scope.fence, handle.id),
        );
      }
      const results: unknown[] = [];
      for (const handle of handles) results.push(await handle.result());
      return results as {
        [K in keyof T]: T[K] extends ChildActivityHandle<infer O> ? O : never;
      };
    });
  }
  /** Internal handle routing: external handles awaited in a handler use durable parking. */
  async waitStored(id: string): Promise<StoredResult> {
    return this.exclusiveWait(async () => {
      const s = this.scope;
      guard(s);
      await s.recover(() => s.storage.registerDependency(s.fence, id));
      const bound = Math.min(Date.now() + s.waitGraceMs, this.budget());
      const ready = await s.recover(() => s.storage.getResult(id));
      if (ready) return ready;
      if (bound > Date.now()) {
        const signal = AbortSignal.any([
          s.signal,
          AbortSignal.timeout(Math.max(1, Math.ceil(bound - Date.now()))),
        ]);
        try {
          return await s.recover(
            () => s.storage.waitResult(id, signal),
            false,
            signal,
          );
        } catch (error) {
          if (!signal.aborted || s.signal.aborted) throw error;
        }
      }
      suspend(s, {
        kind: "await",
        step: `await:${id}`,
        resultId: id,
        producerId: id,
        wakeAt: new Date(Date.now() + 60_000).toISOString(),
      });
    });
  }
  private budget(): number {
    const left = Math.max(0, this.scope.deadline - Date.now());
    return this.scope.deadline - Math.min(2_000, left / 2);
  }
  private sameStorage(handle: ChildActivityHandle<unknown>): void {
    if (handle.storage !== this.scope.storage)
      throw new RunnerQError(
        "configuration",
        "Future belongs to another storage instance",
      );
  }
  private async exclusiveWait<T>(fn: () => Promise<T>): Promise<T> {
    if (this.scope.waitActive || this.scope.activeEffects)
      throw (this.scope.violation = new RunnerQError(
        "configuration",
        "Overlapping durable waits/effects are unsupported; use ctx.waitAll() for child results",
      ));
    this.scope.waitActive = true;
    try {
      return await fn();
    } finally {
      this.scope.waitActive = false;
    }
  }
}
function messageFromData(data: unknown): string {
  return data && typeof data === "object" && "error" in data
    ? String(data.error)
    : "Step failed";
}
function parse<T>(
  value: unknown,
  parser: Parser<T> | undefined,
  label: string,
): T {
  try {
    return parser ? parser(value) : (value as T);
  } catch (error) {
    throw new NonRetryableError(`Invalid stored ${label} result`, {
      cause: error,
    });
  }
}
function timestamp(value: unknown, key: string, nullable = false): number {
  if (value && typeof value === "object" && key in value) {
    const raw = (value as Record<string, JsonValue>)[key];
    if (nullable && raw === null) return Infinity;
    if (typeof raw === "string" && Number.isFinite(Date.parse(raw)))
      return Date.parse(raw);
  }
  throw new NonRetryableError(`Corrupt durable ${key} checkpoint`);
}
