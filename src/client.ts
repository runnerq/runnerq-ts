import { randomUUID } from "node:crypto";
import { Limits } from "./limit.js";
import type { ActivityDefinition } from "./activity.js";
import { businessKey, commandFingerprint, stepKey, uuid } from "./codec.js";
import { ActivityFailedError, RunnerQError } from "./errors.js";
import {
  encode,
  decode,
  serializationFormat,
  type SerializationMode,
} from "./serialization.js";
import { executionOptions, type ActivityOption } from "./options.js";
import {
  isCommandStorage,
  type Command,
  type CommandKind,
  type Storage,
  type StoredResult,
  type Submission,
} from "./storage.js";
import {
  isQueryStorage,
  QueryError,
  type ActivityRecord,
  type ActivityRecordPage,
  type QueryFilter,
  type QuerySort,
  type RecordInclude,
  type RecordStatus,
} from "./query.js";
import type { Priority } from "./options.js";
import { executionScope, guard, track, type AttemptScope } from "./scope.js";
import { Schedules } from "./schedule.js";
import {
  startSpan,
  withTraceparent,
  type Tracer,
  type TraceAttributeValue,
} from "./observability.js";
import {
  spanAttributes as attr,
  spanNames,
  spanOutcomes,
  traceparentKey,
} from "./observability-spec.js";

export interface ResultOptions {
  signal?: AbortSignal;
}
/** A handler-created activity handle whose result uses the durable wait policy. */
export interface ChildActivityHandle<O> {
  readonly id: string;
  readonly storage: Storage;
  result(): Promise<O>;
}
export function unwrap<T>(id: string, result: StoredResult): T {
  if (result.state === "Err") throw new ActivityFailedError(id, decode(result));
  return decode(result) as T;
}
export class ActivityHandle<O> {
  constructor(
    readonly id: string,
    readonly storage: Storage,
    private readonly output?: (value: unknown) => O,
  ) {}
  async result(options: ResultOptions = {}): Promise<O> {
    const scope = executionScope.getStore();
    if (scope) {
      if (scope.storage !== this.storage)
        throw new RunnerQError(
          "configuration",
          "In-handler futures must use the executing worker's storage instance",
        );
      if (options.signal)
        throw new RunnerQError(
          "configuration",
          "Use the activity execution signal for durable waits inside a handler",
        );
      return this.decode(await track(scope, () => scope.wait(this.id)));
    }
    return this.decode(await this.storage.waitResult(this.id, options.signal));
  }
  decode(stored: StoredResult): O {
    const value = unwrap<O>(this.id, stored);
    try {
      return this.output ? this.output(value) : value;
    } catch (error) {
      throw new RunnerQError(
        "serialization",
        "Stored activity result does not match its output contract",
        { cause: error },
      );
    }
  }
}
export async function submit<I, O>(
  storage: Storage,
  definition: ActivityDefinition<I, O>,
  input: I,
  options: readonly ActivityOption[],
  scope?: AttemptScope,
  child = false,
  tracer: Tracer | undefined = scope?.tracer,
): Promise<ActivityHandle<O>> {
  const config = executionOptions(options);
  if (!scope && (config.step || config.asRoot || config.replay))
    throw new RunnerQError(
      "configuration",
      "Step, asRoot and newOnReplay are handler-only options",
    );
  if (scope) guard(scope);
  if (child && !config.step && !config.idempotency && !config.replay)
    throw new RunnerQError(
      "configuration",
      "Child creation requires runner.step(), runner.idempotencyKey() or runner.newOnReplay()",
    );
  if (!child && config.step)
    throw new RunnerQError(
      "configuration",
      "Named children must use ctx.spawn()",
    );
  if (scope && config.step) {
    const name = `spawn:${config.step}`;
    if (scope.names.has(name))
      throw new RunnerQError(
        "configuration",
        `Child step ${config.step} was used more than once in this invocation`,
      );
    scope.names.add(name);
  }
  const linked = !!scope && child && !config.asRoot;
  const id = randomUUID();
  const depth = linked ? scope.claim.depth + 1 : 0;
  if (scope && depth > scope.maxDepth)
    throw new RunnerQError("configuration", "Maximum activity depth exceeded");
  let payload;
  try {
    payload = encode(
      definition.input ? definition.input(input) : input,
      serializationFormat(definition.serialization),
    );
  } catch (error) {
    throw new RunnerQError("serialization", "Invalid activity input", {
      cause: error,
    });
  }
  const rootId = linked ? scope.claim.rootId : id;
  const key = config.step
    ? stepKey(rootId, scope!.claim.id, config.step)
    : config.idempotency
      ? businessKey(config.idempotency.key, definition.name)
      : undefined;
  const parentId = linked ? scope.claim.id : null;
  const span = startSpan(tracer, spanNames.enqueue, scope?.span, () => {
    const a: Record<string, TraceAttributeValue> = {
      [attr.queue]: storage.queue,
      [attr.activityType]: definition.name,
      [attr.activityId]: id,
      [attr.rootId]: rootId,
    };
    if (parentId) a[attr.parentId] = parentId;
    return a;
  });
  const activity: Submission = {
    id,
    type: definition.name,
    payload: payload.data,
    serialization: payload.serialization,
    options: {
      ...config,
      metadata: withTraceparent(
        config.metadata,
        span?.traceparent,
        scope?.claim.metadata[traceparentKey],
      ),
    },
    parentId,
    rootId,
    depth,
    key,
    fence: scope?.fence,
  };
  let existing: string;
  try {
    existing = scope
      ? await scope.recover(() => storage.submit(activity))
      : await storage.submit(activity);
  } catch (error) {
    span?.end(spanOutcomes.error, error);
    throw error;
  }
  if (existing === id) span?.end(spanOutcomes.enqueued);
  else
    span?.end(spanOutcomes.existing, undefined, {
      [attr.activityId]: existing,
    });
  if (scope && existing === id)
    scope.announce({
      type: config.delayMs > 0 ? "activity.scheduled" : "activity.created",
      activityId: id,
      activityType: definition.name,
      rootId,
      at: new Date(),
    });
  return new ActivityHandle(existing, storage, definition.output);
}
/**
 * The activities a command acts on: an id, ids (at most 1000), or up to `max` (1 to 10000)
 * activities matching `filter` that the command can act on, oldest first, so repeating a
 * bounded command makes progress.
 */
export type Target =
  string | readonly string[] | { filter: QueryFilter; max: number };

export interface CommandOptions {
  /**
   * Makes the command idempotent under this id: sending it again (a retry after a timeout,
   * say) returns the first result with `replayed` set, for at least 24 hours. The same id
   * with different input rejects with a `RunnerQError` whose code is "conflict". Without it
   * each call gets a fresh id.
   */
  commandId?: string;
  /** Recorded in each activity's history (and a cancelled activity's error). */
  reason?: string;
  /** Report what the command would do (outcome "would_apply") and change nothing. */
  dryRun?: boolean;
}
export interface CancelOptions extends CommandOptions {
  /** Also cancel unfinished descendants. Default true. */
  cascade?: boolean;
}
export interface RetryOptions extends CommandOptions {
  /** Restore the full attempt budget. By default the activity keeps the attempts it used. */
  resetAttempts?: boolean;
}

/**
 * What a command did to one activity: "applied"; "would_apply" (a dry run);
 * "not_eligible", its status doesn't allow the command; "not_found" in this queue.
 */
export type CommandOutcome =
  "applied" | "would_apply" | "not_eligible" | "not_found";
export interface CommandItem {
  id: string;
  outcome: CommandOutcome;
  /** The status after the command (current when not eligible); absent once deleted or not found. */
  status?: RecordStatus;
  /** Why the command didn't apply. */
  reason?: string;
}
export interface CommandResult {
  /** The command's id: `commandId`, or a generated one. */
  commandId: string;
  /** One per target: per id given, in order, or per activity a filter selected. */
  items: CommandItem[];
  /** Activities the command changed. */
  applied: number;
  /** Descendants a cancel also cancelled. */
  cascaded: number;
  /** A filter matched more than `max`. */
  more: boolean;
  /** The recorded result of an earlier call with the same command id; nothing was applied now. */
  replayed: boolean;
}

/** Selects and orders the activities `list` returns; empty lists the newest 50. */
export interface ListActivitiesOptions {
  /** Keep activities in any of these statuses. */
  statuses?: readonly RecordStatus[];
  /** Keep activities of this type. */
  type?: string;
  /** Leave out child activities. */
  rootsOnly?: boolean;
  /** Further conditions in the query model. */
  filter?: QueryFilter;
  /** Default: newest first. */
  sort?: QuerySort;
  /** Heavy fields, left out by default. */
  include?: RecordInclude;
  /** Page size: default 50, at most 1000. */
  limit?: number;
  /** A previous page's `nextCursor`. */
  cursor?: string;
}

const priorities: Record<Priority, number> = {
  low: 1,
  normal: 2,
  high: 3,
  critical: 4,
};

/**
 * Submits, signals, awaits, inspects and manages activities. It needs only storage, no
 * running worker, so any process sharing the database can use it. Management commands need
 * storage implementing `CommandStorage`, and `get`/`list` storage implementing
 * `QueryStorage`; `PostgresStorage` implements both. All of them act on the storage's queue.
 */
export class RunnerQClient {
  readonly storage: Storage;
  /** The queue's cron schedules: upsert, pause, resume, trigger, delete and list. */
  readonly schedules: Schedules;
  /** The queue's concurrency and rate limits per activity type: set, get, list and delete. */
  readonly limits: Limits;
  /** Traces this client's submissions and signals (see `Tracer`). */
  readonly tracer?: Tracer;
  constructor({ storage, tracer }: { storage: Storage; tracer?: Tracer }) {
    this.storage = storage;
    this.tracer = tracer;
    this.schedules = new Schedules(storage);
    this.limits = new Limits(storage);
  }
  execute<I, O>(
    definition: ActivityDefinition<I, O>,
    payload: I,
    ...options: ActivityOption[]
  ): Promise<ActivityHandle<O>> {
    const scope = executionScope.getStore();
    if (scope) {
      if (scope.storage !== this.storage)
        return Promise.reject(
          new RunnerQError(
            "configuration",
            "Handler execution cannot bypass its storage fence",
          ),
        );
      return track(scope, () =>
        submit(
          this.storage,
          definition,
          payload,
          options,
          scope,
          false,
          this.tracer ?? scope.tracer,
        ),
      );
    }
    return submit(
      this.storage,
      definition,
      payload,
      options,
      undefined,
      false,
      this.tracer,
    );
  }
  handle<O = unknown>(id: string): ActivityHandle<O>;
  handle<I, O>(
    definition: ActivityDefinition<I, O>,
    id: string,
  ): ActivityHandle<O>;
  handle<I, O>(
    definition: string | ActivityDefinition<I, O>,
    id?: string,
  ): ActivityHandle<O> {
    return typeof definition === "string"
      ? new ActivityHandle(uuid(definition), this.storage)
      : new ActivityHandle(uuid(id!), this.storage, definition.output);
  }
  async signal(
    id: string,
    name: string,
    payload: unknown = null,
    options: { serialization?: SerializationMode } = {},
  ): Promise<void> {
    const target = uuid(id);
    const value = encode(
      payload,
      serializationFormat(options.serialization ?? "native"),
    );
    const span = startSpan(
      this.tracer,
      spanNames.signalSend,
      undefined,
      () => ({
        [attr.activityId]: target,
        [attr.signalName]: name,
      }),
    );
    try {
      await (span
        ? span.run(() => this.storage.signal(target, name, value))
        : this.storage.signal(target, name, value));
    } catch (error) {
      span?.end(spanOutcomes.error, error);
      throw error;
    }
    span?.end(spanOutcomes.sent);
  }
  /**
   * The activity with its payload, result and last error. Rejects with a `RunnerQError`
   * whose code is "not_found" when the queue has no such activity.
   */
  async get(id: string): Promise<ActivityRecord> {
    const page = await this.queries().queryActivities({
      filter: this.scope({ field: "id", op: "eq", value: uuid(id) }),
      include: { payload: true, result: true, lastError: true },
      limit: 1,
    });
    const record = page.items[0];
    if (!record)
      throw new RunnerQError("not_found", `Activity ${id} not found`);
    return record;
  }
  /** A page of the queue's activities. */
  async list(options: ListActivitiesOptions = {}): Promise<ActivityRecordPage> {
    const terms: QueryFilter[] = [];
    if (options.statuses?.length)
      terms.push({ field: "status", op: "in", value: [...options.statuses] });
    if (options.type)
      terms.push({ field: "type", op: "eq", value: options.type });
    if (options.rootsOnly)
      terms.push({ field: "parent_id", op: "exists", value: false });
    if (options.filter) terms.push(options.filter);
    return this.queries().queryActivities({
      filter: this.scope(
        terms.length === 0
          ? undefined
          : terms.length === 1
            ? terms[0]
            : { and: terms },
      ),
      sort: options.sort,
      include: options.include,
      limit: options.limit,
      cursor: options.cursor,
    });
  }
  /**
   * Cancels unfinished activities and (unless `cascade: false`) their unfinished
   * descendants. Each stores a cancellation error as its result, so awaiting parents and
   * `result()` callers get an error for which `isCancelled` is true. A running handler's
   * `ctx.signal` aborts promptly, on whichever worker holds it.
   */
  cancel(target: Target, options: CancelOptions = {}): Promise<CommandResult> {
    return this.command("cancel", target, options, {
      cascadeChildren: options.cascade ?? true,
    });
  }
  /**
   * Makes failed, dead-lettered or cancelled activities pending again; completed steps
   * replay from their checkpoints.
   */
  retry(target: Target, options: RetryOptions = {}): Promise<CommandResult> {
    return this.command("retry", target, options, {
      resetAttempts: options.resetAttempts ?? false,
    });
  }
  /**
   * Makes scheduled activities (delayed, or waiting out a retry backoff) and waiting ones
   * (parked on a sleep, signal or child) runnable now.
   */
  runNow(target: Target, options: CommandOptions = {}): Promise<CommandResult> {
    return this.command("run_now", target, options, {});
  }
  /** Moves scheduled activities to run at `at`. */
  reschedule(
    target: Target,
    at: Date,
    options: CommandOptions = {},
  ): Promise<CommandResult> {
    if (!(at instanceof Date) || Number.isNaN(at.getTime()))
      return Promise.reject(
        new RunnerQError("configuration", "reschedule needs a valid Date"),
      );
    return this.command("reschedule", target, options, { at });
  }
  /** Changes unfinished activities' priority. */
  setPriority(
    target: Target,
    priority: Priority,
    options: CommandOptions = {},
  ): Promise<CommandResult> {
    const value = priorities[priority];
    if (!value)
      return Promise.reject(
        new RunnerQError(
          "configuration",
          `Unknown priority ${String(priority)}`,
        ),
      );
    return this.command("set_priority", target, options, { priority: value });
  }
  /**
   * Removes finished root activities with their whole tree. Children can't be deleted
   * alone, and an unfinished tree must be cancelled first.
   */
  delete(target: Target, options: CommandOptions = {}): Promise<CommandResult> {
    return this.command("delete", target, options, {});
  }
  private async command(
    kind: CommandKind,
    target: Target,
    options: CommandOptions,
    extra: Partial<Command>,
  ): Promise<CommandResult> {
    if (!isCommandStorage(this.storage))
      throw new RunnerQError(
        "configuration",
        "This storage does not take commands (CommandStorage)",
      );
    const commandId = options.commandId || randomUUID();
    const cmd: Command = {
      id: commandId,
      kind,
      target:
        typeof target === "string"
          ? { ids: [uuid(target)] }
          : Array.isArray(target)
            ? { ids: (target as readonly string[]).map(uuid) }
            : {
                filter: (target as { filter: QueryFilter }).filter,
                max: (target as { max: number }).max,
              },
      dryRun: options.dryRun ?? false,
      ...(options.reason && { reason: options.reason }),
      ...extra,
    };
    cmd.fingerprint = commandFingerprint(cmd);
    let res;
    try {
      res = await this.storage.applyCommand(cmd);
    } catch (error) {
      throw error instanceof QueryError
        ? new RunnerQError("configuration", error.message, { cause: error })
        : error;
    }
    return {
      commandId,
      applied: res.applied,
      cascaded: res.cascaded,
      more: res.more,
      replayed: res.replayed,
      items: res.items.map((it) => {
        const item: CommandItem = {
          id: it.id,
          outcome:
            it.outcome === "skipped"
              ? it.error?.kind === "not_found"
                ? "not_found"
                : "not_eligible"
              : it.outcome,
        };
        if (it.status) item.status = it.status;
        if (it.error) item.reason = it.error.message;
        return item;
      }),
    };
  }
  private queries() {
    if (!isQueryStorage(this.storage))
      throw new RunnerQError(
        "configuration",
        "This storage cannot be queried (QueryStorage)",
      );
    return this.storage;
  }
  /** Narrows a query to the storage's queue, which commands act on. */
  private scope(filter: QueryFilter | undefined): QueryFilter {
    const queue: QueryFilter = {
      field: "queue",
      op: "eq",
      value: this.storage.queue,
    };
    return filter ? { and: [queue, filter] } : queue;
  }
  async signalByKey<I, O>(
    definition: ActivityDefinition<I, O>,
    key: string,
    name: string,
    payload: unknown = null,
  ): Promise<void> {
    await this.signal(
      await this.storage.lookupKey(businessKey(key, definition.name)),
      name,
      payload,
      { serialization: definition.serialization },
    );
  }
}
