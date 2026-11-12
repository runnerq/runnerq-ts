import type { ActivityDefinition } from "./activity.js";
import { ActivityHandle } from "./client.js";
import { businessKey } from "./codec.js";
import { CronExpression, checkTimeZone } from "./cron.js";
import { RunnerQError } from "./errors.js";
import {
  executionOptions,
  type ActivityOption,
  type Priority,
} from "./options.js";
import { QueryError } from "./query.js";
import { encode, serializationFormat } from "./serialization.js";
import type { SerializationFormat } from "./serialization.js";
import type { JsonValue } from "./codec.js";
import type { Storage } from "./storage.js";
import {
  scheduleCatchUpGraceSeconds,
  scheduleCatchUpLimitDefault,
  scheduleKeyPrefix,
} from "./spec.js";

/**
 * What firing does with ticks that came due while no worker fired them: `one` (the
 * default) fires the latest once, `skip` fires it only when it is under a minute late,
 * `all` fires each, oldest first, up to `catchUpLimit`.
 */
export type CatchUp = "skip" | "one" | "all";

/** The most due ticks `catchUp: "all"` fires at once. */
export const maxCatchUpLimit = 1000;

/** The execution options of the activities a schedule fires. */
export interface ScheduleExecution {
  priority: Priority;
  maxAttempts: number | "unlimited";
  timeoutMs: number;
  maxRetryDelayMs: number;
  metadata: Readonly<Record<string, string>>;
}

/** A schedule as storage upserts it. */
export interface ScheduleDefinition {
  name: string;
  cron: string;
  /** An IANA zone name. */
  timezone: string;
  /** The activity type to enqueue. */
  type: string;
  payload: JsonValue;
  serialization: SerializationFormat;
  options: ScheduleExecution;
  catchUp: CatchUp;
  catchUpLimit: number;
}

/** A stored schedule. Times are ISO-8601. */
export interface ScheduleRecord extends ScheduleDefinition {
  paused: boolean;
  nextFireAt: string;
  lastFireAt: string | null;
  createdAt: string;
  updatedAt: string;
}

/**
 * Cron schedules stored with the queue, which every worker of the queue fires (optional;
 * runnerq-spec schedules.md). Unknown names reject with `not_found`, invalid definitions
 * with a `QueryError` of kind `invalid_argument`.
 */
export interface ScheduleStorage {
  /**
   * Creates or replaces a schedule by name. Keeps `paused` and `lastFireAt`, and
   * `nextFireAt` unless `cron` or `timezone` changed; an unchanged definition changes
   * nothing.
   */
  upsertSchedule(schedule: ScheduleDefinition): Promise<void>;
  getSchedule(name: string): Promise<ScheduleRecord>;
  /** The queue's schedules by name. */
  listSchedules(): Promise<ScheduleRecord[]>;
  pauseSchedule(name: string): Promise<void>;
  /** Unpauses and moves `nextFireAt` to the first fire time after now: no catch-up. */
  resumeSchedule(name: string): Promise<void>;
  deleteSchedule(name: string): Promise<void>;
  /**
   * Enqueues the schedule's activity now, paused or not, keyed as a fire at now truncated
   * to the second, and returns its id (the existing one when that key was fired already).
   */
  triggerSchedule(name: string): Promise<string>;
  /**
   * Fires up to `limit` due schedules, each in one transaction that enqueues its fire
   * times (`scheduleFireTimes`) under `scheduleKey` and advances `nextFireAt`. Resolves
   * to the number of activities enqueued; rejects, after firing the rest, with the error
   * of a schedule it could not fire.
   */
  fireSchedules(limit: number): Promise<number>;
}
export function isScheduleStorage(
  storage: unknown,
): storage is ScheduleStorage {
  return (
    typeof (storage as Partial<ScheduleStorage> | null)?.fireSchedules ===
    "function"
  );
}

const invalid = (field: string, message: string) =>
  new QueryError("invalid_argument", message, field);

function parse(cron: string, timezone: string): CronExpression {
  let e: CronExpression;
  try {
    e = CronExpression.parse(cron);
  } catch (error) {
    throw invalid("cron", (error as Error).message);
  }
  try {
    checkTimeZone(timezone);
  } catch (error) {
    throw invalid("timezone", (error as Error).message);
  }
  return e;
}

/** Checks a definition storage is asked to upsert. */
export function validateSchedule(s: ScheduleDefinition): void {
  if (typeof s.name !== "string" || !s.name.trim())
    throw invalid("name", "Schedule name is required");
  if (typeof s.type !== "string" || !s.type)
    throw invalid("type", "Schedule activity type is required");
  if (!["skip", "one", "all"].includes(s.catchUp))
    throw invalid("catchUp", "catchUp must be skip, one or all");
  if (
    !Number.isInteger(s.catchUpLimit) ||
    s.catchUpLimit < 1 ||
    s.catchUpLimit > maxCatchUpLimit
  )
    throw invalid(
      "catchUpLimit",
      `catchUpLimit must be 1 to ${maxCatchUpLimit}`,
    );
  parse(s.cron, s.timezone);
}

/** The first fire time of `cron` in `timezone` strictly after `after`. */
export function nextFireTime(
  cron: string,
  timezone: string,
  after: Date,
): Date {
  const next = parse(cron, timezone).next(after, timezone);
  if (!next)
    throw invalid("cron", "The expression has no fire time within 50 years");
  return next;
}

/**
 * What firing a due schedule at `now` enqueues, oldest first, and its next `nextFireAt`
 * (runnerq-spec vectors/schedule_fire.json). Not yet due: nothing, and `nextFireAt` kept.
 */
export function scheduleFireTimes(
  s: Pick<
    ScheduleRecord,
    "cron" | "timezone" | "catchUp" | "catchUpLimit" | "nextFireAt"
  >,
  now: Date,
): { fire: Date[]; next: Date } {
  const due = new Date(s.nextFireAt);
  if (due > now) return { fire: [], next: due };
  const e = parse(s.cron, s.timezone);
  let fire = e.last(
    due,
    now,
    s.catchUp === "all" ? Math.max(s.catchUpLimit, 1) : 1,
    s.timezone,
  );
  const latest = fire.at(-1);
  if (
    s.catchUp === "skip" &&
    latest &&
    now.getTime() - latest.getTime() >= scheduleCatchUpGraceSeconds * 1000
  )
    fire = [];
  return { fire, next: nextFireTime(s.cron, s.timezone, now) };
}

/**
 * The application idempotency key of the activity a schedule fires at a time:
 * `schedule:<name>:<time in RFC 3339 UTC, to the second>`. Stored as `businessKey(key, type)`.
 */
export function scheduleKey(name: string, at: Date): string {
  const t = new Date(Math.floor(at.getTime() / 1000) * 1000);
  return `${scheduleKeyPrefix}${name}:${t.toISOString().replace(".000Z", "Z")}`;
}

/** A schedule declared in code. */
export interface Schedule<I = unknown, O = unknown> {
  /** Identifies the schedule in its queue. */
  name: string;
  /** 5 fields, 6 with seconds first, or a macro such as `@daily`. */
  cron: string;
  /** The IANA zone the expression's wall-clock times are in (default "UTC"). */
  timezone?: string;
  /** The activity to enqueue. */
  activity: ActivityDefinition<I, O>;
  /** Each fired activity's input. */
  payload?: I;
  /** `runner.priority`, `maxAttempts`, `timeoutMs`, `maxRetryDelayMs` and `metadata`. */
  options?: readonly ActivityOption[];
  catchUp?: CatchUp;
  /** How many due ticks `catchUp: "all"` fires at once (default 100, at most 1000). */
  catchUpLimit?: number;
}

/** A declared schedule as storage stores it. */
export function scheduleDefinition<I, O>(
  s: Schedule<I, O>,
): ScheduleDefinition {
  const o = executionOptions(s.options ?? []);
  if (o.delayMs || o.idempotency || o.step || o.asRoot || o.replay)
    throw new RunnerQError(
      "configuration",
      "A schedule takes only priority, maxAttempts, timeoutMs, maxRetryDelayMs and metadata",
    );
  if (!s.activity?.name) throw invalid("type", "Schedule activity is required");
  let payload;
  try {
    payload = encode(
      s.activity.input ? s.activity.input(s.payload) : (s.payload ?? null),
      serializationFormat(s.activity.serialization),
    );
  } catch (error) {
    throw new RunnerQError("serialization", "Invalid schedule payload", {
      cause: error,
    });
  }
  const definition: ScheduleDefinition = {
    name: s.name,
    cron: s.cron,
    timezone: s.timezone ?? "UTC",
    type: s.activity.name,
    payload: payload.data,
    serialization: payload.serialization,
    options: {
      priority: o.priority,
      maxAttempts: o.maxAttempts,
      timeoutMs: o.timeoutMs,
      maxRetryDelayMs: o.maxRetryDelayMs,
      metadata: o.metadata,
    },
    catchUp: s.catchUp ?? "one",
    catchUpLimit: s.catchUpLimit ?? scheduleCatchUpLimitDefault,
  };
  validateSchedule(definition);
  return definition;
}

/** Manages a queue's cron schedules from any process, worker or not (`client.schedules`). */
export class Schedules {
  constructor(private readonly storage: Storage) {}
  private get store(): ScheduleStorage {
    if (!isScheduleStorage(this.storage))
      throw new RunnerQError(
        "configuration",
        "This storage does not support schedules",
      );
    return this.storage;
  }
  /**
   * Creates the schedule or replaces its definition. A paused schedule stays paused, and
   * its next fire time is kept unless `cron` or `timezone` changed.
   */
  async upsert<I, O>(schedule: Schedule<I, O>): Promise<void> {
    await this.store.upsertSchedule(scheduleDefinition(schedule));
  }
  async get(name: string): Promise<ScheduleRecord> {
    return this.store.getSchedule(name);
  }
  /** The queue's schedules by name. */
  async list(): Promise<ScheduleRecord[]> {
    return this.store.listSchedules();
  }
  /** Stops a schedule firing until `resume`. */
  async pause(name: string): Promise<void> {
    await this.store.pauseSchedule(name);
  }
  /** Restarts a paused schedule from its next fire time after now; no catch-up. */
  async resume(name: string): Promise<void> {
    await this.store.resumeSchedule(name);
  }
  /**
   * Enqueues the schedule's activity now, even while paused, without changing its fire
   * times. Triggers within the same second share one activity.
   */
  async trigger<O = unknown>(name: string): Promise<ActivityHandle<O>> {
    return new ActivityHandle<O>(
      await this.store.triggerSchedule(name),
      this.storage,
    );
  }
  /** Removes a schedule; activities it already enqueued are unaffected. */
  async delete(name: string): Promise<void> {
    await this.store.deleteSchedule(name);
  }
}

/** The stored idempotency key of a schedule's fire at a time. */
export function scheduleFireKey(name: string, type: string, at: Date): string {
  return businessKey(scheduleKey(name, at), type);
}
