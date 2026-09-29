// The read surface RunnerQ Cloud queries: activities, events and steps (filter, sort,
// paginate, aggregate) in its backend-neutral model; a port of runnerq-go's QueryStorage.
import type { JsonValue } from "./codec.js";

/**
 * What a backend implements so RunnerQ Cloud can query it (optional: without it the agent
 * serves only executor messages). Queries span every queue in the database, so every
 * executor of an app answers alike. Backends advertise what they evaluate efficiently in
 * `queryCapabilities()` and reject anything else with a `QueryError` ("unsupported"), never
 * silently ignoring a filter.
 */
export interface QueryStorage {
  queryCapabilities(): QueryCapabilities;
  queryActivities(query: ActivityQuery): Promise<ActivityRecordPage>;
  /** Counts matches up to `limit`; `exact` is false when the count stopped at `limit`. */
  countActivities(
    filter: QueryFilter | undefined,
    limit: number,
  ): Promise<{ count: number; exact: boolean }>;
  aggregateActivities(query: AggregateQuery): Promise<AggregateRows>;
  queryEvents(query: EventQuery): Promise<EventRecordPage>;
  /** Lists an activity's durable steps, oldest first. */
  listStepEntries(
    activityId: string,
    includeData: boolean,
    limit: number,
    cursor: string,
  ): Promise<StepEntryPage>;
  /** The tree the activity belongs to (any member may be named), root first, up to `maxNodes`. */
  getActivityTree(
    activityId: string,
    include: RecordInclude,
    maxNodes: number,
  ): Promise<ActivityTree>;
}

export function isQueryStorage(storage: unknown): storage is QueryStorage {
  const s = storage as Partial<Record<keyof QueryStorage, unknown>> | null;
  return (
    !!s &&
    typeof s.queryCapabilities === "function" &&
    typeof s.queryActivities === "function" &&
    typeof s.countActivities === "function" &&
    typeof s.aggregateActivities === "function" &&
    typeof s.queryEvents === "function" &&
    typeof s.listStepEntries === "function" &&
    typeof s.getActivityTree === "function"
  );
}

/** Canonical activity statuses. */
export type RecordStatus =
  | "pending"
  | "scheduled"
  | "running"
  | "waiting"
  | "completed"
  | "failed"
  | "dead_letter"
  | "cancelled";

/**
 * Canonical event types. Backends map their internal event names onto these; events with
 * no canonical equivalent keep a namespaced name of their own ("other.<name>").
 */
export const RecordEvent = {
  created: "activity.created",
  scheduled: "activity.scheduled",
  cancelled: "activity.cancelled",
  attemptStarted: "attempt.started",
  attemptSucceeded: "attempt.succeeded",
  attemptFailed: "attempt.failed",
  attemptTimedOut: "attempt.timed_out",
  leaseExpired: "attempt.lease_expired",
  leaseExtended: "attempt.lease_extended",
  waitParked: "wait.parked",
  signalReceived: "signal.received",
  resultStored: "result.stored",
  childLinked: "child.linked",
  deadLetter: "dead_letter.entered",
  redriven: "dead_letter.redriven",
  retried: "activity.retried",
  runNow: "activity.run_now",
  rescheduled: "activity.rescheduled",
  priorityChanged: "activity.priority_changed",
} as const;

export type FilterOp =
  | "eq"
  | "ne"
  | "in"
  | "nin"
  | "lt"
  | "lte"
  | "gt"
  | "gte"
  | "exists"
  | "prefix"
  | "contains";

/**
 * A boolean expression. Exactly one of `and`, `or`, `not` or the `field`/`op` predicate is
 * set. `value` holds decoded JSON (string, number, boolean, array or null); timestamps are
 * RFC 3339 strings. Field "metadata.<key>" addresses a metadata tag.
 */
export interface QueryFilter {
  and?: QueryFilter[];
  or?: QueryFilter[];
  not?: QueryFilter;
  field?: string;
  op?: string;
  value?: unknown;
}

/** Orders by one field; the backend adds a unique tiebreaker. */
export interface QuerySort {
  field: string;
  desc: boolean;
}

/** Selects heavy fields, which are omitted unless requested. */
export interface RecordInclude {
  payload?: boolean;
  result?: boolean;
  lastError?: boolean;
}

export interface ActivityQuery {
  filter?: QueryFilter;
  /** Default: created_at descending. */
  sort?: QuerySort;
  include?: RecordInclude;
  /** Default 50; capped at 1000. */
  limit?: number;
  /** Opaque, from a previous page. */
  cursor?: string;
}

/** A stored value as it is in the database: JSON in the named serialization. */
export interface RecordValue {
  serialization: string;
  data: JsonValue;
}

export interface RecordResult {
  state: "Ok" | "Err";
  serialization: string;
  /** Absent when the backend stored no data. */
  data?: JsonValue;
}

export interface RecordError {
  message: string;
  /** retryable | non_retryable | dead_letter, or "". */
  kind: string;
  at?: Date;
}

/** Why a waiting activity is parked. */
export interface RecordWait {
  kind: "sleep" | "signal" | "children" | "other";
  name: string;
  until?: Date;
}

/** An activity in the canonical model. */
export interface ActivityRecord {
  id: string;
  type: string;
  queue: string;
  status: RecordStatus;
  priority: number;
  rootId: string;
  parentId?: string;
  depth: number;
  /** The key the application set, never the stored encoding; "" when it set none. */
  idempotencyKey: string;
  /** The attempt running or next to run, from 1; for a terminal activity, the attempts made. */
  attempt: number;
  /** Total attempts allowed; absent when unlimited. */
  maxAttempts?: number;
  createdAt: Date;
  scheduledFor?: Date;
  startedAt?: Date;
  completedAt?: Date;
  updatedAt: Date;
  timeoutMs: number;
  leaseExpiresAt?: Date;
  /** The executor running it (running activities only), or "". */
  executorId: string;
  wait?: RecordWait;
  metadata?: Record<string, string>;
  /** Only with `include.lastError`. */
  lastError?: RecordError;
  /** Only with `include.payload`. */
  payload?: RecordValue;
  /** Only with `include.result`. */
  result?: RecordResult;
}

export interface ActivityRecordPage {
  items: ActivityRecord[];
  /** "" on the last page. */
  nextCursor: string;
}

/** Splits an aggregate into fixed time buckets. */
export interface AggregateBucket {
  /** created_at | completed_at */
  field: string;
  intervalMs: number;
  from?: Date;
  to?: Date;
}

/** Percentiles of a duration. */
export interface DurationMetric {
  /** queue | run | total */
  field: string;
  /** 0 < p < 100; default 50, 95, 99. */
  percentiles?: number[];
}

/** Groups and measures activities. */
export interface AggregateQuery {
  filter?: QueryFilter;
  groupBy?: string[];
  bucket?: AggregateBucket;
  count?: boolean;
  durations?: DurationMetric[];
  /** Groups; default 200, capped at 1000. */
  limit?: number;
}

export interface AggregateRow {
  key?: Record<string, string>;
  bucket?: Date;
  count: number;
  /** Duration field → percentile label ("p95") → milliseconds. */
  durations?: Record<string, Record<string, number>>;
}

export interface AggregateRows {
  rows: AggregateRow[];
  truncated: boolean;
}

/**
 * Lifecycle events in log order. "seq" (`EventRecord.id`) grows in insertion order, so
 * seq > cursor tails the log; a late commit can land below a seen seq, so tailers rescan
 * a window below their cursor.
 */
export interface EventQuery {
  filter?: QueryFilter;
  /** Newest first. */
  desc?: boolean;
  limit?: number;
  cursor?: string;
  includeDetail?: boolean;
}

export interface EventRecord {
  /** The log position (seq), a decimal integer. */
  id: string;
  cursor: string;
  activityId: string;
  /** Canonical. */
  type: string;
  at: Date;
  executorId: string;
  /** Only with `includeDetail`; absent when the event has none. */
  detail?: JsonValue;
}

export interface EventRecordPage {
  items: EventRecord[];
  nextCursor: string;
}

/** One durable step of an activity. */
export interface StepEntry {
  id: string;
  activityId: string;
  /** run | sleep | signal | other */
  kind: string;
  name: string;
  state: "Ok" | "Err";
  /** Only when requested. */
  data?: RecordValue;
  createdAt: Date;
}

export interface StepEntryPage {
  items: StepEntry[];
  nextCursor: string;
}

export interface ActivityTree {
  rootId: string;
  items: ActivityRecord[];
  truncated: boolean;
}

/** What a backend can evaluate. */
export interface QueryCapabilities {
  /** Field names; "metadata" covers metadata.<key>. */
  activityFilters: string[];
  activitySorts: string[];
  eventFilters: string[];
  groupBy: string[];
  buckets: string[];
  durations: string[];
}

/** Malformed or out-of-range query input, or a query feature the backend cannot evaluate. */
export class QueryError extends Error {
  constructor(
    readonly kind: "invalid_argument" | "unsupported",
    message: string,
    /** The offending request field, when there is one. */
    readonly field?: string,
  ) {
    super(message);
    this.name = "QueryError";
  }
}
