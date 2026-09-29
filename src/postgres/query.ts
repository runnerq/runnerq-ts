// QueryStorage for PostgreSQL across every queue (a port of runnerq-go's
// storage/postgres/query.go). Filters compile to parameterised SQL: only whitelisted
// expressions are ever spliced into SQL text.
import { RunnerQError } from "../errors.js";
import {
  businessKey,
  isTimestamp,
  parseInt64,
  parseUuid,
  type JsonValue,
} from "../codec.js";
import {
  QueryError,
  RecordEvent,
  type ActivityQuery,
  type ActivityRecord,
  type ActivityRecordPage,
  type ActivityTree,
  type AggregateQuery,
  type AggregateRow,
  type AggregateRows,
  type EventQuery,
  type EventRecord,
  type EventRecordPage,
  type QueryCapabilities,
  type QueryFilter,
  type RecordInclude,
  type RecordStatus,
  type RecordWait,
  type StepEntry,
  type StepEntryPage,
} from "../query.js";

type Row = Record<string, unknown>;
export type Run = (sql: string, values: unknown[]) => Promise<Row[]>;

export const defaultQueryLimit = 50;
export const maxQueryLimit = 1000;
export const defaultAggLimit = 200;
export const maxAggLimit = 1000;
export const maxFilterDepth = 8;
export const maxFilterNodes = 64;
export const maxInValues = 1000;
export const maxTreeNodes = 5000;

type Kind = "string" | "int" | "time" | "uuid" | "status" | "eventType";
interface Field {
  expr: string;
  kind: Kind;
  nullable?: boolean;
}

/** The latest state change we have a timestamp for. */
const updatedAtSQL = `GREATEST(a.created_at, a.started_at, a.completed_at, a.last_error_at)`;

const activityFields: Record<string, Field> = {
  id: { expr: "a.id", kind: "uuid" },
  type: { expr: "a.activity_type", kind: "string" },
  queue: { expr: "a.queue_name", kind: "string" },
  status: { expr: "a.status", kind: "status" },
  priority: { expr: "a.priority", kind: "int" },
  root_id: { expr: "COALESCE(a.root_activity_id, a.id)", kind: "uuid" },
  parent_id: { expr: "a.parent_activity_id", kind: "uuid", nullable: true },
  depth: { expr: "a.depth", kind: "int" },
  idempotency_key: {
    expr: "a.idempotency_key",
    kind: "string",
    nullable: true,
  },
  attempt: { expr: "(a.retry_count + 1)", kind: "int" },
  // max_retries holds the total attempts allowed (0: unlimited), in Go and TypeScript alike.
  max_attempts: {
    expr: "NULLIF(a.max_retries, 0)",
    kind: "int",
    nullable: true,
  },
  created_at: { expr: "a.created_at", kind: "time" },
  scheduled_for: { expr: "a.scheduled_at", kind: "time", nullable: true },
  started_at: { expr: "a.started_at", kind: "time", nullable: true },
  completed_at: { expr: "a.completed_at", kind: "time", nullable: true },
  updated_at: { expr: updatedAtSQL, kind: "time" },
  executor_id: {
    expr: "NULLIF(split_part(a.current_worker_id, ':', 1), '')",
    kind: "string",
    nullable: true,
  },
};

const eventFields: Record<string, Field> = {
  // The log position (cursor): increasing, so "seq > cursor" tails the log.
  seq: { expr: "e.id", kind: "int" },
  activity_id: { expr: "e.activity_id", kind: "uuid" },
  queue: { expr: "e.queue_name", kind: "string" },
  type: { expr: "e.event_type", kind: "eventType" },
  at: { expr: "e.created_at", kind: "time" },
  executor_id: {
    expr: "NULLIF(split_part(e.worker_id, ':', 1), '')",
    kind: "string",
    nullable: true,
  },
  // root_id is special-cased in predicate(): events carry no root.
  root_id: { expr: "", kind: "uuid" },
};

/** Stands in for NULL in nullable sort keys so keyset paging stays total. */
const sortSentinel = "0001-01-01T00:00:00Z";
const activitySorts: Record<string, { expr: string; kind: "time" | "int" }> = {
  created_at: { expr: "a.created_at", kind: "time" },
  completed_at: {
    expr: "COALESCE(a.completed_at, '0001-01-01 00:00:00+00'::timestamptz)",
    kind: "time",
  },
  priority: { expr: "a.priority", kind: "int" },
};

/** A canonical status and the internal ones it covers. */
const canonicalStatuses: Record<string, string[]> = {
  pending: ["pending"],
  scheduled: ["scheduled", "retrying"],
  running: ["processing"],
  waiting: ["waiting"],
  completed: ["completed"],
  failed: ["failed"],
  dead_letter: ["dead_letter"],
  cancelled: ["cancelled"],
};
const canonicalStatusSQL = `CASE a.status WHEN 'processing' THEN 'running' WHEN 'retrying' THEN 'scheduled' ELSE a.status END`;
function canonicalStatus(internal: string): RecordStatus {
  if (internal === "processing") return "running";
  if (internal === "retrying") return "scheduled";
  return internal as RecordStatus;
}

/** Internal event names (Go's and TypeScript's are the same) to canonical event types. */
const canonicalEvents: Record<string, string> = {
  Enqueued: RecordEvent.created,
  Scheduled: RecordEvent.scheduled,
  Dequeued: RecordEvent.attemptStarted,
  Completed: RecordEvent.attemptSucceeded,
  Failed: RecordEvent.attemptFailed,
  Retrying: RecordEvent.attemptFailed,
  DeadLetter: RecordEvent.deadLetter,
  Requeued: RecordEvent.leaseExpired,
  Yielded: RecordEvent.waitParked,
  Signaled: RecordEvent.signalReceived,
  LeaseExtended: RecordEvent.leaseExtended,
  ResultStored: RecordEvent.resultStored,
  SpawnLinked: RecordEvent.childLinked,
  Cancelled: RecordEvent.cancelled,
  Retried: RecordEvent.retried,
  Redriven: RecordEvent.redriven,
  RunNow: RecordEvent.runNow,
  Rescheduled: RecordEvent.rescheduled,
  PriorityChanged: RecordEvent.priorityChanged,
};
export function canonicalEvent(internal: string): string {
  return Object.hasOwn(canonicalEvents, internal)
    ? canonicalEvents[internal]!
    : "other." + internal.toLowerCase();
}
export function internalEvents(canonical: string): string[] {
  const out = Object.entries(canonicalEvents)
    .filter(([, c]) => c === canonical)
    .map(([internal]) => internal);
  // Best effort for types without a canonical name.
  if (!out.length && canonical.startsWith("other."))
    out.push(canonical.slice("other.".length));
  return out.sort();
}

export function queryCapabilities(): QueryCapabilities {
  return {
    activityFilters: [...Object.keys(activityFields), "metadata"].sort(),
    activitySorts: Object.keys(activitySorts).sort(),
    eventFilters: Object.keys(eventFields).sort(),
    groupBy: ["queue", "root", "status", "type"],
    buckets: ["completed_at", "created_at"],
    durations: ["queue", "run", "total"],
  };
}

/** Formats a percentile label as Go does ("p99.9", never exponent notation). */
function percentileLabel(p: number): string {
  let s = String(p);
  if (/e/i.test(s)) s = p.toFixed(20).replace(/\.?0+$/, "");
  return "p" + s;
}

type Values = { type: string; values: unknown[] } | null;

export class SqlBuilder {
  readonly args: unknown[] = [];
  private nodes = 0;
  arg(v: unknown, cast?: string): string {
    this.args.push(v);
    return `$${this.args.length}` + (cast ? `::${cast}` : "");
  }
  /** Compiles an optional filter to a SQL condition ("TRUE" when absent). */
  where(f: QueryFilter | undefined, on: "activities" | "events"): string {
    if (!f) return "TRUE";
    return this.compile(
      f,
      on === "activities" ? activityFields : eventFields,
      0,
    );
  }
  private compile(
    f: QueryFilter,
    fields: Record<string, Field>,
    depth: number,
  ): string {
    if (depth > maxFilterDepth)
      throw invalid("filter", `filter nests deeper than ${maxFilterDepth}`);
    if (++this.nodes > maxFilterNodes)
      throw invalid("filter", `filter has more than ${maxFilterNodes} terms`);
    const and = f.and ?? [],
      or = f.or ?? [];
    const set = [and.length > 0, or.length > 0, !!f.not, !!f.field].filter(
      Boolean,
    ).length;
    if (set !== 1)
      throw invalid(
        "filter",
        "each filter term needs exactly one of and, or, not or field",
      );
    if (and.length || or.length) {
      const terms = and.length ? and : or;
      const joiner = and.length ? " AND " : " OR ";
      return (
        "(" +
        terms.map((t) => this.compile(t, fields, depth + 1)).join(joiner) +
        ")"
      );
    }
    if (f.not) return "(NOT " + this.compile(f.not, fields, depth + 1) + ")";
    return this.predicate(f, fields);
  }
  private predicate(f: QueryFilter, fields: Record<string, Field>): string {
    const name = f.field!;
    const activities = fields === activityFields;
    let fd = Object.hasOwn(fields, name) ? fields[name] : undefined;
    if (!fd && activities && name.startsWith("metadata.")) {
      const key = name.slice("metadata.".length);
      if (!key) throw invalid(name, "metadata filters need a key");
      fd = {
        expr: "(a.metadata->>" + this.arg(key, "text") + ")",
        kind: "string",
        nullable: true,
      };
    }
    if (!fd)
      throw unsupported(name, `field ${JSON.stringify(name)} is not queryable`);
    if (name === "root_id" && fd.expr === "") return this.eventRoot(f);
    if (name === "idempotency_key" && activities) return this.idempotencyKey(f);
    const value = f.value ?? null;
    switch (f.op) {
      case "exists": {
        const want = existsValue(name, value);
        if (!fd.nullable) return String(want);
        return fd.expr + (want ? " IS NOT NULL" : " IS NULL");
      }
      case "eq":
      case "ne":
      case "in":
      case "nin": {
        const raw =
          f.op === "in" || f.op === "nin" ? list(name, f.op, value) : [value];
        const vals = convertValues(name, fd.kind, raw);
        const negate = f.op === "ne" || f.op === "nin";
        // Nothing can match (e.g. an id that is not a UUID).
        if (!vals) return String(negate);
        const cond = `${fd.expr} = ANY(${this.arg(vals.values, vals.type + "[]")})`;
        return negate ? `(NOT COALESCE(${cond}, false))` : cond;
      }
      case "lt":
      case "lte":
      case "gt":
      case "gte": {
        if (fd.kind !== "int" && fd.kind !== "time")
          throw unsupported(
            name,
            `${f.op} is only supported on numeric and time fields`,
          );
        const vals = convertValues(name, fd.kind, [value])!;
        const op = { lt: "<", lte: "<=", gt: ">", gte: ">=" }[f.op];
        return `${fd.expr} ${op} ${this.arg(vals.values[0], vals.type)}`;
      }
      case "prefix": {
        if (fd.kind !== "string")
          throw unsupported(name, "prefix is only supported on string fields");
        if (typeof value !== "string")
          throw invalid(name, "prefix takes a string value");
        const p = this.arg(value, "text");
        return `left(${fd.expr}, char_length(${p})) = ${p}`;
      }
    }
    throw unsupported(
      name,
      `operator ${JSON.stringify(f.op ?? "")} is not supported`,
    );
  }
  /**
   * Matches the application's key (see applicationIdempotencyKey) as stored: a v2 business
   * key for the row's type, or as is; step-derived keys never match. Encoded keys can't be
   * matched by prefix or substring.
   */
  private idempotencyKey(f: QueryFilter): string {
    const stored = "a.idempotency_key";
    const hasKey = `(${stored} IS NOT NULL AND ${stored} <> '' AND left(${stored}, ${stepKeyPrefix.length}) <> ${this.arg(stepKeyPrefix, "text")})`;
    const name = f.field!;
    const value = f.value ?? null;
    switch (f.op) {
      case "exists":
        return existsValue(name, value) ? hasKey : `(NOT ${hasKey})`;
      case "eq":
      case "ne":
      case "in":
      case "nin": {
        const raw =
          f.op === "in" || f.op === "nin" ? list(name, f.op, value) : [value];
        if (!raw.every((v) => typeof v === "string"))
          throw invalid(name, "idempotency_key takes string values");
        // The v2 encoding, computed per row for its type: base64 without padding
        // (Postgres wraps base64 at 76 characters, so drop newlines).
        const v2 = `'rq:key:v2:' || rtrim(translate(encode(convert_to(octet_length(k) || ':' || k || a.activity_type, 'UTF8'), 'base64'), E'\\n', ''), '=')`;
        const match = `EXISTS (SELECT 1 FROM unnest(${this.arg(raw, "text[]")}) AS keys(k) WHERE ${stored} = ${v2} OR (${stored} = k AND left(${stored}, 10) <> 'rq:key:v2:'))`;
        const cond = `(${hasKey} AND ${match})`;
        return f.op === "ne" || f.op === "nin" ? `(NOT ${cond})` : cond;
      }
    }
    throw unsupported(
      name,
      "idempotency_key supports eq, ne, in, nin and exists: stored keys are encoded, so prefix and contains can't match",
    );
  }
  /** Filters events to the trees rooted at the given ids. */
  private eventRoot(f: QueryFilter): string {
    const name = f.field!;
    const value = f.value ?? null;
    let raw: unknown[];
    if (f.op === "eq") raw = [value];
    else if (f.op === "in") {
      if (!Array.isArray(value) || value.length > maxInValues)
        throw invalid(name, "in takes an array of at most 1000 values");
      raw = value;
    } else throw unsupported(name, "root_id supports eq and in");
    const vals = convertValues(name, "uuid", raw);
    if (!vals) return "false";
    const p = this.arg(vals.values, "uuid[]");
    return `e.activity_id IN (SELECT x.id FROM runnerq_activities x WHERE x.id = ANY(${p}) OR x.root_activity_id = ANY(${p}))`;
  }
}

function invalid(field: string, message: string): QueryError {
  return new QueryError("invalid_argument", message, field);
}
function unsupported(field: string, message: string): QueryError {
  return new QueryError("unsupported", message, field);
}
function existsValue(field: string, value: unknown): boolean {
  if (value === null) return true;
  if (typeof value !== "boolean")
    throw invalid(field, "exists takes a boolean value");
  return value;
}
function list(field: string, op: string, value: unknown): unknown[] {
  if (!Array.isArray(value)) throw invalid(field, `${op} takes an array value`);
  if (value.length > maxInValues)
    throw invalid(field, `${op} is limited to ${maxInValues} values`);
  return value;
}

/** Decoded JSON values as typed SQL parameters for the field; null when none can match. */
function convertValues(field: string, kind: Kind, raw: unknown[]): Values {
  switch (kind) {
    case "string":
      if (!raw.every((v) => typeof v === "string"))
        throw invalid(field, "expected a string");
      return { type: "text", values: raw };
    case "int":
      return {
        type: "bigint",
        values: raw.map((v) => {
          // bigint: the stream tailer's own seq bounds, never from a request.
          if (typeof v === "bigint") return v.toString();
          if (
            typeof v !== "number" ||
            !Number.isInteger(v) ||
            Math.abs(v) > 2 ** 53
          )
            throw invalid(field, "expected an integer");
          return v;
        }),
      };
    case "time":
      if (!raw.every(isTimestamp))
        throw invalid(field, "expected an RFC 3339 timestamp");
      return { type: "timestamptz", values: raw };
    case "uuid": {
      const out: string[] = [];
      for (const v of raw) {
        if (typeof v !== "string") throw invalid(field, "expected a string id");
        const id = parseUuid(v);
        if (id) out.push(id);
      }
      return out.length ? { type: "uuid", values: out } : null;
    }
    case "status": {
      const out: string[] = [];
      for (const v of raw) {
        if (typeof v !== "string" || !Object.hasOwn(canonicalStatuses, v))
          throw invalid(field, `unknown status ${String(v)}`);
        out.push(...canonicalStatuses[v]!);
      }
      return { type: "text", values: out };
    }
    case "eventType": {
      const out: string[] = [];
      for (const v of raw) {
        if (typeof v !== "string")
          throw invalid(field, "expected an event type string");
        out.push(...internalEvents(v));
      }
      return out.length ? { type: "text", values: out } : null;
    }
  }
}

export function encodeCursor(v: unknown): string {
  return Buffer.from(JSON.stringify(v)).toString("base64url");
}
function decodeCursor(s: string): Record<string, unknown> {
  const bad = () => invalid("cursor", "invalid cursor");
  // Go's RawURLEncoding: no padding, URL alphabet only.
  if (!/^[A-Za-z0-9_-]*$/.test(s) || s.length % 4 === 1) throw bad();
  try {
    const v = JSON.parse(Buffer.from(s, "base64url").toString("utf8"));
    if (v === null || typeof v !== "object" || Array.isArray(v)) throw bad();
    return v;
  } catch {
    throw bad();
  }
}
const zeroUuid = "00000000-0000-0000-0000-000000000000";
/** A cursor's id ("i"); absent is the zero UUID, as Go decodes it. */
function cursorId(c: Record<string, unknown>): string {
  if (c.i === undefined || c.i === null) return zeroUuid;
  const id = typeof c.i === "string" ? parseUuid(c.i) : undefined;
  if (!id) throw invalid("cursor", "invalid cursor");
  return id;
}
/** A cursor's time ("t"), or undefined when absent. */
function cursorTime(c: Record<string, unknown>): string | undefined {
  if (c.t === undefined || c.t === null) return undefined;
  if (!isTimestamp(c.t)) throw invalid("cursor", "invalid cursor");
  return c.t;
}
/** A timestamp column as RFC 3339 with the database's microseconds, formatted as Go's. */
function timeText(expr: string): string {
  return `to_char(${expr} AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`;
}
function trimTime(s: string): string {
  return s.replace(/\.?0+Z$/, "Z");
}

export function clampLimit(
  limit: number | undefined,
  def: number,
  max: number,
): number {
  if (!limit || limit <= 0) return def;
  return Math.min(Math.trunc(limit), max);
}

const businessKeyPrefix = "rq:key:v2:";
/** Starts the keys the engine derives for activities spawned by a step. */
const stepKeyPrefix = "rq:step:";
/** Decodes a key `businessKey` encoded; anything it could not have produced is undefined. */
function decodeBusinessKey(
  encoded: string,
): { key: string; type: string } | undefined {
  if (!encoded.startsWith(businessKeyPrefix)) return undefined;
  const data = Buffer.from(encoded.slice(businessKeyPrefix.length), "base64");
  const colon = data.indexOf(":");
  if (colon < 0) return undefined;
  const n = Number(data.subarray(0, colon).toString("utf8"));
  if (!Number.isSafeInteger(n) || n < 0 || n > data.length - colon - 1)
    return undefined;
  const key = data.subarray(colon + 1, colon + 1 + n).toString("utf8");
  const type = data.subarray(colon + 1 + n).toString("utf8");
  try {
    // Only an exact re-encoding proves the split (and the base64) canonical.
    return businessKey(key, type) === encoded ? { key, type } : undefined;
  } catch {
    return undefined; // an empty key or type: not one businessKey wrote
  }
}
/**
 * The key the application set, from the stored key and the activity's type: a v2 business
 * key decoded, a key written through the storage API as is, or "" for a step child's key.
 */
export function applicationIdempotencyKey(
  stored: string,
  type: string,
): string {
  if (!stored || stored.startsWith(stepKeyPrefix)) return "";
  const business = decodeBusinessKey(stored);
  return business?.type === type ? business.key : stored;
}

function activitySelect(inc: RecordInclude): { cols: string; joins: string } {
  let cols = `a.id, a.activity_type, a.queue_name, a.status, a.priority,
    COALESCE(a.root_activity_id, a.id) AS root_id, a.parent_activity_id, a.depth, a.idempotency_key,
    a.retry_count, a.max_retries, a.created_at, a.scheduled_at, a.started_at, a.completed_at,
    ${updatedAtSQL} AS updated_at, a.timeout_seconds, a.lease_deadline_ms, a.current_worker_id, a.metadata,
    y.detail AS yield_detail`;
  // The latest park reason, only for waiting rows.
  let joins = ` LEFT JOIN LATERAL (
      SELECT e.detail FROM runnerq_events e
      WHERE e.activity_id = a.id AND e.event_type = 'Yielded'
      ORDER BY e.created_at DESC, e.id DESC LIMIT 1
    ) y ON a.status = 'waiting'`;
  if (inc.payload) {
    // JSON as text: SQL NULL (no input row) stays distinct from a JSON null.
    cols += `, i.payload::text AS payload, i.serialization AS payload_serialization`;
    joins += " LEFT JOIN runnerq_inputs i ON i.activity_id = a.id";
  }
  if (inc.lastError) cols += ", a.last_error, a.last_error_at";
  if (inc.result) {
    cols += `, r.state AS result_state, r.data::text AS result_data, r.serialization AS result_serialization`;
    joins += " LEFT JOIN runnerq_results r ON r.activity_id = a.id";
  }
  return { cols, joins };
}

function date(v: unknown): Date | undefined {
  return v === null || v === undefined ? undefined : new Date(v as Date);
}
function parseJsonText(v: unknown): JsonValue | undefined {
  return typeof v === "string" ? (JSON.parse(v) as JsonValue) : undefined;
}

function errorKind(status: string): string {
  switch (status) {
    case "retrying":
    case "scheduled":
    case "pending":
    case "processing":
      return "retryable";
    case "failed":
      return "non_retryable";
    case "dead_letter":
      return "dead_letter";
  }
  return "";
}

export function parseWait(detail: unknown): RecordWait {
  const w: RecordWait = { kind: "other", name: "" };
  if (!detail || typeof detail !== "object" || Array.isArray(detail)) return w;
  const d = detail as Record<string, unknown>;
  const kind = typeof d.kind === "string" ? d.kind : "";
  const step = typeof d.step === "string" ? d.step : "";
  if (kind === "sleep" || kind === "signal") w.kind = kind;
  else if (kind === "await") w.kind = "children";
  w.name = step;
  // Go records "<kind>:<name>" steps, TypeScript the bare name: strip only that prefix.
  if (kind !== "await" && step.startsWith(kind + ":"))
    w.name = step.slice(kind.length + 1);
  if (isTimestamp(d.wake_at)) w.until = new Date(d.wake_at);
  return w;
}

function scanRecord(r: Row, inc: RecordInclude): ActivityRecord {
  const status = r.status as string;
  const type = r.activity_type as string;
  const maxRetries = Number(r.max_retries);
  const rec: ActivityRecord = {
    id: r.id as string,
    type,
    queue: r.queue_name as string,
    status: canonicalStatus(status),
    priority: Number(r.priority),
    rootId: r.root_id as string,
    depth: Number(r.depth),
    idempotencyKey: r.idempotency_key
      ? applicationIdempotencyKey(r.idempotency_key as string, type)
      : "",
    attempt: Number(r.retry_count) + 1,
    createdAt: date(r.created_at)!,
    updatedAt: date(r.updated_at)!,
    timeoutMs: Number(r.timeout_seconds) * 1000,
    executorId: "",
  };
  if (r.parent_activity_id) rec.parentId = r.parent_activity_id as string;
  if (maxRetries > 0) rec.maxAttempts = maxRetries;
  const scheduled = date(r.scheduled_at),
    started = date(r.started_at),
    completed = date(r.completed_at);
  if (scheduled) rec.scheduledFor = scheduled;
  if (started) rec.startedAt = started;
  if (completed) rec.completedAt = completed;
  if (r.lease_deadline_ms !== null && status === "processing")
    rec.leaseExpiresAt = new Date(Number(r.lease_deadline_ms));
  if (r.current_worker_id && status === "processing")
    rec.executorId = (r.current_worker_id as string).split(":")[0]!;
  if (r.metadata && typeof r.metadata === "object") {
    const metadata: Record<string, string> = {};
    for (const [k, v] of Object.entries(r.metadata))
      if (typeof v === "string") metadata[k] = v;
    rec.metadata = metadata;
  }
  if (status === "waiting") rec.wait = parseWait(r.yield_detail);
  if (inc.payload && r.payload_serialization !== null)
    rec.payload = {
      serialization: r.payload_serialization as string,
      data: parseJsonText(r.payload) ?? null,
    };
  if (inc.lastError && r.last_error !== null) {
    rec.lastError = {
      message: r.last_error as string,
      kind: errorKind(status),
    };
    const at = date(r.last_error_at);
    if (at) rec.lastError.at = at;
  }
  if (inc.result && r.result_state !== null) {
    rec.result = {
      state: r.result_state === "Ok" ? "Ok" : "Err",
      serialization: r.result_serialization as string,
    };
    const data = parseJsonText(r.result_data);
    if (data !== undefined) rec.result.data = data;
  }
  return rec;
}

/** The query layer over `run`, which reports storage failures, never raw driver errors. */
export class PostgresQueries {
  constructor(private readonly run: Run) {}

  /** A filtered, sorted, keyset-paginated activity query across every queue. */
  async activities(q: ActivityQuery): Promise<ActivityRecordPage> {
    const sort = q.sort ?? { field: "created_at", desc: true };
    const sf = Object.hasOwn(activitySorts, sort.field)
      ? activitySorts[sort.field]!
      : undefined;
    if (!sf)
      throw unsupported(
        sort.field,
        `cannot sort by ${JSON.stringify(sort.field)}`,
      );
    const limit = clampLimit(q.limit, defaultQueryLimit, maxQueryLimit);
    const sb = new SqlBuilder();
    let where = sb.where(q.filter, "activities");
    const [cmp, dir] = sort.desc ? ["<", "DESC"] : [">", "ASC"];
    if (q.cursor) {
      const c = decodeCursor(q.cursor);
      if (c.s !== sort.field || !!c.d !== sort.desc)
        throw invalid("cursor", "cursor was issued for a different sort");
      let pv: string;
      const t = cursorTime(c);
      if (sf.kind === "time" && t !== undefined) pv = sb.arg(t, "timestamptz");
      else if (
        sf.kind === "int" &&
        typeof c.n === "number" &&
        Number.isSafeInteger(c.n)
      )
        pv = sb.arg(c.n, "bigint");
      else throw invalid("cursor", "invalid cursor");
      const pid = sb.arg(cursorId(c), "uuid");
      where += ` AND (${sf.expr} ${cmp} ${pv} OR (${sf.expr} = ${pv} AND a.id ${cmp} ${pid}))`;
    }
    const inc = q.include ?? {};
    const { cols, joins } = activitySelect(inc);
    const key = sf.kind === "time" ? timeText(sf.expr) : sf.expr;
    const rows = await this.run(
      `SELECT ${cols}, ${key} AS sort_key FROM runnerq_activities a${joins} WHERE ${where}
      ORDER BY ${sf.expr} ${dir}, a.id ${dir} LIMIT ${limit + 1}`,
      sb.args,
    );
    const page: ActivityRecordPage = { items: [], nextCursor: "" };
    for (const [i, row] of rows.entries()) {
      if (i === limit) {
        // A row beyond the page: there is more. Cursor at the last kept row.
        const last = rows[limit - 1]!;
        const c: Record<string, unknown> = { s: sort.field, d: sort.desc };
        if (sf.kind === "time")
          c.t = trimTime((last.sort_key as string | null) ?? sortSentinel);
        else c.n = Number(last.sort_key);
        c.i = last.id;
        page.nextCursor = encodeCursor(c);
        break;
      }
      page.items.push(scanRecord(row, inc));
    }
    return page;
  }

  async count(
    filter: QueryFilter | undefined,
    limit: number,
  ): Promise<{ count: number; exact: boolean }> {
    const max = limit > 0 ? Math.trunc(limit) : 0;
    const sb = new SqlBuilder();
    const where = sb.where(filter, "activities");
    const rows = await this.run(
      `SELECT count(*) AS n FROM (SELECT 1 FROM runnerq_activities a WHERE ${where} LIMIT ${max + 1}) s`,
      sb.args,
    );
    const n = Number(rows[0]?.n ?? 0);
    return n > max ? { count: max, exact: false } : { count: n, exact: true };
  }

  async aggregate(q: AggregateQuery): Promise<AggregateRows> {
    if (!q.count && !q.durations?.length)
      throw invalid("metrics", "ask for at least one metric");
    const sb = new SqlBuilder();
    let where = sb.where(q.filter, "activities");
    const groupBy = q.groupBy ?? [];
    const selects: string[] = [];
    const groups: string[] = [];
    for (const g of groupBy) {
      const expr = Object.hasOwn(groupExprs, g) ? groupExprs[g] : undefined;
      if (!expr) throw unsupported(g, `cannot group by ${JSON.stringify(g)}`);
      selects.push(expr);
      groups.push(String(selects.length));
    }
    const bucket = q.bucket;
    if (bucket) {
      const field = Object.hasOwn(bucketFields, bucket.field)
        ? bucketFields[bucket.field]
        : undefined;
      if (!field)
        throw unsupported(
          bucket.field,
          `cannot bucket by ${JSON.stringify(bucket.field)}`,
        );
      if (!(bucket.intervalMs >= 1000))
        throw invalid(
          "bucket.interval_ms",
          "bucket interval must be at least 1s",
        );
      const ms = sb.arg(Math.trunc(bucket.intervalMs), "float8");
      selects.push(
        `to_timestamp(floor(EXTRACT(EPOCH FROM ${field}) * 1000 / ${ms}) * ${ms} / 1000)`,
      );
      groups.push(String(selects.length));
      where += ` AND ${field} IS NOT NULL`;
      if (bucket.from)
        where += ` AND ${field} >= ${sb.arg(bucket.from, "timestamptz")}`;
      if (bucket.to)
        where += ` AND ${field} < ${sb.arg(bucket.to, "timestamptz")}`;
    }
    selects.push("count(*)");
    const countCol = selects.length;
    const durs: { field: string; pcts: number[] }[] = [];
    for (const d of q.durations ?? []) {
      const expr = Object.hasOwn(durationExprs, d.field)
        ? durationExprs[d.field]
        : undefined;
      if (!expr)
        throw unsupported(
          d.field,
          `unknown duration ${JSON.stringify(d.field)}`,
        );
      const pcts = d.percentiles?.length ? d.percentiles : [50, 95, 99];
      for (const p of pcts)
        if (!(p > 0 && p < 100))
          throw invalid("percentiles", "percentiles must be between 0 and 100");
      selects.push(
        `percentile_cont(${sb.arg(
          pcts.map((p) => p / 100),
          "float8[]",
        )}) WITHIN GROUP (ORDER BY ${expr})`,
      );
      durs.push({ field: d.field, pcts });
    }
    const limit = clampLimit(q.limit, defaultAggLimit, maxAggLimit);
    let sql =
      "SELECT " +
      selects.map((s, i) => `${s} AS c${i + 1}`).join(", ") +
      " FROM runnerq_activities a WHERE " +
      where;
    if (groups.length) sql += " GROUP BY " + groups.join(", ");
    let order = `${countCol} DESC`;
    if (bucket) order = `${groupBy.length + 1} ASC, ${order}`;
    sql += ` ORDER BY ${order} LIMIT ${limit + 1}`;
    const rows = await this.run(sql, sb.args);
    const out: AggregateRows = { rows: [], truncated: false };
    for (const [i, r] of rows.entries()) {
      if (i === limit) {
        out.truncated = true;
        break;
      }
      let col = 1;
      const key: Record<string, string> = {};
      for (const g of groupBy) {
        const v = r[`c${col++}`];
        if (v !== null && v !== undefined) key[g] = String(v);
      }
      const row: AggregateRow = { count: 0 };
      if (groupBy.length) row.key = key;
      if (bucket) {
        const b = date(r[`c${col}`]);
        if (b) row.bucket = b;
        col++;
      }
      row.count = Number(r[`c${col++}`]);
      for (const d of durs) {
        const vals: Record<string, number> = {};
        const got = r[`c${col++}`] as (number | null)[] | null;
        d.pcts.forEach((p, j) => {
          const v = got?.[j];
          if (v !== null && v !== undefined)
            vals[percentileLabel(p)] = Number(v);
        });
        (row.durations ??= {})[d.field] = vals;
      }
      out.rows.push(row);
    }
    return out;
  }

  async events(q: EventQuery): Promise<EventRecordPage> {
    const limit = clampLimit(q.limit, defaultQueryLimit, maxQueryLimit);
    const sb = new SqlBuilder();
    let where = sb.where(q.filter, "events");
    const [cmp, dir] = q.desc ? ["<", "DESC"] : [">", "ASC"];
    if (q.cursor) {
      if (parseInt64(q.cursor) === undefined)
        throw invalid("cursor", "invalid cursor");
      where += ` AND e.id ${cmp} ${sb.arg(q.cursor, "bigint")}`;
    }
    const detail = q.includeDetail ? "e.detail::text" : "NULL::text";
    const rows = await this.run(
      `SELECT e.id::text AS id, e.activity_id, e.event_type, e.created_at, e.worker_id, ${detail} AS detail
      FROM runnerq_events e WHERE ${where} ORDER BY e.id ${dir} LIMIT ${limit + 1}`,
      sb.args,
    );
    const page: EventRecordPage = { items: [], nextCursor: "" };
    for (const [i, r] of rows.entries()) {
      if (i === limit) {
        page.nextCursor = page.items[limit - 1]!.cursor;
        break;
      }
      const ev: EventRecord = {
        id: r.id as string,
        cursor: r.id as string,
        activityId: r.activity_id as string,
        type: canonicalEvent(r.event_type as string),
        at: date(r.created_at)!,
        executorId: r.worker_id ? (r.worker_id as string).split(":")[0]! : "",
      };
      const d = parseJsonText(r.detail);
      if (d !== undefined) ev.detail = d;
      page.items.push(ev);
    }
    return page;
  }

  async steps(
    activityId: string,
    includeData: boolean,
    limit: number,
    cursor: string,
  ): Promise<StepEntryPage> {
    const n = clampLimit(limit, defaultQueryLimit, maxQueryLimit);
    const id = parseUuid(activityId);
    if (!id) return { items: [], nextCursor: "" };
    const sb = new SqlBuilder();
    let where = `owner_activity_id = ${sb.arg(id, "uuid")} AND step IS NOT NULL`;
    if (cursor) {
      const c = decodeCursor(cursor);
      const t = cursorTime(c) ?? sortSentinel;
      const pt = sb.arg(t, "timestamptz"),
        pid = sb.arg(cursorId(c), "uuid");
      where += ` AND (created_at > ${pt} OR (created_at = ${pt} AND activity_id > ${pid}))`;
    }
    const data = includeData ? "data::text" : "NULL::text";
    const rows = await this.run(
      `SELECT activity_id, step, state, ${data} AS data, serialization, created_at,
      ${timeText("created_at")} AS sort_key FROM runnerq_results
      WHERE ${where} ORDER BY created_at, activity_id LIMIT ${n + 1}`,
      sb.args,
    );
    const page: StepEntryPage = { items: [], nextCursor: "" };
    for (const [i, r] of rows.entries()) {
      if (i === n) {
        const last = rows[n - 1]!;
        page.nextCursor = encodeCursor({
          t: trimTime(last.sort_key as string),
          i: last.activity_id,
        });
        break;
      }
      const step = r.step as string;
      const colon = step.indexOf(":");
      let kind = colon >= 0 ? step.slice(0, colon) : step;
      const name = colon >= 0 ? step.slice(colon + 1) : "";
      if (!["run", "sleep", "signal"].includes(kind)) kind = "other";
      const s: StepEntry = {
        id: r.activity_id as string,
        activityId: id,
        kind,
        name,
        state: r.state === "Ok" ? "Ok" : "Err",
        createdAt: date(r.created_at)!,
      };
      if (includeData && r.data !== null)
        s.data = {
          serialization: r.serialization as string,
          data: parseJsonText(r.data)!,
        };
      page.items.push(s);
    }
    return page;
  }

  async tree(
    activityId: string,
    inc: RecordInclude,
    maxNodes: number,
  ): Promise<ActivityTree> {
    const max = clampLimit(maxNodes, maxTreeNodes, maxTreeNodes);
    const id = parseUuid(activityId);
    const found = id
      ? await this.run(
          "SELECT COALESCE(root_activity_id, id) AS root FROM runnerq_activities WHERE id = $1::uuid",
          [id],
        )
      : [];
    if (!found[0])
      throw new RunnerQError("not_found", `activity ${activityId} not found`);
    const root = found[0].root as string;
    const { cols, joins } = activitySelect(inc);
    const rows = await this.run(
      `SELECT ${cols} FROM runnerq_activities a${joins}
      WHERE a.id = $1::uuid OR a.root_activity_id = $1::uuid
      ORDER BY a.depth, a.created_at, a.id LIMIT ${max + 1}`,
      [root],
    );
    const tree: ActivityTree = { rootId: root, items: [], truncated: false };
    for (const [i, r] of rows.entries()) {
      if (i === max) {
        tree.truncated = true;
        break;
      }
      tree.items.push(scanRecord(r, inc));
    }
    return tree;
  }
}

const groupExprs: Record<string, string> = {
  status: canonicalStatusSQL,
  type: "a.activity_type",
  queue: "a.queue_name",
  root: "CASE WHEN a.parent_activity_id IS NULL THEN 'true' ELSE 'false' END",
};
const durationExprs: Record<string, string> = {
  queue:
    "EXTRACT(EPOCH FROM (a.started_at - COALESCE(a.scheduled_at, a.created_at))) * 1000",
  run: "EXTRACT(EPOCH FROM (a.completed_at - a.started_at)) * 1000",
  total: "EXTRACT(EPOCH FROM (a.completed_at - a.created_at)) * 1000",
};
const bucketFields: Record<string, string> = {
  created_at: "a.created_at",
  completed_at: "a.completed_at",
};
