// The query messages (activities.*, steps.list, events.list, results.get, trees.get),
// served from a QueryStorage: a port of runnerq-go's conductor/handlers.go.
import { isTimestamp, json, parseUuid, type JsonValue } from "../codec.js";
import { decode, type SerializationFormat } from "../serialization.js";
import { serializationSuperjson } from "../spec.js";
import type { Storage } from "../storage.js";
import type {
  ActivityRecord,
  EventRecord,
  QuerySort,
  QueryStorage,
  RecordInclude,
  StepEntry,
} from "../query.js";
import { decodeRequest } from "./decode.js";
import type {
  Activity,
  ActivityPage,
  AggregateGroup,
  AggregateRequest,
  AggregateResult,
  CountRequest,
  CountResult,
  Event as WireEvent,
  EventPage,
  GetRequest,
  Query,
  Result,
  ResultRequest,
  Sort,
  Step,
  StepKind,
  StepPage,
  StepsRequest,
  Tree,
  TreeRequest,
} from "./protocol.js";
import { specs } from "./specs.js";
import { WireError, ts, type Routes } from "./wire.js";

/** Bounds steps and events embedded in activities.get. */
const maxEmbedded = 1000;
/** activities.count stops counting here and says the count is not exact. */
const countLimit = 100_000;

const recordIncludes = ["last_error", "payload", "result"];
const getIncludes = ["events", "last_error", "payload", "result", "steps"];

/** At most one sort key (the backend adds the tiebreaker); the default order is desc. */
function oneSort(sorts?: Sort[]): QuerySort | undefined {
  if (!sorts?.length) return undefined;
  if (sorts.length > 1)
    throw fieldError("unsupported", "sort", "only one sort key is supported");
  const s = sorts[0]!;
  const field = s.field ?? "";
  if (!s.order || s.order === "desc") return { field, desc: true };
  if (s.order === "asc") return { field, desc: false };
  throw fieldError("invalid_argument", "sort", "order must be asc or desc");
}

function fieldError(
  code: "invalid_argument" | "unsupported" | "forbidden",
  field: string,
  message: string,
): WireError {
  return new WireError(code, message, { field });
}
const quote = (s: string) => JSON.stringify(s);

/**
 * The plain JSON a stored value shows the user: "json-v1" as stored; "superjson-v1" decoded,
 * or, when that isn't plain JSON (Dates, Maps, bigints, recipes), SuperJSON's JSON-safe
 * `json` part without its type metadata. A value that won't decode is shown as stored.
 */
export function plainJson(serialization: string, data: JsonValue): JsonValue {
  if (serialization !== serializationSuperjson) return data;
  const part =
    data && typeof data === "object" && !Array.isArray(data) && "json" in data
      ? (data.json ?? null)
      : data;
  try {
    return json(
      decode({ serialization: serialization as SerializationFormat, data }),
    );
  } catch {
    return part;
  }
}

export function toResult(r: {
  state: "Ok" | "Err";
  serialization: string;
  data?: JsonValue;
}): Result {
  const data =
    r.data === undefined ? undefined : plainJson(r.serialization, r.data);
  if (r.state === "Ok")
    return data === undefined ? { state: "ok" } : { state: "ok", data };
  if (data && typeof data === "object" && !Array.isArray(data)) {
    const { error, type } = data as Record<string, JsonValue>;
    if (
      typeof error === "string" &&
      error !== "" &&
      (type === undefined || type === null || typeof type === "string")
    )
      return {
        state: "error",
        error: { message: error, ...(type ? { kind: type } : {}) },
      };
  }
  return data === undefined ? { state: "error" } : { state: "error", data };
}

/** Drops undefined fields: fields that don't apply are omitted, never null. */
function compact<T extends object>(v: T): T {
  for (const k in v) if (v[k] === undefined) delete v[k];
  return v;
}
/** "" is unset, as Go's omitempty. */
const opt = (s?: string) => s || undefined;
const tsp = (d?: Date) => (d ? ts(d) : undefined);

export function toActivity(r: ActivityRecord): Activity {
  return compact<Activity>({
    id: r.id,
    type: r.type,
    queue: opt(r.queue),
    status: r.status,
    priority: r.priority,
    root_id: opt(r.rootId),
    parent_id: opt(r.parentId),
    depth: r.depth,
    idempotency_key: opt(r.idempotencyKey),
    attempt: r.attempt,
    max_attempts: r.maxAttempts, // unlimited: omitted
    created_at: ts(r.createdAt),
    scheduled_for: tsp(r.scheduledFor),
    started_at: tsp(r.startedAt),
    completed_at: tsp(r.completedAt),
    updated_at: tsp(r.updatedAt),
    timeout_ms: r.timeoutMs || undefined,
    lease_expires_at: tsp(r.leaseExpiresAt),
    executor_id: opt(r.executorId),
    wait:
      r.wait &&
      compact({
        kind: r.wait.kind,
        name: opt(r.wait.name),
        until: tsp(r.wait.until),
      }),
    metadata:
      r.metadata && Object.keys(r.metadata).length ? r.metadata : undefined,
    last_error:
      r.lastError &&
      compact({
        message: opt(r.lastError.message),
        kind: opt(r.lastError.kind),
        at: tsp(r.lastError.at),
      }),
    payload: r.payload && plainJson(r.payload.serialization, r.payload.data),
    result: r.result && toResult(r.result),
  });
}

export function toStep(s: StepEntry): Step {
  return compact<Step>({
    id: s.id,
    activity_id: s.activityId,
    name: s.name,
    kind: s.kind as StepKind,
    status: s.state === "Ok" ? "completed" : "failed",
    created_at: ts(s.createdAt),
    result:
      s.data &&
      toResult({
        state: s.state,
        serialization: s.data.serialization,
        data: s.data.data,
      }),
  });
}

export function toEvent(e: EventRecord): WireEvent {
  return compact<WireEvent>({
    id: e.cursor,
    cursor: e.cursor,
    activity_id: e.activityId,
    type: e.type,
    at: ts(e.at),
    executor_id: opt(e.executorId),
    detail: e.detail,
  });
}

/** The query handlers and what they advertise, over `qs` (results through `storage`). */
export class Queries {
  constructor(
    private readonly qs: QueryStorage,
    private readonly storage: Storage,
    private readonly metadataOnly: () => boolean,
  ) {}

  /** Every query and stream request type; stream ones have no handler (Streams serve them). */
  routes(): Routes {
    const qc = this.qs.queryCapabilities();
    const nonEmpty = (list: string[]) => (list.length ? list : undefined);
    const filters = nonEmpty(qc.activityFilters);
    const eventFilters = nonEmpty(qc.eventFilters);
    return {
      "activities.list": {
        capability: {
          v: 1,
          filters,
          sorts: nonEmpty(qc.activitySorts),
          include: recordIncludes,
        },
        handler: (d) => this.activitiesList(d),
      },
      "activities.get": {
        capability: { v: 1, include: getIncludes },
        handler: (d) => this.activitiesGet(d),
      },
      "activities.count": {
        capability: { v: 1, filters },
        handler: (d) => this.activitiesCount(d),
      },
      "activities.aggregate": {
        capability: {
          v: 1,
          filters,
          group_by: nonEmpty(qc.groupBy),
          buckets: nonEmpty(qc.buckets),
          metrics: ["count", ...qc.durations.map((d) => "duration." + d)],
        },
        handler: (d) => this.activitiesAggregate(d),
      },
      "steps.list": {
        capability: { v: 1, include: ["result"] },
        handler: (d) => this.stepsList(d),
      },
      "events.list": {
        capability: {
          v: 1,
          filters: eventFilters,
          sorts: ["at"],
          include: ["detail"],
        },
        handler: (d) => this.eventsList(d),
      },
      "results.get": {
        capability: { v: 1 },
        handler: (d) => this.resultsGet(d),
      },
      "trees.get": {
        capability: { v: 1, include: recordIncludes },
        handler: (d) => this.treesGet(d),
      },
      "events.subscribe": { capability: { v: 1, filters: eventFilters } },
      "events.unsubscribe": { capability: { v: 1 } },
    };
  }

  /** Checks includes against `allowed`; customer data is refused in metadata-only mode. */
  private includes(list: string[] | undefined, allowed: string[]): Set<string> {
    const out = new Set<string>();
    for (const inc of list ?? []) {
      if (!allowed.includes(inc))
        throw fieldError(
          "unsupported",
          "include",
          `cannot include ${quote(inc)}`,
        );
      if (
        this.metadataOnly() &&
        ["payload", "result", "last_error", "detail"].includes(inc)
      )
        throw fieldError(
          "forbidden",
          "include",
          `${inc} is not sent in metadata-only mode`,
        );
      out.add(inc);
    }
    return out;
  }

  private async activitiesList(data: unknown): Promise<ActivityPage> {
    const q = decodeRequest<Query>(specs.Query, data);
    const inc = this.includes(q.include, recordIncludes);
    const sort = oneSort(q.sort);
    const res = await this.qs.queryActivities({
      filter: q.filter,
      include: recordInclude(inc),
      limit: q.limit,
      cursor: q.cursor,
      ...(sort && { sort }),
    });
    return page(res.items.map(toActivity), res.nextCursor);
  }

  private async activitiesGet(data: unknown): Promise<Activity> {
    const req = decodeRequest<GetRequest>(specs.GetRequest, data);
    const inc = this.includes(req.include, getIncludes);
    const raw = req.id ?? "";
    // Ids are opaque on the wire; one this backend could not have issued does not exist.
    const id = parseUuid(raw);
    const missing = () =>
      new WireError("not_found", `activity ${quote(raw)} not found`);
    if (!id) throw missing();
    const res = await this.qs.queryActivities({
      filter: { field: "id", op: "eq", value: id },
      include: recordInclude(inc),
      limit: 1,
    });
    if (!res.items[0]) throw missing();
    const v = toActivity(res.items[0]);
    if (inc.has("steps")) {
      const steps = await this.qs.listStepEntries(id, false, maxEmbedded, "");
      v.steps = steps.items.map(toStep);
    }
    if (inc.has("events")) {
      const events = await this.qs.queryEvents({
        filter: { field: "activity_id", op: "eq", value: id },
        limit: maxEmbedded,
      });
      v.events = events.items.map(toEvent);
    }
    return v;
  }

  private async activitiesCount(data: unknown): Promise<CountResult> {
    const req = decodeRequest<CountRequest>(specs.CountRequest, data);
    const { count, exact } = await this.qs.countActivities(
      req.filter,
      countLimit,
    );
    return { count, exact };
  }

  private async activitiesAggregate(data: unknown): Promise<AggregateResult> {
    const req = decodeRequest<AggregateRequest>(specs.AggregateRequest, data);
    let count = false;
    const durations: { field: string; percentiles?: number[] }[] = [];
    for (const m of req.metrics ?? []) {
      if (m.name === "count") count = true;
      else if (m.name === "duration")
        durations.push({ field: m.field ?? "", percentiles: m.percentiles });
      else
        throw fieldError(
          "unsupported",
          "metrics",
          `unknown metric ${quote(m.name ?? "")}`,
        );
    }
    let bucket;
    if (req.bucket) {
      const b = req.bucket;
      bucket = {
        field: b.field ?? "",
        intervalMs: b.interval_ms ?? 0,
        from: parseTime("bucket.from", b.from),
        to: parseTime("bucket.to", b.to),
      };
    }
    const rows = await this.qs.aggregateActivities({
      filter: req.filter,
      groupBy: req.group_by,
      bucket,
      count,
      durations,
      limit: req.limit,
    });
    return {
      groups: rows.rows.map((r) =>
        compact<AggregateGroup>({
          key: r.key && Object.keys(r.key).length ? r.key : undefined,
          bucket: tsp(r.bucket),
          count: count ? r.count : undefined,
          durations:
            r.durations && Object.keys(r.durations).length
              ? r.durations
              : undefined,
        }),
      ),
      truncated: rows.truncated,
    };
  }

  private async stepsList(data: unknown): Promise<StepPage> {
    const req = decodeRequest<StepsRequest>(specs.StepsRequest, data);
    const inc = this.includes(req.include, ["result"]);
    const id = parseUuid(req.activity_id ?? "");
    if (!id) return { items: [] };
    const res = await this.qs.listStepEntries(
      id,
      inc.has("result"),
      req.limit ?? 0,
      req.cursor ?? "",
    );
    return page(res.items.map(toStep), res.nextCursor);
  }

  private async eventsList(data: unknown): Promise<EventPage> {
    const q = decodeRequest<Query>(specs.Query, data);
    const inc = this.includes(q.include, ["detail"]);
    const sort = oneSort(q.sort);
    if (sort && sort.field !== "at")
      throw fieldError("unsupported", "sort", "events sort by at only");
    const res = await this.qs.queryEvents({
      filter: q.filter,
      limit: q.limit,
      cursor: q.cursor,
      includeDetail: inc.has("detail"),
      desc: sort?.desc ?? false,
    });
    return page(res.items.map(toEvent), res.nextCursor);
  }

  private async resultsGet(data: unknown): Promise<Result> {
    if (this.metadataOnly())
      throw new WireError(
        "forbidden",
        "results are not sent in metadata-only mode",
      );
    const req = decodeRequest<ResultRequest>(specs.ResultRequest, data);
    const raw = req.activity_id ?? "";
    const missing = () =>
      new WireError("not_found", `no result for activity ${quote(raw)}`);
    const id = parseUuid(raw);
    if (!id) throw missing();
    const res = await this.storage.getResult(id);
    if (!res) throw missing();
    return toResult(res);
  }

  private async treesGet(data: unknown): Promise<Tree> {
    const req = decodeRequest<TreeRequest>(specs.TreeRequest, data);
    const inc = this.includes(req.include, recordIncludes);
    const raw = req.id ?? "";
    const id = parseUuid(raw);
    if (!id)
      throw new WireError("not_found", `activity ${quote(raw)} not found`);
    const tree = await this.qs.getActivityTree(
      id,
      recordInclude(inc),
      req.max_nodes ?? 0,
    );
    return {
      root_id: tree.rootId,
      items: tree.items.map(toActivity),
      truncated: tree.truncated,
    };
  }
}

function recordInclude(inc: Set<string>): RecordInclude {
  return {
    payload: inc.has("payload"),
    result: inc.has("result"),
    lastError: inc.has("last_error"),
  };
}
function page<T>(
  items: T[],
  nextCursor: string,
): { items: T[]; next_cursor?: string } {
  return nextCursor ? { items, next_cursor: nextCursor } : { items };
}
function parseTime(field: string, s?: string): Date | undefined {
  if (!s) return undefined;
  if (!isTimestamp(s))
    throw fieldError(
      "invalid_argument",
      field,
      "expected an RFC 3339 timestamp",
    );
  return new Date(s);
}
