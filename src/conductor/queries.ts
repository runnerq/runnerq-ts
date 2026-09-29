// The query messages (activities.*, steps.list, events.list, results.get, trees.get),
// served from a QueryStorage: a port of runnerq-go's conductor/handlers.go.
import { isTimestamp, json, parseUuid, type JsonValue } from "../codec.js";
import { decode, type SerializationFormat } from "../serialization.js";
import type { Storage } from "../storage.js";
import type {
  ActivityRecord,
  EventRecord,
  QueryFilter,
  QuerySort,
  QueryStorage,
  RecordInclude,
  StepEntry,
} from "../query.js";
import { decodeRequest, type Spec } from "./decode.js";
import {
  WireError,
  ts,
  typeActivitiesAggregate,
  typeActivitiesCount,
  typeActivitiesGet,
  typeActivitiesList,
  typeEventsList,
  typeEventsSubscribe,
  typeEventsUnsubscribe,
  typeResultsGet,
  typeStepsList,
  typeTreesGet,
  type Capability,
  type Handler,
} from "./wire.js";

/** Bounds steps and events embedded in activities.get. */
const maxEmbedded = 1000;
/** activities.count stops counting here and says the count is not exact. */
const countLimit = 100_000;

const recordIncludes = ["last_error", "payload", "result"];
const getIncludes = ["events", "last_error", "payload", "result", "steps"];

interface WireSort {
  field?: string;
  order?: string;
}
interface Query {
  filter?: QueryFilter;
  sort?: WireSort[];
  include?: string[];
  limit?: number;
  cursor?: string;
}

export const filterSpec: Spec = {
  object: {
    and: { array: () => filterSpec },
    or: { array: () => filterSpec },
    not: () => filterSpec,
    field: "string",
    op: "string",
    value: "any",
  },
};
const includeSpec: Spec = { array: "string" };
const querySpec: Spec = {
  object: {
    filter: filterSpec,
    sort: { array: { object: { field: "string", order: "string" } } },
    include: includeSpec,
    limit: "int",
    cursor: "string",
  },
};
const getSpec: Spec = { object: { id: "string", include: includeSpec } };
const countSpec: Spec = { object: { filter: filterSpec } };
const stepsSpec: Spec = {
  object: {
    activity_id: "string",
    include: includeSpec,
    limit: "int",
    cursor: "string",
  },
};
const resultSpec: Spec = { object: { activity_id: "string" } };
const treeSpec: Spec = {
  object: { id: "string", include: includeSpec, max_nodes: "int" },
};
const aggregateSpec: Spec = {
  object: {
    filter: filterSpec,
    group_by: { array: "string" },
    bucket: {
      object: {
        field: "string",
        interval_ms: "int",
        from: "string",
        to: "string",
      },
    },
    metrics: {
      array: {
        object: {
          name: "string",
          field: "string",
          percentiles: { array: "number" },
        },
      },
    },
    limit: "int",
  },
};

/** At most one sort key (the backend adds the tiebreaker); the default order is desc. */
function oneSort(sorts?: WireSort[]): QuerySort | undefined {
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
  if (serialization !== "superjson-v1") return data;
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

interface ResultView {
  state: "ok" | "error";
  data?: JsonValue;
  error?: { message?: string; kind?: string };
}
export function toResult(r: {
  state: "Ok" | "Err";
  serialization: string;
  data?: JsonValue;
}): ResultView {
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

type View = Record<string, unknown>;
/** Sets `key` unless the value is empty ("", undefined), as Go's omitempty. */
function put(v: View, key: string, value: unknown): void {
  if (value !== undefined && value !== "") v[key] = value;
}
const tsp = (d?: Date) => (d ? ts(d) : undefined);

export function toActivity(r: ActivityRecord): View {
  const v: View = { id: r.id, type: r.type };
  put(v, "queue", r.queue);
  v.status = r.status;
  v.priority = r.priority;
  put(v, "root_id", r.rootId);
  put(v, "parent_id", r.parentId);
  v.depth = r.depth;
  put(v, "idempotency_key", r.idempotencyKey);
  v.attempt = r.attempt;
  // Unlimited attempts: omitted ("fields that do not apply are omitted").
  put(v, "max_attempts", r.maxAttempts);
  v.created_at = ts(r.createdAt);
  put(v, "scheduled_for", tsp(r.scheduledFor));
  put(v, "started_at", tsp(r.startedAt));
  put(v, "completed_at", tsp(r.completedAt));
  put(v, "updated_at", tsp(r.updatedAt));
  if (r.timeoutMs) v.timeout_ms = r.timeoutMs;
  put(v, "lease_expires_at", tsp(r.leaseExpiresAt));
  put(v, "executor_id", r.executorId);
  if (r.wait) {
    const w: View = { kind: r.wait.kind };
    put(w, "name", r.wait.name);
    put(w, "until", tsp(r.wait.until));
    v.wait = w;
  }
  if (r.metadata && Object.keys(r.metadata).length) v.metadata = r.metadata;
  if (r.lastError) {
    const e: View = {};
    put(e, "message", r.lastError.message);
    put(e, "kind", r.lastError.kind);
    put(e, "at", tsp(r.lastError.at));
    v.last_error = e;
  }
  if (r.payload) v.payload = plainJson(r.payload.serialization, r.payload.data);
  if (r.result) v.result = toResult(r.result);
  return v;
}

export function toStep(s: StepEntry): View {
  const v: View = {
    id: s.id,
    activity_id: s.activityId,
    name: s.name,
    kind: s.kind,
    status: s.state === "Ok" ? "completed" : "failed",
    created_at: ts(s.createdAt),
  };
  if (s.data)
    v.result = toResult({
      state: s.state,
      serialization: s.data.serialization,
      data: s.data.data,
    });
  return v;
}

export function toEvent(e: EventRecord): View {
  const v: View = {
    id: e.cursor,
    cursor: e.cursor,
    activity_id: e.activityId,
    type: e.type,
    at: ts(e.at),
  };
  put(v, "executor_id", e.executorId);
  if (e.detail !== undefined) v.detail = e.detail;
  return v;
}

/** A request type's advertised capability and, unless it is bound to a session, its handler. */
export interface Route {
  capability: Capability;
  handler?: Handler;
}

/** The query handlers and what they advertise, over `qs` (results through `storage`). */
export class Queries {
  constructor(
    private readonly qs: QueryStorage,
    private readonly storage: Storage,
    private readonly metadataOnly: () => boolean,
  ) {}

  /** Every query and stream request type; stream ones have no handler (Streams serve them). */
  routes(): Record<string, Route> {
    const qc = this.qs.queryCapabilities();
    const nonEmpty = (list: string[]) => (list.length ? list : undefined);
    const filters = nonEmpty(qc.activityFilters);
    const eventFilters = nonEmpty(qc.eventFilters);
    return {
      [typeActivitiesList]: {
        capability: {
          v: 1,
          filters,
          sorts: nonEmpty(qc.activitySorts),
          include: recordIncludes,
        },
        handler: (d) => this.activitiesList(d),
      },
      [typeActivitiesGet]: {
        capability: { v: 1, include: getIncludes },
        handler: (d) => this.activitiesGet(d),
      },
      [typeActivitiesCount]: {
        capability: { v: 1, filters },
        handler: (d) => this.activitiesCount(d),
      },
      [typeActivitiesAggregate]: {
        capability: {
          v: 1,
          filters,
          group_by: nonEmpty(qc.groupBy),
          buckets: nonEmpty(qc.buckets),
          metrics: ["count", ...qc.durations.map((d) => "duration." + d)],
        },
        handler: (d) => this.activitiesAggregate(d),
      },
      [typeStepsList]: {
        capability: { v: 1, include: ["result"] },
        handler: (d) => this.stepsList(d),
      },
      [typeEventsList]: {
        capability: {
          v: 1,
          filters: eventFilters,
          sorts: ["at"],
          include: ["detail"],
        },
        handler: (d) => this.eventsList(d),
      },
      [typeResultsGet]: {
        capability: { v: 1 },
        handler: (d) => this.resultsGet(d),
      },
      [typeTreesGet]: {
        capability: { v: 1, include: recordIncludes },
        handler: (d) => this.treesGet(d),
      },
      [typeEventsSubscribe]: { capability: { v: 1, filters: eventFilters } },
      [typeEventsUnsubscribe]: { capability: { v: 1 } },
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

  private async activitiesList(data: unknown): Promise<unknown> {
    const q = decodeRequest<Query>(querySpec, data);
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

  private async activitiesGet(data: unknown): Promise<unknown> {
    const req = decodeRequest<{ id?: string; include?: string[] }>(
      getSpec,
      data,
    );
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

  private async activitiesCount(data: unknown): Promise<unknown> {
    const req = decodeRequest<{ filter?: QueryFilter }>(countSpec, data);
    const { count, exact } = await this.qs.countActivities(
      req.filter,
      countLimit,
    );
    return { count, exact };
  }

  private async activitiesAggregate(data: unknown): Promise<unknown> {
    const req = decodeRequest<{
      filter?: QueryFilter;
      group_by?: string[];
      bucket?: {
        field?: string;
        interval_ms?: number;
        from?: string;
        to?: string;
      };
      metrics?: { name?: string; field?: string; percentiles?: number[] }[];
      limit?: number;
    }>(aggregateSpec, data);
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
      groups: rows.rows.map((r) => {
        const g: View = {};
        if (r.key && Object.keys(r.key).length) g.key = r.key;
        put(g, "bucket", tsp(r.bucket));
        if (count) g.count = r.count;
        if (r.durations && Object.keys(r.durations).length)
          g.durations = r.durations;
        return g;
      }),
      truncated: rows.truncated,
    };
  }

  private async stepsList(data: unknown): Promise<unknown> {
    const req = decodeRequest<{
      activity_id?: string;
      include?: string[];
      limit?: number;
      cursor?: string;
    }>(stepsSpec, data);
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

  private async eventsList(data: unknown): Promise<unknown> {
    const q = decodeRequest<Query>(querySpec, data);
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

  private async resultsGet(data: unknown): Promise<unknown> {
    if (this.metadataOnly())
      throw new WireError(
        "forbidden",
        "results are not sent in metadata-only mode",
      );
    const req = decodeRequest<{ activity_id?: string }>(resultSpec, data);
    const raw = req.activity_id ?? "";
    const missing = () =>
      new WireError("not_found", `no result for activity ${quote(raw)}`);
    const id = parseUuid(raw);
    if (!id) throw missing();
    const res = await this.storage.getResult(id);
    if (!res) throw missing();
    return toResult(res);
  }

  private async treesGet(data: unknown): Promise<unknown> {
    const req = decodeRequest<{
      id?: string;
      include?: string[];
      max_nodes?: number;
    }>(treeSpec, data);
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
function page(items: unknown[], nextCursor: string): View {
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
