import type { PoolClient } from "pg";
import { RunnerQError } from "../errors.js";
import {
  validateLimit,
  type Limit,
  type LimitRecord,
  type LimitStorage,
} from "../limit.js";
import { limitsRefreshMs } from "../spec.js";

type Row = Record<string, any>;

/** What an ordinary claim may take: the same eligibility, $1 being the queue. */
export const eligibleSQL = `queue_name=$1 AND status IN ('pending','scheduled','retrying','waiting')
        AND (status='pending' OR scheduled_at<=NOW())`;
export const claimOrderSQL =
  "priority DESC,retry_count DESC,COALESCE(scheduled_at,created_at) ASC";

const available = (at: string) =>
  `LEAST(l.rate_tokens::float8,l.tokens+GREATEST(0,EXTRACT(EPOCH FROM ${at}-l.refilled_at))*l.rate_tokens/l.rate_per_seconds)`;
const running = `(SELECT count(*) FROM runnerq_activities r
  WHERE r.queue_name=l.queue_name AND r.activity_type=l.activity_type AND r.status='processing')`;

// The limited claim (runnerq-spec limits.md). A lock-free check finds the limited types with
// claimable work and room; only those are locked. The claim statement runs after the lock
// statement, so its snapshot sees every claim committed before the lock was granted: the
// running count and bucket it reads are current, and no other claimer of these types can change
// them until commit.
const checkSQL = `SELECT l.activity_type,
  EXISTS(SELECT 1 FROM runnerq_activities WHERE ${eligibleSQL} AND activity_type=l.activity_type) AS has_work,
  (l.max_concurrent IS NULL OR l.max_concurrent>${running})
    AND (l.rate_tokens IS NULL OR ${available("clock_timestamp()")}>=1) AS has_room
  FROM runnerq_limits l WHERE l.queue_name=$1 AND l.activity_type=ANY($2::text[])`;
const lockSQL = `SELECT activity_type FROM runnerq_limits WHERE queue_name=$1 AND activity_type=ANY($2::text[])
  ORDER BY activity_type FOR UPDATE`;
/** $1 queue, $2 claim limit, $3 token prefix, $4 lease ms, $5 locked types. */
export const limitedClaimSQL = `WITH clock AS (SELECT clock_timestamp() AS at),
  lim AS (
    SELECT l.activity_type,l.rate_tokens,clock.at,b.avail,
      LEAST($2::bigint,COALESCE(l.max_concurrent-${running},$2::bigint),COALESCE(floor(b.avail)::bigint,$2::bigint)) AS allowance
    FROM runnerq_limits l,clock,
      LATERAL (SELECT CASE WHEN l.rate_tokens IS NULL THEN NULL ELSE ${available("clock.at")} END AS avail) b
    WHERE l.queue_name=$1 AND l.activity_type=ANY($5::text[])),
  picked AS (
    SELECT c.id,c.activity_type AS claim_type FROM lim CROSS JOIN LATERAL (
      SELECT id,activity_type,priority,retry_count,scheduled_at,created_at FROM runnerq_activities
      WHERE ${eligibleSQL} AND activity_type=lim.activity_type
      ORDER BY ${claimOrderSQL} LIMIT GREATEST(lim.allowance,0) FOR UPDATE SKIP LOCKED) c
    ORDER BY c.priority DESC,c.retry_count DESC,COALESCE(c.scheduled_at,c.created_at) ASC
    LIMIT $2),
  spent AS (
    UPDATE runnerq_limits l SET tokens=lim.avail-s.n,refilled_at=lim.at
    FROM lim,(SELECT claim_type,count(*) AS n FROM picked GROUP BY claim_type) s
    WHERE l.queue_name=$1 AND l.activity_type=lim.activity_type AND s.claim_type=lim.activity_type
      AND lim.rate_tokens IS NOT NULL),`;

const columns =
  "activity_type,max_concurrent,rate_tokens,rate_per_seconds,updated_at";
function record(r: Row): LimitRecord {
  const out: LimitRecord = {
    type: r.activity_type,
    updatedAt: new Date(r.updated_at).toISOString(),
  };
  if (r.max_concurrent !== null) out.maxConcurrent = r.max_concurrent;
  if (r.rate_tokens !== null) {
    out.rateLimit = r.rate_tokens;
    out.ratePeriodMs = Math.round(Number(r.rate_per_seconds) * 1000);
  }
  return out;
}
const notFound = (type: string) =>
  new RunnerQError(
    "not_found",
    `No limit for activity type ${JSON.stringify(type)}`,
  );

/** A claim's types split by whether they have a limit. */
export interface ClaimPlan {
  limited: string[];
  rest: string[];
}

export class PostgresLimits implements LimitStorage {
  /** The queue's limited types, read at most limitsRefreshMs ago. */
  private cache?: { types: Set<string>; at: number };
  private loading?: Promise<Set<string>>;
  constructor(
    private readonly deps: {
      queue: string;
      query: (sql: string, values?: unknown[]) => Promise<Row[]>;
      tx: <T>(body: (c: PoolClient) => Promise<T>) => Promise<T>;
    },
  ) {}
  private async limitedTypes(): Promise<Set<string>> {
    if (this.cache && Date.now() - this.cache.at < limitsRefreshMs)
      return this.cache.types;
    this.loading ??= this.deps
      .query("SELECT activity_type FROM runnerq_limits WHERE queue_name=$1", [
        this.deps.queue,
      ])
      .then((rows) => {
        const types = new Set(rows.map((r) => r.activity_type as string));
        this.cache = { types, at: Date.now() };
        return types;
      })
      .finally(() => {
        this.loading = undefined;
      });
    return this.loading;
  }
  /** Records a change this storage made, so its own next claim sees it at once. */
  private mark(type: string, limited: boolean): void {
    if (!this.cache || this.cache.types.has(type) === limited) return;
    const types = new Set(this.cache.types);
    if (limited) types.add(type);
    else types.delete(type);
    this.cache = { types, at: this.cache.at };
  }
  /** Null when no type of the claim has a limit: the ordinary claim applies unchanged. */
  async plan(types: readonly string[]): Promise<ClaimPlan | null> {
    const limited = await this.limitedTypes();
    if (!limited.size) return null;
    const plan: ClaimPlan = { limited: [], rest: [] };
    for (const t of types) (limited.has(t) ? plan.limited : plan.rest).push(t);
    return plan.limited.length ? plan : null;
  }
  /**
   * Claims up to `limit` of the plan's limited types and resolves to the claimed rows, shaped
   * by `select` (the claim's final SELECT over the `claimed` CTE). Types whose limit has gone
   * move to `plan.rest`. `heldBack` reports that a limit kept claimable work back.
   */
  async claim(
    plan: ClaimPlan,
    limit: number,
    prefix: string,
    leaseMs: number,
    claimed: string,
  ): Promise<{ rows: Row[]; heldBack: boolean }> {
    const q = this.deps.queue;
    const checked = await this.deps.query(checkSQL, [q, plan.limited]);
    const found = new Set<string>();
    const ready: string[] = [];
    let heldBack = false;
    for (const r of checked) {
      found.add(r.activity_type);
      if (r.has_work && r.has_room) ready.push(r.activity_type);
      else if (r.has_work) heldBack = true;
    }
    for (const t of plan.limited)
      if (!found.has(t)) {
        plan.rest.push(t);
        this.mark(t, false);
      }
    if (!ready.length) return { rows: [], heldBack };
    const rows = await this.deps.tx(async (c) => {
      const locked = await c.query(lockSQL, [q, ready]);
      if (!locked.rowCount) return [];
      const types = locked.rows.map((r) => r.activity_type as string);
      return (
        await c.query(limitedClaimSQL + claimed, [
          q,
          limit,
          prefix,
          leaseMs,
          types,
        ])
      ).rows;
    });
    return { rows, heldBack: heldBack || !rows.length };
  }
  async setLimit(l: Limit): Promise<void> {
    validateLimit(l);
    // A changed rate settles the bucket: the old rate's refill up to now, capped at the new
    // size (limits.md).
    await this.deps.query(
      `INSERT INTO runnerq_limits AS l(queue_name,activity_type,max_concurrent,rate_tokens,rate_per_seconds,
        tokens,refilled_at,updated_at)
      VALUES($1,$2,$3,$4,$5,COALESCE($4,0),clock_timestamp(),clock_timestamp())
      ON CONFLICT(queue_name,activity_type) DO UPDATE SET
        max_concurrent=EXCLUDED.max_concurrent,rate_tokens=EXCLUDED.rate_tokens,rate_per_seconds=EXCLUDED.rate_per_seconds,
        tokens=CASE
          WHEN EXCLUDED.rate_tokens IS NULL THEN 0
          WHEN (l.rate_tokens,l.rate_per_seconds) IS NOT DISTINCT FROM (EXCLUDED.rate_tokens,EXCLUDED.rate_per_seconds) THEN l.tokens
          WHEN l.rate_tokens IS NULL THEN EXCLUDED.rate_tokens
          ELSE LEAST(EXCLUDED.rate_tokens,l.rate_tokens,
            l.tokens+GREATEST(0,EXTRACT(EPOCH FROM EXCLUDED.refilled_at-l.refilled_at))*l.rate_tokens/l.rate_per_seconds)
        END,
        refilled_at=CASE
          WHEN (l.rate_tokens,l.rate_per_seconds) IS NOT DISTINCT FROM (EXCLUDED.rate_tokens,EXCLUDED.rate_per_seconds) THEN l.refilled_at
          ELSE EXCLUDED.refilled_at
        END,
        updated_at=EXCLUDED.updated_at
      WHERE (l.max_concurrent,l.rate_tokens,l.rate_per_seconds)
        IS DISTINCT FROM (EXCLUDED.max_concurrent,EXCLUDED.rate_tokens,EXCLUDED.rate_per_seconds)`,
      [
        this.deps.queue,
        l.type,
        l.maxConcurrent ?? null,
        l.rateLimit ?? null,
        l.ratePeriodMs === undefined ? null : l.ratePeriodMs / 1000,
      ],
    );
    this.mark(l.type, true);
  }
  async getLimit(type: string): Promise<LimitRecord> {
    const rows = await this.deps.query(
      `SELECT ${columns} FROM runnerq_limits WHERE queue_name=$1 AND activity_type=$2`,
      [this.deps.queue, type],
    );
    const row = rows[0];
    if (!row) throw notFound(type);
    return record(row);
  }
  async listLimits(): Promise<LimitRecord[]> {
    const rows = await this.deps.query(
      `SELECT ${columns} FROM runnerq_limits WHERE queue_name=$1 ORDER BY activity_type`,
      [this.deps.queue],
    );
    return rows.map(record);
  }
  async deleteLimit(type: string): Promise<void> {
    const rows = await this.deps.query(
      "DELETE FROM runnerq_limits WHERE queue_name=$1 AND activity_type=$2 RETURNING activity_type",
      [this.deps.queue, type],
    );
    this.mark(type, false);
    if (!rows.length) throw notFound(type);
  }
}
