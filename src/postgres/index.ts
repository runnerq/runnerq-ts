import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import {
  Pool,
  type PoolClient,
  type PoolConfig,
  type QueryResultRow,
} from "pg";
import { checkpointId, nonempty } from "../codec.js";
import { databaseError, RunnerQError, type FailureDetails } from "../errors.js";
import type { SerializedValue } from "../serialization.js";
import { integer } from "../options.js";
import { pause } from "../async.js";
import type {
  Storage,
  Submission,
  Fence,
  Claim,
  StoredResult,
  Park,
  Retention,
  ActivitySnapshot,
  ListOptions,
  StepRecord,
  ActivityEvent,
  Command,
  CommandResult,
  CommandStorage,
} from "../storage.js";
import {
  additions,
  additionTables,
  indexNames,
  schema,
  schemaLock,
  tableNames,
} from "./schema.js";
import { PostgresQueries, queryCapabilities } from "./query.js";
import type {
  QueryStorage,
  QueryCapabilities,
  QueryFilter,
  ActivityQuery,
  ActivityRecordPage,
  AggregateQuery,
  AggregateRows,
  EventQuery,
  EventRecordPage,
  StepEntryPage,
  RecordInclude,
  ActivityTree,
} from "../query.js";
import { Notifications } from "./notifications.js";
import { PostgresCommands } from "./command.js";
import { deleteTree, lockTree, terminalSQL } from "./trees.js";

export interface PostgresConfig {
  connectionString: string;
  queue: string;
  poolSize?: number;
  statementTimeoutMs?: number;
  ssl?: PoolConfig["ssl"];
}
type Row = QueryResultRow;
const lost = () =>
  new RunnerQError("claim_lost", "Execution no longer owns this activity");
const iso = (v: Date | string | null): string | null =>
  v === null ? null : new Date(v).toISOString();
function poolConfig(config: Omit<PostgresConfig, "queue">): PoolConfig {
  return {
    connectionString: config.connectionString,
    ssl: config.ssl,
    max: integer(config.poolSize ?? 10, "poolSize", 1),
    connectionTimeoutMillis: 5_000,
    statement_timeout: integer(
      config.statementTimeoutMs ?? 5_000,
      "statementTimeoutMs",
      1,
    ),
    application_name: "runnerq-ts",
    idle_in_transaction_session_timeout: 10_000,
  };
}
function canonicalIndex(sql: string): string {
  return sql
    .toLowerCase()
    .replace(/\bon\s+(?:"(?:[^"]|"")+"|[a-z_]\w*)\./g, "on ")
    .replace(/::text(?:\[\])?/g, "")
    .replace(/=\s*any\s*\(\s*array\s*\[/g, "in(")
    .replace(/using\s+btree/g, "")
    .replace(/\s+asc\b/g, "")
    .replace(/[\s"()[\];]/g, "");
}
/** Checks the schema without DDL; addition tables may be absent, and are checked when present. */
async function verifySchema(client: PoolClient): Promise<void> {
  const ddl = schema + additions;
  const columns = await client.query(
    `SELECT table_name,column_name,udt_name,is_nullable,column_default FROM information_schema.columns
    WHERE table_schema=current_schema() AND table_name=ANY($1)`,
    [tableNames],
  );
  const actual = new Map(
    columns.rows.map((r) => [`${r.table_name}.${r.column_name}`, r]),
  );
  const present = new Set(columns.rows.map((r) => r.table_name as string));
  const absent = (table: string) =>
    additionTables.includes(table) && !present.has(table);
  const types: Record<string, string> = {
    UUID: "uuid",
    TEXT: "text",
    JSONB: "jsonb",
    INTEGER: "int4",
    SMALLINT: "int2",
    BIGINT: "int8",
    BIGSERIAL: "int8",
    TIMESTAMPTZ: "timestamptz",
  };
  for (const table of ddl.matchAll(/CREATE TABLE (\w+) \(([\s\S]*?)\n\);/g)) {
    if (absent(table[1]!)) continue;
    for (const col of table[2]!.matchAll(
      /\b(\w+) (UUID|TEXT|JSONB|INTEGER|SMALLINT|BIGINT|BIGSERIAL|TIMESTAMPTZ)(\[\])?([^,\n]*)/g,
    )) {
      const row = actual.get(`${table[1]}.${col[1]}`);
      const expectedDefault = col[4]!.match(/DEFAULT\s+(.+)$/)?.[1] ?? null;
      const normalizeDefault = (value: string | null) =>
        value
          ?.toLowerCase()
          .replace(/::(?:text|integer|bigint|smallint)/g, "")
          .replace(/[\s()]/g, "") ?? null;
      const defaultMatches =
        col[2] === "BIGSERIAL"
          ? String(row?.column_default).startsWith("nextval(")
          : normalizeDefault(row?.column_default ?? null) ===
            normalizeDefault(expectedDefault);
      if (
        !row ||
        row.udt_name !== (col[3] ? "_" : "") + types[col[2]!] ||
        row.is_nullable !==
          (/NOT NULL|PRIMARY KEY/.test(col[4]!) ? "NO" : "YES") ||
        !defaultMatches
      ) {
        throw new RunnerQError(
          "configuration",
          `Incompatible schema: ${table[1]}.${col[1]}; initialize the separate-input RunnerQ schema before connecting`,
        );
      }
    }
  }
  if (actual.has("runnerq_activities.payload"))
    throw new RunnerQError(
      "configuration",
      "Inline-payload schemas are unsupported; migrate Go and the database to the separate-input contract first",
    );
  const indexes = await client.query(
    `SELECT c.relname,i.indisvalid,pg_get_indexdef(c.oid) AS definition
    FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=current_schema() AND c.relname=ANY($1)`,
    [indexNames],
  );
  for (const expected of ddl.matchAll(
    /CREATE INDEX (\w+) ON (\w+)[\s\S]*?;/g,
  )) {
    if (absent(expected[2]!)) continue;
    const row = indexes.rows.find((r) => r.relname === expected[1]);
    if (
      !row?.indisvalid ||
      canonicalIndex(row.definition) !== canonicalIndex(expected[0])
    )
      throw new RunnerQError(
        "configuration",
        `Missing or incompatible index ${expected[1]}`,
      );
  }
  const keys = await client.query(
    `SELECT c.relname,array_agg(a.attname::text ORDER BY k.ordinality) AS columns FROM pg_constraint p
    JOIN pg_class c ON c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(num,ordinality)
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.num
    WHERE p.contype='p' AND n.nspname=current_schema() AND c.relname=ANY($1) GROUP BY c.relname`,
    [tableNames],
  );
  const expectedKeys: Record<string, string[]> = {
    runnerq_activities: ["id"],
    runnerq_inputs: ["activity_id"],
    runnerq_results: ["activity_id"],
    runnerq_events: ["id"],
    runnerq_worker_pools: ["pool_id"],
    runnerq_idempotency: ["queue_name", "idempotency_key"],
    runnerq_dependencies: ["queue_name", "waiter_activity_id", "result_id"],
    runnerq_commands: ["queue_name", "command_id"],
  };
  for (const [name, columns] of Object.entries(expectedKeys))
    if (
      !absent(name) &&
      JSON.stringify(keys.rows.find((r) => r.relname === name)?.columns) !==
        JSON.stringify(columns)
    )
      throw new RunnerQError(
        "configuration",
        `Incompatible primary key on ${name}`,
      );
}

export class PostgresStorage
  extends EventEmitter<{ storageError: [error: Error] }>
  implements Storage, QueryStorage, CommandStorage
{
  readonly queue: string;
  private readonly pool: Pool;
  private readonly notifications: Notifications;
  private closing?: Promise<void>;
  private readonly queries = new PostgresQueries((sql, values) =>
    this.query(sql, values),
  );
  private readonly commands: PostgresCommands;
  private constructor(config: PostgresConfig) {
    super();
    this.queue = config.queue;
    this.pool = new Pool(poolConfig(config));
    this.pool.on("error", (error) => {
      for (const listener of this.rawListeners("storageError")) {
        try {
          void Promise.resolve(listener.call(this, error)).catch(() => {});
        } catch {
          /* An observer cannot affect storage or other observers. */
        }
      }
    });
    this.notifications = new Notifications(
      poolConfig({ ...config, poolSize: 1 }),
      this.pool,
      config.queue,
    );
    this.commands = new PostgresCommands({
      queue: this.queue,
      pool: this.pool,
      putResult: (c, id, owner, result, step) =>
        this.putResult(c, id, owner, result, step),
      event: (c, id, type, token, detail) =>
        this.event(c, id, type, token, detail),
      notify: (results, work) => {
        for (const id of results) this.notifications.hint("result", id);
        if (work) this.notifications.hint("work");
      },
    });
  }
  static async initialize(
    config: Omit<PostgresConfig, "queue">,
  ): Promise<void> {
    const pool = new Pool(poolConfig(config));
    try {
      const client = await pool.connect();
      try {
        const deadline = Date.now() + 30_000;
        while (
          !(
            await client.query(
              "SELECT pg_try_advisory_lock($1::bigint) AS locked",
              [schemaLock],
            )
          ).rows[0].locked
        ) {
          if (Date.now() > deadline)
            throw new RunnerQError(
              "timeout",
              "Timed out acquiring schema initialization lock",
            );
          await pause(50);
        }
        const present = await client.query(
          "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND tablename=ANY($1)",
          [tableNames],
        );
        const tables = new Set(present.rows.map((r) => r.tablename));
        // A database from before an addition gets it; IF NOT EXISTS tolerates Go's creating it.
        const ddl = !tables.size
          ? schema + additions
          : additionTables.some((t) => !tables.has(t))
            ? additions.replace(
                /CREATE (TABLE|INDEX) /g,
                "CREATE $1 IF NOT EXISTS ",
              )
            : "";
        if (ddl) {
          await client.query("BEGIN");
          try {
            await client.query(ddl);
            await client.query("COMMIT");
          } catch (error) {
            await client.query("ROLLBACK");
            throw error;
          }
        }
        await verifySchema(client);
      } finally {
        // Destroy the setup session so its advisory lock never returns to a pool.
        client.release(true);
      }
    } finally {
      await pool.end();
    }
  }
  static async connect(config: PostgresConfig): Promise<PostgresStorage> {
    nonempty(config.queue, "Queue");
    if (
      Buffer.byteLength(config.queue) > 48 ||
      !/^[\p{L}_][\p{L}\p{N}_]*$/u.test(config.queue)
    )
      throw new RunnerQError(
        "configuration",
        "Queue must be at most 48 UTF-8 bytes, start with a letter or underscore, and contain only letters, numbers or underscores",
      );
    const storage = new PostgresStorage(config);
    try {
      const client = await storage.pool.connect();
      try {
        await verifySchema(client);
      } finally {
        client.release();
      }
    } catch (error) {
      await storage.close();
      throw databaseError(error);
    }
    return storage;
  }
  private async query(sql: string, values: unknown[] = []): Promise<Row[]> {
    try {
      return (await this.pool.query(sql, values)).rows;
    } catch (error) {
      throw databaseError(error);
    }
  }
  private async tx<T>(body: (c: PoolClient) => Promise<T>): Promise<T> {
    let c: PoolClient | undefined;
    let destroy = false;
    try {
      c = await this.pool.connect();
      await c.query("BEGIN");
      const value = await body(c);
      await c.query("COMMIT");
      return value;
    } catch (error) {
      try {
        await c?.query("ROLLBACK");
      } catch {
        destroy = true;
      }
      throw databaseError(error);
    } finally {
      c?.release(destroy);
    }
  }
  private async fence(c: PoolClient, f: Fence): Promise<void> {
    const r = await c.query(
      "SELECT id FROM runnerq_activities WHERE queue_name=$1 AND id=$2 AND status='processing' AND current_worker_id=$3 FOR UPDATE",
      [this.queue, f.ownerId, f.token],
    );
    if (!r.rowCount) throw lost();
  }
  private async event(
    c: PoolClient,
    id: string,
    type: string,
    token: string | null = null,
    detail: unknown = null,
  ): Promise<void> {
    await c.query(
      "INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail) VALUES($1,$2,$3,$4,$5::jsonb)",
      [this.queue, id, type, token, JSON.stringify(detail)],
    );
  }
  private hints(id?: string, work = true): void {
    if (id) this.notifications.hint("result", id);
    if (work) this.notifications.hint("work");
  }
  private async dependency(
    c: PoolClient,
    waiter: string,
    result: string,
    producer: string | null,
  ): Promise<void> {
    await c.query(
      "INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
      [this.queue, waiter, result, producer],
    );
  }
  private async lockProducer(c: PoolClient, id: string): Promise<void> {
    const r = await c.query(
      `SELECT producer.id FROM runnerq_activities producer JOIN runnerq_activities root
      ON root.id=producer.root_activity_id AND root.queue_name=producer.queue_name
      WHERE producer.queue_name=$1 AND producer.id=$2 FOR SHARE OF producer FOR KEY SHARE OF root`,
      [this.queue, id],
    );
    if (!r.rowCount)
      throw new RunnerQError(
        "not_found",
        "Result producer no longer exists in this queue",
      );
  }
  private async wake(c: PoolClient, id: string): Promise<void> {
    await c.query(
      `UPDATE runnerq_activities a SET status='pending',scheduled_at=NULL,waiting_result_id=NULL
      WHERE a.queue_name=$1 AND a.status='waiting' AND a.waiting_result_id=$2
      AND EXISTS(SELECT 1 FROM runnerq_dependencies d WHERE d.queue_name=$1 AND d.waiter_activity_id=a.id AND d.result_id=$2)`,
      [this.queue, id],
    );
  }
  async submit(a: Submission): Promise<string> {
    const id = await this.tx(async (c) => {
      if (a.fence) await this.fence(c, a.fence);
      // Caller-generated ids make a retry after a lost commit reply idempotent (allowReuse too).
      const committed = await c.query(
        "SELECT id FROM runnerq_activities WHERE queue_name=$1 AND id=$2",
        [this.queue, a.id],
      );
      if (committed.rowCount) return a.id;
      if (a.key) {
        // Loops only when retention deleted the key between the conflict and the lock.
        for (;;) {
          const fresh = await c.query(
            "INSERT INTO runnerq_idempotency(queue_name,idempotency_key,activity_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING RETURNING activity_id",
            [this.queue, a.key, a.id],
          );
          if (fresh.rowCount) break;
          const r = await c.query(
            `SELECT i.activity_id,a.status,a.parent_activity_id FROM runnerq_idempotency i
            LEFT JOIN runnerq_activities a ON a.id=i.activity_id AND a.queue_name=i.queue_name
            WHERE i.queue_name=$1 AND i.idempotency_key=$2 FOR UPDATE OF i`,
            [this.queue, a.key],
          );
          const existing = r.rows[0];
          if (!existing) continue;
          if (!existing.status)
            throw new RunnerQError(
              "internal",
              "Idempotency key points to a missing activity",
            );
          const policy = a.options.idempotency?.onDuplicate ?? "returnExisting";
          if (policy === "returnExisting") {
            if (a.parentId) {
              await this.dependency(
                c,
                a.parentId,
                existing.activity_id,
                existing.activity_id,
              );
              if (existing.parent_activity_id !== a.parentId)
                await this.event(c, existing.activity_id, "SpawnLinked", null, {
                  parent_activity_id: a.parentId,
                });
            }
            return existing.activity_id as string;
          }
          if (policy === "noReuse")
            throw new RunnerQError(
              "duplicate",
              "Idempotency key already exists",
            );
          if (
            policy === "allowReuseOnFailure" &&
            !["failed", "dead_letter", "cancelled"].includes(existing.status)
          )
            throw new RunnerQError(
              "idempotency_conflict",
              "Previous activity has not failed",
            );
          await c.query(
            "UPDATE runnerq_idempotency SET activity_id=$3,updated_at=NOW() WHERE queue_name=$1 AND idempotency_key=$2",
            [this.queue, a.key, a.id],
          );
          break;
        }
      }
      const o = a.options;
      const priority =
        ["low", "normal", "high", "critical"].indexOf(o.priority) + 1;
      // One statement for the activity, its input, the parent link and the event.
      await c.query(
        `WITH activity AS (INSERT INTO runnerq_activities(id,queue_name,activity_type,priority,status,scheduled_at,max_retries,
        timeout_seconds,retry_delay_seconds,max_retry_delay_seconds,metadata,idempotency_key,parent_activity_id,root_activity_id,depth)
        VALUES($1,$2,$3,$4,$5,CASE WHEN $6::bigint>0 THEN NOW()+$6*INTERVAL '1 millisecond' ELSE NULL END,$7,$8,1,$9,$10::jsonb,$11,$12,$13,$14)),
        input AS (INSERT INTO runnerq_inputs(activity_id,queue_name,payload,serialization) VALUES($1,$2,$15::jsonb,$16)),
        link AS (INSERT INTO runnerq_dependencies(queue_name,waiter_activity_id,result_id,producer_activity_id)
        SELECT $2,$12,$1,$1 WHERE $12::uuid IS NOT NULL ON CONFLICT DO NOTHING)
        INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail) VALUES($2,$1,$17,NULL,$18::jsonb)`,
        [
          a.id,
          this.queue,
          a.type,
          priority,
          o.delayMs > 0 ? "scheduled" : "pending",
          o.delayMs,
          o.maxAttempts === "unlimited" ? 0 : o.maxAttempts,
          o.timeoutMs / 1000,
          o.maxRetryDelayMs / 1000,
          JSON.stringify(o.metadata),
          a.key ?? null,
          a.parentId,
          a.rootId,
          a.depth,
          JSON.stringify(a.payload),
          a.serialization,
          o.delayMs > 0 ? "Scheduled" : "Enqueued",
          JSON.stringify({ activity_type: a.type, priority }),
        ],
      );
      return a.id;
    });
    this.hints(undefined, a.options.delayMs === 0);
    return id;
  }
  async claim(
    limit: number,
    types: readonly string[],
    leaseMs: number,
    executorId?: string,
  ): Promise<Claim[]> {
    integer(limit, "claim limit", 1);
    integer(leaseMs, "leaseMs", 1);
    if (!types.length) return [];
    // Tokens are "<executor>:batch:<uuid>:<activity>", as Go's: unique per claim; the part
    // before the first colon is the executor (queries' executor_id).
    const prefix =
      executorId && !executorId.includes(":")
        ? `${executorId}:batch:${randomUUID()}`
        : randomUUID();
    return this.tx(async (c) => {
      const filter =
        types.length === 1
          ? "activity_type=$5"
          : "activity_type=ANY($5::text[])";
      // One statement claims, logs Dequeued and reads the inputs (one round trip for the
      // batch); the order is the claim order, which UPDATE RETURNING alone doesn't keep.
      const r = await c.query(
        `WITH picked AS (SELECT id FROM runnerq_activities
        WHERE queue_name=$1 AND status IN ('pending','scheduled','retrying','waiting')
        AND (status='pending' OR scheduled_at<=NOW()) AND ${filter}
        ORDER BY priority DESC,retry_count DESC,COALESCE(scheduled_at,created_at) ASC
        LIMIT $2 FOR UPDATE SKIP LOCKED),
        claimed AS (UPDATE runnerq_activities a SET status='processing',current_worker_id=$3||':'||a.id::text,
        started_at=NOW(),waiting_result_id=NULL,
        lease_deadline_ms=(EXTRACT(EPOCH FROM NOW())*1000)::bigint+GREATEST($4::bigint,(timeout_seconds+10)*1000)
        FROM picked WHERE a.id=picked.id RETURNING a.*),
        dequeued AS (INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail)
        SELECT $1,id,'Dequeued',current_worker_id,jsonb_build_object('activity_type',activity_type) FROM claimed
        ORDER BY priority DESC,retry_count DESC,COALESCE(scheduled_at,created_at) ASC)
        SELECT c.id,c.activity_type,c.current_worker_id,c.scheduled_at,c.created_at,c.retry_count,c.timeout_seconds,
        c.parent_activity_id,c.root_activity_id,c.depth,c.metadata,c.lease_deadline_ms,
        i.activity_id AS input_id,i.payload,i.serialization
        FROM claimed c LEFT JOIN runnerq_inputs i ON i.queue_name=$1 AND i.activity_id=c.id
        ORDER BY c.priority DESC,c.retry_count DESC,COALESCE(c.scheduled_at,c.created_at) ASC`,
        [
          this.queue,
          limit,
          prefix,
          leaseMs,
          types.length === 1 ? types[0] : types,
        ],
      );
      return r.rows.map((a): Claim => {
        if (!a.input_id)
          throw new RunnerQError(
            "internal",
            `Missing input for activity ${a.id}`,
          );
        return {
          id: a.id,
          type: a.activity_type,
          payload: a.payload,
          serialization: a.serialization,
          token: a.current_worker_id,
          dueAt: new Date(a.scheduled_at ?? a.created_at).toISOString(),
          retryCount: a.retry_count,
          timeoutMs: Number(a.timeout_seconds) * 1000,
          parentId: a.parent_activity_id,
          rootId: a.root_activity_id,
          depth: a.depth,
          metadata: a.metadata ?? {},
          leaseDeadlineMs: Number(a.lease_deadline_ms),
        };
      });
    });
  }
  async renew(f: Fence, leaseMs: number): Promise<boolean> {
    const rows = await this.query(
      `UPDATE runnerq_activities SET lease_deadline_ms=GREATEST(lease_deadline_ms,(EXTRACT(EPOCH FROM NOW())*1000)::bigint+$4)
      WHERE queue_name=$1 AND id=$2 AND current_worker_id=$3 AND status='processing' RETURNING id`,
      [this.queue, f.ownerId, f.token, leaseMs],
    );
    return rows.length > 0;
  }
  private async putResult(
    c: PoolClient,
    id: string,
    owner: string,
    result: Omit<StoredResult, "data"> & { data?: StoredResult["data"] },
    step: string | null,
  ): Promise<void> {
    await c.query(
      `INSERT INTO runnerq_results(activity_id,queue_name,state,data,owner_activity_id,step,serialization) VALUES($1,$2,$3,$4::jsonb,$5,$6,$7)
      ON CONFLICT(activity_id) DO UPDATE SET data=excluded.data,state=excluded.state,serialization=excluded.serialization,created_at=NOW(),step=excluded.step
      WHERE runnerq_results.queue_name=excluded.queue_name AND runnerq_results.owner_activity_id=excluded.owner_activity_id`,
      [
        id,
        this.queue,
        result.state,
        result.data === undefined ? null : JSON.stringify(result.data),
        owner,
        step,
        result.serialization,
      ],
    );
    await this.wake(c, id);
  }
  async complete(f: Fence, value: SerializedValue): Promise<void> {
    const data = JSON.stringify(value.data);
    await this.tx(async (c) => {
      const done = await c.query(
        `WITH done AS (UPDATE runnerq_activities SET status='completed',completed_at=NOW(),last_worker_id=$3,
        current_worker_id=NULL,lease_deadline_ms=NULL,waiting_result_id=NULL
        WHERE queue_name=$1 AND id=$2 AND status='processing' AND current_worker_id=$3 RETURNING id),
        stored AS (INSERT INTO runnerq_results(activity_id,queue_name,state,data,owner_activity_id,step,serialization)
        SELECT id,$1,'Ok',$4::jsonb,id,NULL,$5 FROM done
        ON CONFLICT(activity_id) DO UPDATE SET data=excluded.data,state=excluded.state,serialization=excluded.serialization,created_at=NOW(),step=excluded.step
        WHERE runnerq_results.queue_name=excluded.queue_name AND runnerq_results.owner_activity_id=excluded.owner_activity_id),
        logged AS (INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail)
        SELECT $1,id,'Completed',$3,'{"result_stored":true}'::jsonb FROM done)
        SELECT id FROM done`,
        [this.queue, f.ownerId, f.token, data, value.serialization],
      );
      if (!done.rowCount) {
        const previous = await c.query(
          `SELECT (r.data IS NOT DISTINCT FROM $4::jsonb AND r.serialization=$5) AS same FROM runnerq_activities a
          JOIN runnerq_results r ON r.activity_id=a.id AND r.queue_name=a.queue_name
          WHERE a.queue_name=$1 AND a.id=$2 AND a.status='completed' AND a.last_worker_id=$3 AND r.state='Ok'`,
          [this.queue, f.ownerId, f.token, data, value.serialization],
        );
        if (previous.rows[0]?.same) return;
        if (previous.rowCount)
          throw new RunnerQError(
            "checkpoint_conflict",
            "Execution completed with a different result",
          );
        throw lost();
      }
      // Its own statement: a fresh snapshot sees a park that committed while the UPDATE
      // above waited on the row lock, so that waiter is not stranded.
      await this.wake(c, f.ownerId);
    });
    this.hints(f.ownerId);
  }
  async fail(
    f: Fence,
    reason: string,
    retry: boolean,
    failure?: FailureDetails,
  ): Promise<"failed" | "retrying" | "dead_letter"> {
    const status = await this.tx(async (c) => {
      const r = await c.query(
        "SELECT * FROM runnerq_activities WHERE queue_name=$1 AND id=$2 AND status='processing' AND current_worker_id=$3 FOR UPDATE",
        [this.queue, f.ownerId, f.token],
      );
      const a = r.rows[0];
      if (!a) {
        const previous = await c.query(
          `SELECT event_type FROM runnerq_events WHERE queue_name=$1 AND activity_id=$2 AND worker_id=$3
          AND detail->>'error'=$4 AND event_type=ANY($5::text[]) AND (detail->'failure') IS NOT DISTINCT FROM $6::jsonb ORDER BY id DESC LIMIT 1`,
          [
            this.queue,
            f.ownerId,
            f.token,
            reason,
            retry ? ["Retrying", "DeadLetter"] : ["Failed"],
            failure === undefined ? null : JSON.stringify(failure),
          ],
        );
        const event = previous.rows[0]?.event_type;
        if (event)
          return event === "Retrying"
            ? "retrying"
            : event === "DeadLetter"
              ? "dead_letter"
              : "failed";
        throw lost();
      }
      const again =
        retry && (a.max_retries === 0 || a.retry_count + 1 < a.max_retries);
      const status = again ? "retrying" : retry ? "dead_letter" : "failed";
      const delay = Math.min(
        Number(a.max_retry_delay_seconds) || 3600,
        Number(a.retry_delay_seconds) * 2 ** Math.min(a.retry_count + 1, 52),
      );
      // Transition, terminal result and event in one statement; the wake stays separate.
      const result = again
        ? null
        : JSON.stringify({
            error: reason,
            ...(failure ? { failure } : {}),
            type: retry ? "dead_letter" : "non_retryable",
            failed_at: new Date().toISOString(),
          });
      await c.query(
        `WITH failed AS (UPDATE runnerq_activities SET status=$4,last_error=$5,last_error_at=NOW(),last_worker_id=$3,
        current_worker_id=NULL,lease_deadline_ms=NULL,waiting_result_id=NULL,
        retry_count=retry_count+CASE WHEN $4='retrying' THEN 1 ELSE 0 END,
        scheduled_at=CASE WHEN $4='retrying' THEN NOW()+$6*INTERVAL '1 second' ELSE scheduled_at END,
        started_at=CASE WHEN $4='retrying' THEN NULL ELSE started_at END,
        completed_at=CASE WHEN $4='retrying' THEN NULL ELSE NOW() END WHERE queue_name=$1 AND id=$2),
        stored AS (INSERT INTO runnerq_results(activity_id,queue_name,state,data,owner_activity_id,step,serialization)
        SELECT $2::uuid,$1,'Err',$7::jsonb,$2::uuid,NULL,'json-v1' WHERE $7::jsonb IS NOT NULL
        ON CONFLICT(activity_id) DO UPDATE SET data=excluded.data,state=excluded.state,serialization=excluded.serialization,created_at=NOW(),step=excluded.step
        WHERE runnerq_results.queue_name=excluded.queue_name AND runnerq_results.owner_activity_id=excluded.owner_activity_id)
        INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail) VALUES($1,$2,$8,$3,$9::jsonb)`,
        [
          this.queue,
          f.ownerId,
          f.token,
          status,
          reason,
          delay,
          result,
          again ? "Retrying" : retry ? "DeadLetter" : "Failed",
          JSON.stringify({
            error: reason,
            retryable: retry,
            ...(failure ? { failure } : {}),
          }),
        ],
      );
      if (!again) await this.wake(c, f.ownerId);
      return status;
    });
    this.hints(status === "retrying" ? undefined : f.ownerId);
    return status;
  }
  async checkpoint(
    f: Fence,
    id: string,
    result: StoredResult,
    step: string,
  ): Promise<void> {
    await this.tx(async (c) => {
      // The fence, the result and (only when the result is new) its ResultStored event:
      // one statement. The fence's row lock is taken first, as a statement of its own would.
      const r = await c.query(
        `WITH fenced AS (SELECT id FROM runnerq_activities
        WHERE queue_name=$2 AND id=$5 AND status='processing' AND current_worker_id=$8 FOR UPDATE),
        stored AS (INSERT INTO runnerq_results(activity_id,queue_name,state,data,owner_activity_id,step,serialization)
        SELECT $1::uuid,$2,$3,$4::jsonb,$5::uuid,NULLIF($6,''),$7 WHERE EXISTS(SELECT 1 FROM fenced)
        ON CONFLICT(activity_id) DO NOTHING RETURNING activity_id),
        logged AS (INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail)
        SELECT $2,activity_id,'ResultStored',$8,$9::jsonb FROM stored)
        SELECT EXISTS(SELECT 1 FROM fenced) AS fenced,EXISTS(SELECT 1 FROM stored) AS stored`,
        [
          id,
          this.queue,
          result.state,
          JSON.stringify(result.data),
          f.ownerId,
          step,
          result.serialization,
          f.token,
          JSON.stringify({ state: result.state }),
        ],
      );
      if (!r.rows[0].fenced) throw lost();
      if (!r.rows[0].stored) {
        const same = await c.query(
          `SELECT 1 FROM runnerq_results WHERE activity_id=$1 AND queue_name=$2 AND state=$3
          AND data IS NOT DISTINCT FROM $4::jsonb AND owner_activity_id=$5 AND COALESCE(step,'')=$6 AND serialization=$7`,
          [
            id,
            this.queue,
            result.state,
            JSON.stringify(result.data),
            f.ownerId,
            step,
            result.serialization,
          ],
        );
        if (!same.rowCount)
          throw new RunnerQError(
            "checkpoint_conflict",
            "Checkpoint already contains a different outcome",
          );
        return;
      }
      await this.wake(c, id);
    });
    this.hints(id);
  }
  async getResult(id: string): Promise<StoredResult | null> {
    const r = await this.query(
      "SELECT state,data,serialization FROM runnerq_results WHERE queue_name=$1 AND activity_id=$2",
      [this.queue, id],
    );
    const row = r[0];
    if (!row) return null;
    if (row.state !== "Ok" && row.state !== "Err")
      throw new RunnerQError("serialization", "Invalid stored result state");
    return {
      state: row.state,
      data: row.data,
      serialization: row.serialization,
    };
  }
  async waitResult(id: string, signal?: AbortSignal): Promise<StoredResult> {
    const sub = this.notifications.subscribe(`result:${id}`, signal);
    try {
      for (;;) {
        signal?.throwIfAborted();
        const value = await this.getResult(id);
        if (value) return value;
        await sub.wait(5_000);
      }
    } finally {
      sub.close();
    }
  }
  async waitForWork(signal: AbortSignal, timeoutMs = 2_000): Promise<void> {
    const sub = this.notifications.subscribe("work", signal);
    try {
      await sub.wait(timeoutMs);
    } finally {
      sub.close();
    }
  }
  async registerDependency(f: Fence, id: string): Promise<void> {
    if (id === f.ownerId)
      throw new RunnerQError(
        "configuration",
        "An activity cannot await itself",
      );
    await this.tx(async (c) => {
      await this.lockProducer(c, id);
      await this.fence(c, f);
      await this.dependency(c, f.ownerId, id, id);
    });
  }
  async park(f: Fence, wait: Park): Promise<void> {
    await this.tx(async (c) => {
      try {
        await this.fence(c, f);
      } catch (error) {
        if (!(error instanceof RunnerQError) || error.code !== "claim_lost")
          throw error;
        const prior = await c.query(
          `SELECT 1 FROM runnerq_events WHERE queue_name=$1 AND activity_id=$2 AND worker_id=$3 AND event_type='Yielded'
          AND detail->>'kind'=$4 AND detail->>'step'=$5 AND detail->>'wake_at'=$6
          AND COALESCE(detail->>'result_id','')=$7 LIMIT 1`,
          [
            this.queue,
            f.ownerId,
            f.token,
            wait.kind,
            wait.step,
            wait.wakeAt,
            wait.resultId ?? "",
          ],
        );
        if (prior.rowCount) return;
        throw error;
      }
      if (wait.producerId) await this.lockProducer(c, wait.producerId);
      let ready = false;
      if (wait.resultId) {
        await this.dependency(
          c,
          f.ownerId,
          wait.resultId,
          wait.producerId ?? null,
        );
        ready = !!(
          await c.query(
            "SELECT 1 FROM runnerq_results WHERE queue_name=$1 AND activity_id=$2",
            [this.queue, wait.resultId],
          )
        ).rowCount;
      }
      await c.query(
        `WITH parked AS (UPDATE runnerq_activities SET status=CASE WHEN $4 THEN 'pending' ELSE 'waiting' END,
        scheduled_at=CASE WHEN $4 THEN NULL ELSE $5::timestamptz END,waiting_result_id=CASE WHEN $4 THEN NULL ELSE $6::uuid END,
        last_worker_id=$3,current_worker_id=NULL,lease_deadline_ms=NULL,started_at=NULL WHERE queue_name=$1 AND id=$2)
        INSERT INTO runnerq_events(queue_name,activity_id,event_type,worker_id,detail) VALUES($1,$2,'Yielded',$3,$7::jsonb)`,
        [
          this.queue,
          f.ownerId,
          f.token,
          ready,
          wait.wakeAt,
          wait.resultId ?? null,
          JSON.stringify({
            kind: wait.kind,
            step: wait.step,
            wake_at: wait.wakeAt,
            result_id: wait.resultId ?? null,
            ready,
          }),
        ],
      );
    });
    this.hints();
  }
  async signal(
    id: string,
    name: string,
    payload: SerializedValue,
  ): Promise<void> {
    const result = checkpointId(id, "signal", name);
    await this.tx(async (c) => {
      if (
        !(
          await c.query(
            "SELECT id FROM runnerq_activities WHERE queue_name=$1 AND id=$2 FOR NO KEY UPDATE",
            [this.queue, id],
          )
        ).rowCount
      )
        throw new RunnerQError("not_found", "Signal target does not exist");
      await this.putResult(
        c,
        result,
        id,
        { state: "Ok", ...payload },
        `signal:${name}`,
      );
      const wake = await c.query(
        "UPDATE runnerq_activities SET status='pending',scheduled_at=NULL,waiting_result_id=NULL WHERE queue_name=$1 AND id=$2 AND status='waiting' RETURNING id",
        [this.queue, id],
      );
      await this.event(c, id, "Signaled", null, {
        name,
        signal_id: result,
        woke: !!wake.rowCount,
      });
    });
    this.hints(result);
  }
  async lookupKey(key: string): Promise<string> {
    const r = await this.query(
      "SELECT activity_id FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2",
      [this.queue, key],
    );
    if (!r[0]) throw new RunnerQError("not_found", "No activity owns this key");
    return r[0].activity_id;
  }
  async reap(limit: number): Promise<number> {
    const ids = await this.tx(async (c) => {
      const rows = await c.query(
        `UPDATE runnerq_activities SET retry_count=retry_count+1,
        status=CASE WHEN max_retries>0 AND retry_count+1>=max_retries THEN 'dead_letter' ELSE 'pending' END,
        completed_at=CASE WHEN max_retries>0 AND retry_count+1>=max_retries THEN NOW() ELSE NULL END,
        last_error='lease expired before completion; worker presumed crashed or wedged',last_error_at=NOW(),
        last_worker_id=current_worker_id,current_worker_id=NULL,lease_deadline_ms=NULL,started_at=NULL,waiting_result_id=NULL
        WHERE id IN (SELECT id FROM runnerq_activities WHERE queue_name=$1 AND status='processing'
          AND lease_deadline_ms<(EXTRACT(EPOCH FROM NOW())*1000)::bigint LIMIT $2 FOR UPDATE SKIP LOCKED)
        RETURNING id,status,last_error`,
        [this.queue, integer(limit, "reaper limit", 1)],
      );
      for (const a of rows.rows) {
        if (a.status === "dead_letter")
          await this.putResult(
            c,
            a.id,
            a.id,
            {
              state: "Err",
              serialization: "json-v1",
              data: {
                error: a.last_error,
                type: "dead_letter",
                failed_at: new Date().toISOString(),
              },
            },
            null,
          );
        await this.event(
          c,
          a.id,
          a.status === "dead_letter" ? "DeadLetter" : "Requeued",
          null,
          { reason: "lease_expired", error: a.last_error },
        );
      }
      return rows.rows;
    });
    for (const a of ids)
      this.hints(a.status === "dead_letter" ? a.id : undefined);
    return ids.length;
  }
  async cleanup(policy: Retention): Promise<number> {
    const completed = integer(policy.completedMs ?? 0, "completedMs"),
      failed = integer(policy.failedMs ?? 0, "failedMs");
    const batch = integer(policy.batchSize ?? 100, "batchSize", 1);
    if (!completed && !failed) return 0;
    return this.tx(async (c) => {
      if (
        !(
          await c.query(
            "SELECT pg_try_advisory_xact_lock(1381913428,hashtext($1)) AS locked",
            [this.queue],
          )
        ).rows[0].locked
      )
        return 0;
      const skipped: string[] = [];
      let removed = 0;
      // Bound examined roots too, so pinned trees cannot make the transaction unbounded.
      for (
        let examined = 0;
        removed < batch && examined < batch * 10;
        examined++
      ) {
        await c.query("SAVEPOINT candidate");
        const r = await c.query(
          `SELECT r.id FROM runnerq_activities r WHERE r.queue_name=$1 AND r.parent_activity_id IS NULL
          AND r.id<>ALL($4::uuid[]) AND ((r.status='completed' AND $2::bigint>0 AND r.completed_at<NOW()-$2*INTERVAL '1 millisecond')
          OR (r.status IN ('failed','dead_letter','cancelled') AND $3::bigint>0 AND r.completed_at<NOW()-$3*INTERVAL '1 millisecond'))
          AND NOT EXISTS(SELECT 1 FROM runnerq_activities a WHERE a.queue_name=$1 AND a.root_activity_id=r.id AND a.status NOT IN ${terminalSQL})
          ORDER BY r.completed_at LIMIT 1 FOR UPDATE SKIP LOCKED`,
          [this.queue, completed, failed, skipped],
        );
        const root = r.rows[0]?.id;
        if (!root) {
          await c.query("RELEASE SAVEPOINT candidate");
          break;
        }
        if (await lockTree(c, this.queue, root)) {
          skipped.push(root);
          await c.query("ROLLBACK TO SAVEPOINT candidate");
          await c.query("RELEASE SAVEPOINT candidate");
          continue;
        }
        await deleteTree(c, this.queue, root);
        await c.query("RELEASE SAVEPOINT candidate");
        removed++;
      }
      return removed;
    });
  }
  private snapshot(a: Row): ActivitySnapshot {
    return {
      id: a.id,
      type: a.activity_type,
      status: a.status,
      priority: a.priority,
      createdAt: iso(a.created_at)!,
      scheduledAt: iso(a.scheduled_at),
      startedAt: iso(a.started_at),
      completedAt: iso(a.completed_at),
      retryCount: a.retry_count,
      maxAttempts: a.max_retries === 0 ? "unlimited" : a.max_retries,
      timeoutMs: Number(a.timeout_seconds) * 1000,
      currentWorkerId: a.current_worker_id,
      lastWorkerId: a.last_worker_id,
      leaseDeadlineMs:
        a.lease_deadline_ms === null ? null : Number(a.lease_deadline_ms),
      parentId: a.parent_activity_id,
      rootId: a.root_activity_id,
      depth: a.depth,
      metadata: a.metadata ?? {},
      lastError: a.last_error,
      lastErrorAt: iso(a.last_error_at),
      idempotencyKey: a.idempotency_key,
      waitingResultId: a.waiting_result_id,
    };
  }
  async list(options: ListOptions = {}): Promise<ActivitySnapshot[]> {
    const values: unknown[] = [this.queue];
    const predicates = ["queue_name=$1"];
    if (options.rootsOnly) predicates.push("parent_activity_id IS NULL");
    for (const [column, value] of [
      ["status", options.status],
      ["parent_activity_id", options.parentId],
      ["root_activity_id", options.rootId],
      ["metadata->>'source'", options.source],
    ]) {
      if (value !== undefined) {
        values.push(value);
        predicates.push(`${column}=$${values.length}`);
      }
    }
    values.push(
      integer(options.limit ?? 50, "limit", 1, 1000),
      integer(options.offset ?? 0, "offset"),
    );
    const rows = await this.query(
      `SELECT * FROM runnerq_activities WHERE ${predicates.join(" AND ")} ORDER BY created_at DESC,id LIMIT $${values.length - 1} OFFSET $${values.length}`,
      values,
    );
    return rows.map((a) => this.snapshot(a));
  }
  async getActivity(id: string): Promise<ActivitySnapshot | null> {
    const rows = await this.query(
      "SELECT * FROM runnerq_activities WHERE queue_name=$1 AND id=$2",
      [this.queue, id],
    );
    return rows[0] ? this.snapshot(rows[0]) : null;
  }
  async getInput(id: string): Promise<SerializedValue> {
    const rows = await this.query(
      "SELECT payload,serialization FROM runnerq_inputs WHERE queue_name=$1 AND activity_id=$2",
      [this.queue, id],
    );
    if (!rows[0])
      throw new RunnerQError("not_found", "Activity input not found");
    return {
      data: rows[0].payload,
      serialization: rows[0].serialization,
    };
  }
  async steps(id: string): Promise<StepRecord[]> {
    return (
      await this.query(
        "SELECT * FROM runnerq_results WHERE queue_name=$1 AND owner_activity_id=$2 AND step IS NOT NULL ORDER BY created_at,activity_id",
        [this.queue, id],
      )
    ).map((r) => {
      const colon = r.step.indexOf(":");
      return {
        id: r.activity_id,
        kind: r.step.slice(0, colon),
        name: r.step.slice(colon + 1),
        state: r.state,
        data: r.data,
        serialization: r.serialization,
        createdAt: iso(r.created_at)!,
      };
    });
  }
  private toEvent(r: Row): ActivityEvent {
    return {
      id: String(r.id),
      activityId: r.activity_id,
      type: r.event_type,
      timestamp: iso(r.created_at)!,
      workerId: r.worker_id,
      detail: r.detail,
    };
  }
  async events(id: string, limit = 100): Promise<ActivityEvent[]> {
    return (
      await this.query(
        "SELECT * FROM runnerq_events WHERE queue_name=$1 AND activity_id=$2 ORDER BY id DESC LIMIT $3",
        [this.queue, id, integer(limit, "limit", 1, 1000)],
      )
    ).map((r) => this.toEvent(r));
  }
  // QueryStorage (RunnerQ Cloud's reads) spans every queue in the schema.
  queryCapabilities(): QueryCapabilities {
    return queryCapabilities();
  }
  queryActivities(query: ActivityQuery): Promise<ActivityRecordPage> {
    return this.queries.activities(query);
  }
  countActivities(
    filter: QueryFilter | undefined,
    limit: number,
  ): Promise<{ count: number; exact: boolean }> {
    return this.queries.count(filter, limit);
  }
  aggregateActivities(query: AggregateQuery): Promise<AggregateRows> {
    return this.queries.aggregate(query);
  }
  queryEvents(query: EventQuery): Promise<EventRecordPage> {
    return this.queries.events(query);
  }
  listStepEntries(
    activityId: string,
    includeData: boolean,
    limit: number,
    cursor: string,
  ): Promise<StepEntryPage> {
    return this.queries.steps(activityId, includeData, limit, cursor);
  }
  getActivityTree(
    activityId: string,
    include: RecordInclude,
    maxNodes: number,
  ): Promise<ActivityTree> {
    return this.queries.tree(activityId, include, maxNodes);
  }
  /** Applies a RunnerQ Cloud command to this storage's queue; see `CommandStorage`. */
  applyCommand(command: Command): Promise<CommandResult> {
    return this.commands.apply(command);
  }
  close(): Promise<void> {
    return (this.closing ??= (async () => {
      await this.notifications.close();
      await this.pool.end();
    })());
  }
}
