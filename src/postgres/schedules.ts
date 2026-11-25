import { randomUUID } from "node:crypto";
import type { PoolClient } from "pg";
import { RunnerQError } from "../errors.js";
import type { Priority } from "../options.js";
import {
  nextFireTime,
  scheduleFireKey,
  scheduleFireTimes,
  validateSchedule,
  type ScheduleDefinition,
  type ScheduleRecord,
  type ScheduleStorage,
} from "../schedule.js";
import type { JsonValue } from "../codec.js";
import type { SerializationFormat } from "../serialization.js";

type Row = Record<string, any>;
const priorities: Priority[] = ["low", "normal", "high", "critical"];
const columns = `name,cron,timezone,activity_type,payload,serialization,options,catch_up,catch_up_limit,
  paused,next_fire_at,last_fire_at,created_at,updated_at`;

function notFound(name: string): RunnerQError {
  return new RunnerQError(
    "not_found",
    `Schedule ${JSON.stringify(name)} not found`,
  );
}

/** runnerq_schedules.options, absent keys taking runnerq-spec's defaults. */
function storedOptions(s: ScheduleDefinition): Record<string, JsonValue> {
  const o = s.options;
  return {
    priority: priorities.indexOf(o.priority) + 1,
    max_retries: o.maxAttempts === "unlimited" ? 0 : o.maxAttempts,
    timeout_seconds: o.timeoutMs / 1000,
    retry_delay_seconds: 1,
    max_retry_delay_seconds: o.maxRetryDelayMs / 1000,
    metadata: { ...o.metadata },
    ...(o.version && { code_version: o.version }),
  };
}
function readOptions(raw: Row | null): ActivityColumns {
  const o = raw ?? {};
  return {
    priority: o.priority ?? 2,
    maxRetries: o.max_retries ?? 0,
    timeoutSeconds: o.timeout_seconds ?? 300,
    retryDelaySeconds: o.retry_delay_seconds ?? 1,
    maxRetryDelaySeconds: o.max_retry_delay_seconds ?? 0,
    metadata: o.metadata ?? {},
    codeVersion: typeof o.code_version === "string" ? o.code_version : null,
  };
}
interface ActivityColumns {
  priority: number;
  maxRetries: number;
  timeoutSeconds: number;
  retryDelaySeconds: number;
  maxRetryDelaySeconds: number;
  metadata: Record<string, string>;
  codeVersion: string | null;
}

function record(r: Row): ScheduleRecord {
  const o = readOptions(r.options);
  return {
    name: r.name,
    cron: r.cron,
    timezone: r.timezone,
    type: r.activity_type,
    payload: r.payload,
    serialization: r.serialization as SerializationFormat,
    options: {
      priority: priorities[o.priority - 1] ?? "normal",
      maxAttempts: o.maxRetries === 0 ? "unlimited" : o.maxRetries,
      timeoutMs: o.timeoutSeconds * 1000,
      maxRetryDelayMs: o.maxRetryDelaySeconds * 1000,
      metadata: o.metadata,
      ...(o.codeVersion && { version: o.codeVersion }),
    },
    catchUp: r.catch_up,
    catchUpLimit: r.catch_up_limit,
    paused: r.paused,
    nextFireAt: r.next_fire_at.toISOString(),
    lastFireAt: r.last_fire_at?.toISOString() ?? null,
    createdAt: r.created_at.toISOString(),
    updatedAt: r.updated_at.toISOString(),
  };
}

export interface ScheduleHost {
  queue: string;
  query(sql: string, values?: unknown[]): Promise<Row[]>;
  tx<T>(body: (c: PoolClient) => Promise<T>): Promise<T>;
  /** Announces committed runnable work. */
  work(): void;
}

/** PostgresStorage's ScheduleStorage (runnerq-spec schedules.md). */
export class PostgresSchedules implements ScheduleStorage {
  constructor(private readonly host: ScheduleHost) {}

  async upsertSchedule(s: ScheduleDefinition): Promise<void> {
    validateSchedule(s);
    const [{ now }] = (await this.host.query("SELECT NOW() AS now")) as [Row];
    const next = nextFireTime(s.cron, s.timezone, now);
    await this.host.query(
      `INSERT INTO runnerq_schedules AS s (queue_name,name,cron,timezone,activity_type,payload,serialization,
        options,catch_up,catch_up_limit,next_fire_at)
      VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9,$10,$11)
      ON CONFLICT (queue_name,name) DO UPDATE SET
        cron=EXCLUDED.cron,timezone=EXCLUDED.timezone,activity_type=EXCLUDED.activity_type,
        payload=EXCLUDED.payload,serialization=EXCLUDED.serialization,options=EXCLUDED.options,
        catch_up=EXCLUDED.catch_up,catch_up_limit=EXCLUDED.catch_up_limit,
        next_fire_at=CASE WHEN s.cron=EXCLUDED.cron AND s.timezone=EXCLUDED.timezone
          THEN s.next_fire_at ELSE EXCLUDED.next_fire_at END,
        updated_at=NOW()
      WHERE (s.cron,s.timezone,s.activity_type,s.payload,s.serialization,s.options,s.catch_up,s.catch_up_limit)
        IS DISTINCT FROM (EXCLUDED.cron,EXCLUDED.timezone,EXCLUDED.activity_type,EXCLUDED.payload,
          EXCLUDED.serialization,EXCLUDED.options,EXCLUDED.catch_up,EXCLUDED.catch_up_limit)`,
      [
        this.host.queue,
        s.name,
        s.cron,
        s.timezone,
        s.type,
        JSON.stringify(s.payload ?? null),
        s.serialization,
        JSON.stringify(storedOptions(s)),
        s.catchUp,
        s.catchUpLimit,
        next,
      ],
    );
  }

  async getSchedule(name: string): Promise<ScheduleRecord> {
    const [r] = await this.host.query(
      `SELECT ${columns} FROM runnerq_schedules WHERE queue_name=$1 AND name=$2`,
      [this.host.queue, name],
    );
    if (!r) throw notFound(name);
    return record(r);
  }

  async listSchedules(): Promise<ScheduleRecord[]> {
    return (
      await this.host.query(
        `SELECT ${columns} FROM runnerq_schedules WHERE queue_name=$1 ORDER BY name`,
        [this.host.queue],
      )
    ).map(record);
  }

  async pauseSchedule(name: string): Promise<void> {
    const rows = await this.host.query(
      `UPDATE runnerq_schedules SET paused=TRUE,updated_at=CASE WHEN paused THEN updated_at ELSE NOW() END
      WHERE queue_name=$1 AND name=$2 RETURNING name`,
      [this.host.queue, name],
    );
    if (!rows.length) throw notFound(name);
  }

  async resumeSchedule(name: string): Promise<void> {
    await this.host.tx(async (c) => {
      const r = (
        await c.query(
          `SELECT cron,timezone,paused,NOW() AS now FROM runnerq_schedules
          WHERE queue_name=$1 AND name=$2 FOR UPDATE`,
          [this.host.queue, name],
        )
      ).rows[0];
      if (!r) throw notFound(name);
      if (!r.paused) return;
      await c.query(
        `UPDATE runnerq_schedules SET paused=FALSE,next_fire_at=$3,updated_at=NOW()
        WHERE queue_name=$1 AND name=$2`,
        [this.host.queue, name, nextFireTime(r.cron, r.timezone, r.now)],
      );
    });
  }

  async deleteSchedule(name: string): Promise<void> {
    const rows = await this.host.query(
      "DELETE FROM runnerq_schedules WHERE queue_name=$1 AND name=$2 RETURNING name",
      [this.host.queue, name],
    );
    if (!rows.length) throw notFound(name);
  }

  async triggerSchedule(name: string): Promise<string> {
    const { id, created } = await this.host.tx(async (c) => {
      const r = (
        await c.query(
          `SELECT ${columns},NOW() AS now FROM runnerq_schedules
          WHERE queue_name=$1 AND name=$2 FOR SHARE`,
          [this.host.queue, name],
        )
      ).rows[0];
      if (!r) throw notFound(name);
      return this.enqueue(c, r, r.now, r.now);
    });
    if (created) this.host.work();
    return id;
  }

  async fireSchedules(limit: number): Promise<number> {
    let fired = 0;
    const failed: string[] = [];
    const errors: Error[] = [];
    for (let i = 0; i < Math.max(limit, 1); i++) {
      let name: string | undefined;
      try {
        const n = await this.host.tx(async (c) => {
          const r = (
            await c.query(
              `SELECT ${columns},NOW() AS now FROM runnerq_schedules
              WHERE queue_name=$1 AND NOT paused AND next_fire_at<=NOW() AND NOT (name=ANY($2))
              ORDER BY next_fire_at LIMIT 1 FOR UPDATE SKIP LOCKED`,
              [this.host.queue, failed],
            )
          ).rows[0];
          if (!r) return undefined;
          name = r.name;
          const { fire, next } = scheduleFireTimes(
            {
              cron: r.cron,
              timezone: r.timezone,
              catchUp: r.catch_up,
              catchUpLimit: r.catch_up_limit,
              nextFireAt: r.next_fire_at.toISOString(),
            },
            r.now,
          );
          let created = 0;
          for (const at of fire)
            if ((await this.enqueue(c, r, at, r.now)).created) created++;
          await c.query(
            `UPDATE runnerq_schedules SET next_fire_at=$3,last_fire_at=COALESCE($4,last_fire_at)
            WHERE queue_name=$1 AND name=$2`,
            [this.host.queue, r.name, next, fire.at(-1) ?? null],
          );
          return created;
        });
        if (n === undefined) break;
        fired += n;
      } catch (error) {
        if (name === undefined) {
          errors.push(error as Error);
          break;
        }
        failed.push(name);
        errors.push(error as Error);
      }
    }
    if (fired) this.host.work();
    if (errors.length) {
      const first = errors[0]!;
      if (errors.length === 1) throw first;
      throw new AggregateError(errors, first.message);
    }
    return fired;
  }

  /** Enqueues the schedule's activity for a fire time unless its key exists. */
  private async enqueue(
    c: PoolClient,
    r: Row,
    at: Date,
    now: Date,
  ): Promise<{ id: string; created: boolean }> {
    const o = readOptions(r.options);
    const id = randomUUID();
    const result = await c.query(
      `WITH key AS (
        INSERT INTO runnerq_idempotency(queue_name,idempotency_key,activity_id) VALUES($1,$2,$3)
        ON CONFLICT DO NOTHING RETURNING activity_id),
      activity AS (
        INSERT INTO runnerq_activities(id,queue_name,activity_type,priority,status,created_at,max_retries,
          timeout_seconds,retry_delay_seconds,max_retry_delay_seconds,metadata,idempotency_key,root_activity_id,depth,code_version)
        SELECT $3,$1,$4,$5,'pending',$6,$7,$8,$9,$10,$11::jsonb,$2,$3,0,$14 FROM key RETURNING id),
      input AS (
        INSERT INTO runnerq_inputs(activity_id,queue_name,payload,serialization)
        SELECT id,$1,$12::jsonb,$13 FROM activity)
      SELECT (SELECT activity_id FROM key) AS created,
        (SELECT activity_id FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2) AS existing`,
      [
        this.host.queue,
        scheduleFireKey(r.name, r.activity_type, at),
        id,
        r.activity_type,
        o.priority,
        now,
        o.maxRetries,
        o.timeoutSeconds,
        o.retryDelaySeconds,
        o.maxRetryDelaySeconds,
        JSON.stringify(o.metadata),
        JSON.stringify(r.payload ?? null),
        r.serialization,
        o.codeVersion,
      ],
    );
    const row = result.rows[0]!;
    return row.created
      ? { id: row.created, created: true }
      : { id: row.existing, created: false };
  }
}
