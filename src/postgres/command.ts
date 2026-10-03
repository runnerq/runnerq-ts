// CommandStorage for PostgreSQL: a port of runnerq-go's storage/postgres/command.go. The
// ledger (runnerq_commands) and its JSON are shared with Go, so a command id replays alike
// whichever SDK's executor receives it again.
import type { Pool, PoolClient } from "pg";
import {
  checkpointId,
  isTimestamp,
  parseUuid,
  type JsonValue,
} from "../codec.js";
import { databaseError, RunnerQError } from "../errors.js";
import { pause } from "../async.js";
import { storageErrorKind } from "../spec.js";
import { QueryError, type RecordStatus } from "../query.js";
import type {
  Command,
  CommandItem,
  CommandKind,
  CommandResult,
  StoredResult,
} from "../storage.js";
import { canonicalStatus, SqlBuilder } from "./query.js";
import { deleteTree, lockTree, terminalSQL } from "./trees.js";

const maxCommandIds = 1000;
const maxCommandFilter = 10_000;
/** The protocol promises replay for at least 24 hours; Go keeps a week. */
const ledgerKeepMs = 7 * 24 * 3600_000;

const terminal = new Set(["completed", "failed", "dead_letter", "cancelled"]);
const failedStatuses = new Set(["failed", "dead_letter", "cancelled"]);

/**
 * Narrows filter targets to what the command can act on, so repeating a bounded command
 * ("retry the next 100 dead letters") makes progress instead of re-selecting handled rows.
 */
const eligibleSQL: Record<CommandKind, string> = {
  cancel: `a.status NOT IN ${terminalSQL}`,
  set_priority: `a.status NOT IN ${terminalSQL}`,
  signal: `a.status NOT IN ${terminalSQL}`,
  retry: "a.status IN ('failed','dead_letter','cancelled')",
  run_now: "a.status IN ('scheduled','retrying','waiting')",
  reschedule: "a.status IN ('scheduled','retrying')",
  delete: `a.status IN ${terminalSQL} AND a.parent_activity_id IS NULL`,
};

interface LedgerItem {
  id: string;
  outcome: CommandItem["outcome"];
  status?: string;
  err_kind?: number;
  err_message?: string;
}
interface Ledger {
  matched: number;
  applied: number;
  cascaded: number;
  more: boolean;
  items: LedgerItem[] | null;
}
function toLedger(r: CommandResult): Ledger {
  return {
    matched: r.matched,
    applied: r.applied,
    cascaded: r.cascaded,
    more: r.more,
    items: r.items.map((it) => {
      const l: LedgerItem = { id: it.id, outcome: it.outcome };
      if (it.status) l.status = it.status;
      if (it.error) {
        l.err_kind = storageErrorKind[it.error.kind];
        l.err_message = it.error.message;
      }
      return l;
    }),
  };
}
function fromLedger(l: Ledger): CommandResult {
  return {
    matched: l.matched,
    applied: l.applied,
    cascaded: l.cascaded,
    more: l.more,
    replayed: true,
    items: (l.items ?? []).map((it) => {
      const item: CommandItem = { id: it.id, outcome: it.outcome };
      if (it.status) item.status = it.status as RecordStatus;
      if (it.err_message)
        item.error = {
          kind:
            it.err_kind === storageErrorKind.not_found
              ? "not_found"
              : "conflict",
          message: it.err_message,
        };
      return item;
    }),
  };
}

/** A reschedule time: `sql` truncated to microseconds, as Go sends it; `detail` as Go formats it. */
interface Time {
  sql: string;
  detail: string;
  ms: number;
}
/** RFC 3339 in UTC with the full fraction (Go's RFC3339Nano), or undefined. */
function toTime(at: Date | string | undefined): Time | undefined {
  let text: string;
  if (at instanceof Date) {
    if (Number.isNaN(at.getTime())) return undefined;
    text = at.toISOString();
  } else if (isTimestamp(at)) text = at.toUpperCase();
  else return undefined;
  const m = /^(.+T\d\d:\d\d:\d\d)(?:\.(\d+))?(.*)$/.exec(text)!;
  const whole = new Date(m[1]! + m[3]!);
  const seconds = whole.toISOString().slice(0, 19);
  const fraction = (m[2] ?? "").replace(/0+$/, "");
  // Go's zero time means "no time".
  if (seconds === "0001-01-01T00:00:00" && !fraction) return undefined;
  const micro = fraction.slice(0, 6);
  return {
    sql: `${seconds}${micro ? "." + micro : ""}Z`,
    detail: `${seconds}${fraction ? "." + fraction : ""}Z`,
    ms: whole.getTime() + Number(`0.${fraction || 0}`) * 1000,
  };
}

function validate(cmd: Command): Time | undefined {
  const t = cmd.target ?? {};
  const ids = t.ids ?? [];
  const kinds = [ids.length > 0, !!t.filter, !!t.idempotencyKey].filter(
    Boolean,
  ).length;
  const invalid = (field: string, message: string) =>
    new QueryError("invalid_argument", message, field);
  if (kinds !== 1)
    throw invalid(
      "target",
      "target needs exactly one of ids, filter or idempotency key",
    );
  if (ids.length > maxCommandIds)
    throw invalid("target.ids", `at most ${maxCommandIds} ids`);
  const max = t.max ?? 0;
  if (
    t.filter &&
    !(Number.isInteger(max) && max >= 1 && max <= maxCommandFilter)
  )
    throw invalid(
      "target.max",
      `a filter target needs max between 1 and ${maxCommandFilter}`,
    );
  if (t.idempotencyKey && cmd.kind !== "signal")
    throw invalid(
      "target.idempotency_key",
      "idempotency key targets are only valid for signal",
    );
  switch (cmd.kind) {
    case "cancel":
    case "retry":
    case "run_now":
    case "delete":
      return undefined;
    case "reschedule": {
      const at = toTime(cmd.at);
      if (!at) throw invalid("at", "reschedule needs a time");
      return at;
    }
    case "set_priority": {
      const p = cmd.priority;
      if (!Number.isInteger(p) || p! < 1 || p! > 4)
        throw invalid("priority", "priority must be 1 (low) to 4 (critical)");
      return undefined;
    }
    case "signal":
      if (!cmd.signalName) throw invalid("name", "signal needs a name");
      return undefined;
  }
  throw new QueryError(
    "unsupported",
    `unknown command ${JSON.stringify(cmd.kind)}`,
    "kind",
  );
}

/** Notifications to send once the command commits. */
interface Post {
  work: boolean;
  results: string[];
}
interface Row {
  id: string;
  status: string;
  parent: string | null;
}
type ResultInput = Omit<StoredResult, "data"> & { data?: JsonValue };

/** What the commands need from PostgresStorage. */
export interface CommandHost {
  queue: string;
  pool: Pool;
  putResult(
    c: PoolClient,
    id: string,
    owner: string,
    result: ResultInput,
    step: string | null,
  ): Promise<void>;
  event(
    c: PoolClient,
    id: string,
    type: string,
    token: null,
    detail: unknown,
  ): Promise<void>;
  notify(results: string[], work: boolean): void;
}

export class PostgresCommands {
  constructor(private readonly host: CommandHost) {}

  async apply(cmd: Command): Promise<CommandResult> {
    const at = validate(cmd);
    let out: { res: CommandResult; post: Post };
    // Multi-row locking can lose a deadlock/serialization race; the transaction rolled back
    // whole, so rerunning is safe.
    for (let attempt = 0; ; attempt++) {
      try {
        out = await this.transaction(cmd, at);
        break;
      } catch (error) {
        if (error instanceof QueryError || error instanceof RunnerQError)
          throw error;
        const code = (error as { code?: unknown }).code;
        if (code === "42P01" && /runnerq_commands/.test(String(error)))
          throw new RunnerQError(
            "configuration",
            "The runnerq_commands table is missing: run PostgresStorage.initialize to add it",
            { cause: error },
          );
        if (code !== "40P01" && code !== "40001") throw databaseError(error);
        if (attempt === 3)
          throw new RunnerQError("internal", String(error), { cause: error });
        await pause(20 << attempt);
      }
    }
    if (!out.res.replayed && !cmd.dryRun)
      this.host.notify(out.post.results, out.post.work);
    return out.res;
  }

  private async transaction(
    cmd: Command,
    at: Time | undefined,
  ): Promise<{ res: CommandResult; post: Post }> {
    const c = await this.host.pool.connect();
    let destroy = false;
    try {
      await c.query("BEGIN");
      try {
        const out = await this.body(c, cmd, at);
        await c.query(cmd.dryRun || out.res.replayed ? "ROLLBACK" : "COMMIT");
        return out;
      } catch (error) {
        try {
          await c.query("ROLLBACK");
        } catch {
          destroy = true;
        }
        throw error;
      }
    } finally {
      c.release(destroy);
    }
  }

  private async body(
    c: PoolClient,
    cmd: Command,
    at: Time | undefined,
  ): Promise<{ res: CommandResult; post: Post }> {
    const { queue } = this.host;
    const post: Post = { work: false, results: [] };
    const ledger = !!cmd.id && !cmd.dryRun;
    if (ledger) {
      // Serialize deliveries of one command id (with Go's too), then replay a recorded
      // result instead of applying twice.
      await c.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        `runnerq_command:${queue}:${cmd.id}`,
      ]);
      const r = await c.query(
        "SELECT fingerprint,result FROM runnerq_commands WHERE queue_name=$1 AND command_id=$2",
        [queue, cmd.id],
      );
      if (r.rows[0]) {
        if (r.rows[0].fingerprint !== (cmd.fingerprint ?? ""))
          throw new RunnerQError(
            "conflict",
            `command ${JSON.stringify(cmd.id)} was already applied with different input`,
          );
        return { res: fromLedger(r.rows[0].result), post };
      }
    }

    const { ids, more } = await this.resolve(c, cmd);
    const rows = await this.lock(c, ids);
    const res: CommandResult = {
      matched: 0,
      applied: 0,
      cascaded: 0,
      more,
      items: [],
      replayed: false,
    };
    const now = new Date();
    const cancelled: string[] = [];
    for (const id of ids) {
      const row = rows.get(id);
      if (!row) {
        res.items.push({
          id,
          outcome: "skipped",
          error: {
            kind: "not_found",
            message: "no such activity in this queue",
          },
        });
        continue;
      }
      res.matched++;
      const item = await this.applyOne(c, cmd, at, row, now, post);
      if (item.outcome === "applied") res.applied++;
      if (item.outcome !== "skipped" && cmd.kind === "cancel")
        cancelled.push(id);
      res.items.push(item);
    }
    if (cmd.kind === "cancel" && cmd.cascadeChildren && cancelled.length)
      res.cascaded = await this.cancelDescendants(c, cmd, cancelled, now, post);

    if (ledger) {
      await c.query(
        "INSERT INTO runnerq_commands(queue_name,command_id,fingerprint,kind,result) VALUES($1,$2,$3,$4,$5::jsonb)",
        [
          queue,
          cmd.id,
          cmd.fingerprint ?? "",
          cmd.kind,
          JSON.stringify(toLedger(res)),
        ],
      );
      if (Math.random() < 1 / 64)
        await c.query(
          `DELETE FROM runnerq_commands WHERE ctid IN (
          SELECT ctid FROM runnerq_commands WHERE created_at<$1 LIMIT 1000)`,
          [new Date(now.getTime() - ledgerKeepMs)],
        );
    }
    return { res, post };
  }

  /** The target's ids in order, without duplicates; `more` when a filter matched over max. */
  private async resolve(
    c: PoolClient,
    cmd: Command,
  ): Promise<{ ids: string[]; more: boolean }> {
    const { queue } = this.host;
    const t = cmd.target;
    if (t.ids?.length) {
      // An id that isn't a UUID matches nothing: reported as not found.
      const ids = t.ids.map((id) => parseUuid(id) ?? id);
      return { ids: [...new Set(ids)], more: false };
    }
    if (t.idempotencyKey) {
      const r = await c.query(
        "SELECT activity_id FROM runnerq_idempotency WHERE queue_name=$1 AND idempotency_key=$2",
        [queue, t.idempotencyKey],
      );
      return { ids: r.rows.map((x) => x.activity_id), more: false };
    }
    const sb = new SqlBuilder();
    const where = sb.where(t.filter, "activities");
    const max = t.max!;
    const r = await c.query(
      `SELECT a.id FROM runnerq_activities a WHERE a.queue_name=${sb.arg(queue)} AND ${eligibleSQL[cmd.kind]} AND ${where}
      ORDER BY a.created_at,a.id LIMIT ${max + 1}`,
      sb.args,
    );
    const ids = r.rows.map((x) => x.id as string);
    return ids.length > max
      ? { ids: ids.slice(0, max), more: true }
      : { ids, more: false };
  }

  /** Locks in id order so concurrent commands don't deadlock each other. */
  private async lock(c: PoolClient, ids: string[]): Promise<Map<string, Row>> {
    const valid = ids.filter((id) => parseUuid(id));
    const out = new Map<string, Row>();
    if (!valid.length) return out;
    const r = await c.query(
      `SELECT id,status,parent_activity_id FROM runnerq_activities
      WHERE queue_name=$1 AND id=ANY($2::uuid[]) ORDER BY id FOR UPDATE`,
      [this.host.queue, valid],
    );
    for (const x of r.rows)
      out.set(x.id, {
        id: x.id,
        status: x.status,
        parent: x.parent_activity_id,
      });
    return out;
  }

  private async applyOne(
    c: PoolClient,
    cmd: Command,
    at: Time | undefined,
    row: Row,
    now: Date,
    post: Post,
  ): Promise<CommandItem> {
    const { queue } = this.host;
    const dry = !!cmd.dryRun;
    const ok = (after: string): CommandItem => ({
      id: row.id,
      outcome: dry ? "would_apply" : "applied",
      status: canonicalStatus(after),
    });
    const detail: Record<string, unknown> = { command_id: cmd.id ?? "" };
    if (cmd.reason) detail.reason = cmd.reason;
    const exec = async (sql: string, values: unknown[]) => {
      if (!dry) await c.query(sql, values);
    };
    const event = async (type: string) => {
      if (!dry) await this.host.event(c, row.id, type, null, detail);
    };

    switch (cmd.kind) {
      case "cancel":
        if (terminal.has(row.status)) return skipped(row, "already finished");
        await this.cancelRow(c, cmd, row, now, post);
        return ok("cancelled");

      case "retry": {
        if (!failedStatuses.has(row.status))
          return skipped(
            row,
            "only failed, dead-lettered or cancelled activities can be retried",
          );
        await exec(
          `UPDATE runnerq_activities SET status='pending',completed_at=NULL,started_at=NULL,
          scheduled_at=NULL,current_worker_id=NULL,lease_deadline_ms=NULL,waiting_result_id=NULL,
          retry_count=CASE WHEN $3 THEN 0 ELSE retry_count END
          WHERE id=$1 AND queue_name=$2`,
          [row.id, queue, !!cmd.resetAttempts],
        );
        // Checkpoints stay, so the rerun replays completed steps.
        await exec(
          "DELETE FROM runnerq_results WHERE queue_name=$1 AND activity_id=$2",
          [queue, row.id],
        );
        detail.from = row.status;
        detail.reset_attempts = !!cmd.resetAttempts;
        await event(row.status === "dead_letter" ? "Redriven" : "Retried");
        post.work = true;
        return ok("pending");
      }

      case "run_now": {
        if (row.status === "scheduled" || row.status === "retrying")
          // The database clock: claims compare against NOW(), and an app clock ahead of
          // the database would leave it not yet due.
          await exec(
            "UPDATE runnerq_activities SET scheduled_at=NOW() WHERE id=$1 AND queue_name=$2",
            [row.id, queue],
          );
        else if (row.status === "waiting")
          await exec(
            "UPDATE runnerq_activities SET status='pending',scheduled_at=NULL,waiting_result_id=NULL WHERE id=$1 AND queue_name=$2",
            [row.id, queue],
          );
        else if (row.status === "pending")
          return skipped(row, "already runnable");
        else
          return skipped(
            row,
            "only scheduled or waiting activities can be run now",
          );
        await event("RunNow");
        post.work = true;
        return ok(row.status === "waiting" ? "pending" : row.status);
      }

      case "reschedule":
        if (row.status !== "scheduled" && row.status !== "retrying")
          return skipped(row, "only scheduled activities can be rescheduled");
        await exec(
          "UPDATE runnerq_activities SET scheduled_at=$3::timestamptz WHERE id=$1 AND queue_name=$2",
          [row.id, queue, at!.sql],
        );
        detail.at = at!.detail;
        await event("Rescheduled");
        if (at!.ms <= now.getTime()) post.work = true;
        return ok(row.status);

      case "set_priority":
        if (terminal.has(row.status)) return skipped(row, "already finished");
        await exec(
          "UPDATE runnerq_activities SET priority=$3 WHERE id=$1 AND queue_name=$2",
          [row.id, queue, cmd.priority],
        );
        detail.priority = cmd.priority;
        await event("PriorityChanged");
        return ok(row.status);

      case "delete":
        return this.deleteRow(c, cmd, row);

      case "signal": {
        if (terminal.has(row.status))
          return skipped(
            row,
            "already finished; nothing is waiting for the signal",
          );
        if (dry) return ok(row.status);
        const name = cmd.signalName!;
        const signalId = checkpointId(row.id, "signal", name);
        await this.host.putResult(
          c,
          signalId,
          row.id,
          { state: "Ok", serialization: "json-v1", data: cmd.signalPayload },
          `signal:${name}`,
        );
        const woke = !!(
          await c.query(
            "UPDATE runnerq_activities SET status='pending',scheduled_at=NULL,waiting_result_id=NULL WHERE id=$1 AND queue_name=$2 AND status='waiting'",
            [row.id, queue],
          )
        ).rowCount;
        Object.assign(detail, { signal_id: signalId, name, woke });
        await this.host.event(c, row.id, "Signaled", null, detail);
        post.results.push(signalId);
        if (woke) post.work = true;
        return ok(woke ? "pending" : row.status);
      }
    }
  }

  /**
   * A running handler finds its claim gone at its next heartbeat, and its ack is fenced out.
   * The stored error result wakes awaiters with a cancellation error.
   */
  private async cancelRow(
    c: PoolClient,
    cmd: Command,
    row: Row,
    now: Date,
    post: Post,
  ): Promise<void> {
    if (cmd.dryRun) return;
    const msg = "activity cancelled" + (cmd.reason ? ": " + cmd.reason : "");
    await c.query(
      `UPDATE runnerq_activities SET status='cancelled',completed_at=$3,
      last_worker_id=COALESCE(current_worker_id,last_worker_id),current_worker_id=NULL,
      lease_deadline_ms=NULL,waiting_result_id=NULL,scheduled_at=NULL,
      last_error=$4,last_error_at=$3
      WHERE id=$1 AND queue_name=$2`,
      [row.id, this.host.queue, now, msg],
    );
    await this.host.putResult(
      c,
      row.id,
      row.id,
      {
        state: "Err",
        serialization: "json-v1",
        data: {
          error: msg,
          type: "cancelled",
          failed_at: now.toISOString().replace(/\.\d+Z$/, "Z"),
        },
      },
      null,
    );
    const detail: Record<string, unknown> = {
      command_id: cmd.id ?? "",
      from: row.status,
    };
    if (cmd.reason) detail.reason = cmd.reason;
    await this.host.event(c, row.id, "Cancelled", null, detail);
    post.results.push(row.id);
    post.work = true; // waiters woken by the result become runnable
  }

  private async cancelDescendants(
    c: PoolClient,
    cmd: Command,
    roots: string[],
    now: Date,
    post: Post,
  ): Promise<number> {
    const r = await c.query(
      `WITH RECURSIVE d(id) AS (
        SELECT id FROM runnerq_activities WHERE queue_name=$1 AND parent_activity_id=ANY($2::uuid[])
        UNION
        SELECT a.id FROM runnerq_activities a JOIN d ON a.parent_activity_id=d.id WHERE a.queue_name=$1
      ) SELECT id FROM d`,
      [this.host.queue, roots],
    );
    const ids = r.rows.map((x) => x.id as string);
    const locked = await this.lock(c, ids);
    let n = 0;
    for (const id of ids) {
      const row = locked.get(id);
      if (!row || terminal.has(row.status)) continue;
      await this.cancelRow(c, cmd, row, now, post);
      n++;
    }
    return n;
  }

  private async deleteRow(
    c: PoolClient,
    cmd: Command,
    row: Row,
  ): Promise<CommandItem> {
    const { queue } = this.host;
    if (row.parent)
      return skipped(
        row,
        "not a root: delete the root to remove the whole tree",
      );
    if (!terminal.has(row.status))
      return skipped(row, "still running: cancel it first");
    const live = await c.query(
      `SELECT EXISTS(SELECT 1 FROM runnerq_activities
      WHERE queue_name=$1 AND root_activity_id=$2 AND parent_activity_id IS NOT NULL
      AND status NOT IN ${terminalSQL}) AS live`,
      [queue, row.id],
    );
    if (live.rows[0].live)
      return skipped(row, "part of its tree is still running");
    if (await lockTree(c, queue, row.id))
      return skipped(row, "another running workflow depends on this tree");
    if (cmd.dryRun) return { id: row.id, outcome: "would_apply" };
    await deleteTree(c, queue, row.id);
    return { id: row.id, outcome: "applied" };
  }
}

function skipped(row: Row, message: string): CommandItem {
  return {
    id: row.id,
    outcome: "skipped",
    status: canonicalStatus(row.status),
    error: { kind: "conflict", message },
  };
}
