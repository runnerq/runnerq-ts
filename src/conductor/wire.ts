// The RunnerQ Cloud agent protocol's wire shapes (runnerq-cloud docs/protocol.md).
import type { ExecutorSnapshot } from "../executor.js";

export const protocolVersion = 1;

export type Kind = "req" | "res" | "evt";
export interface Envelope {
  v: number;
  kind: Kind;
  id?: string;
  type: string;
  data?: unknown;
  error?: WireErrorBody;
  meta?: Record<string, unknown>;
}
export interface WireErrorBody {
  code: ErrorCode;
  message: string;
  details?: Record<string, unknown>;
}
export type ErrorCode =
  | "invalid_argument"
  | "not_found"
  | "failed_precondition"
  | "conflict"
  | "forbidden"
  | "unsupported"
  | "resource_exhausted"
  | "deadline_exceeded"
  | "unavailable"
  | "internal";

/** A failed request, as it goes on the wire. */
export class WireError extends Error {
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly details?: Record<string, unknown>,
  ) {
    super(message);
  }
  body(): WireErrorBody {
    return this.details
      ? { code: this.code, message: this.message, details: this.details }
      : { code: this.code, message: this.message };
  }
}

export const typeHello = "hello";
export const typeGoodbye = "goodbye";
export const typeConfigUpdate = "config.update";
export const typeExecutorDescribe = "executor.describe";
export const typeExecutorReport = "executor.report";
export const typeActivitiesList = "activities.list";
export const typeActivitiesGet = "activities.get";
export const typeActivitiesCount = "activities.count";
export const typeActivitiesAggregate = "activities.aggregate";
export const typeStepsList = "steps.list";
export const typeEventsList = "events.list";
export const typeResultsGet = "results.get";
export const typeTreesGet = "trees.get";
export const typeEventsSubscribe = "events.subscribe";
export const typeEventsUnsubscribe = "events.unsubscribe";
export const typeStreamEvents = "stream.events";
export const typeStreamGap = "stream.gap";

export interface Capability {
  v: number;
  filters?: string[];
  sorts?: string[];
  include?: string[];
  group_by?: string[];
  buckets?: string[];
  metrics?: string[];
  targets?: string[];
}
export interface SessionConfig {
  data_mode?: string;
  report_interval_ms?: number;
}
export interface Welcome {
  version: number;
  session_id: string;
  app?: { id: string; name: string };
  config?: SessionConfig;
  limits?: { max_frame_bytes?: number; max_concurrent_requests?: number };
}

/** Times on the wire: UTC, milliseconds, Z. */
export const ts = (d: Date): string => d.toISOString();

export interface ExecutorStateWire {
  id: string;
  uptime_ms: number;
  max_concurrency: number;
  in_flight: number;
  running?: {
    activity_id: string;
    type: string;
    attempt: number;
    started_at: string;
  }[];
  claim_lag_ms: number;
  heartbeat_failures: number;
  draining: boolean;
  counters: {
    claimed: number;
    succeeded: number;
    retried: number;
    failed: number;
    timed_out: number;
    dead_lettered: number;
    claims_lost: number;
  };
}

/** A snapshot's state on the wire; `running` only for `executor.describe`. */
export function stateOf(
  snap: ExecutorSnapshot,
  agentStarted: Date,
  withRunning: boolean,
): ExecutorStateWire {
  const c = snap.counters;
  const started = snap.info.startedAt ?? agentStarted;
  const state: ExecutorStateWire = {
    id: snap.info.id,
    uptime_ms: Math.max(0, snap.at.getTime() - started.getTime()),
    max_concurrency: snap.info.maxConcurrency,
    in_flight: snap.state.running.length,
    claim_lag_ms: Math.round(c.lastClaimLagMs),
    heartbeat_failures: c.heartbeatFailures,
    draining: snap.state.draining,
    counters: {
      claimed: c.claimed,
      succeeded: c.succeeded,
      retried: c.retried,
      failed: c.failed,
      timed_out: c.timedOut,
      dead_lettered: c.deadLettered,
      claims_lost: c.claimsLost,
    },
  };
  if (withRunning)
    state.running = snap.state.running.map((a) => ({
      activity_id: a.id,
      type: a.type,
      attempt: a.attempt,
      started_at: ts(a.startedAt),
    }));
  return state;
}
