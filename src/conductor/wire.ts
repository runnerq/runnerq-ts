// Runtime helpers around the conductor protocol's generated shapes (protocol.ts).
import type { ExecutorSnapshot } from "../executor.js";
import type {
  Capability,
  Error as ErrorBody,
  ErrorCode,
  ExecutorState,
  Messages,
} from "./protocol.js";

/** A failed request, as it goes on the wire. */
export class WireError extends Error {
  constructor(
    readonly code: ErrorCode,
    message: string,
    readonly details?: ErrorBody["details"],
  ) {
    super(message);
  }
  body(): ErrorBody {
    return this.details
      ? { code: this.code, message: this.message, details: this.details }
      : { code: this.code, message: this.message };
  }
}

/** The requests the Cloud sends. */
export type RequestType = {
  [T in keyof Messages]: Messages[T] extends { kind: "req"; from: "cloud" }
    ? T
    : never;
}[keyof Messages];

export type Handler = (data: unknown, signal: AbortSignal) => unknown;

/** A request type's advertised capability and, unless it is bound to a session, its handler. */
export interface Route<T extends RequestType = RequestType> {
  capability: Capability;
  handler?: (
    data: unknown,
    signal: AbortSignal,
  ) => Messages[T]["response"] | Promise<Messages[T]["response"]>;
}
export type Routes = { [T in RequestType]?: Route<T> };

/** The margin left in every frame for the envelope around a reply's or push's data. */
export const frameSlack = 1_024;

export function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Times on the wire: UTC, milliseconds, Z. */
export const ts = (d: Date): string => d.toISOString();

/** A snapshot's state on the wire; `running` only for `executor.describe`. */
export function stateOf(
  snap: ExecutorSnapshot,
  agentStarted: Date,
  withRunning: boolean,
): ExecutorState {
  const c = snap.counters;
  const started = snap.info.startedAt ?? agentStarted;
  return {
    id: snap.info.id,
    uptime_ms: Math.max(0, snap.at.getTime() - started.getTime()),
    max_concurrency: snap.info.maxConcurrency,
    in_flight: snap.state.running.length,
    ...(withRunning && {
      running: snap.state.running.map((a) => ({
        activity_id: a.id,
        type: a.type,
        attempt: a.attempt,
        started_at: ts(a.startedAt),
      })),
    }),
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
}
