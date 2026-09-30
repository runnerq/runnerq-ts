import type { FailureDetails } from "./errors.js";
import type { JsonValue } from "./codec.js";
import type { ExecutionOptions } from "./options.js";
import type { SerializedValue, SerializationFormat } from "./serialization.js";
import type { QueryFilter, RecordStatus } from "./query.js";
export type ActivityStatus =
  | "pending"
  | "scheduled"
  | "processing"
  | "retrying"
  | "waiting"
  | "completed"
  | "failed"
  | "dead_letter"
  | "cancelled";
export interface StoredResult extends SerializedValue {
  state: "Ok" | "Err";
  data: JsonValue;
}
export interface Fence {
  ownerId: string;
  token: string;
}
export interface Submission {
  serialization: SerializationFormat;
  id: string;
  type: string;
  payload: JsonValue;
  options: ExecutionOptions;
  parentId: string | null;
  rootId: string;
  depth: number;
  key?: string;
  fence?: Fence;
}
export interface Claim {
  serialization: SerializationFormat;
  id: string;
  type: string;
  payload: JsonValue;
  token: string;
  retryCount: number;
  timeoutMs: number;
  parentId: string | null;
  rootId: string;
  depth: number;
  metadata: Record<string, string>;
  leaseDeadlineMs: number;
  /** When the activity became due (ISO-8601); the worker reports claim lag from it. */
  dueAt?: string;
}
export interface Park {
  kind: "sleep" | "signal" | "await";
  step: string;
  wakeAt: string;
  resultId?: string;
  producerId?: string;
}
export interface Retention {
  completedMs?: number;
  failedMs?: number;
  intervalMs?: number;
  batchSize?: number;
}
export interface ActivityEvent {
  id: string;
  activityId: string;
  type: string;
  timestamp: string;
  workerId: string | null;
  detail: JsonValue;
}
export interface ActivitySnapshot {
  id: string;
  type: string;
  status: ActivityStatus;
  priority: number;
  createdAt: string;
  scheduledAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  retryCount: number;
  maxAttempts: number | "unlimited";
  timeoutMs: number;
  currentWorkerId: string | null;
  lastWorkerId: string | null;
  leaseDeadlineMs: number | null;
  parentId: string | null;
  rootId: string;
  depth: number;
  metadata: Record<string, string>;
  lastError: string | null;
  lastErrorAt: string | null;
  idempotencyKey: string | null;
  waitingResultId: string | null;
}
export interface ListOptions {
  status?: ActivityStatus;
  rootsOnly?: boolean;
  parentId?: string;
  rootId?: string;
  source?: string;
  limit?: number;
  offset?: number;
}
export interface StepRecord extends StoredResult {
  id: string;
  kind: string;
  name: string;
  createdAt: string;
}
/** Required durable guarantees. A backend must implement all fencing/dependency operations. */
export interface Storage {
  readonly queue: string;
  submit(activity: Submission): Promise<string>;
  /** Backends that can should record `executorId` (the worker's id) for queries' executor_id. */
  claim(
    limit: number,
    types: readonly string[],
    leaseMs: number,
    executorId?: string,
  ): Promise<Claim[]>;
  renew(fence: Fence, leaseMs: number): Promise<boolean>;
  complete(fence: Fence, value: SerializedValue): Promise<void>;
  fail(
    fence: Fence,
    reason: string,
    retryable: boolean,
    failure?: FailureDetails,
  ): Promise<"failed" | "retrying" | "dead_letter">;
  checkpoint(
    fence: Fence,
    resultId: string,
    result: StoredResult,
    step: string,
  ): Promise<void>;
  getResult(id: string): Promise<StoredResult | null>;
  waitResult(id: string, signal?: AbortSignal): Promise<StoredResult>;
  waitForWork(signal: AbortSignal, timeoutMs?: number): Promise<void>;
  registerDependency(fence: Fence, producerId: string): Promise<void>;
  park(fence: Fence, wait: Park): Promise<void>;
  signal(id: string, name: string, payload: SerializedValue): Promise<void>;
  lookupKey(key: string): Promise<string>;
  reap(limit: number): Promise<number>;
  cleanup(retention: Retention): Promise<number>;
  close(): Promise<void>;
}

/**
 * Applies RunnerQ Cloud commands to the backend's own queue (optional: the agent advertises
 * commands only for a backend that has it). Commands are idempotent by `Command.id`: the
 * result is kept for at least 24 hours and replayed for the same id, and the same id with a
 * different `fingerprint` is a `conflict`. Per-target problems (not found, wrong state) are
 * reported per item and do not fail the command; a rejected promise means nothing was applied.
 */
export interface CommandStorage {
  applyCommand(command: Command): Promise<CommandResult>;
}
export function isCommandStorage(storage: unknown): storage is CommandStorage {
  return (
    typeof (storage as Partial<CommandStorage> | null)?.applyCommand ===
    "function"
  );
}
export type CommandKind =
  | "cancel"
  | "retry"
  | "run_now"
  | "reschedule"
  | "set_priority"
  | "delete"
  | "signal";
/** Exactly one of `ids`, a `filter` bounded by `max`, or an `idempotencyKey` (signal only). */
export interface CommandTarget {
  /** At most 1000. */
  ids?: readonly string[];
  /** Selects only activities the command can act on, oldest first, up to `max` (1 to 10000). */
  filter?: QueryFilter;
  max?: number;
  /** The stored key (see `businessKey`), not the one the application passed. */
  idempotencyKey?: string;
}
export interface Command {
  /** Makes the command idempotent; absent or "" disables the ledger. */
  id?: string;
  /** Identifies the input, to detect a reused id. */
  fingerprint?: string;
  kind: CommandKind;
  target: CommandTarget;
  /** Reports what would happen and changes nothing. */
  dryRun?: boolean;
  /** Recorded in the activity's history. */
  reason?: string;
  /** cancel: also cancel non-terminal descendants. */
  cascadeChildren?: boolean;
  /** retry: restore the full attempt budget. */
  resetAttempts?: boolean;
  /** reschedule: the new time (an RFC 3339 string keeps its full precision). */
  at?: Date | string;
  /** set_priority: 1 (low) to 4 (critical). */
  priority?: number;
  signalName?: string;
  /** signal: the payload; absent stores none. */
  signalPayload?: JsonValue;
}
export type CommandOutcome = "applied" | "skipped" | "would_apply";
export interface CommandItem {
  id: string;
  outcome: CommandOutcome;
  /** The canonical status after the command (current when skipped); absent once deleted. */
  status?: RecordStatus;
  /** Why it was skipped: the activity doesn't exist here, or is in the wrong state. */
  error?: { kind: "not_found" | "conflict"; message: string };
}
export interface CommandResult {
  matched: number;
  applied: number;
  /** Descendants a cascading cancel also cancelled. */
  cascaded: number;
  /** A filter target matched more than `max`. */
  more: boolean;
  items: CommandItem[];
  /** The result came from the ledger. */
  replayed: boolean;
}

export { QueryError, RecordEvent, isQueryStorage } from "./query.js";
export type * from "./query.js";
