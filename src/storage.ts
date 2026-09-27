import type { FailureDetails } from "./errors.js";
import type { JsonValue } from "./codec.js";
import type { ExecutionOptions } from "./options.js";
import type { SerializedValue, SerializationFormat } from "./serialization.js";
export type ActivityStatus =
  | "pending"
  | "scheduled"
  | "processing"
  | "retrying"
  | "waiting"
  | "completed"
  | "failed"
  | "dead_letter";
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
  claim(
    limit: number,
    types: readonly string[],
    leaseMs: number,
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
