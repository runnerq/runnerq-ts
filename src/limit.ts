import { RunnerQError } from "./errors.js";
import { QueryError } from "./query.js";
import type { Storage } from "./storage.js";

/**
 * A limit on one activity type of a queue, across every worker (runnerq-spec limits.md). It
 * needs `maxConcurrent`, a rate, or both.
 */
export interface Limit {
  /** The activity type, by persisted name. */
  type: string;
  /**
   * At most this many of the type running at once in the queue. Activities waiting on a
   * sleep, a signal, a child or a retry's backoff don't count.
   */
  maxConcurrent?: number;
  /** At most this many claims per `ratePeriodMs`, in bursts of up to `rateLimit`. */
  rateLimit?: number;
  /** The rate's period, in whole milliseconds. */
  ratePeriodMs?: number;
}

/** A stored limit. */
export interface LimitRecord extends Limit {
  /** When the definition last changed (ISO-8601). */
  updatedAt: string;
}

/**
 * Per-type limits stored with the queue (optional). A storage that implements it enforces
 * every stored limit in `claim`: a claim never takes an activity of a limited type beyond the
 * room under `maxConcurrent` or the tokens of its rate. Unknown types reject with
 * `not_found`, invalid limits with a `QueryError` of kind `invalid_argument`.
 */
export interface LimitStorage {
  /**
   * Creates or replaces the type's limit. The rate's bucket is kept when the rate is
   * unchanged, so an unchanged definition changes nothing.
   */
  setLimit(limit: Limit): Promise<void>;
  getLimit(type: string): Promise<LimitRecord>;
  /** The queue's limits by type. */
  listLimits(): Promise<LimitRecord[]>;
  deleteLimit(type: string): Promise<void>;
}
export function isLimitStorage(storage: unknown): storage is LimitStorage {
  return (
    typeof (storage as Partial<LimitStorage> | null)?.setLimit === "function"
  );
}

const invalid = (field: string, message: string) =>
  new QueryError("invalid_argument", message, field);
const positive = (n: unknown) =>
  typeof n === "number" && Number.isInteger(n) && n >= 1;

/** Checks a limit storage is asked to set. */
export function validateLimit(l: Limit): void {
  if (typeof l.type !== "string" || !l.type)
    throw invalid("type", "Limit activity type is required");
  if (l.maxConcurrent !== undefined && !positive(l.maxConcurrent))
    throw invalid("maxConcurrent", "maxConcurrent must be a positive integer");
  if (l.rateLimit !== undefined && !positive(l.rateLimit))
    throw invalid("rateLimit", "rateLimit must be a positive integer");
  if (l.ratePeriodMs !== undefined && !positive(l.ratePeriodMs))
    throw invalid(
      "ratePeriodMs",
      "ratePeriodMs must be a positive whole number of milliseconds",
    );
  if ((l.rateLimit === undefined) !== (l.ratePeriodMs === undefined))
    throw invalid("rateLimit", "A rate needs both rateLimit and ratePeriodMs");
  if (l.maxConcurrent === undefined && l.rateLimit === undefined)
    throw invalid(
      "limit",
      "A limit needs maxConcurrent, a rate or both; delete it to remove it",
    );
}

/**
 * The queue's limits, from any process: `RunnerQClient.limits`. Limits a worker declares at
 * `register` replace what is stored for their type when it starts.
 */
export class Limits {
  constructor(private readonly storage: Storage) {}
  private get store(): LimitStorage {
    if (!isLimitStorage(this.storage))
      throw new RunnerQError(
        "configuration",
        "This storage does not support limits",
      );
    return this.storage;
  }
  /** Creates or replaces the type's limit, for every worker of the queue. */
  async set(limit: Limit): Promise<void> {
    await this.store.setLimit(limit);
  }
  /** Rejects with `not_found` when the type has no limit. */
  async get(type: string): Promise<LimitRecord> {
    return this.store.getLimit(type);
  }
  /** The queue's limits by type. */
  async list(): Promise<LimitRecord[]> {
    return this.store.listLimits();
  }
  /** Removes the type's limit, so it runs unlimited; `not_found` when it has none. */
  async delete(type: string): Promise<void> {
    await this.store.deleteLimit(type);
  }
}
