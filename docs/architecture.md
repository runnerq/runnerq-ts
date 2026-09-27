# Architecture and durable guarantees

## Runtime

One dispatcher owns each worker's concurrency budget. It claims only free slots, launches async handler invocations, and refills on completion or the next work probe. No unstarted activity waits in an in-memory claimed backlog. Worker registration is immutable after startup.

Activity handlers run with an explicit ActivityContext and an internal AsyncLocalStorage attempt scope. Handles reconstructed inside a handler use that scope to register durable dependencies. Every invocation has a unique execution token, a handler cancellation signal, and a separate persistence lifetime. Checkpoint and acknowledgement recovery preserve captured inputs and retry writes rather than user callbacks.

Lease renewal verifies ownership; claim loss cancels the handler signal. Timeout and shutdown are cooperative. Promise rejection cannot stop arbitrary JavaScript effects. A running handler keeps its local slot until it settles. Heartbeats stop when the handler signal aborts, so an uncooperative execution cannot renew forever after timeout or shutdown. Persistence retries may continue renewing while a captured outcome is being reconciled.

## Schema baseline

`PostgresStorage.initialize` is an explicit, advisory-lock-coordinated baseline initializer. It creates all seven tables and current indexes atomically only when none exists. On an existing schema it validates instead of migrating. `connect` performs catalog reads only. Validation covers types, nullability, defaults, primary keys, index definitions and index validity, and rejects inline payload columns.

The schema lock uses the same numeric key as Go. The baseline preserves final names such as the `_v2` dequeue indexes without reproducing old indexes or migration history. The separate-input design is a deliberate schema break. Initialization must never be run as an attempted conversion of a live Go installation.

Inputs are immutable rows in `runnerq_inputs`. Submission writes activity state, input, idempotency ownership, dependency and event in one transaction. A duplicated key retaining an existing activity never replaces its input. Claiming selects and locks state rows first, then loads only the selected input batch in that transaction. Missing inputs fail the claim transaction.

The default attempt budget is unlimited, stored as `max_retries = 0`; a positive value is the total attempt limit. Existing separate-input TS databases created with the earlier column default need `ALTER TABLE runnerq_activities ALTER COLUMN max_retries SET DEFAULT 0` before connecting with this version. This changes the default only; existing activities retain their configured budgets. Initialization does not perform this change automatically.

## Serialization

User data is encoded before entering the storage contract. `Submission` and `Claim` carry raw JSON payload plus its format. `StoredResult` and `SerializedValue` pair JSON data with a required serialization identifier. Custom storage backends must preserve that identifier in submit/claim, checkpoint/complete/signal, input/result reads and inspection; immutable-write reconciliation compares both data and format.

`runnerq_inputs.serialization` and `runnerq_results.serialization` are non-null text columns, defaulting to `json-v1` for protocol writers. The TS API explicitly writes `superjson-v1` for native values. Their JSONB columns hold SuperJSON's `{ json, meta? }` representation for native values and the raw application JSON for portable values. Encoding metadata stays in the separate input/result tables. Activity scheduling state and event details remain portable JSON.

The input row selects the activity's output format for its entire lifetime, independently of later handler registration defaults. Every checkpoint and signal records its own format. Successful `ctx.run` results are encoded once, decoded before returning, and decoded identically on replay. Persistence retries reuse the captured representation instead of rerunning callbacks or custom serialization recipes. Native checkpoints remain owned by TS handlers even when their activity input/output contract is portable.

An existing separate-input TS database without these columns must be explicitly upgraded before connecting. Existing rows from the JSON-only SDK are correctly labeled portable by this metadata-only change:

```sql
BEGIN;
ALTER TABLE runnerq_inputs ADD COLUMN serialization TEXT NOT NULL DEFAULT 'json-v1';
ALTER TABLE runnerq_results ADD COLUMN serialization TEXT NOT NULL DEFAULT 'json-v1';
COMMIT;
```

Coordinate the change with clients and workers; old readers cannot decode new native values. The initializer does not apply migrations, and missing/unknown formats do not trigger decoder inference. This SQL only upgrades the previous TS layout; it does not migrate inline-input Go schemas.

## Atomic transitions

| Transition       | Transaction invariants                                                                                     |
| ---------------- | ---------------------------------------------------------------------------------------------------------- |
| Claim            | Ordered `FOR UPDATE SKIP LOCKED`; fresh token per claimed row; database-clock lease                        |
| Completion       | Fenced state update, terminal result (including null), event and waiter wakeup                             |
| Failure          | One failure decision per token; retry schedule or terminal error result; event and wakeup                  |
| Checkpoint       | Lock owner first; immutable `(state, data, owner, step)`; identical repeat succeeds                        |
| Child submission | Lock executing parent's claim before inserting child or resolving business key                             |
| Signal           | Lock target, upsert mutable signal result, wake parked target, record event                                |
| Durable park     | Lock waiter and producer/owner, register dependency, recheck readiness, park or immediately pend           |
| Retention        | Queue leadership lock, candidate root lock, idempotency-key locks, dependency recheck, whole-tree deletion |

Checkpoints and signals use distinct write paths: checkpoints cannot be overwritten, signals intentionally can. Result presence, including an explicit JSON null, is distinct from no result row.

Completion reconciles a lost reply against the terminal result and `last_worker_id`. Failure and park reconcile against durable events from the same execution token. A stale/conflicting write is rejected. The statement timeout bounds each database statement; transient failures are classified by SQLSTATE/network error rather than treating every database error as retryable.

## Dependency and retention ordering

Dependencies are independent of parent lineage. Both named child reuse and rehydrated handles register consumers. Result publication locks the owner before storing/waking; result parking takes compatible producer/root locks so it cannot miss a concurrent publication. Signals coordinate on their target row. A periodic bounded park deadline remains as a recovery safety net.

Retention deletes only a terminal root with no nonterminal descendant. A live consumer tree pins producer trees even if the particular consumer activity already completed. Root row locks serialize explicit dependency registration with retention. Idempotency-row locks serialize reused-child references with retention. Deletion removes separate inputs, all owned/synthetic results, checkpoint events, activity events, dependencies and keys in the same transaction.

Retention bounds both deleted roots and examined candidates per batch. Separating payloads narrows the frequently updated activity row; it does not guarantee physical insertion-order locality or eliminate TOAST costs.

## Notifications and observations

The PostgreSQL adapter has a normal query pool and one lazy LISTEN client. Channels match Go: `rq_w_<queue>`, `rq_r_<queue>`, and `rq_e_<queue>`. Notification sends are coalesced after business transactions commit, with at most 200 result UUIDs per payload and bounded pending IDs. Notifications may be dropped. Result waiters subscribe before querying; work and result paths requery on bounded fallbacks. A reconnect wakes local subscriptions.

Inspector streaming tails persisted event IDs with bounded per-consumer queues. Sequence IDs are strings because JavaScript numbers cannot represent every PostgreSQL BIGINT. A dashboard feed may omit late-committing rows below its cursor; authoritative activity history is read directly. Worker EventEmitter observations are local and cannot replace durable state transitions.

## Replay limitations

Step names are explicit and stable; no call-order counter identifies a checkpoint. A callback can repeat after an effect/commit crash window. Durable waits unwind via an internal exception and replay the handler later. Sticky suspension intent prevents a swallowed exception from falsely completing an activity, but cannot intercept arbitrary external effects in user catch/finally blocks.

Only one durable wait may be active in an invocation. `waitAll` joins children sequentially through recorded results and parks on an unresolved dependency. Children execute concurrently according to fleet capacity. Durable race/any, distributed cancellation, cron scheduling and an automatic CPU worker-thread executor are not part of this version.
