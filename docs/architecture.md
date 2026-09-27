# Architecture and durable guarantees

## Runtime

One dispatcher owns each worker's concurrency budget. It claims only free slots, launches async handler invocations, and refills on completion or the next work probe. No unstarted activity waits in an in-memory claimed backlog. Worker registration is immutable after startup.

Activity handlers run with an explicit ActivityContext and an internal AsyncLocalStorage attempt scope. Handles reconstructed inside a handler use that scope to register durable dependencies. Every invocation has a unique execution token, a handler cancellation signal, and a separate persistence lifetime. Checkpoint and acknowledgement recovery preserve captured inputs and retry writes rather than user callbacks.

Lease renewal verifies ownership; claim loss cancels the handler signal. Timeout and shutdown are cooperative. Promise rejection cannot stop arbitrary JavaScript effects. A running handler keeps its local slot until it settles. Heartbeats stop when the handler signal aborts, so an uncooperative execution cannot renew forever after timeout or shutdown. Persistence retries may continue renewing while a captured outcome is being reconciled.

## Schema baseline

`PostgresStorage.initialize` is an explicit, advisory-lock-coordinated baseline initializer. It creates all seven tables and current indexes atomically only when none exists. On an existing schema it validates instead of migrating. `connect` performs catalog reads only. Validation covers types, nullability, defaults, primary keys, index definitions and index validity, and rejects inline payload columns.

The schema lock uses the same numeric key as Go. The baseline preserves final names such as the `_v2` dequeue indexes without reproducing old indexes or migration history. The separate-input design is a deliberate schema break. Initialization must never be run as an attempted conversion of a live Go installation.

Inputs are immutable rows in `runnerq_inputs`. Submission writes activity state, input, idempotency ownership, dependency and event in one transaction. A duplicated key retaining an existing activity never replaces its input. Claiming selects and locks state rows first, then loads only the selected input batch in that transaction. Missing inputs fail the claim transaction.

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
