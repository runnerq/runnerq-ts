# RunnerQ Cloud

Connecting workers to RunnerQ Cloud with the conductor agent.

The conductor agent connects a worker to RunnerQ Cloud. It dials out over a WebSocket (no
inbound port), describes the worker, and reports it on an interval and within about a
second of a change.

```ts
import { startAgent } from "runnerq/conductor";

await worker.start();
const agent = startAgent(worker, {
  url: "wss://cloud.runnerq.dev",
  apiKey: process.env.RUNNERQ_CONDUCTOR_KEY!,
});
// ...
await agent.close(); // before stopping the worker: the Cloud records a clean shutdown
await worker.stop();
```

- The worker shows in Fleet as an executor (`worker.id`), with its host, queue, activity
  types, capacity, labels, what it's running and its counters.
- `startAgent` returns at once; the agent reconnects with backoff when the connection
  drops. `signal` stops it like `close()`.
- `metadataOnly: true` keeps payloads, results, errors and event details from ever
  leaving the process, whatever the Cloud asks.
- Requests beyond `maxConcurrentRequests` (16) are refused rather than queued; each is
  bounded by `requestTimeoutMs` (30s) or the Cloud's deadline.

## Queries and live events

When the worker's storage implements `QueryStorage` (from `runnerq/storage`;
`PostgresStorage` does), the agent also answers the Cloud's queries about activities,
steps, events and trees, and streams live events, across every queue in the database.
Without it, the agent serves only the executor. Payloads and results are shown as plain
JSON: a SuperJSON `Date`, `Map` or `bigint` appears as its JSON projection (an ISO
string, entry pairs, a decimal string).

## Commands

The agent is read-only unless you pass `allowControl: true`. Then, when the storage
implements `CommandStorage` (`PostgresStorage` does), the Cloud can cancel, retry, run
now, reschedule, reprioritize, delete and signal activities of the worker's queue.
Commands are idempotent by id (replayed for at least 24 hours) and work in metadata-only
mode too. A cancelled activity stops at once when it runs on this worker, or at its next
heartbeat elsewhere: its `ctx.signal` aborts with a `claim_lost` error, and it counts as a
lost claim, not a failure. The command ledger is the `runnerq_commands` table, which
`PostgresStorage.initialize()` adds to an existing database.
