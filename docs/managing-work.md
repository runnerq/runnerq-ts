# Managing work

`RunnerQClient` submits, signals, awaits, inspects and manages activities. It needs only
storage, not a running worker, so a web handler, a CLI or a cron job can use it from any
process that shares the database.

```ts
import { RunnerQClient } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";

const storage = await PostgresStorage.connect({
  connectionString,
  queue: "my_app",
});
const client = new RunnerQClient({ storage });
```

## Submitting, signalling, awaiting

```ts
const handle = await client.execute(SendInvoice, { orderId });
await client.signal(handle.id, "approved", { by: "ops" });
const receipt = await client.handle(SendInvoice, handle.id).result();
```

`signalByKey` signals the activity that owns an idempotency key.

## Reading

```ts
const record = await client.get(id); // payload, result and last error included
// rejects with RunnerQError code "not_found" when the queue has no such activity

const page = await client.list({
  statuses: ["failed", "dead_letter"],
  type: "SendInvoice",
  limit: 100,
});
// page.items, then page.nextCursor as `cursor` for the next page
```

Statuses are the canonical ones: `pending`, `scheduled` (delayed, or waiting out a retry
backoff), `running`, `waiting` (parked on a sleep, signal or child), `completed`, `failed`,
`dead_letter` and `cancelled`. `filter` takes any query filter for conditions the options
don't cover. Reads and commands are scoped to the storage's queue.

## Commands

| Method        | Acts on                          | Effect                                                                                                     |
| ------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `cancel`      | unfinished activities            | Cancelled, with their unfinished descendants (`cascade: false` keeps them)                                 |
| `retry`       | failed, dead-lettered, cancelled | Pending again; completed steps replay from checkpoints (`resetAttempts: true` restores the attempt budget) |
| `runNow`      | scheduled, waiting               | Runnable now                                                                                               |
| `reschedule`  | scheduled                        | Runs at the new time                                                                                       |
| `setPriority` | unfinished                       | New priority                                                                                               |
| `delete`      | finished roots                   | Removed with their whole tree                                                                              |

Each takes a target: an id, an array of ids, or a bounded filter.

```ts
await client.cancel(id, { reason: "customer asked" });

// Retry up to 100 dead letters of a type; repeat while res.more.
const res = await client.retry(
  {
    filter: {
      and: [
        { field: "type", op: "eq", value: "SendInvoice" },
        { field: "status", op: "eq", value: "dead_letter" },
      ],
    },
    max: 100,
  },
  { resetAttempts: true },
);
```

A filter target only selects activities the command can act on, oldest first, so repeating
it makes progress.

### Results

A command doesn't fail because one target is in the wrong state. The result has one item
per target:

| `outcome`      | Meaning                                                                      |
| -------------- | ---------------------------------------------------------------------------- |
| `applied`      | Changed; `status` is the status after                                        |
| `not_eligible` | Its status doesn't allow the command; `status` is current, `reason` says why |
| `not_found`    | No such activity in this queue                                               |
| `would_apply`  | A `dryRun` would change it                                                   |

`applied` counts the changed activities and `cascaded` the descendants a cancel also
cancelled. A rejected promise means nothing was applied.

### Idempotent retries

Every command has an id: a fresh one per call, or yours as `commandId`. Sending the same
command again with the same id, say after a timeout, resolves to the first result with
`replayed: true` and applies nothing, for at least 24 hours. The same id with different
input rejects with a `RunnerQError` whose code is `"conflict"`.

```ts
await client.cancel(id, { commandId: `refund-${orderId}` });
```

## Cancelling running work

Cancelling a running activity releases its claim in storage. The worker running it, in any
process, notices at once (`PostgresStorage` sends a notification; otherwise the next
heartbeat) and aborts the handler's `ctx.signal` with a `RunnerQError` whose code is
`"claim_lost"`. Whatever the handler returns afterwards is discarded, and the activity is
not retried. Pass `ctx.signal` to the I/O you start so it stops too.

A cancelled activity stores a cancellation error as its result. A parent awaiting it, and
`handle.result()`, reject with an `ActivityFailedError` for which `isCancelled(error)` is
true.
