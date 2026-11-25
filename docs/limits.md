# Limits

A limit caps one activity type of a queue: how many run at once across every
worker, how many start per period, or both. Limits are stored with the queue
(`runnerq_limits`), so every worker of the queue, TypeScript or Go, enforces
the same value, and a worker that crashes leaves nothing to drift: the running
count is read from the activities themselves on every claim.

Use them where a handler talks to something with its own limits: a mail
provider, a payment API, a model endpoint.

## Declaring limits

Pass options where you register the handler:

```ts
worker.register(sendEmail, handler, {
  maxConcurrent: 10, // at most 10 running, fleet-wide
  rateLimit: 5, // at most 5 started per second, fleet-wide
  ratePeriodMs: 1000,
});
worker.register(resizeImage, resize, {
  maxConcurrentPerWorker: 2, // at most 2 on this worker
});
```

| Option                      | Scope                             | Stored |
| --------------------------- | --------------------------------- | ------ |
| `maxConcurrent`             | Every worker of the queue         | Yes    |
| `rateLimit`, `ratePeriodMs` | Every worker of the queue         | Yes    |
| `maxConcurrentPerWorker`    | This worker, within `concurrency` | No     |

`start()` stores the declared `maxConcurrent` and rate of each type, replacing
what was stored. A type registered without them keeps whatever limit is
stored for it, so a limit set at runtime survives restarts of workers that
don't declare one. Two deployments declaring different limits for one type
overwrite each other on each start.

## What counts

`maxConcurrent` counts **running** activities of the type: claimed and not yet
finished. An activity parked on `ctx.sleep`, a signal or a child holds no
worker and doesn't count, nor does one waiting out a retry's backoff. A worker
that dies keeps its activities counted until their lease expires and the
reaper requeues them.

`rateLimit` per `ratePeriodMs` is a token bucket: a burst of up to `rateLimit`,
refilled at `rateLimit` per period, by the database clock. Every claim takes a
token, retries and wakes included. `rateLimit: 5, ratePeriodMs: 1000` starts
at most 5 at once, then one every 200ms on average.

A limit only holds work back. Within a type, claims keep their order
(priority, then attempts, then age); other types are not held up. A delayed
activity is due at its start time and starts when its type has room.

## Changing limits at runtime

Any client of the queue can set, read, list and delete limits:

```ts
const client = new RunnerQClient({ storage });
await client.limits.set({ type: "send-email", maxConcurrent: 3 });
const limit = await client.limits.get("send-email");
const limits = await client.limits.list();
await client.limits.delete("send-email");
```

`set` replaces the whole limit: leave out `maxConcurrent` for no cap and the
rate for none. Restating the same rate keeps its bucket, so a fleet restart
doesn't refill it.

Lowering a limit never stops running work: activities already running finish,
and the type starts nothing until it is under the new limit. A changed or
deleted limit binds every worker's next claim; a limit added to a type that
had none binds every worker within a second.

## Cost

Claims of unlimited types run exactly as without limits. A claim that includes
a limited type takes a transaction instead of one statement: it locks the
type's limit row, counts its running activities and claims up to the room
left. Workers wait for each other on that lock only while there is work and
room; a type at its limit is skipped without it. When a limit holds work back,
the worker checks again within 200ms instead of waiting for new work.

Every worker serving a limited type must run a version with limits: older
ones ignore them.

## Storage

Limits are the optional `LimitStorage` capability (`runnerq/storage`,
`isLimitStorage`); `PostgresStorage` implements it. `register` throws when a
handler declares `maxConcurrent` or a rate on a storage without it.
`maxConcurrentPerWorker` works with any storage. A backend that implements it
must enforce every stored limit in `claim`. The rules every implementation
follows are in runnerq-spec's
[limits.md](https://github.com/runnerq/runnerq-spec/blob/main/limits.md).
