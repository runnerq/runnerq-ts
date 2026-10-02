# Workers

Running, configuring and observing workers, and reading stored state.

## Cancellation and lifecycle

`ctx.signal` aborts on execution timeout, claim loss, or exhausted shutdown grace. Pass it to fetch, cancellable database clients and other I/O. Cancellation cannot terminate synchronous JavaScript or force an uncooperative Promise to settle. Such handlers remain tracked; the SDK does not release local execution capacity by merely racing a timeout Promise. CPU-heavy work belongs in isolated worker processes.

`worker.start()` resolves after startup. `worker.stop({ graceMs })` stops intake, drains handlers and acknowledgements, then aborts unfinished work when the grace expires. Its result is `{ drained, remaining }`. Stop is idempotent. Worker instances are single-use. `worker.closed` resolves when stop completes, including a forced stop; it does not promise that cancellation-ignoring user code has stopped. Storage is caller-owned and closed separately.

The library installs no process signal handlers and never calls `process.exit()`. Applications should explicitly handle their own `SIGTERM`/`SIGINT` lifecycle.

## Worker configuration and observation

```ts
const worker = new Worker({
  storage,
  concurrency: 20,
  activityTypes: [Checkout],
  leaseMs: 60_000,
  heartbeatMs: 10_000,
  reaperIntervalMs: 5_000,
  reaperBatchSize: 100,
  waitGraceMs: 2_000,
  shutdownGraceMs: 30_000,
  maxActivityDepth: 32,
  retention: {
    completedMs: 7 * 86_400_000,
    failedMs: 30 * 86_400_000,
    intervalMs: 600_000,
    batchSize: 100,
  },
});

worker.on("activityCompleted", (event) => console.log(event.activityId));
worker.on("activityDeadLetter", (event) => console.error(event));
worker.on("workerError", (error) => console.error(error));
worker.on("listenerError", (error) => console.error("Observer failed", error));
```

By default a worker claims only its registered types. Separate workers with distinct type registrations provide workload isolation. Each claims at most its free capacity. Database pool size is independent of handler concurrency; default query pool size is 10 plus one lazy dedicated LISTEN connection. Notifications are batched after commit and are only hints; periodic queries recover missed notifications and due schedules.

Worker events are local observations, emitted after corresponding commits where applicable. Listener exceptions and rejected Promises are contained. Synchronous expensive listeners still block the event loop. Reaper dead letters appear in persisted event history, not necessarily a local worker callback. Use durable activities for required follow-up work rather than relying on an event listener.

Retention is disabled by default. It deletes complete terminal trees and their inputs, results, checkpoints, events, keys and dependencies atomically. Live consumer trees pin shared producer results. Completion/failure retention clocks are separate; zero keeps that class forever.

### Executor snapshots

A started worker is an executor: `worker.snapshot()` describes it as it is now, and is
what RunnerQ Cloud shows in Fleet.

```ts
const worker = new Worker({ storage, labels: { region: "eu-west-1" } });
const { info, state, counters } = worker.snapshot();
// info: id, queue, activityTypes, maxConcurrency, startedAt, hostname, sdk, labels
// state: running (id, type, attempt, startedAt), draining
// counters since construction: claimed, succeeded, retried, failed, timedOut,
//   deadLettered, claimsLost, heartbeatFailures, lastClaimLagMs
```

`worker.id` is random and fixed for the worker's lifetime. `worker.changed()` resolves at
the next change (an activity starting or finishing, or a drain beginning), so a reporter
needn't wait for its interval; `reportExecutor()` is that loop, spacing reports by a
minimum gap. `worker.observe(observer)` (before `start()`) calls `executorStarted(worker)`
and `executorStopped(id)`; a storage backend that implements both is attached
automatically, which is how RunnerQ Cloud's storage adapter reports hosted workers.

## Reading state

The PostgreSQL storage has read methods for scripts and tests. They aren't
part of the `Storage` contract, so a custom backend needn't provide them.

```ts
const waiting = await storage.list({ rootsOnly: true, status: "waiting" });
const activity = await storage.getActivity(activityId);
const input = await storage.getInput(activityId);
const steps = await storage.steps(activityId);
const history = await storage.events(activityId);
```

List responses omit payloads. Inputs, results and steps come back as stored:
JSON data together with its `serialization` format, not decoded.
