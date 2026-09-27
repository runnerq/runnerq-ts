# RunnerQ for TypeScript

Durable activities and workflows for Node.js, backed by PostgreSQL. The runtime uses bounded async concurrency, native event emitters and cooperative cancellation. There is no separate orchestration service.

This SDK uses **separate activity inputs**: `runnerq_inputs.payload`, not `runnerq_activities.payload`. It cannot share a schema with the current Go SDK until Go adopts this layout and the serialization format columns. Existing Go databases require a coordinated migration; this package deliberately contains no legacy schema or key-encoding fallback.

## Quick start

Node.js 22 or later, an ESM application, and PostgreSQL are required. PostgreSQL 17 and 18 are covered by CI. The package ships compiled JavaScript and TypeScript declarations.

```ts
import { activity, runner, RunnerQClient, Worker } from "runnerq";
import { PostgresStorage } from "runnerq/postgres";

const connectionString = process.env.DATABASE_URL!;

// Explicit setup against an empty database/schema. Run during deployment.
await PostgresStorage.initialize({ connectionString });

// Normal startup verifies the schema using read-only catalog queries.
const storage = await PostgresStorage.connect({
  connectionString,
  queue: "orders",
});

const Signup = activity<{ email: string }, { user_id: string }>(
  "SignupWorkflow",
);
const client = new RunnerQClient({ storage });
const worker = new Worker({ storage, concurrency: 20 });

worker.register(Signup, async (ctx, input) => {
  return ctx.run("create-account", async ({ signal }) => {
    // Call your external service with signal and an idempotency key here.
    return { user_id: "u_1001" };
  });
});

await worker.start();

try {
  const handle = await client.execute(
    Signup,
    { email: "ada@example.com" },
    runner.priority("high"),
    runner.maxAttempts(3),
    runner.idempotencyKey("signup:ada@example.com"),
  );

  const account = await handle.result({ signal: AbortSignal.timeout(30_000) });
  console.log(handle.id, account.user_id);
} finally {
  try {
    await worker.stop();
  } finally {
    await storage.close();
  }
}
```

`execute()` resolves when submission commits, returning a non-thenable `ActivityHandle`. `handle.result()` awaits completion. Cancelling that wait does not cancel the activity. A workflow is an ordinary activity handler that uses durable primitives.

See [runnable examples](examples/README.md), including the [hello workflow](examples/01-hello-workflow/main.ts).

## Activity contracts and options

Activity definitions contain stable persisted names and portable types, so producers can import them without importing handlers. Never rename a persisted activity or a durable step while existing executions still depend on it; use a new versioned activity name for incompatible changes.

Runtime validators are optional synchronous parsers. TypeScript types alone do not validate data produced by other processes:

```ts
const Signup = activity("SignupWorkflow", {
  input(value: unknown) {
    if (
      !value ||
      typeof value !== "object" ||
      !("email" in value) ||
      typeof value.email !== "string"
    ) {
      throw new Error("Expected an email");
    }
    return { email: value.email };
  },
  output(value: unknown) {
    if (
      !value ||
      typeof value !== "object" ||
      !("user_id" in value) ||
      typeof value.user_id !== "string"
    ) {
      throw new Error("Expected a user ID");
    }
    return { user_id: value.user_id };
  },
});
```

Activity options are immutable values. Later options override earlier ones. These configure the **activity**, not individual `ctx.run` steps.

| Option                                                 | Default / meaning                                                                                |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------ |
| `runner.priority("high")`                              | `"normal"`; choices: low, normal, high, critical                                                 |
| `runner.maxAttempts(3)`                                | Default: `"unlimited"`; `3` means three total attempts; `1` means one total attempt (no retries) |
| `runner.timeoutMs(300_000)`                            | Per-invocation cooperative timeout, in whole seconds expressed as milliseconds                   |
| `runner.maxRetryDelayMs(3_600_000)`                    | Backoff cap, in whole seconds expressed as milliseconds                                          |
| `runner.delayMs(5_000)`                                | Delay initial execution; millisecond precision                                                   |
| `runner.metadata({ source: "webhook" })`               | String-valued metadata                                                                           |
| `runner.idempotencyKey("order-123", "returnExisting")` | Atomic business-key deduplication within queue and activity type                                 |
| `runner.step("ship")`                                  | Replay-safe named child; valid only with `ctx.spawn()`                                           |
| `runner.asRoot()`                                      | Detach a handler-issued child from lineage; still execution-fenced                               |
| `runner.newOnReplay()`                                 | Explicitly allow a new child on every replay                                                     |

Duplicate policies: `returnExisting` returns the original handle and input; `allowReuse` creates a new activity and repoints the key; `allowReuseOnFailure` does so only after failure/dead-letter; `noReuse` rejects an existing key. Named steps, business idempotency and `newOnReplay()` are mutually exclusive. A named step cannot be detached with `asRoot()`.

Ordinary handler errors retry. `NonRetryableError` goes straight to `failed`. Retryable failures that exhaust the budget go to `dead_letter`. The retry base is one second; the first retry waits two seconds, then four, eight, and so on up to the cap, matching the Go contract. Lease expiry consumes the same attempt budget. Parking and replaying a durable wait do not consume an attempt.

## Durable steps, waits and children

```ts
const receipt = await ctx.run("charge", async ({ signal }) => {
  return payments.charge({
    orderId,
    signal,
    idempotencyKey: `${ctx.activityId}:charge`,
  });
});

await ctx.sleep("cooling-off", 86_400_000);

const decision = await ctx.waitForSignal<{ approved: boolean }>("approval", {
  timeoutMs: 172_800_000,
});

const child = await ctx.spawn(ShipOrder, { orderId }, runner.step("ship"));
return ctx.wait(child);
```

`ctx.spawn()` returns a `ChildActivityHandle<Output>`. Its `result()` takes no arguments and uses durable waiting, just like `ctx.wait(child)`. Passing `{ signal }` is a TypeScript error. Handles returned by `client.execute()` or `client.handle()` retain `result({ signal })` for external callers; using a signal on those handles inside a handler is still rejected at runtime.

Successful steps and permanent step failures are checkpointed. Retryable step failures are not checkpointed. During a transient checkpoint write failure, the SDK retains the callback's returned JSON value and retries persistence without rerunning the callback. Optional `{ parse }` on `ctx.run` validates recorded values on replay; signal waits accept the same parser option.

Execution remains **at least once**. A crash after an external effect but before checkpoint commit can repeat that effect. Use the external system's idempotency support. Code outside checkpoints, including `finally` blocks, runs again on replay. Only the handler replays; arbitrary JavaScript stacks are not persisted.

Long waits park in PostgreSQL and release capacity and leases. Short waits can complete in-process. Sleep and signal deadlines persist from first arrival. Signal timeout `0` or omission waits indefinitely; repeated signals with the same name overwrite the payload. Signals are buffered even before a handler begins. They do not bypass an initial delay or retry backoff.

```ts
await client.signal(handle.id, "approval", { approved: true });
await client.signalByKey(Checkout, "order-123", "approval", { approved: true });

// In another process, connected to the same queue:
const restored = client.handle(Checkout, handle.id);
const result = await restored.result({ signal: AbortSignal.timeout(30_000) });
```

Key lookup and signal delivery are separate operations: key reuse can repoint the key between them. Delivery targets the resolved activity ID. Rehydrated handles awaited inside a handler must use that worker's storage instance; AsyncLocalStorage ensures they register dependencies and park correctly.

Fan-out uses a controlled durable join:

```ts
const children = [];
for (const item of items) {
  children.push(
    await ctx.spawn(ProcessItem, item, runner.step(`item:${item.id}`)),
  );
}
const results = await ctx.waitAll(children);
```

Await all SDK operations. Use `ctx.waitAll` for child joins; concurrent durable waits via `Promise.all`/`race` are unsupported and rejected. Do not overlap a durable wait with a checkpointed effect or nest durable orchestration inside `ctx.run`. Ordinary concurrent I/O within a single step is supported.

Suspension uses an internal control-flow exception. Broad catches should rethrow it:

```ts
import { isControlFlow } from "runnerq";
try {
  await ctx.wait(child);
} catch (error) {
  if (isControlFlow(error)) throw error;
  // Handle a business error.
}
```

The runtime remembers suspension even if caught and will not incorrectly complete the activity. It cannot stop arbitrary application side effects after a catch or in a finally block.

## Persisted failures

Terminal handler failures and permanent `ctx.run()` failures retain a portable `failure` record containing `name`, `message`, `stack`, string/integer `code`, optional plain-JSON `data`, and nested `cause`. Retry events retain the same diagnostics. Cause capture stops at eight errors and marks cycles or excessive depth explicitly. Non-JSON error data is replaced with an explanatory string; arbitrary custom properties and custom exception prototypes are not persisted.

`handle.result()` throws `ActivityFailedError` with the recorded exception as a `RecordedError` in `cause`. Replayed permanent steps throw `NonRetryableError` with a `RecordedError` cause. These wrappers preserve SDK failure semantics; the recorded cause exposes the original name, stack, code, data, and cause chain without invoking user constructors. This is separate from returning an `Error` as a successful native value.

```ts
import { ActivityFailedError, RecordedError } from "runnerq";

try {
  await handle.result();
} catch (error) {
  if (
    error instanceof ActivityFailedError &&
    error.cause instanceof RecordedError
  ) {
    reportFailure(error.cause.name, error.cause.code, error.cause.data);
  }
  throw error;
}
```

Worker-captured diagnostics live in result/checkpoint records and failure events; `lastError` remains a compact message. Lease expiry cannot capture an exception from a process that is no longer running. No additional schema migration is required for these diagnostics.

## Cancellation and lifecycle

`ctx.signal` aborts on execution timeout, claim loss, or exhausted shutdown grace. Pass it to fetch, cancellable database clients and other I/O. Cancellation cannot terminate synchronous JavaScript or force an uncooperative Promise to settle. Such handlers remain tracked; the SDK does not release local execution capacity by merely racing a timeout Promise. CPU-heavy work belongs in isolated worker processes.

`worker.start()` resolves after startup. `worker.stop({ graceMs })` stops intake, drains handlers and acknowledgements, then aborts unfinished work when the grace expires. Its result is `{ drained, remaining }`. Stop is idempotent. Worker instances are single-use. `worker.closed` resolves when stop completes, including a forced stop; it does not promise that cancellation-ignoring user code has stopped. Storage is caller-owned and closed separately.

The library installs no process signal handlers and never calls `process.exit()`. Applications should explicitly handle their own `SIGTERM`/`SIGINT` lifecycle.

## Worker configuration and observation

```ts
const worker = new Worker({
  storage,
  concurrency: 20,
  activityTypes: [Checkout.name],
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

## Inspection

```ts
import { Inspector } from "runnerq";

const inspector = new Inspector({ storage });
const waiting = await inspector.list({ rootsOnly: true, status: "waiting" });
const steps = await inspector.steps(activityId);
const history = await inspector.history(activityId);
const input = await inspector.input(activityId);
if (input.decoded) {
  useInput(input.data);
} else {
  reportUnreadable(input.serialization, input.rawData, input.decodeError);
}

await inspector.close();
```

The inspector uses camelCase and canonical lowercase statuses. List responses intentionally omit payloads; use `inspector.input(activityId)` to fetch an activity's input separately. Input, result, and step payloads have a `decoded` discriminator: successful reads expose `data`; failed decoding exposes `rawData`, `serialization`, and structured `decodeError`, with `data` set to undefined. A bad checkpoint does not prevent inspection of the others. `result()` still returns null when no result exists. Database errors and missing inputs still throw. Callers upgrading from the previous input API must read `input.data` after checking `input.decoded`.

`inspector.events({ signal, bufferSize })` is a bounded async iterable; `inspector.on("event", listener)` is also available. A shared tailer runs while subscribers exist. Slow iterators receive an overflow error and should refresh current state. The live feed is best-effort: PostgreSQL sequence order is not commit order. Fetch persisted per-activity history for authoritative inspection. `inspector.close()` stops the tailer without closing storage.

## Serialization and compatibility

Activities use native SuperJSON serialization by default. Inputs, outputs, and signals can contain `Date`, `bigint`, `Map`, `Set`, `Buffer`, `RegExp`, `URL`, `Error`, and `undefined`, including shared references and cycles. Native void results remain `undefined`, distinct from `null`. Functions, symbols, unregistered custom classes, and unsafe integer numbers are rejected; use `bigint` for large integers. Event sequence IDs remain strings.

```ts
const Checkout = activity<{ orderedAt: Date }, { total: bigint }>("Checkout");
const handle = await client.execute(Checkout, { orderedAt: new Date() });
const { total } = await handle.result(); // bigint, including after a restart

// Portable contracts for another SDK: use plain JSON values explicitly.
const ShipOrder = activity<{ orderId: string }, { shippedAt: string }>(
  "ShipOrder",
  { serialization: "portable" },
);
await client.signal(
  shippingId,
  "approval",
  { approved: true },
  {
    serialization: "portable",
  },
);
```

Portable values must be plain JSON: convert dates to ISO strings and big integers to decimal strings yourself. Portable mode rejects nested undefined, cycles, special objects, non-finite numbers and unsafe integers; top-level void becomes JSON null. `client.signal()` defaults to native serialization; `signalByKey()` uses the supplied activity definition's mode. A signal's own recorded format determines how it is decoded.

`ctx.run()` always uses native serialization for successful checkpoints, even inside portable activities. It decodes the captured value before returning it on the first execution, just as on replay. Step parsers receive decoded values. Internal deadlines and failure records remain portable protocol data. Go workers must not resume TS-owned native checkpoints; sharing activity boundaries requires both SDKs to implement the same schema and portable format contract.

Each input and result row records `serialization` separately from user data: `superjson-v1` or `json-v1`. Output serialization follows the persisted input format, so changing a definition's default does not change an already submitted activity. Clients and inspectors decode using the row's format. Unknown formats fail explicitly. There is no guessing from payload fields or automatic legacy fallback. Inspectors return rich values, which consumers must encode themselves if serving them over JSON HTTP APIs.

For custom types, register a versioned recipe in every client, worker and inspector process before the first native serialization operation:

```ts
import { registerSerialization } from "runnerq";

class Money {
  constructor(readonly cents: bigint) {}
}
registerSerialization<Money, string>({
  name: "myapp.Money.v1",
  isApplicable: (value): value is Money => value instanceof Money,
  serialize: (value) => value.cents.toString(),
  deserialize: (value) => new Money(BigInt(value)),
});
```

Recipe output must be portable JSON. Names are unique and the registry locks on first native use. Keep existing recipe names and decoders available for recorded work; changing their meaning breaks replay. RunnerQ uses an isolated SuperJSON instance, so unrelated application registrations do not change the SDK's format. See [serialization storage and upgrades](docs/architecture.md#serialization) before upgrading an existing database.

The schema contains exactly seven tables: activities, inputs, results, idempotency, dependencies, events and worker pools. Checkpoint identity is UUIDv5 of `(activityId, "kind:name")`; business keys use the final `rq:key:v2:` UTF-8 length-prefixed encoding; named children use `rq:step:<root>:<parent>:<name>`. There are no legacy key readers, inline-payload compatibility paths or unfenced fallback backends. `runnerq/storage` exports the required storage contract for custom backends.

The database column `max_retries` retains its shared protocol name but is exposed as `maxAttempts`. Read [architecture and guarantees](docs/architecture.md) for transaction and recovery details.

## Development

```sh
npm ci
npm test                  # Build, type tests, unit tests; database tests skip without DSN
RUNNERQ_TEST_DSN=postgres://postgres:runnerq@localhost:5432/runnerq npm test
npm run test:integration  # Requires RUNNERQ_TEST_DSN; never silently skips
npm run format:check
npm pack --dry-run
```

Integration tests use fresh queue names and delete only their test queues. Use a dedicated database. Tests cover real PostgreSQL transactions, multiple clients, process termination/restart, notification-independent recovery, checkpoint replay, capacity release, retention dependencies, cancellation and shutdown. Mixed-language execution remains gated on the Go SDK adopting the new schema.
