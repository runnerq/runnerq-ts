# RunnerQ for TypeScript

**Durable TypeScript functions, with pluggable storage.**

[![SDK checks](https://github.com/runnerq/runnerq-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/runnerq/runnerq-ts/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Docs](docs/) · [Examples](examples/README.md)

Add durable workflows and background jobs to a Node.js app without running an
orchestration service. RunnerQ is a library: point it at your storage
(PostgreSQL is built in) and write workflows as ordinary async functions. Each
step is checkpointed, so when a process crashes, the workflow resumes where it
stopped instead of redoing completed work.

## Install

Node.js 22 or later, an ESM application, and PostgreSQL are required.
PostgreSQL 17 and 18 are covered by CI. The package ships compiled JavaScript
and TypeScript declarations.

`runnerq` isn't on npm yet. Install it from GitHub:

```bash
npm install github:runnerq/runnerq-ts
```

## Quick start

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

## Why RunnerQ

- **A workflow is just a function.** Orchestration is normal control flow (loops, conditionals, `try`/`catch`), not a DSL or a DAG.
- **Steps are checkpointed.** Completed steps return their recorded results on replay instead of running again.
- **Waiting is free.** Long sleeps and signal waits park in storage and release worker capacity.
- **No orchestration service.** Workers are your own processes; add more to scale.

**Use RunnerQ** when you want durable workflows or queues inside your app with minimal new infrastructure. **Reach for a workflow server** (Temporal) if you want orchestration decoupled from your app and database. **A plain queue** is enough if you only need fire-and-forget tasks without steps, signals or durable timers.

## Features

<details open>
<summary><strong>Durable steps</strong></summary>

`ctx.run` checkpoints a step's result. On replay, a completed step returns its recorded value instead of running again.

```ts
const receipt = await ctx.run("charge", async ({ signal }) => {
  return payments.charge({
    orderId,
    signal,
    idempotencyKey: `${ctx.activityId}:charge`,
  });
});
```

Execution is **at least once**: a crash after an external effect but before its checkpoint commits can repeat that effect, so pass an idempotency key to the external system.

</details>

<details>
<summary><strong>Durable timers and signals</strong></summary>

Sleep and signal deadlines persist, so a restart resumes the wait. Long waits park and release the worker.

```ts
await ctx.sleep("cooling-off", 86_400_000);

const decision = await ctx.waitForSignal<{ approved: boolean }>("approval", {
  timeoutMs: 172_800_000,
});

// From anywhere connected to the same queue:
await client.signal(handle.id, "approval", { approved: true });
```

</details>

<details>
<summary><strong>Child activities and fan-out</strong></summary>

Named children are replay-safe: a retried parent reattaches to them instead of spawning duplicates. `ctx.waitAll` is a durable join.

```ts
const children = [];
for (const item of items) {
  children.push(
    await ctx.spawn(ProcessItem, item, runner.step(`item:${item.id}`)),
  );
}
const results = await ctx.waitAll(children);
```

</details>

<details>
<summary><strong>Deduplicated enqueue</strong></summary>

A business idempotency key deduplicates submissions atomically within a queue and activity type, so duplicate webhook deliveries collapse to one activity.

```ts
await client.execute(
  ProcessEvent,
  event,
  runner.idempotencyKey(event.id, "returnExisting"),
);
```

</details>

<details>
<summary><strong>A real queue underneath</strong></summary>

Priorities, retries with exponential backoff, timeouts, delayed starts, a dead-letter state, retention, and per-worker activity types for workload isolation. See [Activities](docs/activities.md) and [Workers](docs/workers.md).

</details>

<details>
<summary><strong>Rich types</strong></summary>

Inputs, results and signals keep `Date`, `bigint`, `Map`, `Set` and more by default, including after a restart. Use `serialization: "portable"` for plain-JSON contracts another SDK can read. See [Serialization](docs/serialization.md).

</details>

<details>
<summary><strong>Pluggable storage</strong></summary>

`PostgresStorage` is built in. `runnerq/storage` exports the contract for custom backends, and [`@runnerq/cloud-storage`](https://github.com/runnerq/cloud-storage-ts) runs workers on RunnerQ Cloud hosted storage.

</details>

<details>
<summary><strong>RunnerQ Cloud</strong></summary>

The conductor agent (`runnerq/conductor`) connects a worker to RunnerQ Cloud, which shows it in Fleet and can query and, if you allow it, control its activities. `metadataOnly: true` keeps payloads and results in your process. See [RunnerQ Cloud](docs/cloud.md).

</details>

## Examples

Each runs against a local PostgreSQL; see [examples/README.md](examples/README.md) to set it up.

| #   | Example                                              | Shows                                                                      |
| --- | ---------------------------------------------------- | -------------------------------------------------------------------------- |
| 01  | [hello-workflow](examples/01-hello-workflow/main.ts) | two checkpointed steps, typed results, graceful cleanup                    |
| 02  | [crash-and-resume](examples/02-crash-and-resume/)    | **kill it after the charge; on restart it resumes without charging again** |
| 03  | [fan-out](examples/03-fan-out/)                      | children and a durable join with one execution slot                        |
| 04  | [signals-and-sleep](examples/04-signals-and-sleep/)  | buffered signals, persisted deadlines, a durable timer                     |
| 05  | [cloud](examples/05-cloud/)                          | a worker connected to RunnerQ Cloud                                        |

## Documentation

- [Activities](docs/activities.md): contracts, options, retries and recorded failures
- [Durable execution](docs/durable-execution.md): steps, sleeps, signals, children and fan-out
- [Workers](docs/workers.md): lifecycle, configuration, events, executor snapshots, reading state
- [Serialization and compatibility](docs/serialization.md): native and portable formats, custom types, the shared schema
- [RunnerQ Cloud](docs/cloud.md): the conductor agent, queries and commands
- [Architecture and guarantees](docs/architecture.md): transactions, recovery and replay limitations

## Development

```sh
npm ci
npm test                  # Build, type tests, unit tests; database tests skip without DSN
RUNNERQ_TEST_DSN=postgres://postgres:runnerq@localhost:5432/runnerq npm test
npm run test:integration  # Requires RUNNERQ_TEST_DSN; never silently skips
npm run format:check
npm pack --dry-run
```

Values and rules shared with the Go SDK (key formats, notification channels, error kinds) come from [runnerq-spec](https://github.com/runnerq/runnerq-spec), checked out as the `spec` submodule: run `git submodule update --init` before testing. `test/spec.test.mjs` checks the SDK against its vectors. After bumping the submodule, `npm run spec:gen` (needs Go) regenerates `src/spec.ts`.

Integration tests use fresh queue names and delete only their test queues. Use a dedicated database. Tests cover real PostgreSQL transactions, multiple clients, process termination/restart, notification-independent recovery, checkpoint replay, capacity release, retention dependencies, cancellation and shutdown. Mixed-language execution remains gated on the Go SDK adopting the new schema.

## License

MIT — see [LICENSE](LICENSE).
