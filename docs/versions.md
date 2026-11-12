# Version pinning

Step names are keys, so adding, removing and reordering steps is already safe
across a deploy. Version pinning covers the rest: a deploy that changes what
happens _between_ steps, where an old workflow resumed by new code would go
wrong. Pinned work runs only on workers of its version, so old in-flight
workflows finish on old code while new ones start on new code.

It is opt-in. Without a version nothing is stamped and claims are exactly as
before. The rules are runnerq-spec's
[`versions.md`](https://github.com/runnerq/runnerq-spec/blob/main/versions.md),
which the Go SDK follows too, so a mixed fleet pins the same way.

## Turning it on

Give each worker the version of the code it runs, a release tag or a commit:

```ts
const worker = new Worker({ storage, version: process.env.RELEASE }); // e.g. "2026.11.2"
await worker.client.execute(checkout, order); // pinned to the worker's version
```

That worker now:

- pins the roots its `worker.client` submits, those its handlers spawn with
  `runner.asRoot()`, and the schedules it declares, to its version;
- claims only unstamped work and work pinned to its version.

Children always take their **root's** version, whatever process spawns them,
so a workflow tree runs on one version. Retries, lease recovery, `runNow` and
wakes (a sleep ending, a signal, a child's result) never change an activity's
version: parked work resumes only on a worker of its version.

To claim older versions too, list them in `serves`:

```ts
new Worker({ storage, version: "B", serves: ["A"] });
```

A version is 1 to 256 bytes; a worker serves at most 64 versions, its own
included. `serves` without `version` is a configuration error.

A worker **without** a version still claims everything, pinned or not. The
guarantee holds once every worker of the queue has one.

## Producers

A standalone client stamps what it submits, and the schedules it upserts, only
when given a version:

```ts
const client = new RunnerQClient({ storage, version: "B" });
```

Without one, its roots are unstamped and every worker may run them. A
schedule's fires carry the version stored with the schedule at upsert
(`options.version`), so a tick is pinned the same way whichever worker fires
it.

## A rolling deploy from A to B

1. Every worker of the queue runs with a version; today's run `A`.
2. Start workers at `B`. Roots submitted by `B` processes are pinned to `B`;
   `A` producers still pin to `A`, and the `A` workers run that work.
3. Move the producers to `B`.
4. Wait until no unfinished work is pinned to `A` (below), then stop the `A`
   workers.

If `A`'s work turns out to be safe on `B`, repin it, or run some `B` workers
with `serves: ["A"]` and stop the `A` workers.

## Draining: how much work is left on a version

```ts
const { rows } = await storage.aggregateActivities({
  filter: {
    field: "status",
    op: "in",
    value: ["pending", "scheduled", "running", "waiting"],
  },
  groupBy: ["version"], // unstamped work has no "version" key
  count: true,
});
```

`version` is also a filter field for `client.list` and command targets, and
`ActivityRecord.version` reports it. With gauges on (`gaugeIntervalMs`), a
worker with a version reports the same counts as
`activities_by_version{queue, version}`.

## Stranded work

Work is stranded when no live worker serves its version. Each worker with a
version and gauges on reports `activities_unserved{queue, version}`: runnable
work pinned to versions it doesn't serve, and 0 for the versions it serves.
It also logs a warning, `Runnable work is pinned to a version this worker
does not serve`, the first time it sees such work for a version.

A version is stranded when every worker reports it unserved, so alert on the
minimum across the queue's workers:

```promql
min by (queue, version) (runnerq_activities_unserved) > 0
```

with a `for:` longer than a rollout's overlap (10 minutes, say).

## Moving work on purpose: repin

When old work is known to be safe on new code, move it:

```ts
await client.repin(
  { filter: { field: "version", op: "eq", value: "A" }, max: 1000 },
  "B",
  { reason: "fix is compatible" },
);
```

`repin` goes through the same command path as `cancel` and `retry`: it is
idempotent by `commandId`, supports `dryRun`, and records a `Repinned` event
(`activity.repinned`) with the old and new version. It acts on unfinished
activities, a running one included (its current attempt finishes where it
runs); finished activities and ones already on the version are not eligible.
It is per activity: a child spawned after its root is repinned takes the
root's new version, so move a workflow by its tree (`root_id`) or a whole
version by filtering on `version`.

## Executors

A worker with a version reports it as its executor label `version` (and
`snapshot().info.version`, and over the conductor), replacing any label of
that name, so a console reads one version concept.
