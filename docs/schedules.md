# Schedules

A schedule enqueues an activity on a cron expression. Schedules are stored with the queue (`runnerq_schedules`), so every worker of the queue, TypeScript or Go, fires them, and each tick runs once however many workers race for it. No separate scheduler process.

## Declaring schedules

Register schedules on the worker before `start()`; `start()` upserts them, so the schedules your code declares are the ones stored:

```ts
const NightlyReport = activity<{ format: string }, void>("NightlyReport");

worker.registerSchedule({
  name: "nightly-report",
  cron: "30 2 * * *",
  timezone: "Europe/London",
  activity: NightlyReport,
  payload: { format: "pdf" },
  options: [runner.priority("high"), runner.timeoutMs(600_000)],
  catchUp: "one",
});
```

| Field                     | Default           | Meaning                                                                              |
| ------------------------- | ----------------- | ------------------------------------------------------------------------------------ |
| `name`                    | required          | Identifies the schedule in its queue.                                                |
| `cron`                    | required          | The expression (below).                                                              |
| `timezone`                | `"UTC"`           | IANA zone the expression's wall-clock times are in.                                  |
| `activity`                | required          | The activity definition to enqueue; its parser and serialization apply to `payload`. |
| `payload`                 | `null`            | Each fired activity's input.                                                         |
| `options`                 | activity defaults | `runner.priority`, `maxAttempts`, `timeoutMs`, `maxRetryDelayMs` and `metadata`.     |
| `catchUp`, `catchUpLimit` | `"one"`, 100      | What to do with ticks missed while no worker ran (below).                            |

Upserting keeps a schedule's paused state and its next fire time, unless the cron or time zone changed. Two deployments declaring the same name differently overwrite each other on each start: roll such a change out like a schema change.

## Managing schedules

`client.schedules` manages a queue's schedules from any process, worker or not:

```ts
const client = new RunnerQClient({ storage });

await client.schedules.upsert({ name, cron, activity, payload });
await client.schedules.pause("nightly-report");
await client.schedules.resume("nightly-report"); // from the next tick; no catch-up
const handle = await client.schedules.trigger("nightly-report"); // run once now, even if paused
const all = await client.schedules.list();
const s = await client.schedules.get("nightly-report"); // s.nextFireAt, s.lastFireAt, s.paused
await client.schedules.delete("nightly-report");
```

Pause, resume, trigger, get and delete of an unknown name reject with a `not_found` `RunnerQError`; an invalid cron or zone rejects with a `QueryError` of kind `invalid_argument`. Triggers within the same second share one activity.

## Cron expressions

Five fields, `minute hour day-of-month month day-of-week`, or six with `second` first:

| Expression                                            | Fires                                    |
| ----------------------------------------------------- | ---------------------------------------- |
| `*/5 * * * *`                                         | every 5 minutes                          |
| `0 9 * * 1-5`                                         | 09:00 on weekdays                        |
| `30 2 1 * *`                                          | 02:30 on the 1st of each month           |
| `0 0 13 * FRI`                                        | midnight on the 13th and on every Friday |
| `*/10 * * * * *`                                      | every 10 seconds                         |
| `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` | the usual macros                         |

Fields take `*`, values, ranges `a-b`, steps `/n` and comma lists; months `JAN`-`DEC` and days `SUN`-`SAT` in any case; 0 and 7 are Sunday. When both day fields are restricted a day matches either (as in Vixie cron). `L`, `W`, `#` and `?` are not supported. An expression that matches no date, such as `0 0 30 2 *`, is rejected.

**Daylight saving.** Schedules run on wall-clock time in their zone (computed with `Intl`, no dependency):

- A schedule with a fixed hour (`30 2 * * *`) fires once a day. On the spring-forward day it fires when the skipped hour ends; on the fall-back day it fires in the first pass only.
- A schedule whose hour field starts with `*` (`0 * * * *`, `*/15 * * * *`) follows real time: skipped wall times don't fire, repeated ones fire twice, so an hourly job runs every real hour.

TypeScript and Go compute the same fire times; the [runnerq-spec vectors](https://github.com/runnerq/runnerq-spec/blob/main/vectors/cron_next_fire.json) pin them.

## Firing and catch-up

Each worker checks for due schedules about every second, using the database clock. A due schedule is fired in one transaction that enqueues the activity with the idempotency key `schedule:<name>:<fire time>` (for example `schedule:nightly-report:2026-03-08T02:30:00Z`) and advances its next fire time. Racing workers skip a schedule another is firing, and a crash leaves both writes or neither, so a tick never runs twice.

When no worker ran for a while, several ticks may be due at once:

| `catchUp`         | Fires                                                                            |
| ----------------- | -------------------------------------------------------------------------------- |
| `"one"` (default) | the latest due tick, once                                                        |
| `"skip"`          | the latest due tick only if it is under a minute late                            |
| `"all"`           | every due tick, oldest first, at most `catchUpLimit` (default 100, at most 1000) |

## Custom storage

Schedules are the optional `ScheduleStorage` capability (`runnerq/storage`, `isScheduleStorage`). Workers fire schedules whenever their storage implements it. A backend must fire each due schedule in one transaction that enqueues its fire times and advances it, claiming the schedule so concurrent workers skip it, and should compute fire times as the spec does. `PostgresStorage` implements it.
