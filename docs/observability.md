# Observability

Logs, labelled metrics with gauges, and traces. The names are shared with the Go SDK
(runnerq-spec `observability/`), so one dashboard reads both.

## Logs

A worker logs through its `logger` option:

```ts
interface Logger {
  debug(message: string, fields?: Record<string, unknown>): void;
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

new Worker({ storage, logger: jsonLogger({ level: "debug" }) });
```

The default, `jsonLogger()`, writes one JSON object per line on stderr at info and above, shaped
like Go's slog JSON handler:

```json
{
  "time": "2026-11-11T12:00:00.000Z",
  "level": "WARN",
  "msg": "Activity requesting retry",
  "activity_id": "…",
  "activity_type": "Order",
  "reason": "payment gateway busy"
}
```

The messages and field names match the Go SDK's: `Starting worker engine` and `Worker engine
stopped` (`queue`), `Activity completed successfully`, `Activity requesting retry` (`reason`),
`Activity failed` (`reason`), `Activity timed out` (`timeout`), `Activity execution lost its
claim`, `Activity yielded for durable wait` (debug: `step`, `wake_at`), `Failed to confirm
activity completion` (`error`) and `Worker error` (`error`). Every activity line also carries
`activity_id` and `activity_type`. Pass any object with those four methods (pino, winston, a
`console` wrapper) to route them elsewhere. `silentLogger` discards everything. The worker
events (`activityCompleted`, `workerError`, …) are unchanged.

## Metrics

`metrics` takes a `LabeledMetrics`:

```ts
interface LabeledMetrics {
  addCounter(name: string, value: number, labels: MetricLabels): void;
  recordHistogram(name: string, value: number, labels: MetricLabels): void; // seconds
  setGauge(name: string, value: number, labels: MetricLabels): void;
}
// MetricLabels: { queue: string; activityType?: string; status?: string }
```

`metricCatalog` lists every metric with its kind, unit and labels. `metricNames` holds the
names. The older `Metrics` (`increment`, `duration` in milliseconds) still works: the worker
wraps it with `unlabeledMetrics`, which drops labels and gauges.

| Metric                                                                           | Kind          | Meaning                                                                                     |
| -------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------- |
| `activity_started`                                                               | counter       | attempts started here                                                                       |
| `activity_completed`                                                             | counter       | completed successfully                                                                      |
| `activity_retry` / `activity_timeout`                                            | counter       | retryable failures / timeouts                                                               |
| `activity_failed_non_retry`                                                      | counter       | failed permanently                                                                          |
| `activity_dead_lettered`                                                         | counter       | ran out of attempts                                                                         |
| `activity_claim_lost`                                                            | counter       | lost the claim to another execution                                                         |
| `activity_yielded`                                                               | counter       | parked for a durable wait                                                                   |
| `activity_heartbeat_failed`                                                      | counter       | a renewal failed and will be retried                                                        |
| `activity_completion_pending_started` / `_finished`, `activity_completion_error` | counter       | recording completions                                                                       |
| `activity_trees_swept`                                                           | counter       | trees deleted by retention                                                                  |
| `schedule_fires`                                                                 | counter       | activities enqueued by firing due schedules                                                 |
| `storage_retry`                                                                  | counter       | storage writes retried                                                                      |
| `activity_claim_lag`, `activity_execution`, `activity_completion_persistence`    | histogram (s) | due → start, attempt duration, completion recording                                         |
| `queue_depth`                                                                    | gauge         | activities by type and status (`pending`, `scheduled`, `running`, `waiting`, `dead_letter`) |
| `queue_oldest_pending_age`                                                       | gauge (s)     | how long the oldest pending activity has been due                                           |
| `activities_in_flight`                                                           | gauge         | attempts running on this worker, per served type                                            |

`activity_dead_letter_hook_panic` is in the catalog for the Go SDK's dead-letter hook; this SDK
never emits it.

**Gauges** are off by default. `gaugeIntervalMs: 30_000` samples them about every 30 seconds,
jittered by up to a fifth so a fleet's workers don't query in step. Queue depth and age need a
storage that answers queries (`PostgresStorage` does). Each sample costs one
`aggregateActivities` call and one single-row `queryActivities` call. A status that empties out is
reported as 0.

> Breaking: metric names used to be the worker event names (`activityStarted`, `claimLost`,
> …). They are now the catalog names above.

## Tracing

The core package imports no tracing library. A worker's `tracer` option, and a
`RunnerQClient`'s, takes a `Tracer` that starts these spans:

| Span                  | When                                                               |
| --------------------- | ------------------------------------------------------------------ |
| `runnerq.enqueue`     | each `client.execute` / `ctx.spawn`, ended once the insert commits |
| `runnerq.attempt`     | each attempt, until its outcome is recorded                        |
| `runnerq.step`        | each `ctx.run` that runs its function (replays start none)         |
| `runnerq.sleep`       | each `ctx.sleep` reached: `elapsed` or `parked`                    |
| `runnerq.signal.wait` | each `ctx.waitForSignal`: `received`, `parked` or `timeout`        |
| `runnerq.signal.send` | `client.signal` / `client.signalByKey`                             |

Each span ends with `runnerq.outcome` (`spanOutcomes`), and with its error when there is one.

**Trace context in storage.** Every enqueue stores a W3C `traceparent` in the activity's
metadata (`traceparentKey`). It is the value you set yourself with
`runner.metadata({ traceparent })` if you set one, otherwise the enqueue span's, otherwise,
inside a handler, the running activity's. Each attempt span uses it as its parent. So retries,
and the attempts that resume after a durable sleep or signal wait, all sit under the enqueue
span, and child activities join the same trace, whichever SDK runs them.

### OpenTelemetry

`runnerq/otel` implements both interfaces with `@opentelemetry/api`, an optional peer
dependency (install it yourself; nothing else in the package imports it):

```ts
import { otelTracer, otelMetricsSink } from "runnerq/otel";

const tracer = otelTracer(); // global TracerProvider, or { tracerProvider }
const client = new RunnerQClient({ storage, tracer });
const worker = new Worker({
  storage,
  tracer,
  metrics: otelMetricsSink(), // global MeterProvider, or { meterProvider }
  gaugeIntervalMs: 30_000,
});
```

Register a context manager (the Node SDK does) so spans started inside `ctx.run` nest under
the step. Metrics become instruments named `runnerq.` plus the catalog name, with
`queue`, `activity_type` and `status` attributes.
