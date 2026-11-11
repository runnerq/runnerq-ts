import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { context, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from "@opentelemetry/sdk-metrics";
import {
  activity,
  jsonLogger,
  metricCatalog,
  metricNames,
  runner,
  RunnerQClient,
  RunnerQError,
  silentLogger,
  spanAttributes,
  spanNames,
  spanOutcomes,
  traceparentKey,
  unlabeledMetrics,
  Worker,
} from "../dist/index.js";
import { storedTraceparent } from "../dist/observability.js";
import { otelMetricsSink, otelTracer } from "../dist/otel/index.js";
import { dsn, setup, until } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 30000 }, fn);

test("stored traceparent follows the spec vector", () => {
  const vectors = JSON.parse(
    readFileSync(
      new URL("../spec/vectors/traceparent.json", import.meta.url),
      "utf8",
    ),
  );
  for (const c of vectors.cases) {
    const got = storedTraceparent(
      c.input.explicit ?? undefined,
      c.input.active ?? undefined,
      c.input.inherited ?? undefined,
    );
    assert.equal(got ?? null, c.output, c.name);
  }
});

test("the metric names are the spec catalog's", () => {
  assert.equal(metricNames.activityStarted, "activity_started");
  assert.ok(
    metricCatalog.some((m) => m.name === "queue_depth" && m.kind === "gauge"),
  );
});

test("the default logger writes slog-shaped JSON lines at info and above", () => {
  const lines = [];
  const log = jsonLogger({ write: (l) => lines.push(l) });
  log.debug("hidden");
  log.warn("Activity requesting retry", {
    activity_id: "a",
    reason: new Error("busy"),
  });
  assert.equal(lines.length, 1);
  const line = JSON.parse(lines[0]);
  assert.equal(line.level, "WARN");
  assert.equal(line.msg, "Activity requesting retry");
  assert.equal(line.activity_id, "a");
  assert.equal(line.reason, "busy");
  assert.ok(!Number.isNaN(Date.parse(line.time)));
});

test("unlabeledMetrics adapts the names-only interface", () => {
  const seen = [];
  const m = unlabeledMetrics({
    increment: (n, v) => seen.push(["inc", n, v]),
    duration: (n, ms) => seen.push(["dur", n, ms]),
  });
  m.addCounter("activity_started", 2, { queue: "q" });
  m.recordHistogram("activity_claim_lag", 1.5, { queue: "q" });
  m.setGauge("queue_depth", 3, { queue: "q" });
  assert.deepEqual(seen, [
    ["inc", "activity_started", 2],
    ["dur", "activity_claim_lag", 1500],
  ]);
});

integration(
  "one trace from enqueue to the last step, across a retry, a durable sleep and a child",
  async (t) => {
    const contextManager = new AsyncLocalStorageContextManager().enable();
    context.setGlobalContextManager(contextManager);
    t.after(() => context.disable());
    const exporter = new InMemorySpanExporter();
    const tracerProvider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });
    const metricExporter = new InMemoryMetricExporter(
      AggregationTemporality.CUMULATIVE,
    );
    const reader = new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 60_000,
    });
    const meterProvider = new MeterProvider({ readers: [reader] });
    const tracer = otelTracer({ tracerProvider });

    const { storage, queue } = await setup(t);
    const client = new RunnerQClient({ storage, tracer });
    const Order = activity("Order"),
      Email = activity("Email");
    let attempts = 0;
    const w = new Worker({
      storage,
      tracer,
      metrics: otelMetricsSink({ meterProvider }),
      logger: silentLogger,
      waitGraceMs: 20,
    });
    w.register(Order, async (ctx) => {
      await ctx.run("reserve", () => "r1");
      if (++attempts === 1)
        throw new RunnerQError("unavailable", "payment gateway busy");
      await ctx.sleep("cool-off", 2000); // past the 1s timeout: parks
      const child = await ctx.spawn(Email, null, runner.step("email"));
      await ctx.wait(child);
      return ctx.run("ship", () => "shipped");
    });
    w.register(Email, () => "sent");
    await w.start();
    try {
      const caller = tracerProvider.getTracer("test").startSpan("checkout");
      const handle = await context.with(
        trace.setSpan(context.active(), caller),
        () =>
          client.execute(
            Order,
            null,
            runner.timeoutMs(1000),
            runner.maxAttempts(3),
          ),
      );
      caller.end();
      assert.equal(
        await handle.result({ signal: AbortSignal.timeout(25_000) }),
        "shipped",
      );
      const spans = await until(() => {
        const all = exporter.getFinishedSpans();
        return (
          all.some(
            (s) =>
              s.name === spanNames.attempt &&
              s.attributes[spanAttributes.activityType] === "Order" &&
              s.attributes[spanAttributes.outcome] === spanOutcomes.completed,
          ) && all
        );
      });
      const traceId = caller.spanContext().traceId;
      const byId = new Map(spans.map((s) => [s.spanContext().spanId, s]));
      const parentOf = (s) => byId.get(s.parentSpanContext?.spanId);
      for (const s of spans)
        assert.equal(s.spanContext().traceId, traceId, `${s.name} trace`);
      const last = spans.find(
        (s) =>
          s.name === spanNames.step &&
          s.attributes[spanAttributes.stepName] === "ship",
      );
      const chain = [];
      for (let s = last; s; s = parentOf(s)) chain.push(s.name);
      assert.deepEqual(chain, [
        spanNames.step,
        spanNames.attempt,
        spanNames.enqueue,
        "checkout",
      ]);
      const enqueue = parentOf(parentOf(last));
      const orderAttempts = spans.filter(
        (s) =>
          s.name === spanNames.attempt &&
          s.attributes[spanAttributes.activityType] === "Order",
      );
      for (const a of orderAttempts)
        assert.equal(parentOf(a), enqueue, "attempt's parent is the enqueue");
      const seen = new Set(
        orderAttempts.map((a) => a.attributes[spanAttributes.outcome]),
      );
      for (const o of [
        spanOutcomes.retry,
        spanOutcomes.yielded,
        spanOutcomes.completed,
      ])
        assert.ok(seen.has(o), `an attempt ended ${o}: ${[...seen]}`);
      assert.ok(
        spans.some(
          (s) =>
            s.name === spanNames.sleep &&
            s.attributes[spanAttributes.outcome] === spanOutcomes.parked,
        ),
      );
      const emailAttempt = spans.find(
        (s) =>
          s.name === spanNames.attempt &&
          s.attributes[spanAttributes.activityType] === "Email",
      );
      assert.equal(parentOf(parentOf(emailAttempt))?.name, spanNames.attempt);
      const stored = await storage.getActivity(handle.id);
      assert.equal(
        stored.metadata[traceparentKey].slice(3, 35),
        traceId,
        "the stored traceparent carries the trace",
      );

      await reader.forceFlush();
      const metrics = metricExporter
        .getMetrics()
        .flatMap((r) => r.scopeMetrics.flatMap((s) => s.metrics));
      const completed = metrics.find(
        (m) => m.descriptor.name === "runnerq.activity_completed",
      );
      assert.ok(
        completed.dataPoints.some(
          (p) =>
            p.attributes.queue === queue &&
            p.attributes.activity_type === "Order" &&
            p.value === 1,
        ),
      );
    } finally {
      await w.stop();
      await meterProvider.shutdown();
    }
  },
);

integration("labelled metrics and gauges", async (t) => {
  const { storage, queue } = await setup(t);
  const counters = new Map(),
    gauges = new Map(),
    histograms = [];
  const key = (name, l) =>
    `${name}|${l.queue}|${l.activityType ?? ""}|${l.status ?? ""}`;
  const metrics = {
    addCounter: (n, v, l) =>
      counters.set(key(n, l), (counters.get(key(n, l)) ?? 0) + v),
    recordHistogram: (n, _v, l) => histograms.push(key(n, l)),
    setGauge: (n, v, l) => gauges.set(key(n, l), v),
  };
  const Work = activity("Work"),
    Idle = activity("Idle");
  let release;
  const released = new Promise((r) => (release = r));
  const w = new Worker({
    storage,
    metrics,
    gaugeIntervalMs: 50,
    logger: silentLogger,
  });
  w.register(Work, async () => {
    await released;
    return 1;
  });
  w.register(Idle, () => null);
  const client = new RunnerQClient({ storage });
  await w.start();
  try {
    const h = await client.execute(Work, null);
    await client.execute(Work, null, runner.delayMs(3_600_000));
    await until(
      () =>
        gauges.get(
          key("activities_in_flight", { queue, activityType: "Work" }),
        ) === 1 &&
        gauges.get(
          key("activities_in_flight", { queue, activityType: "Idle" }),
        ) === 0 &&
        gauges.get(
          key("queue_depth", {
            queue,
            activityType: "Work",
            status: "running",
          }),
        ) === 1 &&
        gauges.get(
          key("queue_depth", {
            queue,
            activityType: "Work",
            status: "scheduled",
          }),
        ) === 1 &&
        gauges.has(key("queue_oldest_pending_age", { queue })),
      10_000,
      "gauges",
    );
    release();
    await h.result({ signal: AbortSignal.timeout(10_000) });
    await until(
      () =>
        gauges.get(
          key("queue_depth", {
            queue,
            activityType: "Work",
            status: "running",
          }),
        ) === 0,
      10_000,
      "running drops to 0",
    );
    const work = { queue, activityType: "Work" };
    assert.equal(counters.get(key("activity_started", work)), 1);
    assert.equal(counters.get(key("activity_completed", work)), 1);
    for (const name of [
      "activity_execution",
      "activity_completion_persistence",
    ])
      assert.ok(histograms.includes(key(name, work)), name);
  } finally {
    await w.stop();
  }
});
