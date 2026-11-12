// Reports a worker's traces and metrics through OpenTelemetry. The only module of the package
// that imports @opentelemetry/api, an optional peer dependency.
import {
  context,
  metrics as otelMetrics,
  trace,
  isSpanContextValid,
  SpanKind,
  SpanStatusCode,
  TraceFlags,
  type Attributes,
  type Context,
  type Counter,
  type Gauge,
  type Histogram,
  type MeterProvider,
  type Span,
  type TracerProvider,
} from "@opentelemetry/api";
import {
  metricCatalog,
  metricLabels,
  spanAttributes,
  spanNames,
} from "../observability-spec.js";
import { validTraceparent } from "../observability.js";
import type {
  LabeledMetrics,
  MetricLabels,
  SpanStart,
  TraceAttributeValue,
  Tracer,
  TraceSpan,
} from "../observability.js";

/** The instrumentation scope of the tracer and meter. */
export const scopeName = "runnerq";
/** Prepended to every metric name. */
export const metricPrefix = "runnerq.";

export interface OtelOptions {
  /** Defaults to the global provider. */
  tracerProvider?: TracerProvider;
  /** Defaults to the global provider. */
  meterProvider?: MeterProvider;
}

const kinds: Record<string, SpanKind> = {
  [spanNames.enqueue]: SpanKind.PRODUCER,
  [spanNames.signalSend]: SpanKind.PRODUCER,
  [spanNames.attempt]: SpanKind.CONSUMER,
};

/**
 * A `Tracer` that starts OpenTelemetry spans. Pass it as `tracer` to a `Worker` and a
 * `RunnerQClient`. Attempts continue the traceparent stored at enqueue, so retries and the
 * attempts that resume after a sleep or signal form one trace with the enqueue.
 */
export function otelTracer(options: OtelOptions = {}): Tracer {
  const tracer = (
    options.tracerProvider ?? trace.getTracerProvider()
  ).getTracer(scopeName);
  return {
    start({ name, parent, attributes }: SpanStart): TraceSpan {
      const span = tracer.startSpan(
        name,
        { kind: kinds[name] ?? SpanKind.INTERNAL, attributes },
        parentContext(parent),
      );
      return new OtelSpan(span);
    },
  };
}

class OtelSpan implements TraceSpan {
  readonly traceparent: string;
  constructor(readonly span: Span) {
    const sc = span.spanContext();
    this.traceparent = isSpanContextValid(sc)
      ? `00-${sc.traceId}-${sc.spanId}-${sc.traceFlags & TraceFlags.SAMPLED ? "01" : "00"}`
      : "";
  }
  run<T>(fn: () => T): T {
    return context.with(trace.setSpan(context.active(), this.span), fn);
  }
  end(
    outcome: string,
    error?: unknown,
    attributes?: Readonly<Record<string, TraceAttributeValue>>,
  ): void {
    this.span.setAttribute(spanAttributes.outcome, outcome);
    if (attributes) this.span.setAttributes(attributes);
    if (error !== undefined) {
      this.span.recordException(error instanceof Error ? error : String(error));
      this.span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error instanceof Error ? error.message : String(error),
      });
    }
    this.span.end();
  }
}

function parentContext(parent: SpanStart["parent"]): Context {
  if (parent instanceof OtelSpan)
    return trace.setSpan(context.active(), parent.span);
  if (typeof parent === "string" && validTraceparent(parent)) {
    const [, traceId, spanId, flags] = parent.split("-");
    return trace.setSpanContext(context.active(), {
      traceId: traceId!,
      spanId: spanId!,
      traceFlags: parseInt(flags!, 16) & TraceFlags.SAMPLED,
      isRemote: true,
    });
  }
  return context.active();
}

/**
 * A `LabeledMetrics` recording every metric of `metricCatalog` into OpenTelemetry instruments
 * named "runnerq." plus the metric's name, with the catalog's unit and labels as attributes.
 */
export function otelMetricsSink(options: OtelOptions = {}): LabeledMetrics {
  const meter = (
    options.meterProvider ?? otelMetrics.getMeterProvider()
  ).getMeter(scopeName);
  const counters = new Map<string, Counter>();
  const histograms = new Map<string, Histogram>();
  const gauges = new Map<string, Gauge>();
  for (const info of metricCatalog) {
    const name = metricPrefix + info.name;
    const opts = { description: info.description, unit: info.unit };
    if (info.kind === "counter")
      counters.set(info.name, meter.createCounter(name, opts));
    else if (info.kind === "histogram")
      histograms.set(info.name, meter.createHistogram(name, opts));
    else gauges.set(info.name, meter.createGauge(name, opts));
  }
  const attrs = (labels: MetricLabels): Attributes => {
    const a: Attributes = { [metricLabels.queue]: labels.queue };
    if (labels.activityType) a[metricLabels.activityType] = labels.activityType;
    if (labels.status) a[metricLabels.status] = labels.status;
    if (labels.version) a[metricLabels.version] = labels.version;
    return a;
  };
  return {
    addCounter: (name, value, labels) =>
      counters.get(name)?.add(value, attrs(labels)),
    recordHistogram: (name, value, labels) =>
      histograms.get(name)?.record(value, attrs(labels)),
    setGauge: (name, value, labels) =>
      gauges.get(name)?.record(value, attrs(labels)),
  };
}
