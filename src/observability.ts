import { traceparentKey } from "./observability-spec.js";

/** Log severities, lowest first. */
export type LogLevel = "debug" | "info" | "warn" | "error";
/** Structured fields of a log line; keys match the Go SDK's slog attributes. */
export type LogFields = Readonly<Record<string, unknown>>;

/**
 * Receives the worker's log lines. The messages and field names are the ones the Go SDK logs
 * through slog (runnerq-spec observability/README.md).
 */
export interface Logger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  error(message: string, fields?: LogFields): void;
}

const levels: Record<LogLevel, number> = {
  debug: -4,
  info: 0,
  warn: 4,
  error: 8,
};

/**
 * The default logger: one JSON object per line on stderr, `{"time", "level", "msg", ...fields}`
 * as Go's slog JSON handler writes them, at `level` and above (default "info"). Errors become
 * their message.
 */
export function jsonLogger(
  options: { level?: LogLevel; write?: (line: string) => void } = {},
): Logger {
  const min = levels[options.level ?? "info"];
  const write =
    options.write ?? ((line: string) => void process.stderr.write(line));
  const log =
    (level: LogLevel) =>
    (message: string, fields: LogFields = {}): void => {
      if (levels[level] < min) return;
      const line: Record<string, unknown> = {
        time: new Date().toISOString(),
        level: level.toUpperCase(),
        msg: message,
      };
      for (const [key, value] of Object.entries(fields))
        line[key] = value instanceof Error ? value.message : value;
      write(JSON.stringify(line) + "\n");
    };
  return {
    debug: log("debug"),
    info: log("info"),
    warn: log("warn"),
    error: log("error"),
  };
}

/** A logger that discards everything. */
export const silentLogger: Logger = Object.freeze({
  debug() {},
  info() {},
  warn() {},
  error() {},
});

/** The labels of one observation; `metricCatalog` lists which labels each metric carries. */
export interface MetricLabels {
  readonly queue: string;
  readonly activityType?: string;
  /** A canonical activity status, on `queue_depth` only. */
  readonly status?: string;
  /**
   * A code version, on `activities_by_version` and `activities_unserved` only; "" for
   * unstamped activities.
   */
  readonly version?: string;
}

/**
 * Receives every metric of `metricCatalog` with its labels, named by `metricNames`. Called
 * from the worker's hot paths: it must not throw or block (a throw is reported as a
 * `workerError`).
 */
export interface LabeledMetrics {
  addCounter(name: string, value: number, labels: MetricLabels): void;
  /** One observation in the metric's unit: seconds for durations. */
  recordHistogram(name: string, value: number, labels: MetricLabels): void;
  setGauge(name: string, value: number, labels: MetricLabels): void;
}

/** The worker's original metrics interface: names only, durations in milliseconds, no gauges. */
export interface Metrics {
  increment(name: string, value: number): void;
  duration(name: string, milliseconds: number): void;
}

export function isLabeledMetrics(
  metrics: Metrics | LabeledMetrics,
): metrics is LabeledMetrics {
  return typeof (metrics as LabeledMetrics).addCounter === "function";
}

/**
 * Adapts a `Metrics` to `LabeledMetrics`: labels are dropped, histograms reach `duration` in
 * milliseconds, and gauges are dropped.
 */
export function unlabeledMetrics(metrics: Metrics): LabeledMetrics {
  return {
    addCounter: (name, value) => metrics.increment(name, value),
    recordHistogram: (name, value) => metrics.duration(name, value * 1000),
    setGauge() {},
  };
}

/** A span attribute value. */
export type TraceAttributeValue = string | number | boolean;

/** What `Tracer.start` is asked to begin. */
export interface SpanStart {
  /** A `spanNames` value. */
  name: string;
  /**
   * The parent: a W3C traceparent (an attempt continues its activity's), a span this tracer
   * started, or absent for the active context.
   */
  parent?: string | TraceSpan;
  /** Keyed by `spanAttributes` values. */
  attributes: Readonly<Record<string, TraceAttributeValue>>;
}

/** A span a `Tracer` started. */
export interface TraceSpan {
  /** This span as a W3C traceparent, or "" when it carries no valid context. */
  readonly traceparent: string;
  /** Runs `fn` with this span as the active one, so tracing in user code nests under it. */
  run<T>(fn: () => T): T;
  /** Ends the span with an outcome (a `spanOutcomes` value) and the error behind it, if any. */
  end(
    outcome: string,
    error?: unknown,
    attributes?: Readonly<Record<string, TraceAttributeValue>>,
  ): void;
}

/**
 * Starts the spans the SDK reports: one per enqueue, attempt, executed step, durable sleep,
 * signal wait and signal sent (runnerq-spec observability/tracing.json). `runnerq/otel`
 * implements it with OpenTelemetry; the SDK itself depends on no tracing library.
 *
 * Whether or not a tracer is set, every enqueue stores a W3C traceparent in the activity's
 * metadata (`traceparentKey`) when there is one to store: the caller's own, the enqueue
 * span's, or the enqueueing handler's activity's. An attempt continues it, so retries and the
 * attempts that resume after a sleep or signal wait join the enqueue's trace.
 */
export interface Tracer {
  start(span: SpanStart): TraceSpan;
}

/** Starts a span when there is a tracer; `attributes` is only called then. */
export function startSpan(
  tracer: Tracer | undefined,
  name: string,
  parent: string | TraceSpan | undefined,
  attributes: () => Record<string, TraceAttributeValue>,
): TraceSpan | undefined {
  return tracer?.start({ name, parent, attributes: attributes() });
}

/**
 * The traceparent an enqueue stores (runnerq-spec vectors/traceparent.json): the caller's own
 * value as given, else the first valid of `active` and `inherited`, else none.
 */
export function storedTraceparent(
  explicit: string | undefined,
  active: string | undefined,
  inherited: string | undefined,
): string | undefined {
  if (explicit !== undefined) return explicit;
  for (const tp of [active, inherited])
    if (tp !== undefined && validTraceparent(tp)) return tp;
  return undefined;
}

const traceparentPattern =
  /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}(-.*)?$/;

/**
 * Whether `value` is a W3C trace context traceparent this SDK continues: lowercase hex version
 * (not ff), a non-zero 32-digit trace id, a non-zero 16-digit parent id and flags; version 00
 * has nothing after them.
 */
export function validTraceparent(value: string): boolean {
  const m = traceparentPattern.exec(value);
  return (
    !!m &&
    m[1] !== "ff" &&
    !(m[1] === "00" && m[4] !== undefined) &&
    !/^0+$/.test(m[2]!) &&
    !/^0+$/.test(m[3]!)
  );
}

/** Metadata with the traceparent an enqueue stores set, when there is one. */
export function withTraceparent(
  metadata: Readonly<Record<string, string>>,
  active: string | undefined,
  inherited: string | undefined,
): Readonly<Record<string, string>> {
  if (Object.hasOwn(metadata, traceparentKey)) return metadata;
  const tp = storedTraceparent(undefined, active, inherited);
  return tp === undefined ? metadata : { ...metadata, [traceparentKey]: tp };
}
