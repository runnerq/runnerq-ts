import { json, type JsonValue } from "./codec.js";

export type ErrorCode =
  | "configuration"
  | "serialization"
  | "not_found"
  | "duplicate"
  | "idempotency_conflict"
  | "claim_lost"
  | "checkpoint_conflict"
  | "unavailable"
  | "conflict"
  | "timeout"
  | "internal"
  | "non_retryable"
  | "signal_timeout"
  | "activity_failed";

export class RunnerQError extends Error {
  constructor(
    public readonly code: ErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = new.target.name;
  }
}
export class NonRetryableError extends RunnerQError {
  constructor(message: string, options?: ErrorOptions) {
    super("non_retryable", message, options);
  }
}
export class SignalTimeoutError extends RunnerQError {
  constructor(name: string) {
    super("signal_timeout", `Signal ${JSON.stringify(name)} timed out`);
  }
}
/** Portable diagnostics; custom exception constructors are never invoked on replay. */
export type FailureDetails = {
  name: string;
  message: string;
  stack?: string;
  code?: string | number;
  data?: JsonValue;
  cause?: FailureDetails;
};

function property(value: unknown, key: string): unknown {
  try {
    return value !== null &&
      (typeof value === "object" || typeof value === "function")
      ? Reflect.get(value, key)
      : undefined;
  } catch {
    return undefined;
  }
}

/** Diagnostics must never prevent the original failure from being committed. */
export function captureFailure(error: unknown): FailureDetails {
  const seen = new Set<unknown>();
  function capture(value: unknown, depth: number): FailureDetails {
    if (seen.has(value) || depth >= 8)
      return {
        name: "TruncatedCause",
        message: "Circular or deeply nested error cause omitted",
      };
    seen.add(value);
    const name = property(value, "name"),
      text = property(value, "message");
    const result: FailureDetails = {
      name: typeof name === "string" ? name : "Error",
      message: typeof text === "string" ? text : message(value),
    };
    const stack = property(value, "stack"),
      code = property(value, "code");
    if (typeof stack === "string") result.stack = stack;
    if (
      typeof code === "string" ||
      (typeof code === "number" && Number.isSafeInteger(code))
    )
      result.code = code;
    const data = property(value, "data");
    if (data !== undefined) {
      try {
        result.data = json(data);
      } catch {
        result.data = "[Error data is not portable JSON]";
      }
    }
    const cause = property(value, "cause");
    if (cause !== undefined) result.cause = capture(cause, depth + 1);
    return result;
  }
  return capture(error, 0);
}

export class RecordedError extends Error {
  readonly code?: string | number;
  readonly data?: JsonValue;
  constructor(readonly detail: FailureDetails) {
    super(
      detail.message,
      detail.cause ? { cause: new RecordedError(detail.cause) } : undefined,
    );
    this.name = detail.name;
    if (detail.stack !== undefined) this.stack = detail.stack;
    this.code = detail.code;
    this.data = detail.data;
  }
}

export function recordedFailure(data: unknown): RecordedError | undefined {
  const failure = property(data, "failure");
  function valid(value: unknown, depth = 0): value is FailureDetails {
    if (
      depth > 8 ||
      typeof property(value, "name") !== "string" ||
      typeof property(value, "message") !== "string"
    )
      return false;
    const cause = property(value, "cause");
    return cause === undefined || valid(cause, depth + 1);
  }
  return valid(failure) ? new RecordedError(failure) : undefined;
}

export class ActivityFailedError extends RunnerQError {
  constructor(
    public readonly activityId: string,
    public readonly detail: unknown,
  ) {
    super(
      "activity_failed",
      typeof detail === "object" && detail !== null && "error" in detail
        ? String(detail.error)
        : "Activity failed",
      { cause: recordedFailure(detail) },
    );
  }
}
export function message(error: unknown): string {
  const text = property(error, "message");
  if (typeof text === "string") return text;
  try {
    return String(error);
  } catch {
    return "Unknown error";
  }
}
export function retryable(error: unknown): boolean {
  const seen = new Set<unknown>();
  while (!seen.has(error)) {
    seen.add(error);
    if (error instanceof RunnerQError)
      return ["unavailable", "conflict", "timeout", "activity_failed"].includes(
        error.code,
      );
    const cause = error instanceof Error ? property(error, "cause") : undefined;
    if (cause === undefined) return true;
    error = cause;
  }
  return true;
}
export function transient(error: unknown): boolean {
  return (
    error instanceof RunnerQError &&
    ["unavailable", "conflict", "timeout"].includes(error.code)
  );
}
export function databaseError(error: unknown): Error {
  if (error instanceof RunnerQError) return error;
  const code = (error as { code?: string })?.code ?? "";
  const text = message(error);
  const kind = ["40001", "40P01", "55P03"].includes(code)
    ? "conflict"
    : code === "57014"
      ? "timeout"
      : code.startsWith("08") ||
          [
            "57P01",
            "57P02",
            "57P03",
            "53300",
            "ECONNRESET",
            "ECONNREFUSED",
            "EPIPE",
            "ETIMEDOUT",
          ].includes(code) ||
          /connection terminated|connection timeout|timeout exceeded when trying to connect/i.test(
            text,
          )
        ? "unavailable"
        : "internal";
  return new RunnerQError(kind, text, { cause: error });
}
