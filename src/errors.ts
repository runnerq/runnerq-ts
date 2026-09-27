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
    );
  }
}
export function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
export function retryable(error: unknown): boolean {
  if (error instanceof RunnerQError)
    return ["unavailable", "conflict", "timeout", "activity_failed"].includes(
      error.code,
    );
  if (error instanceof Error && error.cause !== undefined)
    return retryable(error.cause);
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
