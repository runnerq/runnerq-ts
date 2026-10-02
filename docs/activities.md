# Activities

Defining activities, the options you can run them with, and how failures are recorded.

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
