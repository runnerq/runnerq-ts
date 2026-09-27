import { RunnerQError } from "./errors.js";
import { nonempty } from "./codec.js";
export type Priority = "low" | "normal" | "high" | "critical";
export type DuplicatePolicy =
  "returnExisting" | "allowReuse" | "allowReuseOnFailure" | "noReuse";
export interface ExecutionOptions {
  priority: Priority;
  maxAttempts: number | "unlimited";
  timeoutMs: number;
  maxRetryDelayMs: number;
  delayMs: number;
  metadata: Readonly<Record<string, string>>;
  idempotency?: { readonly key: string; readonly onDuplicate: DuplicatePolicy };
  step?: string;
  asRoot?: boolean;
  replay?: "new";
}
const brand = Symbol("RunnerQ activity option");
export interface ActivityOption {
  readonly [brand]: true;
  readonly value: Readonly<Partial<ExecutionOptions>>;
}
function option(value: Partial<ExecutionOptions>): ActivityOption {
  return Object.freeze({ [brand]: true as const, value: Object.freeze(value) });
}
export function integer(
  value: number,
  label: string,
  min = 0,
  max = Number.MAX_SAFE_INTEGER,
): number {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new RunnerQError(
      "configuration",
      `${label} must be an integer between ${min} and ${max}`,
    );
  return value;
}
function seconds(ms: number, label: string): number {
  integer(ms, label, 1000, 2_147_483_647_000);
  if (ms % 1000)
    throw new RunnerQError(
      "configuration",
      `${label} must be a whole number of seconds (in milliseconds)`,
    );
  return ms;
}
export const runner = Object.freeze({
  priority(value: Priority): ActivityOption {
    if (!["low", "normal", "high", "critical"].includes(value))
      throw new RunnerQError("configuration", "Invalid priority");
    return option({ priority: value });
  },
  maxAttempts(value: number | "unlimited"): ActivityOption {
    return option({
      maxAttempts:
        value === "unlimited"
          ? value
          : integer(value, "maxAttempts", 1, 2_147_483_647),
    });
  },
  timeoutMs(value: number): ActivityOption {
    return option({ timeoutMs: seconds(value, "timeoutMs") });
  },
  maxRetryDelayMs(value: number): ActivityOption {
    return option({ maxRetryDelayMs: seconds(value, "maxRetryDelayMs") });
  },
  delayMs(value: number): ActivityOption {
    return option({ delayMs: integer(value, "delayMs", 0, 8_000_000_000_000) });
  },
  metadata(value: Record<string, string>): ActivityOption {
    if (!value || Object.values(value).some((v) => typeof v !== "string"))
      throw new RunnerQError(
        "configuration",
        "Metadata values must be strings",
      );
    return option({ metadata: Object.freeze({ ...value }) });
  },
  idempotencyKey(
    key: string,
    onDuplicate: DuplicatePolicy = "returnExisting",
  ): ActivityOption {
    nonempty(key, "Idempotency key");
    if (
      ![
        "returnExisting",
        "allowReuse",
        "allowReuseOnFailure",
        "noReuse",
      ].includes(onDuplicate)
    )
      throw new RunnerQError("configuration", "Invalid duplicate policy");
    return option({ idempotency: Object.freeze({ key, onDuplicate }) });
  },
  step(name: string): ActivityOption {
    nonempty(name, "Step name");
    return option({ step: name });
  },
  asRoot(): ActivityOption {
    return option({ asRoot: true });
  },
  newOnReplay(): ActivityOption {
    return option({ replay: "new" });
  },
});
export function executionOptions(
  options: readonly ActivityOption[],
): ExecutionOptions {
  const result: ExecutionOptions = {
    priority: "normal",
    maxAttempts: 3,
    timeoutMs: 300_000,
    maxRetryDelayMs: 3_600_000,
    delayMs: 0,
    metadata: {},
  };
  for (const item of options) {
    if (!item || item[brand] !== true)
      throw new RunnerQError(
        "configuration",
        "Use runner methods to create activity options",
      );
    Object.assign(result, item.value);
  }
  if (
    Number(!!result.step) +
      Number(!!result.idempotency) +
      Number(!!result.replay) >
      1 ||
    (result.step && result.asRoot)
  )
    throw new RunnerQError(
      "configuration",
      "Step, business idempotency and newOnReplay are exclusive; a step cannot be a detached root",
    );
  return result;
}
