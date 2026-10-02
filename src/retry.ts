import { defaultMaxRetryDelaySeconds } from "./spec.js";

/**
 * Whether a failed attempt may be followed by another. `retryCount` counts the attempts that
 * failed before it; `maxRetries` is the attempt limit, 0 meaning unlimited.
 */
export function attemptsRemain(
  retryCount: number,
  maxRetries: number,
): boolean {
  return maxRetries === 0 || retryCount + 1 < maxRetries;
}

/**
 * Seconds before the retry that follows attempt `retryCount + 1`: the base delay doubled per
 * attempt, capped at `maxRetryDelaySeconds` (0 means the default, 3600).
 */
export function retryDelaySeconds(
  retryCount: number,
  retryDelay: number,
  maxRetryDelay: number,
): number {
  return Math.min(
    maxRetryDelay || defaultMaxRetryDelaySeconds,
    retryDelay * 2 ** Math.min(retryCount + 1, 52),
  );
}
