import { createHash } from "node:crypto";
import { RunnerQError } from "./errors.js";
import { businessKeyPrefix, stepKeyPrefix } from "./spec.js";

export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export function json(value: unknown): JsonValue {
  const ancestors = new Set<object>();
  function visit(v: unknown, path: string): JsonValue {
    if (v === null || typeof v === "string" || typeof v === "boolean") return v;
    if (
      typeof v === "number" &&
      Number.isFinite(v) &&
      (!Number.isInteger(v) || Number.isSafeInteger(v))
    )
      return v;
    if (typeof v !== "object" || !v || ancestors.has(v))
      throw new RunnerQError("serialization", `Invalid JSON value at ${path}`);
    const proto = Object.getPrototypeOf(v);
    if (!Array.isArray(v) && proto !== Object.prototype && proto !== null)
      throw new RunnerQError("serialization", `Expected plain JSON at ${path}`);
    ancestors.add(v);
    let out: JsonValue;
    if (Array.isArray(v))
      out = Array.from(v, (x, i) => visit(x, `${path}[${i}]`));
    else {
      const copy: { [key: string]: JsonValue } = {};
      if (Object.getOwnPropertySymbols(v).length)
        throw new RunnerQError("serialization", `Symbol keys at ${path}`);
      for (const [k, x] of Object.entries(v)) {
        const item = visit(x, `${path}.${k}`);
        // Assignment (twice as fast) makes the same own property, except for keys on
        // Object.prototype: its __proto__ setter, or a polluter's.
        if (k in Object.prototype)
          Object.defineProperty(copy, k, {
            value: item,
            enumerable: true,
            writable: true,
            configurable: true,
          });
        else copy[k] = item;
      }
      out = copy;
    }
    ancestors.delete(v);
    return out;
  }
  return visit(value === undefined ? null : value, "$");
}
export function nonempty(value: string, label: string): void {
  if (typeof value !== "string" || !value.length || value.includes("\0"))
    throw new RunnerQError(
      "configuration",
      `${label} must be a non-empty string without NUL`,
    );
}
const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** A canonical UUID, lowercased, or undefined. */
export function parseUuid(value: string): string | undefined {
  return uuidPattern.test(value) ? value.toLowerCase() : undefined;
}
export function uuid(value: string): string {
  const id = parseUuid(value);
  if (!id) throw new RunnerQError("configuration", "Invalid activity UUID");
  return id;
}
const rfc3339 =
  /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(\.\d{1,9})?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/;
/**
 * Whether `s` is an RFC 3339 timestamp (as Go's time.RFC3339Nano parses it). Callers pass
 * valid ones on as text, so no precision is lost to JavaScript's milliseconds.
 */
export function isTimestamp(s: unknown): s is string {
  if (typeof s !== "string") return false;
  const m = rfc3339.exec(s);
  if (!m) return false;
  const [year, month, day, hour, minute, second] = m
    .slice(1, 7)
    .map(Number) as [number, number, number, number, number, number];
  const days = new Date(Date.UTC(year, month, 0)).getUTCDate();
  if (year < 1 || month < 1 || month > 12 || day < 1 || day > days)
    return false;
  if (hour > 23 || minute > 59 || second > 59) return false;
  return !m[8] || (Number(m[9]) <= 23 && Number(m[10]) <= 59);
}
/** A decimal 64-bit signed integer (as Go's strconv.ParseInt reads it), or undefined. */
export function parseInt64(s: string): bigint | undefined {
  if (!/^[+-]?\d+$/.test(s)) return undefined;
  const n = BigInt(s);
  return n >= -(2n ** 63n) && n < 2n ** 63n ? n : undefined;
}
export function checkpointId(
  owner: string,
  kind: string,
  name: string,
): string {
  nonempty(name, "Step name");
  const digest = createHash("sha1")
    .update(Buffer.from(uuid(owner).replaceAll("-", ""), "hex"))
    .update(`${kind}:${name}`, "utf8")
    .digest();
  digest[6] = (digest[6]! & 15) | 80;
  digest[8] = (digest[8]! & 63) | 128;
  const h = digest.subarray(0, 16).toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
export function businessKey(key: string, type: string): string {
  nonempty(key, "Idempotency key");
  nonempty(type, "Activity name");
  return (
    businessKeyPrefix +
    Buffer.from(`${Buffer.byteLength(key)}:${key}${type}`)
      .toString("base64")
      .replace(/=+$/, "")
  );
}
/** The idempotency key of the activity a step spawns under `parent` in `root`'s tree. */
export function stepKey(root: string, parent: string, step: string): string {
  return `${stepKeyPrefix}${root}:${parent}:${step}`;
}
