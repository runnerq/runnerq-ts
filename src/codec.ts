import { createHash } from "node:crypto";
import { RunnerQError } from "./errors.js";

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
      out = {};
      if (Object.getOwnPropertySymbols(v).length)
        throw new RunnerQError("serialization", `Symbol keys at ${path}`);
      for (const [k, x] of Object.entries(v))
        Object.defineProperty(out, k, {
          value: visit(x, `${path}.${k}`),
          enumerable: true,
          writable: true,
          configurable: true,
        });
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
export function uuid(value: string): string {
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      value,
    )
  )
    throw new RunnerQError("configuration", "Invalid activity UUID");
  return value.toLowerCase();
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
    "rq:key:v2:" +
    Buffer.from(`${Buffer.byteLength(key)}:${key}${type}`)
      .toString("base64")
      .replace(/=+$/, "")
  );
}
