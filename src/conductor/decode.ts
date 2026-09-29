// Strict request decoding, as the Go agent's (encoding/json with DisallowUnknownFields): an
// unknown request field could change a request's meaning, so it fails the request.
import { WireError } from "./wire.js";

export type Spec =
  | "string"
  | "int"
  | "number"
  | "bool"
  | "any"
  | { array: Spec }
  | { object: Record<string, Spec> }
  | (() => Spec);

/**
 * Decodes `data` against `spec`, or throws invalid_argument "decode request: ...". An
 * absent or null object field is left out; a null array element is its type's zero value,
 * as Go decodes it.
 */
export function decodeRequest<T>(spec: Spec, data: unknown): T {
  if (data === undefined || data === null) return {} as T;
  try {
    return visit(spec, data, "") as T;
  } catch (error) {
    if (error instanceof DecodeError)
      throw new WireError(
        "invalid_argument",
        `decode request: ${error.message}`,
      );
    throw error;
  }
}

class DecodeError extends Error {}

function resolve(spec: Spec): Exclude<Spec, () => Spec> {
  return typeof spec === "function" ? resolve(spec()) : spec;
}
function typeName(v: unknown): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  return typeof v;
}
function mismatch(v: unknown, path: string, want: string): DecodeError {
  return new DecodeError(
    `json: cannot unmarshal ${typeName(v)} into ${path || "request"} of type ${want}`,
  );
}
function zero(spec: Spec): unknown {
  const s = resolve(spec);
  if (s === "string") return "";
  if (s === "int" || s === "number") return 0;
  if (s === "bool") return false;
  if (s === "any") return null;
  if ("array" in s) return [];
  return {};
}

function visit(spec: Spec, v: unknown, path: string): unknown {
  const s = resolve(spec);
  switch (s) {
    case "any":
      return v;
    case "string":
      if (typeof v !== "string") throw mismatch(v, path, "string");
      return v;
    case "bool":
      if (typeof v !== "boolean") throw mismatch(v, path, "bool");
      return v;
    case "number":
      if (typeof v !== "number") throw mismatch(v, path, "number");
      return v;
    case "int":
      if (typeof v !== "number" || !Number.isSafeInteger(v))
        throw mismatch(v, path, "int");
      return v;
  }
  if ("array" in s) {
    if (!Array.isArray(v)) throw mismatch(v, path, "array");
    return v.map((x, i) =>
      x === null ? zero(s.array) : visit(s.array, x, `${path}[${i}]`),
    );
  }
  if (typeof v !== "object" || v === null || Array.isArray(v))
    throw mismatch(v, path, "object");
  const out: Record<string, unknown> = {};
  for (const [key, x] of Object.entries(v)) {
    if (!Object.hasOwn(s.object, key))
      throw new DecodeError(`json: unknown field ${JSON.stringify(key)}`);
    if (x === null && s.object[key] !== "any") continue;
    out[key] = visit(s.object[key]!, x, path ? `${path}.${key}` : key);
  }
  return out;
}
