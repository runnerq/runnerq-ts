import SuperJSON, { type SuperJSONResult } from "superjson";
import { json, nonempty, type JsonValue } from "./codec.js";
import { RunnerQError } from "./errors.js";

export type SerializationMode = "native" | "portable";
export type SerializationFormat = "superjson-v1" | "json-v1";
export interface SerializedValue {
  serialization: SerializationFormat;
  data: JsonValue;
}
export interface SerializationRecipe<T, S extends JsonValue> {
  name: string;
  isApplicable(value: unknown): value is T;
  serialize(value: T): S;
  deserialize(value: S): T;
}
const native = new SuperJSON();
native.allowErrorProps("stack");
const recipes = new Map<string, (value: unknown) => boolean>();
let started = false;

/** Register the same versioned recipe in every process before serializing native values. */
export function registerSerialization<T, S extends JsonValue>(
  recipe: SerializationRecipe<T, S>,
): void {
  nonempty(recipe.name, "Serialization recipe name");
  if (started || recipes.has(recipe.name))
    throw new RunnerQError(
      "configuration",
      "Serialization recipes must have unique names and be registered before native serialization starts",
    );
  const { name, isApplicable, serialize, deserialize } = recipe;
  native.registerCustom<T, JsonValue>(
    {
      isApplicable,
      serialize: (value) => json(serialize(value)),
      deserialize: (value) => deserialize(value as S),
    },
    name,
  );
  recipes.set(name, isApplicable);
}
registerSerialization<Buffer, string>({
  name: "runnerq.Buffer.v1",
  isApplicable: Buffer.isBuffer,
  serialize: (value) => value.toString("base64"),
  deserialize: (value) => Buffer.from(value, "base64"),
});

export function serializationFormat(
  mode: SerializationMode,
): SerializationFormat {
  if (mode === "native") return "superjson-v1";
  if (mode === "portable") return "json-v1";
  throw new RunnerQError(
    "configuration",
    "Serialization must be native or portable",
  );
}

// SuperJSON intentionally permits some values JSON.stringify would silently drop.
// Reject those before encoding; only built-ins and explicit recipes are durable.
function validateNative(value: unknown): void {
  const visited = new WeakSet<object>();
  function visit(v: unknown): void {
    const type = typeof v;
    if (
      v === null ||
      type === "undefined" ||
      type === "string" ||
      type === "boolean" ||
      type === "bigint"
    )
      return;
    if (typeof v === "number") {
      if (Number.isInteger(v) && !Number.isSafeInteger(v))
        throw new Error("Use bigint for integers outside the safe range");
      return;
    }
    if (typeof v !== "object" || !v)
      throw new Error("Functions and symbols cannot be persisted");
    if (visited.has(v)) return;
    visited.add(v);
    if (Object.getOwnPropertySymbols(v).length)
      throw new Error("Symbol keys cannot be persisted");
    for (const matches of recipes.values()) if (matches(v)) return;
    if (v instanceof Map) {
      for (const [key, item] of v) {
        visit(key);
        visit(item);
      }
      return;
    }
    if (v instanceof Set) {
      for (const item of v) visit(item);
      return;
    }
    if (v instanceof Date || v instanceof RegExp || v instanceof URL) return;
    if (v instanceof Error) {
      visit(v.cause);
      return;
    }
    if (Array.isArray(v)) {
      for (const item of v) visit(item);
      return;
    }
    const prototype = Object.getPrototypeOf(v);
    if (prototype !== Object.prototype && prototype !== null)
      throw new Error("Register a serialization recipe for custom classes");
    for (const item of Object.values(v)) visit(item);
  }
  visit(value);
}

export function encode(
  value: unknown,
  serialization: SerializationFormat = "superjson-v1",
): SerializedValue {
  try {
    if (serialization === "json-v1")
      return { serialization, data: json(value) };
    if (serialization !== "superjson-v1")
      throw new Error(`Unsupported serialization format: ${serialization}`);
    started = true;
    validateNative(value);
    return {
      serialization,
      data: json(
        native.serialize(value as Parameters<SuperJSON["serialize"]>[0]),
      ),
    };
  } catch (cause) {
    throw new RunnerQError("serialization", "Value cannot be serialized", {
      cause,
    });
  }
}

export function decode(value: SerializedValue): unknown {
  try {
    if (value.serialization === "json-v1") return json(value.data);
    if (value.serialization !== "superjson-v1")
      throw new Error(
        `Unsupported serialization format: ${value.serialization}`,
      );
    started = true;
    const data = json(value.data);
    if (!data || typeof data !== "object" || !("json" in data))
      throw new Error("Invalid SuperJSON envelope");
    const decoded = native.deserialize(data as unknown as SuperJSONResult);
    validateNative(decoded);
    return decoded;
  } catch (cause) {
    throw new RunnerQError(
      "serialization",
      "Stored value cannot be deserialized",
      { cause },
    );
  }
}
