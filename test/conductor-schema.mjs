// The conductor protocol's JSON Schema (spec/protocol/conductor), for checking frames.
import { readFileSync, readdirSync } from "node:fs";
import Ajv2020 from "ajv/dist/2020.js";

const dir = new URL("../spec/protocol/conductor/", import.meta.url);
export const schema = JSON.parse(
  readFileSync(new URL("conductor.schema.json", dir), "utf8"),
);
/** x-messages by type, with `data` and `response` as $defs names. */
export const messages = Object.fromEntries(
  schema["x-messages"].map((m) => [
    m.type,
    { ...m, data: defName(m.data), response: defName(m.response) },
  ]),
);
export const examples = readdirSync(new URL("examples/", dir))
  .filter((f) => f.endsWith(".json"))
  .map((f) => JSON.parse(readFileSync(new URL(`examples/${f}`, dir), "utf8")));

function defName(ref) {
  return ref?.replace("#/$defs/", "");
}

const ajv = new Ajv2020({ strict: false, validateFormats: false });
ajv.addSchema(schema);
const validators = new Map();
/** The schema violations of `value` as the named $def, or "" when it conforms. */
export function violations(def, value) {
  let v = validators.get(def);
  if (!v) {
    v = ajv.getSchema(`${schema.$id}#/$defs/${def}`);
    if (!v) throw new Error(`no $def ${def}`);
    validators.set(def, v);
  }
  return v(value) ? "" : ajv.errorsText(v.errors, { dataVar: def });
}

/** The schema violations of a frame the agent sent, or "". */
export function frameViolations(env) {
  const bad = violations("Envelope", env);
  if (bad) return bad;
  const m = messages[env.type];
  if (!m || env.data === undefined) return "";
  if (env.kind === "res")
    return m.response ? violations(m.response, env.data) : "";
  return violations(m.data, env.data);
}
