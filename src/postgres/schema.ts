// The schema comes from runnerq-spec (src/spec-schema.ts); see the spec's
// schema/postgres/README.md for how it is applied and checked.
import {
  postgresCatalog,
  postgresConcurrentIndexes,
  postgresMigrations,
} from "../spec-schema.js";

export { schemaAdvisoryLockKey as schemaLock } from "../spec.js";
export const catalog = postgresCatalog;
export const concurrentIndexes = postgresConcurrentIndexes;
/** Every migration, run as one transaction; each is safe to re-run. */
export const migrations = postgresMigrations.map((m) => m.sql).join("\n");
export const tableNames = catalog.tables.map((t) => t.name);
export const indexNames = catalog.indexes.map((i) => i.name);

/** An index definition, normalized as the spec's vectors/index_definition.json describes. */
export function normalizeIndex(sql: string): string {
  return sql
    .toLowerCase()
    .replace(/\bon\s+(?:"(?:[^"]|"")+"|[a-z_]\w*)\./g, "on ")
    .replace(/::text(?:\[\])?/g, "")
    .replace(/=\s*any\s*\(\s*array\s*\[/g, "in(")
    .replace(/using btree/g, "")
    .replace(/\s+asc\b/g, "")
    .replace(/[\s"()[\];]/g, "");
}
/** A column default, normalized as the spec's vectors/column_default.json describes. */
export function normalizeDefault(value: string | null): string | null {
  if (value === null) return null;
  const s = value
    .toLowerCase()
    .replace(/::(?:text|integer|bigint|smallint)/g, "")
    .replace(/[\s()]/g, "");
  return s.startsWith("nextval") ? "nextval" : s;
}
