// The schema against runnerq-spec's catalog, in fresh Postgres schemas.
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { PostgresStorage } from "../dist/postgres/index.js";
import {
  catalog,
  normalizeDefault,
  normalizeIndex,
} from "../dist/postgres/schema.js";
import { dsn } from "./helpers.mjs";

const integration = (name, fn) =>
  test(name, { skip: !dsn, timeout: 60_000 }, fn);

/** A connection string for a new, empty schema, dropped after the test. */
async function freshSchema(t) {
  const admin = new pg.Pool({ connectionString: dsn });
  const name = "rq_catalog_" + randomUUID().replaceAll("-", "").slice(0, 12);
  await admin.query(`CREATE SCHEMA ${name}`);
  t.after(async () => {
    await admin.query(`DROP SCHEMA ${name} CASCADE`);
    await admin.end();
  });
  const url = new URL(dsn);
  url.searchParams.set("options", `-c search_path=${name}`);
  const connectionString = url.toString();
  const pool = new pg.Pool({ connectionString });
  t.after(() => pool.end());
  return { connectionString, pool };
}

/** The live catalog, keyed and normalized as the spec says. */
async function liveCatalog(pool) {
  const out = new Map();
  for (const r of (
    await pool.query(`SELECT table_name,column_name,udt_name,is_nullable,column_default
      FROM information_schema.columns WHERE table_schema=current_schema()`)
  ).rows)
    out.set(
      `column ${r.table_name}.${r.column_name}`,
      `${r.udt_name} ${r.is_nullable === "YES"} ${normalizeDefault(r.column_default)}`,
    );
  for (const r of (
    await pool.query(`SELECT c.relname,array_agg(a.attname::text ORDER BY k.ordinality) AS key
      FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
      CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(num,ordinality)
      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.num
      WHERE p.contype='p' AND n.nspname=current_schema() GROUP BY c.relname`)
  ).rows)
    out.set(`primary key ${r.relname}`, r.key.join(","));
  for (const r of (
    await pool.query(`SELECT c.relname,t.relname AS tbl,pg_get_indexdef(c.oid) AS def,i.indisvalid
      FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_class t ON t.oid=i.indrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname=current_schema() AND NOT i.indisprimary`)
  ).rows)
    out.set(
      `index ${r.relname}`,
      `${r.tbl} ${normalizeIndex(r.def)}${r.indisvalid ? "" : " (invalid)"}`,
    );
  return out;
}

function specCatalog() {
  const out = new Map();
  for (const t of catalog.tables) {
    out.set(`primary key ${t.name}`, t.primary_key.join(","));
    for (const c of t.columns)
      out.set(
        `column ${t.name}.${c.name}`,
        `${c.type} ${c.nullable} ${normalizeDefault(c.default)}`,
      );
  }
  for (const i of catalog.indexes)
    out.set(`index ${i.name}`, `${i.table} ${normalizeIndex(i.definition)}`);
  return out;
}

integration(
  "initialize creates exactly the spec's catalog, and again is a no-op",
  async (t) => {
    const { connectionString, pool } = await freshSchema(t);
    await PostgresStorage.initialize({ connectionString });
    await PostgresStorage.initialize({ connectionString });
    assert.deepEqual(
      Object.fromEntries(await liveCatalog(pool)),
      Object.fromEntries(specCatalog()),
    );
  },
);

integration(
  "a database from an older SDK connects as is, and initialize completes it",
  async (t) => {
    const { connectionString, pool } = await freshSchema(t);
    await PostgresStorage.initialize({ connectionString });
    // What older versions of this SDK created: no ledger, none of the newer indexes, and
    // an index since retired.
    await pool.query("DROP TABLE runnerq_commands");
    for (const name of [
      "idx_runnerq_query_created",
      "idx_runnerq_query_status",
      "idx_runnerq_processing",
      "idx_runnerq_root_children",
      "idx_runnerq_root_terminal",
      "idx_runnerq_results_by_owner",
    ])
      await pool.query(`DROP INDEX ${name}`);
    await pool.query(
      "CREATE INDEX idx_runnerq_root_status ON runnerq_activities(queue_name, status) WHERE parent_activity_id IS NULL",
    );
    const storage = await PostgresStorage.connect({
      connectionString,
      queue: "older",
    });
    await storage.close();
    // Missing hot-path indexes are not acceptable, even from an older SDK.
    await pool.query("DROP INDEX idx_runnerq_dequeue_order_v2");
    await assert.rejects(
      PostgresStorage.connect({ connectionString, queue: "broken" }),
      /idx_runnerq_dequeue_order_v2.*initialize/,
    );
    await PostgresStorage.initialize({ connectionString });
    assert.deepEqual(
      Object.fromEntries(await liveCatalog(pool)),
      Object.fromEntries(specCatalog()),
    );
  },
);

integration(
  "initialize replaces a superseded index and rebuilds an invalid one",
  async (t) => {
    const { connectionString, pool } = await freshSchema(t);
    await PostgresStorage.initialize({ connectionString });
    // As a database from before the _v2 dequeue indexes had it.
    await pool.query(
      "CREATE INDEX idx_runnerq_dequeue_order ON runnerq_activities(queue_name)",
    );
    // An interrupted CONCURRENTLY build leaves an invalid index behind.
    await pool.query(
      "UPDATE pg_index SET indisvalid=false WHERE indexrelid='idx_runnerq_query_created'::regclass",
    );
    await PostgresStorage.initialize({ connectionString });
    assert.deepEqual(
      Object.fromEntries(await liveCatalog(pool)),
      Object.fromEntries(specCatalog()),
    );
  },
);
