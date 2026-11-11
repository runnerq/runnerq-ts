// Upgrade test: the previous SDK version creates the schema and parks work
// (a sleep, a signal wait, a parent waiting on a pending child, a delayed
// start), then stops; the current SDK migrates the same database and must
// finish all of it, running every remaining step exactly once.
//
// The previous version is built from a git ref in a temporary directory:
//   RUNNERQ_UPGRADE_FROM  git ref to upgrade from. Default: the latest v* tag
//                         before HEAD, else the merge base with
//                         RUNNERQ_UPGRADE_BASE (default origin/main).
//   RUNNERQ_TEST_DSN      Postgres to use; the test works in its own schema
//                         and drops it afterwards (RUNNERQ_UPGRADE_KEEP=1 keeps it).
// Build this checkout first (npm run test:upgrade does).
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const dsn = process.env.RUNNERQ_TEST_DSN;
if (!dsn) {
  console.error(
    "RUNNERQ_TEST_DSN is required; use a dedicated PostgreSQL test database.",
  );
  process.exit(1);
}

const git = (...args) =>
  execFileSync("git", args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
const tryGit = (...args) => {
  try {
    return git(...args);
  } catch {
    return null;
  }
};

function previousRef() {
  if (process.env.RUNNERQ_UPGRADE_FROM) return process.env.RUNNERQ_UPGRADE_FROM;
  const tag = tryGit(
    "describe",
    "--tags",
    "--abbrev=0",
    "--match",
    "v*",
    "HEAD^",
  );
  if (tag) return tag;
  const base = process.env.RUNNERQ_UPGRADE_BASE ?? "origin/main";
  const mergeBase = tryGit("merge-base", "HEAD", base);
  if (!mergeBase)
    throw new Error(
      `no v* tag and no merge base with ${base}; set RUNNERQ_UPGRADE_FROM`,
    );
  // On the base branch itself, upgrade from the commit before.
  return mergeBase === git("rev-parse", "HEAD") ? "HEAD^" : mergeBase;
}

const ref = previousRef();
const sha = git("rev-parse", "--verify", `${ref}^{commit}`);
console.log(`upgrade test: ${ref} (${sha.slice(0, 12)}) -> working tree`);

// Build the previous version once per commit; reruns reuse it.
const previous = join(tmpdir(), `runnerq-upgrade-${sha}`);
if (!existsSync(join(previous, "dist", "index.js"))) {
  rmSync(previous, { recursive: true, force: true });
  mkdirSync(previous, { recursive: true });
  execFileSync("sh", ["-c", `git archive "${sha}" | tar -x -C "${previous}"`], {
    cwd: root,
    stdio: "inherit",
  });
  // npm ci runs prepare, which builds dist/.
  execFileSync("npm", ["ci", "--no-audit", "--no-fund"], {
    cwd: previous,
    stdio: "inherit",
  });
}

const schema = `runnerq_upgrade_${process.pid}_${Date.now()}`;
const url = new URL(dsn);
url.searchParams.set("options", `-c search_path=${schema}`);
const admin = new pg.Client({ connectionString: dsn });
await admin.connect();
await admin.query(`CREATE SCHEMA ${schema}`);
await admin.query(
  `CREATE TABLE ${schema}.upgrade_ledger (activity_id text NOT NULL, step text NOT NULL, phase text NOT NULL)`,
);

const state = join(tmpdir(), `${schema}.json`);
let failed = false;
try {
  for (const [phase, dir] of [
    ["previous", previous],
    ["current", root],
  ]) {
    const result = spawnSync(
      process.execPath,
      [join(root, "test", "upgrade", "phase.mjs"), phase, dir, state],
      {
        stdio: "inherit",
        env: { ...process.env, RUNNERQ_UPGRADE_DSN: url.toString() },
        timeout: 120_000,
      },
    );
    if (result.status !== 0) {
      console.error(`upgrade test: ${phase} phase failed`);
      failed = true;
      break;
    }
  }
} finally {
  rmSync(state, { force: true });
  if (process.env.RUNNERQ_UPGRADE_KEEP) console.log(`kept schema ${schema}`);
  else await admin.query(`DROP SCHEMA ${schema} CASCADE`);
  await admin.end();
}
if (failed) process.exitCode = 1;
else console.log("upgrade test: ok");
