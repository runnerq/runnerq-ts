import { spawnSync } from "node:child_process";
import { readdirSync } from "node:fs";
if (!process.env.RUNNERQ_TEST_DSN) {
  console.error(
    "RUNNERQ_TEST_DSN is required; use a dedicated PostgreSQL test database.",
  );
  process.exitCode = 1;
} else {
  const files = readdirSync(new URL("../test/", import.meta.url))
    .filter((f) => f.endsWith(".test.mjs"))
    .map((f) => "test/" + f);
  const result = spawnSync(process.execPath, ["--test", ...files], {
    stdio: "inherit",
  });
  process.exitCode = result.status ?? 1;
}
