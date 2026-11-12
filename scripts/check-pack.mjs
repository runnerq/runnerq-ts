// Checks what `npm pack` would put in the tarball: every file package.json
// exports must be there, and nothing from the repository's tooling may be.
// Build first (npm run build); this script does not run lifecycle scripts.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const pkg = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const [report] = JSON.parse(
  execFileSync(npm, ["pack", "--dry-run", "--json", "--ignore-scripts"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "inherit"],
  }),
);
const files = new Set(report.files.map((f) => f.path));

const required = new Set(["package.json", "README.md", "LICENSE"]);
for (const entry of Object.values(pkg.exports)) {
  for (const target of Object.values(entry)) {
    required.add(target.replace(/^\.\//, ""));
  }
}

const forbidden = [
  /^test\//,
  /^spec\//,
  /^scripts\//,
  /^\.github\//,
  /^node_modules\//,
  /^coverage\//,
  /\.tgz$/,
  /(^|\/)\.env/,
  /\.test\.(m?js|ts)$/,
];

const problems = [];
for (const path of required) {
  if (!files.has(path)) problems.push(`missing: ${path}`);
}
for (const path of files) {
  if (forbidden.some((re) => re.test(path)))
    problems.push(`unexpected: ${path}`);
}

if (problems.length > 0) {
  console.error(`npm pack contents check failed for ${report.id}:`);
  for (const p of problems) console.error(`  ${p}`);
  console.error("Run `npm run build` first if dist/ files are missing.");
  process.exitCode = 1;
} else {
  console.log(
    `${report.id}: ${files.size} files, ${report.size} bytes packed, ${report.unpackedSize} unpacked`,
  );
}
