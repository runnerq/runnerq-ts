// Cron vectors from runnerq-spec (the spec/ submodule), shared with the Go SDK.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { CronExpression, checkTimeZone } from "../dist/cron.js";
import { scheduleFireTimes, scheduleKey } from "../dist/schedule.js";

const cases = (path) =>
  JSON.parse(readFileSync(new URL(`../spec/${path}`, import.meta.url), "utf8"))
    .cases;
const iso = (d) => d.toISOString().replace(".000Z", "Z");

test("cron next fire times match the spec vectors", () => {
  for (const c of cases("vectors/cron_next_fire.json")) {
    let got;
    try {
      const e = CronExpression.parse(c.input.cron);
      checkTimeZone(c.input.timezone);
      got = [];
      let from = new Date(c.input.from);
      for (let i = 0; i < c.input.count; i++) {
        const next = e.next(from, c.input.timezone);
        if (!next) break;
        got.push(iso(next));
        from = next;
      }
    } catch {
      got = "invalid";
    }
    assert.deepEqual(got, c.output, c.name);
  }
});

test("firing a due schedule matches the spec vectors", () => {
  for (const c of cases("vectors/schedule_fire.json")) {
    const { fire, next } = scheduleFireTimes(
      {
        cron: c.input.cron,
        timezone: c.input.timezone,
        catchUp: c.input.catch_up,
        catchUpLimit: c.input.limit,
        nextFireAt: c.input.next_fire_at,
      },
      new Date(c.input.now),
    );
    assert.deepEqual(fire.map(iso), c.output.fire, c.name);
    assert.equal(
      next.getTime(),
      new Date(c.output.next_fire_at).getTime(),
      c.name,
    );
  }
});

test("schedule keys are RFC 3339 UTC to the second", () => {
  assert.equal(
    scheduleKey("nightly", new Date("2026-03-08T07:00:00.750Z")),
    "schedule:nightly:2026-03-08T07:00:00Z",
  );
});
