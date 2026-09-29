import test from "node:test";
import assert from "node:assert/strict";
import { linkSignal, maxTimerMs } from "../dist/async.js";

const DAY_MS = 86_400_000;

test("linkSignal re-arms timeouts past setTimeout's range", (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
  const parent = new AbortController();
  const deadline = 30 * DAY_MS;
  const { signal, done } = linkSignal([parent.signal], deadline);
  t.after(done);
  // Overflowing timers fire after ~1 ms; a chunked one must not.
  t.mock.timers.tick(1);
  assert.equal(signal.aborted, false);
  t.mock.timers.tick(maxTimerMs - 1);
  assert.equal(signal.aborted, false);
  t.mock.timers.tick(deadline - maxTimerMs - 1);
  assert.equal(signal.aborted, false);
  t.mock.timers.tick(1);
  assert.equal(signal.aborted, true);
  assert.ok(signal.reason instanceof DOMException);
  assert.equal(signal.reason.name, "TimeoutError");
});

test("linkSignal forwards the parent's abort reason", (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
  const parent = new AbortController();
  const { signal } = linkSignal([parent.signal], 40 * DAY_MS);
  const reason = new Error("stopped");
  parent.abort(reason);
  assert.equal(signal.reason, reason);
  t.mock.timers.tick(40 * DAY_MS);
  assert.equal(signal.reason, reason);

  const aborted = linkSignal([AbortSignal.abort(reason)], 40 * DAY_MS);
  assert.equal(aborted.signal.reason, reason);
});

test("linkSignal's done cancels the timeout", (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "Date"], now: 0 });
  const parent = new AbortController();
  const { signal, done } = linkSignal([parent.signal], 1_000);
  done();
  t.mock.timers.tick(1_000);
  parent.abort(new Error("late"));
  assert.equal(signal.aborted, false);
});
