// The work the upgrade test parks with the previous SDK and finishes with the
// current one. It uses only API that both versions have; if a release changes
// that API on purpose, branch on `phase` here.

export const queue = "upgrade";
export const sleepMs = 5_000;
export const delayMs = 2_000;

/** Types the previous SDK's worker serves. Child and Delayed stay pending until the current SDK starts. */
export const previousTypes = ["Sleeper", "Waiter", "Parent"];

/**
 * @param sdk the `runnerq` module of the SDK under test
 * @param record inserts one ledger row: (activity id, step, phase)
 * @param phase "previous" or "current"
 */
export function workload(sdk, record, phase) {
  const { activity, runner } = sdk;
  const defs = {
    Sleeper: activity("UpgradeSleeper"),
    Waiter: activity("UpgradeWaiter"),
    Parent: activity("UpgradeParent"),
    Child: activity("UpgradeChild"),
    Delayed: activity("UpgradeDelayed"),
  };
  // A step's value is the phase that ran it, so a replayed step reveals which
  // SDK wrote its checkpoint.
  const step = (ctx, name) =>
    ctx.run(name, async () => {
      await record(ctx.activityId, name, phase);
      return phase;
    });
  const handlers = {
    async Sleeper(ctx) {
      const before = await step(ctx, "before");
      await ctx.sleep("nap", sleepMs);
      return { before, after: await step(ctx, "after") };
    },
    async Waiter(ctx) {
      const before = await step(ctx, "before");
      const signal = await ctx.waitForSignal("go", { timeoutMs: 600_000 });
      return { before, after: await step(ctx, "after"), signal };
    },
    async Parent(ctx) {
      const before = await step(ctx, "before");
      const child = await ctx.spawn(defs.Child, 21, runner.step("child"));
      const [result] = await ctx.waitAll([child]);
      return { before, after: await step(ctx, "after"), child: result };
    },
    async Child(ctx, n) {
      return { work: await step(ctx, "work"), value: n * 2 };
    },
    async Delayed(ctx, n) {
      return { work: await step(ctx, "work"), value: n * 2 };
    },
  };
  return { defs, handlers, runner };
}
