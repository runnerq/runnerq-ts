# Durable execution

Checkpointed steps, durable sleeps and signals, child activities and fan-out.

## Durable steps, waits and children

```ts
const receipt = await ctx.run("charge", async ({ signal }) => {
  return payments.charge({
    orderId,
    signal,
    idempotencyKey: `${ctx.activityId}:charge`,
  });
});

await ctx.sleep("cooling-off", 86_400_000);

const decision = await ctx.waitForSignal<{ approved: boolean }>("approval", {
  timeoutMs: 172_800_000,
});

const child = await ctx.spawn(ShipOrder, { orderId }, runner.step("ship"));
return ctx.wait(child);
```

`ctx.spawn()` returns a `ChildActivityHandle<Output>`. Its `result()` takes no arguments and uses durable waiting, just like `ctx.wait(child)`. Passing `{ signal }` is a TypeScript error. Handles returned by `client.execute()` or `client.handle()` retain `result({ signal })` for external callers; using a signal on those handles inside a handler is still rejected at runtime.

Successful steps and permanent step failures are checkpointed. Retryable step failures are not checkpointed. During a transient checkpoint write failure, the SDK retains the callback's returned JSON value and retries persistence without rerunning the callback. Optional `{ parse }` on `ctx.run` validates recorded values on replay; signal waits accept the same parser option.

Execution remains **at least once**. A crash after an external effect but before checkpoint commit can repeat that effect. Use the external system's idempotency support. Code outside checkpoints, including `finally` blocks, runs again on replay. Only the handler replays; arbitrary JavaScript stacks are not persisted.

Long waits park in PostgreSQL and release capacity and leases. Short waits can complete in-process. Sleep and signal deadlines persist from first arrival. Signal timeout `0` or omission waits indefinitely; repeated signals with the same name overwrite the payload. Signals are buffered even before a handler begins. They do not bypass an initial delay or retry backoff.

```ts
await client.signal(handle.id, "approval", { approved: true });
await client.signalByKey(Checkout, "order-123", "approval", { approved: true });

// In another process, connected to the same queue:
const restored = client.handle(Checkout, handle.id);
const result = await restored.result({ signal: AbortSignal.timeout(30_000) });
```

Key lookup and signal delivery are separate operations: key reuse can repoint the key between them. Delivery targets the resolved activity ID. Rehydrated handles awaited inside a handler must use that worker's storage instance; AsyncLocalStorage ensures they register dependencies and park correctly.

Fan-out uses a controlled durable join:

```ts
const children = [];
for (const item of items) {
  children.push(
    await ctx.spawn(ProcessItem, item, runner.step(`item:${item.id}`)),
  );
}
const results = await ctx.waitAll(children);
```

Await all SDK operations. Use `ctx.waitAll` for child joins; concurrent durable waits via `Promise.all`/`race` are unsupported and rejected. Do not overlap a durable wait with a checkpointed effect or nest durable orchestration inside `ctx.run`. Ordinary concurrent I/O within a single step is supported.

Suspension uses an internal control-flow exception. Broad catches should rethrow it:

```ts
import { isControlFlow } from "runnerq";
try {
  await ctx.wait(child);
} catch (error) {
  if (isControlFlow(error)) throw error;
  // Handle a business error.
}
```

The runtime remembers suspension even if caught and will not incorrectly complete the activity. It cannot stop arbitrary application side effects after a catch or in a finally block.
