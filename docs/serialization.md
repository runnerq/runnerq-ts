# Serialization and compatibility

How inputs, results and signals are serialized, and how the schema is shared with the Go SDK.

Activities use native SuperJSON serialization by default. Inputs, outputs, and signals can contain `Date`, `bigint`, `Map`, `Set`, `Buffer`, `RegExp`, `URL`, `Error`, and `undefined`, including shared references and cycles. Native void results remain `undefined`, distinct from `null`. Functions, symbols, unregistered custom classes, and unsafe integer numbers are rejected; use `bigint` for large integers. Event sequence IDs remain strings.

```ts
const Checkout = activity<{ orderedAt: Date }, { total: bigint }>("Checkout");
const handle = await client.execute(Checkout, { orderedAt: new Date() });
const { total } = await handle.result(); // bigint, including after a restart

// Portable contracts for another SDK: use plain JSON values explicitly.
const ShipOrder = activity<{ orderId: string }, { shippedAt: string }>(
  "ShipOrder",
  { serialization: "portable" },
);
await client.signal(
  shippingId,
  "approval",
  { approved: true },
  {
    serialization: "portable",
  },
);
```

Portable values must be plain JSON: convert dates to ISO strings and big integers to decimal strings yourself. Portable mode rejects nested undefined, cycles, special objects, non-finite numbers and unsafe integers; top-level void becomes JSON null. `client.signal()` defaults to native serialization; `signalByKey()` uses the supplied activity definition's mode. A signal's own recorded format determines how it is decoded.

`ctx.run()` always uses native serialization for successful checkpoints, even inside portable activities. It decodes the captured value before returning it on the first execution, just as on replay. Step parsers receive decoded values. Internal deadlines and failure records remain portable protocol data. Go workers must not resume TS-owned native checkpoints; sharing activity boundaries requires both SDKs to implement the same schema and portable format contract.

Each input and result row records `serialization` separately from user data: `superjson-v1` or `json-v1`. Output serialization follows the persisted input format, so changing a definition's default does not change an already submitted activity. Clients decode using the row's format. Unknown formats fail explicitly. There is no guessing from payload fields or automatic legacy fallback.

For custom types, register a versioned recipe in every client and worker process before the first native serialization operation:

```ts
import { registerSerialization } from "runnerq";

class Money {
  constructor(readonly cents: bigint) {}
}
registerSerialization<Money, string>({
  name: "myapp.Money.v1",
  isApplicable: (value): value is Money => value instanceof Money,
  serialize: (value) => value.cents.toString(),
  deserialize: (value) => new Money(BigInt(value)),
});
```

Recipe output must be portable JSON. Names are unique and the registry locks on first native use. Keep existing recipe names and decoders available for recorded work; changing their meaning breaks replay. RunnerQ uses an isolated SuperJSON instance, so unrelated application registrations do not change the SDK's format. See [serialization storage and upgrades](architecture.md#serialization) before upgrading an existing database.

The schema is [runnerq-spec](https://github.com/runnerq/runnerq-spec)'s, shared with the Go SDK: eight tables (activities, inputs, results, idempotency, dependencies, events, worker pools and the command ledger) and their indexes. `PostgresStorage.initialize` creates it or brings an existing database up to date, building large indexes concurrently; `connect` only checks it, accepting a database an older version of this SDK initialized until `initialize` completes it. Checkpoint identity is UUIDv5 of `(activityId, "kind:name")`; business keys use the final `rq:key:v2:` UTF-8 length-prefixed encoding; named children use `rq:step:<root>:<parent>:<name>`. There are no legacy key readers, inline-payload compatibility paths or unfenced fallback backends. `runnerq/storage` exports the required storage contract for custom backends.

The database column `max_retries` retains its shared protocol name but is exposed as `maxAttempts`. Read [architecture and guarantees](architecture.md) for transaction and recovery details.
