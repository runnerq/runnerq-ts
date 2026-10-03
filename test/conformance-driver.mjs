// runnerq-spec's conformance driver (spec/conformance/README.md): storage operations on
// PostgresStorage, one JSON request per stdin line, one reply per stdout line.
import { createInterface } from "node:readline";
import { PostgresStorage } from "../dist/postgres/index.js";
import { businessKey } from "../dist/codec.js";

const priorities = ["low", "normal", "high", "critical"];
const policies = {
  return_existing: "returnExisting",
  no_reuse: "noReuse",
  allow_reuse: "allowReuse",
  allow_reuse_on_failure: "allowReuseOnFailure",
};
// The TypeScript SDK's error codes as storage error kinds.
const kinds = { duplicate: "duplicate_activity" };
const json = (data) => ({ serialization: "json-v1", data: data ?? null });
const fence = (c) => ({ ownerId: c.id, token: c.token });

let storage;
const ops = {
  async open({ dsn, queue }) {
    await storage?.close();
    await PostgresStorage.initialize({ connectionString: dsn });
    storage = await PostgresStorage.connect({ connectionString: dsn, queue });
    return {};
  },
  async submit(a) {
    const options = {
      priority: priorities[(a.priority ?? 2) - 1],
      maxAttempts:
        (a.max_attempts ?? 3) === 0 ? "unlimited" : (a.max_attempts ?? 3),
      timeoutMs: (a.timeout_s ?? 30) * 1000,
      maxRetryDelayMs: 0,
      delayMs: (a.delay_s ?? 0) * 1000,
      metadata: a.metadata ?? {},
      ...(a.key && {
        idempotency: {
          key: a.key.key,
          onDuplicate: policies[a.key.on_duplicate],
        },
      }),
    };
    const id = await storage.submit({
      serialization: "json-v1",
      id: a.id,
      type: a.type,
      payload: a.payload ?? null,
      options,
      parentId: a.parent ?? null,
      rootId: a.root ?? a.id,
      depth: a.depth ?? 0,
      ...(a.key && { key: businessKey(a.key.key, a.type) }),
      ...(a.fence && { fence: fence(a.fence) }),
    });
    return id === a.id ? {} : { existing: id };
  },
  async claim(a) {
    const claims = await storage.claim(
      a.limit ?? 1,
      a.types,
      a.lease_ms ?? 30_000,
    );
    return { claims: claims.map((c) => ({ id: c.id, token: c.token })) };
  },
  async renew(a) {
    return { renewed: await storage.renew(fence(a.claim), a.lease_ms) };
  },
  async complete(a) {
    await storage.complete(fence(a.claim), json(a.value));
    return {};
  },
  async fail(a) {
    return {
      outcome: await storage.fail(fence(a.claim), a.reason, a.retryable),
    };
  },
  async checkpoint(a) {
    await storage.checkpoint(
      fence(a.claim),
      a.result_id,
      { ...json(a.data), state: a.state === "error" ? "Err" : "Ok" },
      a.step,
    );
    return {};
  },
  async register_dependency(a) {
    await storage.registerDependency(fence(a.claim), a.producer);
    return {};
  },
  async park(a) {
    await storage.park(fence(a.claim), {
      kind: a.kind,
      step: a.step,
      wakeAt: a.wake_at,
      ...(a.result_id && { resultId: a.result_id }),
      ...(a.producer && { producerId: a.producer }),
    });
    return {};
  },
  async signal(a) {
    await storage.signal(a.target, a.name, json(a.payload));
    return {};
  },
  async lookup_key(a) {
    return { id: await storage.lookupKey(a.key) };
  },
  async reap(a) {
    return { count: await storage.reap(a.limit ?? 1) };
  },
  async cleanup(a) {
    return {
      count: await storage.cleanup({
        completedMs: (a.completed_s ?? 0) * 1000,
        failedMs: (a.failed_s ?? 0) * 1000,
        eventsMs: (a.events_s ?? 0) * 1000,
        batchSize: a.batch ?? 1,
      }),
    };
  },
  async get_result(a) {
    const r = await storage.getResult(a.id);
    return {
      result: r && {
        state: r.state === "Err" ? "error" : "ok",
        data: r.data,
        serialization: r.serialization,
      },
    };
  },
};

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
  const { op, args } = JSON.parse(line);
  let reply;
  try {
    if (!ops[op])
      throw Object.assign(new Error(`unknown op ${op}`), {
        code: "unsupported",
      });
    if (op !== "open" && !storage) throw new Error("open first");
    reply = { ok: await ops[op](args ?? {}) };
  } catch (error) {
    const code = error?.code ?? "internal";
    reply = {
      error: {
        kind: kinds[code] ?? code,
        message: String(error?.message ?? error),
      },
    };
  }
  process.stdout.write(JSON.stringify(reply) + "\n");
}
await storage?.close();
