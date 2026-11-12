import { randomUUID } from "node:crypto";
import type { ActivityDefinition } from "./activity.js";
import { businessKey, stepKey, uuid } from "./codec.js";
import { ActivityFailedError, RunnerQError } from "./errors.js";
import {
  encode,
  decode,
  serializationFormat,
  type SerializationMode,
} from "./serialization.js";
import { executionOptions, type ActivityOption } from "./options.js";
import type { Storage, StoredResult, Submission } from "./storage.js";
import { executionScope, guard, track, type AttemptScope } from "./scope.js";
import {
  startSpan,
  withTraceparent,
  type Tracer,
  type TraceAttributeValue,
} from "./observability.js";
import {
  spanAttributes as attr,
  spanNames,
  spanOutcomes,
  traceparentKey,
} from "./observability-spec.js";

export interface ResultOptions {
  signal?: AbortSignal;
}
/** A handler-created activity handle whose result uses the durable wait policy. */
export interface ChildActivityHandle<O> {
  readonly id: string;
  readonly storage: Storage;
  result(): Promise<O>;
}
export function unwrap<T>(id: string, result: StoredResult): T {
  if (result.state === "Err") throw new ActivityFailedError(id, decode(result));
  return decode(result) as T;
}
export class ActivityHandle<O> {
  constructor(
    readonly id: string,
    readonly storage: Storage,
    private readonly output?: (value: unknown) => O,
  ) {}
  async result(options: ResultOptions = {}): Promise<O> {
    const scope = executionScope.getStore();
    if (scope) {
      if (scope.storage !== this.storage)
        throw new RunnerQError(
          "configuration",
          "In-handler futures must use the executing worker's storage instance",
        );
      if (options.signal)
        throw new RunnerQError(
          "configuration",
          "Use the activity execution signal for durable waits inside a handler",
        );
      return this.decode(await track(scope, () => scope.wait(this.id)));
    }
    return this.decode(await this.storage.waitResult(this.id, options.signal));
  }
  decode(stored: StoredResult): O {
    const value = unwrap<O>(this.id, stored);
    try {
      return this.output ? this.output(value) : value;
    } catch (error) {
      throw new RunnerQError(
        "serialization",
        "Stored activity result does not match its output contract",
        { cause: error },
      );
    }
  }
}
export async function submit<I, O>(
  storage: Storage,
  definition: ActivityDefinition<I, O>,
  input: I,
  options: readonly ActivityOption[],
  scope?: AttemptScope,
  child = false,
  tracer: Tracer | undefined = scope?.tracer,
): Promise<ActivityHandle<O>> {
  const config = executionOptions(options);
  if (!scope && (config.step || config.asRoot || config.replay))
    throw new RunnerQError(
      "configuration",
      "Step, asRoot and newOnReplay are handler-only options",
    );
  if (scope) guard(scope);
  if (child && !config.step && !config.idempotency && !config.replay)
    throw new RunnerQError(
      "configuration",
      "Child creation requires runner.step(), runner.idempotencyKey() or runner.newOnReplay()",
    );
  if (!child && config.step)
    throw new RunnerQError(
      "configuration",
      "Named children must use ctx.spawn()",
    );
  if (scope && config.step) {
    const name = `spawn:${config.step}`;
    if (scope.names.has(name))
      throw new RunnerQError(
        "configuration",
        `Child step ${config.step} was used more than once in this invocation`,
      );
    scope.names.add(name);
  }
  const linked = !!scope && child && !config.asRoot;
  const id = randomUUID();
  const depth = linked ? scope.claim.depth + 1 : 0;
  if (scope && depth > scope.maxDepth)
    throw new RunnerQError("configuration", "Maximum activity depth exceeded");
  let payload;
  try {
    payload = encode(
      definition.input ? definition.input(input) : input,
      serializationFormat(definition.serialization),
    );
  } catch (error) {
    throw new RunnerQError("serialization", "Invalid activity input", {
      cause: error,
    });
  }
  const rootId = linked ? scope.claim.rootId : id;
  const key = config.step
    ? stepKey(rootId, scope!.claim.id, config.step)
    : config.idempotency
      ? businessKey(config.idempotency.key, definition.name)
      : undefined;
  const parentId = linked ? scope.claim.id : null;
  const span = startSpan(tracer, spanNames.enqueue, scope?.span, () => {
    const a: Record<string, TraceAttributeValue> = {
      [attr.queue]: storage.queue,
      [attr.activityType]: definition.name,
      [attr.activityId]: id,
      [attr.rootId]: rootId,
    };
    if (parentId) a[attr.parentId] = parentId;
    return a;
  });
  const activity: Submission = {
    id,
    type: definition.name,
    payload: payload.data,
    serialization: payload.serialization,
    options: {
      ...config,
      metadata: withTraceparent(
        config.metadata,
        span?.traceparent,
        scope?.claim.metadata[traceparentKey],
      ),
    },
    parentId,
    rootId,
    depth,
    key,
    fence: scope?.fence,
  };
  let existing: string;
  try {
    existing = scope
      ? await scope.recover(() => storage.submit(activity))
      : await storage.submit(activity);
  } catch (error) {
    span?.end(spanOutcomes.error, error);
    throw error;
  }
  if (existing === id) span?.end(spanOutcomes.enqueued);
  else
    span?.end(spanOutcomes.existing, undefined, {
      [attr.activityId]: existing,
    });
  if (scope && existing === id)
    scope.announce({
      type: config.delayMs > 0 ? "activity.scheduled" : "activity.created",
      activityId: id,
      activityType: definition.name,
      rootId,
      at: new Date(),
    });
  return new ActivityHandle(existing, storage, definition.output);
}
export class RunnerQClient {
  readonly storage: Storage;
  /** Traces this client's submissions and signals (see `Tracer`). */
  readonly tracer?: Tracer;
  constructor({ storage, tracer }: { storage: Storage; tracer?: Tracer }) {
    this.storage = storage;
    this.tracer = tracer;
  }
  execute<I, O>(
    definition: ActivityDefinition<I, O>,
    payload: I,
    ...options: ActivityOption[]
  ): Promise<ActivityHandle<O>> {
    const scope = executionScope.getStore();
    if (scope) {
      if (scope.storage !== this.storage)
        return Promise.reject(
          new RunnerQError(
            "configuration",
            "Handler execution cannot bypass its storage fence",
          ),
        );
      return track(scope, () =>
        submit(
          this.storage,
          definition,
          payload,
          options,
          scope,
          false,
          this.tracer ?? scope.tracer,
        ),
      );
    }
    return submit(
      this.storage,
      definition,
      payload,
      options,
      undefined,
      false,
      this.tracer,
    );
  }
  handle<O = unknown>(id: string): ActivityHandle<O>;
  handle<I, O>(
    definition: ActivityDefinition<I, O>,
    id: string,
  ): ActivityHandle<O>;
  handle<I, O>(
    definition: string | ActivityDefinition<I, O>,
    id?: string,
  ): ActivityHandle<O> {
    return typeof definition === "string"
      ? new ActivityHandle(uuid(definition), this.storage)
      : new ActivityHandle(uuid(id!), this.storage, definition.output);
  }
  async signal(
    id: string,
    name: string,
    payload: unknown = null,
    options: { serialization?: SerializationMode } = {},
  ): Promise<void> {
    const target = uuid(id);
    const value = encode(
      payload,
      serializationFormat(options.serialization ?? "native"),
    );
    const span = startSpan(
      this.tracer,
      spanNames.signalSend,
      undefined,
      () => ({
        [attr.activityId]: target,
        [attr.signalName]: name,
      }),
    );
    try {
      await (span
        ? span.run(() => this.storage.signal(target, name, value))
        : this.storage.signal(target, name, value));
    } catch (error) {
      span?.end(spanOutcomes.error, error);
      throw error;
    }
    span?.end(spanOutcomes.sent);
  }
  async signalByKey<I, O>(
    definition: ActivityDefinition<I, O>,
    key: string,
    name: string,
    payload: unknown = null,
  ): Promise<void> {
    await this.signal(
      await this.storage.lookupKey(businessKey(key, definition.name)),
      name,
      payload,
      { serialization: definition.serialization },
    );
  }
}
