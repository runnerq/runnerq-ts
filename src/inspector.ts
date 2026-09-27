import { EventEmitter } from "node:events";
import { pause } from "./async.js";
import { integer } from "./options.js";
import { RunnerQError } from "./errors.js";
import type {
  Storage,
  ListOptions,
  ActivityEvent,
  ActivitySnapshot,
  QueueStats,
} from "./storage.js";
interface Subscriber {
  queue: ActivityEvent[];
  capacity: number;
  wake?: () => void;
  overflow?: Error;
}
export class Inspector extends EventEmitter<{
  event: [event: ActivityEvent];
  inspectorError: [error: unknown];
  newListener: [name: string | symbol, listener: (...args: unknown[]) => void];
}> {
  readonly storage: Storage;
  private readonly subscriptions = new Set<Subscriber>();
  private readonly controller = new AbortController();
  private tail?: Promise<void>;
  private cache?: { expires: number; value: Promise<QueueStats> };
  constructor({ storage }: { storage: Storage }) {
    super();
    this.storage = storage;
    this.on("newListener", (name) => {
      if (name === "event") queueMicrotask(() => this.startTail());
    });
  }
  list(options: ListOptions = {}) {
    return this.storage.list(options);
  }
  get(id: string) {
    return this.storage.getActivity(id);
  }
  input(id: string) {
    return this.storage.getInput(id);
  }
  result(id: string) {
    return this.storage.getResult(id);
  }
  steps(id: string) {
    return this.storage.steps(id);
  }
  history(id: string, limit = 100) {
    return this.storage.events(id, limit);
  }
  children(id: string, options: Pick<ListOptions, "limit" | "offset"> = {}) {
    return this.storage.list({ ...options, parentId: id });
  }
  async subtree(id: string): Promise<ActivitySnapshot[]> {
    const activity = await this.storage.getActivity(id);
    if (!activity) throw new RunnerQError("not_found", "Activity not found");
    const all: ActivitySnapshot[] = [];
    for (;;) {
      const page = await this.storage.list({
        rootId: activity.rootId,
        limit: 1000,
        offset: all.length,
      });
      all.push(...page);
      if (page.length < 1000) return all;
    }
  }
  stats(): Promise<QueueStats> {
    if (!this.cache || this.cache.expires <= Date.now()) {
      const value = this.storage.stats().catch((error) => {
        this.cache = undefined;
        throw error;
      });
      this.cache = { expires: Date.now() + 1000, value };
    }
    return this.cache.value;
  }
  async *events(
    options: { signal?: AbortSignal; bufferSize?: number } = {},
  ): AsyncGenerator<ActivityEvent> {
    const sub: Subscriber = {
      queue: [],
      capacity: integer(options.bufferSize ?? 100, "bufferSize", 1, 10000),
    };
    const signal = AbortSignal.any(
      options.signal
        ? [options.signal, this.controller.signal]
        : [this.controller.signal],
    );
    const wake = () => sub.wake?.();
    signal.addEventListener("abort", wake);
    this.subscriptions.add(sub);
    this.startTail();
    try {
      while (!signal.aborted) {
        if (sub.overflow) throw sub.overflow;
        const event = sub.queue.shift();
        if (event) {
          yield event;
          continue;
        }
        await new Promise<void>((resolve) => {
          sub.wake = resolve;
          if (signal.aborted) resolve();
        });
        sub.wake = undefined;
      }
    } finally {
      signal.removeEventListener("abort", wake);
      this.subscriptions.delete(sub);
    }
  }
  private startTail(): void {
    if (!this.tail && !this.controller.signal.aborted)
      this.tail = this.tailEvents().finally(() => {
        this.tail = undefined;
        if (this.subscriptions.size || this.listenerCount("event"))
          this.startTail();
      });
  }
  private safeEmit(name: "event" | "inspectorError", value: unknown): void {
    for (const listener of this.rawListeners(name)) {
      try {
        const result = (listener as (value: unknown) => unknown).call(
          this,
          value,
        );
        if (
          result &&
          typeof (result as PromiseLike<unknown>).then === "function"
        )
          void Promise.resolve(result).catch((error) => {
            if (name !== "inspectorError")
              this.safeEmit("inspectorError", error);
          });
      } catch (error) {
        if (name !== "inspectorError") this.safeEmit("inspectorError", error);
      }
    }
  }
  private async tailEvents(): Promise<void> {
    let cursor: string | undefined;
    while (
      !this.controller.signal.aborted &&
      (this.subscriptions.size || this.listenerCount("event"))
    ) {
      try {
        cursor ??= await this.storage.latestEventId();
        const events = await this.storage.readEvents(cursor);
        for (const event of events) {
          cursor = event.id;
          this.safeEmit("event", event);
          for (const sub of this.subscriptions) {
            if (sub.queue.length >= sub.capacity)
              sub.overflow = new RunnerQError(
                "internal",
                "Live event buffer overflow; refresh inspector state",
              );
            else if (!sub.overflow) sub.queue.push(event);
            sub.wake?.();
          }
        }
        await pause(events.length === 500 ? 1 : 1000, this.controller.signal);
      } catch (error) {
        if (!this.controller.signal.aborted) {
          this.safeEmit("inspectorError", error);
          await pause(1000, this.controller.signal).catch(() => {});
        }
      }
    }
  }
  async close(): Promise<void> {
    this.controller.abort();
    for (const sub of this.subscriptions) sub.wake?.();
    await this.tail;
    this.removeAllListeners();
  }
}
