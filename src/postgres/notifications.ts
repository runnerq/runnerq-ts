import { EventEmitter } from "node:events";
import { Client, type Pool, type ClientConfig } from "pg";
import { pause } from "../async.js";

/** Notifications are bounded, lossy hints. Every consumer must recheck stored data. */
export class Notifications {
  private readonly bus = new EventEmitter();
  private readonly lifetime = new AbortController();
  private listener?: Client;
  private listening?: Promise<void>;
  private flushTimer?: NodeJS.Timeout;
  private flushing?: Promise<void>;
  private work = false;
  private events = false;
  private results = new Set<string>();
  constructor(
    private readonly config: ClientConfig,
    private readonly pool: Pool,
    private readonly queue: string,
  ) {
    // Waiters explicitly unsubscribe; capacity is bounded by callers, not by a shared listener limit.
    this.bus.setMaxListeners(0);
  }
  private channel(kind: string): string {
    return `rq_${kind}_${this.queue}`;
  }
  private start(): void {
    this.listening ??= this.listen();
  }
  private async listen(): Promise<void> {
    let retry = 100;
    while (!this.lifetime.signal.aborted) {
      const client = new Client(this.config);
      this.listener = client;
      let disconnected!: () => void;
      const ended = new Promise<void>((resolve) => {
        disconnected = resolve;
      });
      client.on("error", disconnected);
      client.on("end", disconnected);
      client.on("notification", (notification) => {
        if (notification.channel === this.channel("w")) this.bus.emit("work");
        if (notification.channel === this.channel("e")) this.bus.emit("events");
        if (notification.channel === this.channel("r")) {
          for (const id of (notification.payload ?? "").split(","))
            this.bus.emit(`result:${id}`);
        }
      });
      try {
        await client.connect();
        for (const kind of ["w", "r", "e"])
          await client.query(`LISTEN "${this.channel(kind)}"`);
        retry = 100;
        this.bus.emit("reconnect");
        if (!this.lifetime.signal.aborted) await ended;
      } catch {
        /* fallback probes preserve progress while disconnected */
      } finally {
        await client.end().catch(() => {});
      }
      if (!this.lifetime.signal.aborted)
        await pause(retry, this.lifetime.signal).catch(() => {});
      retry = Math.min(retry * 2, 5_000);
    }
  }
  hint(kind: "work" | "events" | "result", id?: string): void {
    if (this.lifetime.signal.aborted) return;
    if (kind === "work") this.work = true;
    if (kind === "events") this.events = true;
    if (kind === "result" && id && this.results.size < 4096)
      this.results.add(id);
    this.bus.emit(kind === "result" ? `result:${id}` : kind);
    if (!this.flushTimer && !this.flushing)
      this.flushTimer = setTimeout(() => {
        this.flushTimer = undefined;
        this.flushing = this.flush().finally(() => {
          this.flushing = undefined;
          if (this.work || this.events || this.results.size)
            this.hint("events");
        });
      }, 50).unref();
  }
  private async flush(): Promise<void> {
    const jobs: [string, string][] = [];
    if (this.work) jobs.push([this.channel("w"), ""]);
    if (this.events) jobs.push([this.channel("e"), ""]);
    const ids = [...this.results];
    for (let i = 0; i < ids.length; i += 200)
      jobs.push([this.channel("r"), ids.slice(i, i + 200).join(",")]);
    this.work = this.events = false;
    this.results.clear();
    // These transactions never contain activity writes.
    for (const job of jobs)
      await this.pool.query("SELECT pg_notify($1,$2)", job).catch(() => {});
  }
  subscribe(
    name: string,
    signal?: AbortSignal,
  ): { wait: (ms: number) => Promise<void>; close: () => void } {
    this.start();
    let dirty = false;
    let wake: (() => void) | undefined;
    const listener = () => {
      dirty = true;
      wake?.();
    };
    this.bus.on(name, listener);
    this.bus.on("reconnect", listener);
    const stop = AbortSignal.any(
      signal ? [signal, this.lifetime.signal] : [this.lifetime.signal],
    );
    return {
      wait: async (ms) => {
        stop.throwIfAborted();
        if (dirty) {
          dirty = false;
          return;
        }
        await new Promise<void>((resolve, reject) => {
          const done = () => {
            clearTimeout(timer);
            stop.removeEventListener("abort", abort);
            wake = undefined;
            dirty = false;
            resolve();
          };
          const abort = () => {
            clearTimeout(timer);
            stop.removeEventListener("abort", abort);
            wake = undefined;
            reject(stop.reason);
          };
          const timer = setTimeout(done, ms);
          wake = done;
          stop.addEventListener("abort", abort, { once: true });
          if (stop.aborted) abort();
        });
      },
      close: () => {
        this.bus.off(name, listener);
        this.bus.off("reconnect", listener);
        wake?.();
      },
    };
  }
  async close(): Promise<void> {
    this.lifetime.abort();
    clearTimeout(this.flushTimer);
    await this.listener?.end().catch(() => {});
    await this.listening;
    await this.flushing;
    this.bus.removeAllListeners();
  }
}
