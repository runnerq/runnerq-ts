import { EventEmitter } from "node:events";
import { Client, type Pool, type ClientConfig } from "pg";
import { linkSignal, pause } from "../async.js";

/** Notifications are bounded, lossy hints. Every consumer must recheck stored data. */
export class Notifications {
  private readonly bus = new EventEmitter();
  private readonly lifetime = new AbortController();
  private listener?: Client;
  private listening?: Promise<void>;
  private flushTimer?: NodeJS.Timeout;
  private flushing?: Promise<void>;
  private work = false;
  private readonly results = new Set<string>();
  private readonly workChannel: string;
  private readonly resultChannel: string;
  constructor(
    private readonly config: ClientConfig,
    private readonly pool: Pool,
    queue: string,
  ) {
    this.workChannel = `rq_w_${queue}`;
    this.resultChannel = `rq_r_${queue}`;
    // Waiters unsubscribe themselves; callers bound how many there are.
    this.bus.setMaxListeners(0);
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
        if (notification.channel === this.workChannel) this.bus.emit("work");
        else if (notification.channel === this.resultChannel)
          for (const id of (notification.payload ?? "").split(","))
            this.bus.emit(`result:${id}`);
      });
      try {
        // pg can leave connect() pending when end() interrupts the handshake; racing
        // the disconnect lets close() join this loop.
        await Promise.race([
          client.connect(),
          ended.then(() => {
            throw new Error("Notification connection ended during startup");
          }),
        ]);
        for (const channel of [this.workChannel, this.resultChannel])
          await client.query(`LISTEN "${channel}"`);
        retry = 100;
        this.bus.emit("reconnect");
        if (!this.lifetime.signal.aborted) await ended;
      } catch {
        /* waiters' timeouts keep polling while disconnected */
      } finally {
        await client.end().catch(() => {});
      }
      if (!this.lifetime.signal.aborted)
        await pause(retry, this.lifetime.signal).catch(() => {});
      retry = Math.min(retry * 2, 5_000);
    }
  }
  hint(kind: "work" | "result", id?: string): void {
    if (this.lifetime.signal.aborted) return;
    if (kind === "work") this.work = true;
    if (kind === "result" && id && this.results.size < 4096)
      this.results.add(id);
    this.bus.emit(kind === "result" ? `result:${id}` : kind);
    this.scheduleFlush();
  }
  // One flush at a time; hints that arrive during a flush get the next one.
  private scheduleFlush(): void {
    if (!this.flushTimer && !this.flushing)
      this.flushTimer = setTimeout(() => {
        this.flushTimer = undefined;
        this.flushing = this.flush().finally(() => {
          this.flushing = undefined;
          if (this.work || this.results.size) this.scheduleFlush();
        });
      }, 50).unref();
  }
  private async flush(): Promise<void> {
    const channels: string[] = [],
      payloads: string[] = [];
    if (this.work) {
      channels.push(this.workChannel);
      payloads.push("");
    }
    const ids = [...this.results];
    for (let i = 0; i < ids.length; i += 200) {
      channels.push(this.resultChannel);
      payloads.push(ids.slice(i, i + 200).join(","));
    }
    this.work = false;
    this.results.clear();
    if (!channels.length) return;
    // Every pending hint in one statement, outside the activity transactions, so a hint
    // never precedes its commit.
    await this.pool
      .query(
        "SELECT pg_notify(c,p) FROM unnest($1::text[],$2::text[]) AS n(c,p)",
        [channels, payloads],
      )
      .catch(() => {});
  }
  subscribe(
    name: string,
    signal?: AbortSignal,
  ): { wait: (ms: number) => Promise<void>; close: () => void } {
    this.listening ??= this.listen();
    let dirty = false;
    let wake: (() => void) | undefined;
    const listener = () => {
      dirty = true;
      wake?.();
    };
    this.bus.on(name, listener);
    this.bus.on("reconnect", listener);
    const { signal: stop, done: unlink } = linkSignal(
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
          const settle = () => {
            clearTimeout(timer);
            stop.removeEventListener("abort", abort);
            wake = undefined;
          };
          const done = () => {
            settle();
            dirty = false;
            resolve();
          };
          const abort = () => {
            settle();
            reject(stop.reason);
          };
          const timer = setTimeout(done, ms);
          wake = done;
          stop.addEventListener("abort", abort, { once: true });
        });
      },
      close: () => {
        unlink();
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
