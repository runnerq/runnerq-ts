// Event streams (a port of runnerq-go's conductor/stream.go): tail the event log through
// QueryStorage and push stream.events batches, resumable after a cursor so the Cloud can
// move a stream to another executor without loss.
//
// Ids grow in insertion order, but a late commit can surface below ids already sent, so
// each poll rescans `rescan` ids below the cursor and skips ids it sent. A resumed
// subscription treats that window as sent. Delivery is at least once; consumers dedupe.
import { randomUUID } from "node:crypto";
import { pause } from "../async.js";
import { parseInt64 } from "../codec.js";
import type { EventRecord, QueryFilter, QueryStorage } from "../query.js";
import { decodeRequest, type Spec } from "./decode.js";
import { filterSpec, toEvent } from "./queries.js";
import {
  WireError,
  describe,
  frameSlack,
  typeEventsSubscribe,
  typeEventsUnsubscribe,
  typeStreamEvents,
  typeStreamGap,
  type Handler,
} from "./wire.js";

export const maxSubscriptions = 4;
const defaultBatch = 200;
const maxBatch = 1000;
const defaultDelayMs = 500;
const minDelayMs = 50;
/** How many ids below the cursor each poll looks at again for late commits. */
export const rescan = 256n;
/** Pushes wait while the socket holds more than this, rather than buffering without bound. */
const maxBuffered = 8 << 20;

/** Where a session's streams write. */
export interface StreamOutput {
  /** Writes one event frame; false when the connection can't take it. */
  send(type: string, data: unknown): boolean;
  /** Bytes queued on the socket and not yet written. */
  buffered(): number;
  frameLimit(): number;
  metadataOnly(): boolean;
  log: Pick<Console, "warn">;
}

const subscribeSpec: Spec = {
  object: {
    filter: filterSpec,
    after_cursor: "string",
    max_batch: "int",
    max_delay_ms: "int",
  },
};
const unsubscribeSpec: Spec = {
  object: { subscription_id: "string", cursor: "string" },
};

/** One session's subscriptions; they end with the session. */
export class Streams {
  private readonly subs = new Map<string, AbortController>();
  private readonly session = new AbortController();

  constructor(
    private readonly qs: QueryStorage,
    private readonly out: StreamOutput,
  ) {}

  handler(type: string): Handler | undefined {
    if (type === typeEventsSubscribe)
      return (d, signal) => this.subscribe(d, signal);
    if (type === typeEventsUnsubscribe) return (d) => this.unsubscribe(d);
    return undefined;
  }

  close(): void {
    this.session.abort();
    for (const sub of this.subs.values()) sub.abort();
    this.subs.clear();
  }

  private async subscribe(
    data: unknown,
    request: AbortSignal,
  ): Promise<unknown> {
    const req = decodeRequest<{
      filter?: QueryFilter;
      after_cursor?: string;
      max_batch?: number;
      max_delay_ms?: number;
    }>(subscribeSpec, data);
    const filter = req.filter;
    const batch = Math.min(
      req.max_batch && req.max_batch > 0 ? req.max_batch : defaultBatch,
      maxBatch,
    );
    const delay = Math.max(
      req.max_delay_ms && req.max_delay_ms > 0
        ? req.max_delay_ms
        : defaultDelayMs,
      minDelayMs,
    );
    const after = req.after_cursor ?? "";
    const seq = parseInt64(after);
    const resume = seq !== undefined && seq >= 0n ? seq : undefined;
    // Not a cursor this backend issued: start from the end and say so.
    const gap = after !== "" && resume === undefined;
    let cursor = resume ?? 0n;
    if (after === "" || gap) {
      // Validate the filter and find the log's end in one query.
      const last = await this.qs.queryEvents({ filter, desc: true, limit: 1 });
      if (last.items[0]) cursor = BigInt(last.items[0].id);
    } else {
      // An invalid filter fails the subscribe, not the stream.
      await this.qs.queryEvents({ filter, limit: 1 });
    }
    if (this.session.signal.aborted)
      throw new WireError("unavailable", "the session ended");
    // A request that ran out of time has been answered already: start nothing.
    if (request.aborted)
      throw new WireError(
        "deadline_exceeded",
        "the request ran past its deadline",
      );
    if (this.subs.size >= maxSubscriptions)
      throw new WireError(
        "resource_exhausted",
        `at most ${maxSubscriptions} event subscriptions per executor`,
      );
    const id = "sub_" + randomUUID();
    const stop = new AbortController();
    this.subs.set(id, stop);
    const t = new Tailer(this.qs, this.out, id, filter, batch, delay, cursor);
    // The window below the start was delivered before (or predates the subscription):
    // only late commits into it are new.
    try {
      for (const ev of await t.window(false)) t.sent.add(BigInt(ev.id));
    } catch {
      /* the first poll reads it again */
    }
    void (async () => {
      try {
        if (gap)
          await t.push(stop.signal, typeStreamGap, {
            subscription_id: id,
            since_cursor: after,
          });
        await t.run(stop.signal);
      } catch {
        /* the subscription ended */
      } finally {
        if (this.subs.get(id) === stop) this.subs.delete(id);
      }
    })();
    return { subscription_id: id, cursor: cursor.toString() };
  }

  private unsubscribe(data: unknown): unknown {
    const req = decodeRequest<{ subscription_id?: string }>(
      unsubscribeSpec,
      data,
    );
    const id = req.subscription_id ?? "";
    const stop = this.subs.get(id);
    this.subs.delete(id);
    if (!stop)
      throw new WireError("not_found", `no subscription ${JSON.stringify(id)}`);
    stop.abort();
    return {};
  }
}

/** One subscription's poll loop. */
export class Tailer {
  /** Ids delivered within the rescan window, so a rescan never repeats them. */
  readonly sent = new Set<bigint>();

  constructor(
    private readonly qs: QueryStorage,
    private readonly out: StreamOutput,
    readonly id: string,
    private readonly filter: QueryFilter | undefined,
    private readonly batch: number,
    private readonly delayMs: number,
    public cursor: bigint,
  ) {}

  async run(signal: AbortSignal): Promise<void> {
    let next = 0;
    for (;;) {
      await pause(next, signal);
      let full = false;
      try {
        full = await this.poll(signal);
      } catch (error) {
        if (signal.aborted) return;
        this.out.log.warn(
          `runnerq-conductor: event stream ${this.id} poll failed; retrying: ${describe(error)}`,
        );
      }
      next = full ? 0 : this.delayMs; // catching up: keep reading
    }
  }

  /** Events in (after, upTo] (upTo 0: unbounded), oldest first. */
  private async query(
    after: bigint,
    upTo: bigint,
    limit: number,
    detail: boolean,
  ): Promise<EventRecord[]> {
    const terms: QueryFilter[] = [{ field: "seq", op: "gt", value: after }];
    if (upTo > 0n) terms.push({ field: "seq", op: "lte", value: upTo });
    if (this.filter) terms.push(this.filter);
    const page = await this.qs.queryEvents({
      filter: { and: terms },
      limit,
      includeDetail: detail && !this.out.metadataOnly(),
    });
    return page.items;
  }

  /** The rescan window below the cursor. */
  window(detail: boolean): Promise<EventRecord[]> {
    if (this.cursor <= 0n) return Promise.resolve([]);
    const from = this.cursor - rescan;
    return this.query(
      from > 0n ? from : 0n,
      this.cursor,
      Number(rescan),
      detail,
    );
  }

  /** Sends late commits in the rescan window and the next batch; true when the batch was full. */
  async poll(signal: AbortSignal): Promise<boolean> {
    const late = await this.window(true);
    const fresh = await this.query(this.cursor, 0n, this.batch, true);
    const pending = [...late, ...fresh].filter(
      (ev) => !this.sent.has(BigInt(ev.id)),
    );
    await this.send(signal, pending);
    for (const id of this.sent)
      if (id <= this.cursor - rescan) this.sent.delete(id);
    return fresh.length === this.batch;
  }

  /**
   * Pushes events in frames within the Cloud's limit (an oversized frame drops the session,
   * and the resumed stream would reread the batch). Events count as sent, and move the
   * cursor, only once their frame is written. An event too large alone loses its detail.
   */
  async send(signal: AbortSignal, events: EventRecord[]): Promise<void> {
    const budget = this.out.frameLimit() - frameSlack;
    let items: Record<string, unknown>[] = [];
    let ids: bigint[] = [];
    let size = 0;
    const flush = async () => {
      if (!items.length) return;
      let cursor = this.cursor;
      for (const id of ids) if (id > cursor) cursor = id;
      await this.push(signal, typeStreamEvents, {
        subscription_id: this.id,
        items,
        cursor: cursor.toString(),
      });
      for (const id of ids) this.sent.add(id);
      this.cursor = cursor;
      items = [];
      ids = [];
      size = 0;
    };
    for (const ev of events) {
      const item = toEvent(ev);
      let n = encodedSize(item);
      if (budget > 0 && n > budget && "detail" in item) {
        this.out.log.warn(
          `runnerq-conductor: event ${ev.id} detail is ${n} bytes, over the ${budget}-byte frame limit; streaming the event without it`,
        );
        delete item.detail;
        n = encodedSize(item);
      }
      if (budget > 0 && items.length && size + n + 1 > budget) await flush();
      items.push(item);
      ids.push(BigInt(ev.id));
      size += n + 1;
    }
    await flush();
  }

  /**
   * Writes one frame, first waiting while the socket is backed up. An ended subscription
   * stops before writing; a started write is never cut short, so an unsubscribe mid-push
   * cannot drop the session.
   */
  async push(signal: AbortSignal, type: string, data: unknown): Promise<void> {
    while (this.out.buffered() > maxBuffered) await pause(20, signal);
    signal.throwIfAborted();
    if (!this.out.send(type, data))
      throw new Error("the connection is not open");
  }
}

function encodedSize(v: unknown): number {
  return Buffer.byteLength(JSON.stringify(v));
}
