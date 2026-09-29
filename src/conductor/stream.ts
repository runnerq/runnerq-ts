// Event streams: a port of runnerq-go's conductor/stream.go.
//
// Streams tail the event log through QueryStorage and push batches to the Cloud as
// stream.events. They are resumable: a subscription starts after a cursor, so when the
// Cloud moves the stream to another executor it loses nothing.
//
// Event ids grow in insertion order but a transaction can commit late, surfacing an event
// below ids already delivered. Each poll therefore rescans the `rescan` ids below the
// cursor and sends only ids it has not sent. A subscription resuming after a cursor treats
// that window as already sent, so a failover does not replay it. Delivery is at least once
// in edge cases, so consumers dedupe by event id.
import { randomUUID } from "node:crypto";
import { pause } from "../async.js";
import { parseInt64 } from "../codec.js";
import type { EventRecord, QueryFilter, QueryStorage } from "../query.js";
import { decodeRequest, type Spec } from "./decode.js";
import {
  filterSpec,
  toEvent,
  toStorageFilter,
  type WireFilter,
} from "./queries.js";
import {
  WireError,
  typeEventsSubscribe,
  typeEventsUnsubscribe,
  typeStreamEvents,
  typeStreamGap,
} from "./wire.js";

export const maxSubscriptions = 4;
const defaultBatch = 200;
const maxBatch = 1000;
const defaultDelayMs = 500;
const minDelayMs = 50;
/** How many ids below the cursor each poll looks at again for late commits. */
export const rescan = 256n;
/** The margin the agent leaves in every frame for the envelope. */
const frameSlack = 1_024;
/** Pushes wait while the socket holds more than this, rather than buffering without bound. */
const maxBuffered = 8 << 20;

/** Where a session's streams write. */
export interface StreamOutput {
  /** Writes one event frame; false when the connection can't take it. */
  send(type: string, data: unknown): boolean;
  /** Bytes queued on the socket and not yet written. */
  buffered(): number;
  /** The Cloud's frame limit. */
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

type Handler = (data: unknown, signal: AbortSignal) => unknown;

/** One session's subscriptions; they end with the session. */
export class Streams {
  private readonly subs = new Map<string, AbortController>();
  private readonly session = new AbortController();

  constructor(
    private readonly qs: QueryStorage,
    private readonly out: StreamOutput,
  ) {}

  /** The session-bound handlers. */
  handler(type: string): Handler | undefined {
    if (type === typeEventsSubscribe)
      return (d, signal) => this.subscribe(d, signal);
    if (type === typeEventsUnsubscribe) return (d) => this.unsubscribe(d);
    return undefined;
  }

  /** Stops every subscription. */
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
      filter?: WireFilter;
      after_cursor?: string;
      max_batch?: number;
      max_delay_ms?: number;
    }>(subscribeSpec, data);
    const filter = toStorageFilter(req.filter);
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
    // What is already in the window below the start was delivered before (or predates
    // the subscription): only late commits into it are new.
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
          `runnerq-conductor: event stream ${this.id} poll failed; retrying: ${error instanceof Error ? error.message : String(error)}`,
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

  /**
   * Sends late commits found in the rescan window and the next batch past the cursor;
   * reports whether that batch was full.
   */
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
   * Pushes events as stream.events frames that each fit the Cloud's frame limit (an
   * oversized frame would drop the session, and the resumed stream would read the same
   * batch again). An event counts as sent, and moves the cursor, only once its frame is
   * written, so each frame's cursor covers what the Cloud has received. An event too large
   * for a frame by itself goes without its detail.
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
   * Writes one frame for the subscription. A subscription that has ended stops before
   * writing; a write, once started, is never cut short (the socket takes the whole frame),
   * so an unsubscribe landing during a push cannot drop the session. While the socket is
   * backed up, the push waits for it to drain.
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
