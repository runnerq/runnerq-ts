// activity.notices: lifecycle changes no event is stored for, announced while the Cloud asks
// (runnerq-go's conductor/notices.go).
import type { ActivityNotices, Notice } from "./protocol.js";
import type { Change } from "../worker.js";

const everyMs = 250;
const batch = 500;
/** Past this many waiting, the oldest go, counted in the next batch's `dropped`. */
export const noticeBuffer = 5000;

export interface NoticeOutput {
  /** Sends one batch as JSON text; false when the connection can't take it. */
  send(data: string): boolean;
  frameLimit(): number;
}

/** A session's notices: the worker announces into it, and `start` sends what gathers. */
export class Notices {
  private items: Notice[] = [];
  private dropped = 0;
  constructor(
    private readonly queue: string,
    private readonly executor: string,
  ) {}

  readonly announce = (c: Change): void => {
    if (this.items.length >= noticeBuffer) {
      this.items.shift();
      this.dropped++;
    }
    this.items.push({
      activity_id: c.activityId,
      type: c.type,
      at: c.at.toISOString(),
      queue: this.queue,
      activity_type: c.activityType,
      root_id: c.rootId,
      ...(c.attempt !== undefined && { attempt: c.attempt }),
      executor_id: this.executor,
    });
  };

  take(): { items: Notice[]; dropped: number } {
    const taken = { items: this.items, dropped: this.dropped };
    this.items = [];
    this.dropped = 0;
    return taken;
  }

  /** Sends batches every 250 ms, each within the frame limit, until `signal` aborts. */
  start(out: NoticeOutput, signal: AbortSignal): void {
    const timer = setInterval(() => {
      let { items, dropped } = this.take();
      while (items.length) {
        let size = Math.min(items.length, batch);
        let data: string;
        for (;;) {
          const body: ActivityNotices = {
            items: items.slice(0, size),
            ...(dropped && { dropped }),
          };
          data = JSON.stringify(body);
          if (size === 1 || data.length <= out.frameLimit() - 1024) break;
          size = Math.ceil(size / 2);
        }
        if (!out.send(data)) return;
        items = items.slice(size);
        dropped = 0;
      }
    }, everyMs);
    timer.unref?.();
    signal.addEventListener("abort", () => clearInterval(timer), {
      once: true,
    });
  }
}
