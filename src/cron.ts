// runnerq-spec's cron dialect and fire times (schedules.md, vectors/cron_next_fire.json).
// Instants and wall-clock times are both whole seconds since the Unix epoch: a wall time
// is the instant it would be if the zone were UTC.

interface Field {
  bits: bigint;
  /** The field's text begins with "*". */
  star: boolean;
}
const has = (f: Field, v: number) => (f.bits & (1n << BigInt(v))) !== 0n;

const macros: Record<string, string> = {
  "@yearly": "0 0 1 1 *",
  "@annually": "0 0 1 1 *",
  "@monthly": "0 0 1 * *",
  "@weekly": "0 0 * * 0",
  "@daily": "0 0 * * *",
  "@midnight": "0 0 * * *",
  "@hourly": "0 * * * *",
};
const monthNames = "jan feb mar apr may jun jul aug sep oct nov dec".split(" ");
const dayNames = "sun mon tue wed thu fri sat".split(" ");
const daysInMonth = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const digits = (s: string) => /^[0-9]{1,2}$/.test(s);
const searchYears = 50;
const day = 86400;
const week = 7 * day;

export class CronError extends Error {}

function parseField(
  text: string,
  lo: number,
  hi: number,
  names: string[] = [],
  nameBase = 0,
): Field {
  const f: Field = { bits: 0n, star: text.startsWith("*") };
  const value = (s: string): number => {
    const i = names.indexOf(s.toLowerCase());
    if (i >= 0) return i + nameBase;
    if (!digits(s)) throw new CronError(`bad value ${JSON.stringify(s)}`);
    const v = Number(s);
    if (v < lo || v > hi) throw new CronError(`${v} is outside ${lo}-${hi}`);
    return v;
  };
  for (const item of text.split(",")) {
    const slash = item.indexOf("/");
    const range = slash < 0 ? item : item.slice(0, slash);
    let step = 1;
    if (slash >= 0) {
      const s = item.slice(slash + 1);
      if (!digits(s)) throw new CronError(`bad step ${JSON.stringify(s)}`);
      step = Number(s);
      if (step === 0) throw new CronError("step 0");
    }
    let start: number, end: number;
    if (range === "*") [start, end] = [lo, hi];
    else if (range.includes("-")) {
      const dash = range.indexOf("-");
      start = value(range.slice(0, dash));
      end = value(range.slice(dash + 1));
      if (start > end)
        throw new CronError(`range ${JSON.stringify(range)} runs backwards`);
    } else {
      start = end = value(range);
      if (slash >= 0) end = hi;
    }
    for (let v = start; v <= end; v += step) f.bits |= 1n << BigInt(v);
  }
  return f;
}

/** A parsed cron expression. */
export class CronExpression {
  private constructor(
    private readonly sec: Field,
    private readonly min: Field,
    private readonly hour: Field,
    private readonly dom: Field,
    private readonly month: Field,
    private readonly dow: Field,
  ) {}

  /** Parses 5 fields, 6 with seconds first, or a macro such as `@daily`. */
  static parse(text: string): CronExpression {
    let t = String(text).trim();
    if (t.startsWith("@")) {
      const expanded = macros[t.toLowerCase()];
      if (!expanded)
        throw new CronError(`unknown macro ${JSON.stringify(text)}`);
      t = expanded;
    }
    const fields = t.split(/\s+/).filter(Boolean);
    if (fields.length === 5) fields.unshift("0");
    else if (fields.length !== 6)
      throw new CronError(`want 5 or 6 fields, got ${fields.length}`);
    const specs: [number, number, string[]?, number?][] = [
      [0, 59],
      [0, 59],
      [0, 23],
      [1, 31],
      [1, 12, monthNames, 1],
      [0, 7, dayNames, 0],
    ];
    const parsed = fields.map((f, i) => {
      const [lo, hi, names, base] = specs[i]!;
      try {
        return parseField(f, lo, hi, names, base);
      } catch (error) {
        throw new CronError(
          `field ${i + 1} ${JSON.stringify(f)}: ${(error as Error).message}`,
        );
      }
    });
    const dow = parsed[5]!;
    if (has(dow, 7)) dow.bits = (dow.bits & ~(1n << 7n)) | 1n;
    const e = new CronExpression(
      parsed[0]!,
      parsed[1]!,
      parsed[2]!,
      parsed[3]!,
      parsed[4]!,
      dow,
    );
    if (!e.matchesSomeDate())
      throw new CronError("the expression matches no date");
    return e;
  }

  private matchesSomeDate(): boolean {
    if (!this.dom.star && !this.dow.star) return true;
    for (let m = 1; m <= 12; m++)
      for (let d = 1; d <= daysInMonth[m - 1]!; d++)
        if (has(this.month, m) && has(this.dom, d)) return true;
    return false;
  }

  private dayMatches(d: Date): boolean {
    const dom = has(this.dom, d.getUTCDate());
    const dow = has(this.dow, d.getUTCDay());
    return this.dom.star || this.dow.star ? dom && dow : dom || dow;
  }

  /** The earliest matching wall time at or after w, or undefined. */
  private firstMatch(w: number): number | undefined {
    const d = new Date(w * 1000);
    const limit = d.getUTCFullYear() + searchYears;
    while (d.getUTCFullYear() <= limit) {
      if (!has(this.month, d.getUTCMonth() + 1)) {
        d.setUTCMonth(d.getUTCMonth() + 1, 1);
        d.setUTCHours(0, 0, 0, 0);
      } else if (!this.dayMatches(d)) {
        d.setUTCDate(d.getUTCDate() + 1);
        d.setUTCHours(0, 0, 0, 0);
      } else if (!has(this.hour, d.getUTCHours())) {
        d.setUTCHours(d.getUTCHours() + 1, 0, 0, 0);
      } else if (!has(this.min, d.getUTCMinutes())) {
        d.setUTCMinutes(d.getUTCMinutes() + 1, 0, 0);
      } else if (!has(this.sec, d.getUTCSeconds())) {
        d.setUTCSeconds(d.getUTCSeconds() + 1, 0);
      } else return d.getTime() / 1000;
    }
    return undefined;
  }

  /** The first fire instant at or after t (seconds). */
  private atOrAfter(z: Zone, t: number): number | undefined {
    const fixed = !this.hour.star;
    if (fixed) {
      // Inside the repeated hour after a fall-back the wall times have fired once
      // already: move to its end.
      const before = z.offset(t - day);
      if (before > z.offset(t)) {
        const tr = z.transition(t - day, t);
        const end = tr + before - z.offset(tr);
        if (t < end) t = end;
      }
    }
    for (let i = 0; i < 100_000; i++) {
      const o = z.offset(t);
      const w = this.firstMatch(t + o);
      if (w === undefined) return undefined;
      const c = w - o;
      const lim = Math.min(c, t + week);
      if (z.offset(lim) !== o) {
        const tr = z.transition(t, lim);
        const after = z.offset(tr);
        if (after > o) {
          // Spring forward: wall times [tr+o, tr+after) never happen. A fixed-hour
          // schedule fires once at tr for any of them.
          if (fixed) {
            const m = this.firstMatch(tr + o);
            if (m !== undefined && m < tr + after) return tr;
          }
          t = tr;
        } else {
          // Fall back: wall times [tr+after, tr+o) happen again. A fixed-hour schedule
          // skips the second pass.
          t = fixed ? tr + o - after : tr;
        }
        continue;
      }
      if (c === lim) return c;
      t = lim;
    }
    return undefined;
  }

  /** The first fire time strictly after `from` in `timeZone`; undefined if none within 50 years. */
  next(from: Date, timeZone: string): Date | undefined {
    const t = this.atOrAfter(
      zone(timeZone),
      Math.floor(from.getTime() / 1000) + 1,
    );
    return t === undefined ? undefined : new Date(t * 1000);
  }

  /** The last `n` fire times in [from, to], oldest first. */
  last(from: Date, to: Date, n: number, timeZone: string): Date[] {
    const z = zone(timeZone);
    const a = Math.ceil(from.getTime() / 1000);
    const b = Math.floor(to.getTime() / 1000);
    let window = 60;
    for (;;) {
      const start = Math.max(a, b - window);
      const ticks: number[] = [];
      for (
        let t = this.atOrAfter(z, start);
        t !== undefined && t <= b;
        t = this.atOrAfter(z, t + 1)
      ) {
        ticks.push(t);
        if (ticks.length > n) ticks.shift();
      }
      if (ticks.length >= n || start <= a)
        return ticks.map((t) => new Date(t * 1000));
      window = window < 3600 ? 3600 : window < day ? day : window * 8;
    }
  }
}

interface Zone {
  offset(t: number): number;
  transition(lo: number, hi: number): number;
}
const zones = new Map<string, Zone>();
const utc: Zone = { offset: () => 0, transition: (_lo, hi) => hi };

/**
 * Checks an IANA zone name. "" and "Local" are rejected: a schedule's zone must mean the
 * same on every host.
 */
export function checkTimeZone(name: string): void {
  zone(name);
}

function zone(name: string): Zone {
  const cached = zones.get(name);
  if (cached) return cached;
  if (typeof name !== "string" || !name || name === "Local")
    throw new CronError(`unknown time zone ${JSON.stringify(name)}`);
  if (name === "UTC") return utc;
  let format: Intl.DateTimeFormat;
  try {
    format = new Intl.DateTimeFormat("en-US", {
      timeZone: name,
      hourCycle: "h23",
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
      second: "numeric",
      era: "short",
    });
  } catch {
    throw new CronError(`unknown time zone ${JSON.stringify(name)}`);
  }
  const offset = (t: number): number => {
    const p: Record<string, string> = {};
    for (const part of format.formatToParts(new Date(t * 1000)))
      p[part.type] = part.value;
    let year = Number(p.year);
    if (p.era === "BC" || p.era === "B") year = 1 - year;
    const wall = Date.UTC(
      year,
      Number(p.month) - 1,
      Number(p.day),
      Number(p.hour),
      Number(p.minute),
      Number(p.second),
    );
    return wall / 1000 - t;
  };
  const z: Zone = {
    offset,
    transition(lo, hi) {
      const o = offset(lo);
      while (hi - lo > 1) {
        const mid = lo + Math.floor((hi - lo) / 2);
        if (offset(mid) === o) lo = mid;
        else hi = mid;
      }
      return hi;
    },
  };
  zones.set(name, z);
  return z;
}
