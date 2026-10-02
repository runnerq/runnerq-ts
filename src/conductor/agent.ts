import { RunnerQError } from "../errors.js";
import { linkSignal, pause } from "../async.js";
import { reportExecutor } from "../executor.js";
import { interruptActivity, type Worker } from "../worker.js";
import { isCommandStorage } from "../storage.js";
import { Commands } from "./commands.js";
import { isQueryStorage, QueryError, type QueryStorage } from "../query.js";
import { Queries } from "./queries.js";
import { Streams } from "./stream.js";
import {
  conductorVersion,
  type Capability,
  type Envelope,
  type Hello,
  type Messages,
  type SessionConfig,
  type Welcome,
} from "./protocol.js";
import {
  stateOf,
  WireError,
  describe,
  frameSlack,
  ts,
  type Handler,
} from "./wire.js";

/** The events an agent sends. */
type AgentEvent = {
  [T in keyof Messages]: Messages[T] extends { kind: "evt"; from: "agent" }
    ? T
    : never;
}[keyof Messages];

const agentPath = "/v1/agent";
const handshakeTimeoutMs = 10_000;
const maxMessageBytes = 4 << 20;
const defaultReportIntervalMs = 15_000;
/** Spaces the reports an executor's changes trigger. */
const reportMinGapMs = 1_000;

export interface AgentConfig {
  /** The Cloud gateway, e.g. "wss://cloud.runnerq.dev". `/v1/agent` is appended when missing; http(s) maps to ws(s). */
  url: string;
  /** Authenticates the app; sent in the Authorization header, never in the URL. */
  apiKey: string;
  /**
   * Lets the Cloud run commands (cancel, retry, run now, reschedule, set priority, delete,
   * signal) on this worker's queue, when its storage supports them. Off by default: read-only.
   */
  allowControl?: boolean;
  /** Strips payloads, results, errors and event details from every reply, whatever the Cloud asks. */
  metadataOnly?: boolean;
  /** Added to the worker's labels (and win on a clash); prefer `WorkerConfig.labels`. */
  labels?: Readonly<Record<string, string>>;
  /** Requests served at once (default 16); more are answered `resource_exhausted`. */
  maxConcurrentRequests?: number;
  /** Bounds one request (default 30s). */
  requestTimeoutMs?: number;
  minReconnectDelayMs?: number;
  maxReconnectDelayMs?: number;
  /** Stops the agent (with a goodbye) when aborted, as `close()` does. */
  signal?: AbortSignal;
  /** Where connection problems are logged (default: console). */
  logger?: Pick<Console, "info" | "warn">;
}

/**
 * Connects a worker to RunnerQ Cloud over an outbound WebSocket: describes it, reports it on
 * change and on an interval, and answers the Cloud's requests. `close()` it before stopping
 * the worker so the Cloud records a clean shutdown rather than a lost executor.
 */
export class Agent {
  private readonly url: string;
  private readonly maxRequests: number;
  private readonly requestTimeoutMs: number;
  private readonly minDelay: number;
  private readonly maxDelay: number;
  private readonly log: Pick<Console, "info" | "warn">;
  private readonly started = new Date();
  private readonly stop = new AbortController();
  private readonly table = new Map<string, Handler>();
  private readonly caps: Record<string, Capability> = {};
  private readonly done: Promise<void>;
  private socket?: WebSocket;
  private session = "";
  private closing = false;
  private goodbyeSent = false;
  private reportEveryMs = defaultReportIntervalMs;
  private cloudMetadataOnly = false;
  private peerFrameLimit = maxMessageBytes;
  private inFlight = 0;
  private readonly closeOnAbort = () => void this.close();
  /** The worker's storage, when queryable; queries and streams read it. */
  private readonly qs?: QueryStorage;

  constructor(
    private readonly worker: Worker,
    private readonly config: AgentConfig,
  ) {
    this.url = agentUrl(config.url);
    if (!config.apiKey)
      throw new RunnerQError("configuration", "The agent needs an API key");
    this.maxRequests = positive(config.maxConcurrentRequests, 16);
    this.requestTimeoutMs = positive(config.requestTimeoutMs, 30_000);
    this.minDelay = positive(config.minReconnectDelayMs, 1_000);
    this.maxDelay = Math.max(
      positive(config.maxReconnectDelayMs, 30_000),
      this.minDelay,
    );
    this.log = config.logger ?? console;
    this.handle("executor.describe", { v: 1 }, () =>
      stateOf(this.worker.snapshot(), this.started, true),
    );
    const storage = worker.storage;
    if (isQueryStorage(storage)) {
      this.qs = storage;
      const queries = new Queries(storage, storage, () => this.metadataOnly);
      for (const [type, route] of Object.entries(queries.routes())) {
        // Stream requests have no handler: they are bound to a session (see connect).
        if (route.handler) this.handle(type, route.capability, route.handler);
        else this.caps[type] = route.capability;
      }
    }
    if (config.allowControl && isCommandStorage(storage)) {
      const commands = new Commands(storage, storage.queue, (id) =>
        worker[interruptActivity](id),
      );
      for (const [type, route] of Object.entries(commands.routes()))
        this.handle(type, route.capability, route.handler!);
    }
    config.signal?.addEventListener("abort", this.closeOnAbort, {
      once: true,
    });
    this.done = this.run();
  }

  get connected(): boolean {
    return this.session !== "";
  }
  /** The current Cloud session id, or "" while disconnected. */
  get sessionId(): string {
    return this.session;
  }
  /** Says goodbye and stops. Resolves once the agent has stopped. */
  async close(): Promise<void> {
    this.config.signal?.removeEventListener("abort", this.closeOnAbort);
    this.closing = true;
    if (this.socket) this.goodbye(this.socket);
    this.stop.abort();
    await this.done;
  }

  protected handle(type: string, capability: Capability, fn: Handler): void {
    this.table.set(type, fn);
    this.caps[type] = capability;
  }
  /** Whether replies must leave out payloads, results, errors and event details. */
  protected get metadataOnly(): boolean {
    return !!this.config.metadataOnly || this.cloudMetadataOnly;
  }

  private async run(): Promise<void> {
    let delay = this.minDelay;
    while (!this.closing && !this.stop.signal.aborted) {
      const began = Date.now();
      let code = 0;
      try {
        code = await this.connect();
      } catch (error) {
        if (!this.closing)
          this.log.warn(
            `runnerq-conductor: connecting to RunnerQ Cloud failed: ${describe(error)}`,
          );
      }
      this.session = "";
      if (this.closing || this.stop.signal.aborted) return;
      const lasted = Date.now() - began;
      let wait = delay;
      if (code === 1001) wait = this.minDelay; // the gateway node is draining
      if (lasted > this.maxDelay) {
        wait = this.minDelay;
        delay = this.minDelay;
      }
      delay = Math.min(delay * 2, this.maxDelay);
      await pause(wait * (0.5 + Math.random()), this.stop.signal).catch(
        () => {},
      );
    }
  }

  /** One session: dial, handshake, serve until the connection ends; resolves with its close code. */
  private connect(): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      // Node's WebSocket takes headers in its init; the DOM type doesn't know them.
      const ws = new WebSocket(this.url, {
        headers: { Authorization: `Bearer ${this.config.apiKey}` },
      } as unknown as string[]);
      const session = new AbortController();
      let streams: Streams | undefined;
      let welcomed = false;
      const handshake = setTimeout(() => {
        reject(new Error("the handshake timed out"));
        ws.close();
      }, handshakeTimeoutMs);
      const end = (code: number) => {
        clearTimeout(handshake);
        session.abort();
        streams?.close();
        if (this.socket === ws) this.socket = undefined;
        if (welcomed) resolve(code);
        else reject(new Error(`the connection closed (${code})`));
      };
      ws.addEventListener("error", () => {
        if (!welcomed)
          reject(
            new Error(
              "could not connect (a rejected API key shows as this too)",
            ),
          );
      });
      ws.addEventListener("close", (event) => end(event.code));
      ws.addEventListener("open", () => {
        this.send(ws, {
          v: conductorVersion,
          kind: "req",
          id: "hello",
          type: "hello",
          data: this.hello(),
        });
      });
      ws.addEventListener("message", (event) => {
        if (typeof event.data !== "string") return; // binary frames are reserved
        if (event.data.length > maxMessageBytes) return;
        let env: Envelope;
        try {
          env = JSON.parse(event.data);
        } catch {
          return;
        }
        if (!welcomed) {
          try {
            this.welcome(env);
          } catch (error) {
            clearTimeout(handshake);
            reject(error);
            ws.close();
            return;
          }
          welcomed = true;
          clearTimeout(handshake);
          if (this.closing) {
            this.goodbye(ws);
            return;
          }
          this.socket = ws;
          this.goodbyeSent = false;
          if (this.qs)
            streams = new Streams(this.qs, {
              send: (type, data) =>
                this.send(ws, { v: conductorVersion, kind: "evt", type }, data),
              buffered: () => ws.bufferedAmount,
              frameLimit: () => this.peerFrameLimit,
              metadataOnly: () => this.metadataOnly,
              log: this.log,
            });
          void reportExecutor({
            signal: session.signal,
            source: this.worker,
            intervalMs: () => this.reportEveryMs,
            minGapMs: reportMinGapMs,
            send: () =>
              void this.event(
                ws,
                "executor.report",
                stateOf(this.worker.snapshot(), this.started, false),
              ),
          });
          return;
        }
        this.dispatch(ws, env, session.signal, streams);
      });
    });
  }

  private hello(): Hello {
    const { info } = this.worker.snapshot();
    const labels = { ...info.labels, ...this.config.labels };
    return {
      protocol_versions: [conductorVersion],
      sdk: info.sdk,
      executor: {
        id: info.id,
        ...(info.hostname && { hostname: info.hostname }),
        queues: [info.queue],
        ...(info.activityTypes.length && {
          activity_types: info.activityTypes,
        }),
        max_concurrency: info.maxConcurrency,
        ...(info.startedAt && { started_at: ts(info.startedAt) }),
        ...(Object.keys(labels).length && { labels }),
      },
      capabilities: this.caps,
      limits: {
        max_frame_bytes: maxMessageBytes,
        max_concurrent_requests: this.maxRequests,
      },
    };
  }

  private welcome(env: Envelope): void {
    if (env.kind !== "res" || env.type !== "hello")
      throw new Error("unexpected handshake reply");
    if (env.error)
      throw new Error(`the Cloud rejected the handshake: ${env.error.message}`);
    const welcome = env.data as Welcome | undefined;
    if (welcome?.version !== conductorVersion)
      throw new Error(`the Cloud chose protocol version ${welcome?.version}`);
    const frame = welcome.limits?.max_frame_bytes ?? 0;
    this.peerFrameLimit =
      frame > 0 ? Math.min(frame, maxMessageBytes) : maxMessageBytes;
    this.applyConfig(welcome.config);
    this.session = welcome.session_id;
  }

  private applyConfig(config?: SessionConfig): void {
    if (config?.data_mode)
      this.cloudMetadataOnly = config.data_mode === "metadata_only";
    if (config?.report_interval_ms && config.report_interval_ms > 0)
      this.reportEveryMs = Math.max(config.report_interval_ms, 1_000);
  }

  private dispatch(
    ws: WebSocket,
    env: Envelope,
    session: AbortSignal,
    streams?: Streams,
  ): void {
    if (env.kind === "evt") {
      if (env.type === "config.update")
        this.applyConfig(env.data as SessionConfig);
      return;
    }
    if (env.kind !== "req") return;
    const reply = (res: Partial<Envelope>, data?: string) =>
      this.send(
        ws,
        { v: env.v, kind: "res", id: env.id, type: env.type, ...res },
        data,
      );
    if (this.inFlight >= this.maxRequests) {
      reply({
        error: new WireError(
          "resource_exhausted",
          "agent is at its request limit",
        ).body(),
      });
      return;
    }
    this.inFlight++;
    void this.serve(env, session, streams)
      .then(
        (data) => reply({}, data),
        (error) => reply({ error: toWireError(error).body() }),
      )
      .finally(() => this.inFlight--);
  }

  private async serve(
    env: Envelope,
    session: AbortSignal,
    streams?: Streams,
  ): Promise<string | undefined> {
    const handler = this.table.get(env.type) ?? streams?.handler(env.type);
    if (!handler)
      throw new WireError(
        "unsupported",
        `this agent does not serve "${env.type}"`,
      );
    let deadline = Date.now() + this.requestTimeoutMs;
    const asked = env.meta?.deadline;
    if (typeof asked === "string") {
      const at = Date.parse(asked);
      if (!Number.isNaN(at)) deadline = Math.min(deadline, at);
    }
    const remaining = deadline - Date.now();
    if (remaining <= 0)
      throw new WireError(
        "deadline_exceeded",
        "the request expired before it started",
      );
    const { signal, done } = linkSignal([session], remaining);
    const result = await Promise.race([
      Promise.resolve().then(() => handler(env.data ?? {}, signal)),
      new Promise((_, reject) =>
        signal.addEventListener(
          "abort",
          () =>
            reject(
              new WireError(
                "deadline_exceeded",
                "the request ran past its deadline",
              ),
            ),
          { once: true },
        ),
      ),
    ]).finally(done);
    // Serialized once: measured here, then spliced into the reply frame by send().
    const data = JSON.stringify(result ?? null);
    const size = Buffer.byteLength(data);
    const limit = this.peerFrameLimit - frameSlack;
    if (size > limit)
      throw new WireError(
        "resource_exhausted",
        `the reply is ${size} bytes, over the ${limit}-byte frame limit; ask for fewer rows or fields`,
      );
    return result === undefined ? undefined : data;
  }

  private goodbye(ws: WebSocket): void {
    if (this.goodbyeSent && this.socket === ws) return;
    this.goodbyeSent = true;
    this.event(ws, "goodbye", { reason: "shutdown" });
    ws.close(1000, "shutdown");
  }

  private event<T extends AgentEvent>(
    ws: WebSocket,
    type: T,
    data: Messages[T]["data"],
  ): boolean {
    return this.send(ws, { v: conductorVersion, kind: "evt", type, data });
  }

  /**
   * Writes one frame, splicing in `data` (JSON text) when given; false when the connection
   * can't take it.
   */
  private send(ws: WebSocket, env: Envelope, data?: string): boolean {
    if (ws.readyState !== WebSocket.OPEN) return false;
    try {
      const frame = JSON.stringify(env);
      ws.send(
        data === undefined ? frame : `${frame.slice(0, -1)},"data":${data}}`,
      );
      return true;
    } catch (error) {
      this.log.warn(`runnerq-conductor: send failed: ${describe(error)}`);
      return false;
    }
  }
}

/** Connects `worker` to RunnerQ Cloud; see `Agent`. */
export function startAgent(worker: Worker, config: AgentConfig): Agent {
  if (!worker)
    throw new RunnerQError("configuration", "The agent needs a worker");
  return new Agent(worker, config);
}

function agentUrl(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new RunnerQError("configuration", "The agent URL is not a URL");
  }
  const scheme = {
    "ws:": "ws:",
    "wss:": "wss:",
    "http:": "ws:",
    "https:": "wss:",
  }[url.protocol];
  if (!scheme)
    throw new RunnerQError(
      "configuration",
      "The agent URL must use ws, wss, http or https",
    );
  url.protocol = scheme;
  if (!url.pathname.endsWith(agentPath))
    url.pathname = url.pathname.replace(/\/+$/, "") + agentPath;
  return url.toString();
}
function positive(value: number | undefined, fallback: number): number {
  return value && value > 0 ? value : fallback;
}
export function toWireError(error: unknown): WireError {
  if (error instanceof WireError) return error;
  if (error instanceof QueryError)
    return new WireError(
      error.kind,
      error.message,
      error.field ? { field: error.field } : undefined,
    );
  if (error instanceof RunnerQError) {
    switch (error.code) {
      case "not_found":
      case "conflict":
        return new WireError(error.code, error.message);
      case "unavailable":
      case "timeout":
        return new WireError("unavailable", error.message);
    }
  }
  const first = describe(error).split("\n")[0] ?? "";
  return new WireError("internal", first);
}
