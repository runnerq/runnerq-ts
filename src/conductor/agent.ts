import { RunnerQError } from "../errors.js";
import { pause } from "../async.js";
import { reportExecutor } from "../executor.js";
import type { Worker } from "../worker.js";
import { isQueryStorage, QueryError, type QueryStorage } from "../query.js";
import { Queries } from "./queries.js";
import { Streams } from "./stream.js";
import {
  protocolVersion,
  stateOf,
  typeConfigUpdate,
  typeExecutorDescribe,
  typeExecutorReport,
  typeEventsSubscribe,
  typeEventsUnsubscribe,
  typeGoodbye,
  typeHello,
  WireError,
  ts,
  type Capability,
  type Envelope,
  type SessionConfig,
  type Welcome,
} from "./wire.js";

const agentPath = "/v1/agent";
const handshakeTimeoutMs = 10_000;
const maxMessageBytes = 4 << 20;
const defaultReportIntervalMs = 15_000;
/** Spaces the reports an executor's changes trigger. */
const reportMinGapMs = 1_000;
/** Room for the envelope around a reply's data. */
const frameSlack = 1_024;

export interface AgentConfig {
  /** The Cloud gateway, e.g. "wss://cloud.runnerq.dev". `/v1/agent` is appended when missing; http(s) maps to ws(s). */
  url: string;
  /** Authenticates the app; sent in the Authorization header, never in the URL. */
  apiKey: string;
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

type Handler = (data: unknown, signal: AbortSignal) => unknown;

/**
 * Connects a worker to RunnerQ Cloud. It dials out over a WebSocket, describes the worker
 * (hello), reports it as it changes and on an interval, and answers the Cloud's requests.
 * Start it with the worker; `close()` it before stopping the worker, so the Cloud records
 * a clean shutdown rather than a lost executor.
 */
export class Agent {
  private readonly url: string;
  private readonly apiKey: string;
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
  /** The worker's storage when it can be queried; queries and streams are served from it. */
  private readonly qs?: QueryStorage;

  constructor(
    private readonly worker: Worker,
    private readonly config: AgentConfig,
  ) {
    this.url = agentUrl(config.url);
    if (!config.apiKey)
      throw new RunnerQError("configuration", "The agent needs an API key");
    this.apiKey = config.apiKey;
    this.maxRequests = positive(config.maxConcurrentRequests, 16);
    this.requestTimeoutMs = positive(config.requestTimeoutMs, 30_000);
    this.minDelay = positive(config.minReconnectDelayMs, 1_000);
    this.maxDelay = Math.max(
      positive(config.maxReconnectDelayMs, 30_000),
      this.minDelay,
    );
    this.log = config.logger ?? console;
    this.handle(typeExecutorDescribe, { v: 1 }, () =>
      stateOf(this.worker.snapshot(), this.started, true),
    );
    const storage = worker.storage;
    if (isQueryStorage(storage)) {
      this.qs = storage;
      const queries = new Queries(storage, storage, () => this.metadataOnly);
      const caps = queries.capabilities();
      for (const [type, fn] of Object.entries(queries.handlers()))
        this.handle(type, caps[type]!, fn);
      // Stream requests are bound to a session's connection (see connect).
      this.caps[typeEventsSubscribe] = caps[typeEventsSubscribe]!;
      this.caps[typeEventsUnsubscribe] = caps[typeEventsUnsubscribe]!;
    }
    config.signal?.addEventListener("abort", () => void this.close(), {
      once: true,
    });
    this.done = this.run();
  }

  /** Whether a session with the Cloud is open. */
  get connected(): boolean {
    return this.session !== "";
  }
  /** The current Cloud session id, or "" while disconnected. */
  get sessionId(): string {
    return this.session;
  }
  /** Says goodbye and stops. Resolves once the agent has stopped. */
  async close(): Promise<void> {
    this.closing = true;
    this.goodbye();
    this.stop.abort();
    await this.done;
  }

  /** Serves `type` with `fn`, advertised with `capability`. */
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
        headers: { Authorization: `Bearer ${this.apiKey}` },
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
          v: protocolVersion,
          kind: "req",
          id: "hello",
          type: typeHello,
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
            this.goodbyeOn(ws);
            return;
          }
          this.socket = ws;
          this.goodbyeSent = false;
          if (this.qs)
            streams = new Streams(this.qs, {
              send: (type, data) =>
                this.send(ws, { v: protocolVersion, kind: "evt", type, data }),
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
            send: () => {
              this.send(ws, {
                v: protocolVersion,
                kind: "evt",
                type: typeExecutorReport,
                data: stateOf(this.worker.snapshot(), this.started, false),
              });
            },
          });
          return;
        }
        this.dispatch(ws, env, session.signal, streams);
      });
    });
  }

  private hello(): unknown {
    const { info } = this.worker.snapshot();
    const labels = { ...info.labels, ...this.config.labels };
    return {
      protocol_versions: [protocolVersion],
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
    if (env.kind !== "res" || env.type !== typeHello)
      throw new Error("unexpected handshake reply");
    if (env.error)
      throw new Error(`the Cloud rejected the handshake: ${env.error.message}`);
    const welcome = env.data as Welcome;
    if (welcome?.version !== protocolVersion)
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
      if (env.type === typeConfigUpdate)
        this.applyConfig(env.data as SessionConfig);
      return;
    }
    if (env.kind !== "req") return;
    const reply = (res: Partial<Envelope>) =>
      this.send(ws, {
        v: env.v,
        kind: "res",
        id: env.id,
        type: env.type,
        ...res,
      });
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
        (data) => reply({ data }),
        (error) => reply({ error: toWireError(error).body() }),
      )
      .finally(() => this.inFlight--);
  }

  private async serve(
    env: Envelope,
    session: AbortSignal,
    streams?: Streams,
  ): Promise<unknown> {
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
    const signal = AbortSignal.any([session, AbortSignal.timeout(remaining)]);
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
    ]);
    const size = Buffer.byteLength(JSON.stringify(result ?? null));
    const limit = this.peerFrameLimit - frameSlack;
    if (size > limit)
      throw new WireError(
        "resource_exhausted",
        `the reply is ${size} bytes, over the ${limit}-byte frame limit; ask for fewer rows or fields`,
      );
    return result;
  }

  private goodbye(): void {
    const ws = this.socket;
    if (ws) this.goodbyeOn(ws);
  }
  private goodbyeOn(ws: WebSocket): void {
    if (this.goodbyeSent && this.socket === ws) return;
    this.goodbyeSent = true;
    this.send(ws, {
      v: protocolVersion,
      kind: "evt",
      type: typeGoodbye,
      data: { reason: "shutdown" },
    });
    ws.close(1000, "shutdown");
  }

  /** Writes one frame; false when the connection can't take it. */
  private send(ws: WebSocket, env: Envelope): boolean {
    if (ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(env));
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
function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
/** Maps a handler's error to the wire. */
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
        return new WireError("not_found", error.message);
      case "conflict":
        return new WireError("conflict", error.message);
      case "unavailable":
      case "timeout":
        return new WireError("unavailable", error.message);
    }
  }
  const first = describe(error).split("\n")[0] ?? "";
  return new WireError("internal", first);
}
