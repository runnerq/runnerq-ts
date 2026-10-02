// The command messages (activities.cancel, .retry, .run_now, .reschedule, .set_priority,
// .delete, .signal), applied through a CommandStorage: a port of runnerq-go's
// conductor/commands.go.
import { createHash } from "node:crypto";
import { businessKey, isTimestamp, parseUuid } from "../codec.js";
import { RunnerQError } from "../errors.js";
import type {
  Command,
  CommandKind,
  CommandResult,
  CommandStorage,
} from "../storage.js";
import { decodeRequest, type Spec } from "./decode.js";
import {
  cancelCascadeValues,
  deleteCascadeValues,
  type CancelRequest,
  type CommandItem,
  type CommandResult as CommandReply,
  type DeleteRequest,
  type RescheduleRequest,
  type RetryRequest,
  type RunNowRequest,
  type SetPriorityRequest,
  type SignalRequest,
} from "./protocol.js";
import { specs } from "./specs.js";
import { WireError, type RequestType, type Routes } from "./wire.js";

/** Each command's kind and request: a command accepts only its own fields. */
const commandTypes = [
  ["activities.cancel", "cancel", specs.CancelRequest],
  ["activities.retry", "retry", specs.RetryRequest],
  ["activities.run_now", "run_now", specs.RunNowRequest],
  ["activities.reschedule", "reschedule", specs.RescheduleRequest],
  ["activities.set_priority", "set_priority", specs.SetPriorityRequest],
  ["activities.delete", "delete", specs.DeleteRequest],
  ["activities.signal", "signal", specs.SignalRequest],
] as const satisfies readonly (readonly [RequestType, CommandKind, Spec])[];

type CommandRequest =
  | CancelRequest
  | RetryRequest
  | RunNowRequest
  | RescheduleRequest
  | SetPriorityRequest
  | DeleteRequest
  | SignalRequest;

const fieldError = (
  code: "invalid_argument" | "failed_precondition",
  field: string,
  message: string,
) => new WireError(code, message, { field });
const quote = (s: string) => JSON.stringify(s);

const loneSurrogate =
  /[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/g;
/**
 * JSON as Go's encoding/json re-marshals decoded JSON: object keys in UTF-8 byte order,
 * `<`, `>`, `&`, U+2028 and U+2029 escaped, invalid UTF-16 replaced, -0 kept.
 */
function goJson(v: unknown): string {
  if (v === null || typeof v !== "object") {
    if (typeof v === "string")
      return JSON.stringify(v.replace(loneSurrogate, "\ufffd")).replace(
        /[<>&\u2028\u2029]/g,
        (ch) => "\\u" + ch.charCodeAt(0).toString(16).padStart(4, "0"),
      );
    if (typeof v === "number" && Object.is(v, -0)) return "-0";
    return JSON.stringify(v);
  }
  if (Array.isArray(v)) return `[${v.map(goJson).join(",")}]`;
  const keys = Object.keys(v).sort((a, b) =>
    Buffer.compare(Buffer.from(a), Buffer.from(b)),
  );
  return `{${keys.map((k) => `${goJson(k)}:${goJson((v as Record<string, unknown>)[k])}`).join(",")}}`;
}

/** Identifies a command's input independent of JSON key order, as Go's agent does. */
export function fingerprint(data: unknown): string {
  return createHash("sha256").update(goJson(data)).digest("hex");
}

/** The command handlers and what they advertise, acting on `queue` through `cs`. */
export class Commands {
  constructor(
    private readonly cs: CommandStorage,
    private readonly queue: string,
    /** Stops the activity if it is running in this process. */
    private readonly interrupt?: (activityId: string) => void,
  ) {}

  routes(): Routes {
    const out: Routes = {};
    for (const [type, kind, spec] of commandTypes)
      out[type] = {
        capability: {
          v: 1,
          targets:
            kind === "signal"
              ? ["filter", "idempotency_key", "ids"]
              : ["filter", "ids"],
        },
        handler: (data) => this.command(kind, spec, data),
      };
    return out;
  }

  private async command(
    kind: CommandKind,
    spec: Spec,
    data: unknown,
  ): Promise<CommandReply> {
    const req = decodeRequest<CommandRequest>(spec, data);
    const { cmd, badIds } = this.toCommand(kind, req);
    cmd.fingerprint = fingerprint(data ?? {});

    let res: CommandResult = {
      matched: 0,
      applied: 0,
      cascaded: 0,
      more: false,
      items: [],
      replayed: false,
    };
    // Unless every id was foreign.
    if (!req.target?.ids?.length || cmd.target.ids?.length)
      try {
        res = await this.cs.applyCommand(cmd);
      } catch (error) {
        if (error instanceof RunnerQError && error.code === "conflict")
          throw new WireError("conflict", error.message);
        throw error;
      }
    // Stop a cancelled activity running here now, not at its next heartbeat.
    if (kind === "cancel" && !cmd.dryRun && !res.replayed && this.interrupt)
      for (const it of res.items)
        if (it.outcome === "applied") this.interrupt(it.id);

    const results = res.items.map((it) => {
      const ci: CommandItem = { id: it.id, outcome: it.outcome };
      if (it.status) ci.status = it.status;
      if (it.error?.message) {
        const e = it.error;
        ci.error =
          e.kind === "not_found"
            ? { code: "not_found", message: e.message }
            : {
                code: "failed_precondition",
                message: e.message,
                ...(it.status && { details: { status: it.status } }),
              };
      }
      return ci;
    });
    // Ids this backend could not have issued do not exist.
    for (const id of badIds)
      results.push({
        id,
        outcome: "skipped",
        error: { code: "not_found", message: "no such activity" },
      });
    return {
      matched: res.matched,
      applied: res.applied,
      ...(res.cascaded && { cascaded: res.cascaded }),
      more: res.more,
      ...(res.replayed && { replayed: true }),
      results,
    };
  }

  /** Also returns the target ids this backend could not have issued. */
  private toCommand(
    kind: CommandKind,
    req: CommandRequest,
  ): { cmd: Command; badIds: string[] } {
    const t = req.target ?? {};
    const cmd: Command = {
      id: req.command_id ?? "",
      kind,
      target: {},
      dryRun: !!req.dry_run,
      reason: req.reason ?? "",
    };
    if (t.queue && t.queue !== this.queue)
      throw fieldError(
        "failed_precondition",
        "target.queue",
        `this executor serves queue ${quote(this.queue)}, not ${quote(t.queue)}`,
      );

    // Enums decode as plain strings: they are checked here.
    switch (kind) {
      case "cancel": {
        const { cascade } = req as CancelRequest;
        if (cascade && !cancelCascadeValues.includes(cascade))
          throw fieldError(
            "invalid_argument",
            "cascade",
            "cascade must be children or none",
          );
        cmd.cascadeChildren = cascade !== "none"; // cascading is the default
        break;
      }
      case "delete": {
        const { cascade } = req as DeleteRequest;
        if (cascade && !deleteCascadeValues.includes(cascade))
          throw fieldError(
            "invalid_argument",
            "cascade",
            "delete always removes the whole tree",
          );
        break;
      }
      case "retry":
        cmd.resetAttempts = !!(req as RetryRequest).reset_attempts;
        break;
      case "reschedule": {
        const { at } = req as RescheduleRequest;
        if (!isTimestamp(at))
          throw fieldError(
            "invalid_argument",
            "at",
            "at must be an RFC 3339 timestamp",
          );
        cmd.at = at;
        break;
      }
      case "set_priority":
        cmd.priority = (req as SetPriorityRequest).priority ?? 0;
        break;
      case "signal": {
        const r = req as SignalRequest;
        cmd.signalName = r.name ?? "";
        if ("payload" in r) cmd.signalPayload = r.payload as never;
        break;
      }
    }

    const badIds: string[] = [];
    if (t.ids?.length) {
      const ids: string[] = [];
      for (const s of t.ids) {
        const id = parseUuid(s);
        if (id) ids.push(id);
        else badIds.push(s);
      }
      cmd.target.ids = ids;
    } else if (t.filter) {
      cmd.target.filter = t.filter;
      cmd.target.max = t.max ?? 0;
    } else if (t.idempotency_key) {
      if (!t.type)
        throw fieldError(
          "invalid_argument",
          "target.type",
          "an idempotency_key target needs the activity type",
        );
      cmd.target.idempotencyKey = businessKey(t.idempotency_key, t.type);
    }
    return { cmd, badIds };
  }
}
