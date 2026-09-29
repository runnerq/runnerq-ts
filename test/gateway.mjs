import http from "node:http";
import { WebSocketServer } from "ws";
import { until } from "./helpers.mjs";

export const key = "rqk_test";
// fakeGateway is RunnerQ Cloud's side of an agent session.
export async function fakeGateway(
  t,
  { config = {}, rejectN = 0, frame = 4 << 20 } = {},
) {
  const server = http.createServer();
  const wss = new WebSocketServer({ noServer: true });
  const g = {
    hellos: [],
    events: [],
    sockets: [],
    replies: new Map(),
    rejected: 0,
    nextId: 0,
  };
  server.on("upgrade", (req, socket, head) => {
    if (
      !new URL(req.url, "http://x").pathname.endsWith("/v1/agent") ||
      req.headers.authorization !== `Bearer ${key}` ||
      g.rejected < rejectN
    ) {
      g.rejected++;
      socket.end("HTTP/1.1 401 Unauthorized\r\n\r\n");
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
      g.sockets.push(ws);
      ws.on("message", (raw) => {
        const env = JSON.parse(raw.toString());
        Object.defineProperty(env, "bytes", { value: raw.length });
        if (env.type === "hello") {
          g.hellos.push(env.data);
          ws.send(
            JSON.stringify({
              v: 1,
              kind: "res",
              id: env.id,
              type: "hello",
              data: {
                version: 1,
                session_id: "sess-" + g.hellos.length,
                app: { id: "app-1", name: "test-app" },
                config,
                limits: { max_frame_bytes: frame },
              },
            }),
          );
        } else if (env.kind === "res") {
          g.replies.get(env.id)?.(env);
        } else if (env.kind === "evt") {
          g.events.push(env);
        }
      });
    });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  g.url = `http://127.0.0.1:${server.address().port}`;
  g.socket = () => g.sockets.at(-1);
  g.until = (fn, what, ms = 5_000) => until(fn, ms, what);
  g.call = (type, data, meta) =>
    new Promise((resolve, reject) => {
      const id = String(++g.nextId);
      const timer = setTimeout(
        () => reject(new Error(`no reply to ${type}`)),
        10_000,
      );
      g.replies.set(id, (env) => {
        clearTimeout(timer);
        g.replies.delete(id);
        resolve(env);
      });
      g.socket().send(
        JSON.stringify({ v: 1, kind: "req", id, type, data, meta }),
      );
    });
  g.send = (type, data) =>
    g.socket().send(JSON.stringify({ v: 1, kind: "evt", type, data }));
  g.eventsOf = (type) => g.events.filter((e) => e.type === type);
  t.after(async () => {
    for (const s of g.sockets) s.terminate();
    wss.close();
    await new Promise((r) => server.close(r));
  });
  return g;
}
