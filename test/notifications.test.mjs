import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { Notifications } from "../dist/postgres/notifications.js";

test(
  "notification shutdown finishes during an unfinished PostgreSQL handshake",
  { timeout: 5000 },
  async () => {
    const sockets = new Set();
    // Accept TCP but withhold PostgreSQL's authentication response, making the
    // close-during-connect race deterministic without requiring a database.
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
      socket.on("error", () => {});
      socket.resume();
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const notifications = new Notifications(
      {
        host: "127.0.0.1",
        port: server.address().port,
        user: "postgres",
        database: "test",
        connectionTimeoutMillis: 2000,
      },
      {},
      "test_shutdown",
    );
    const timer = new AbortController();
    let subscription;
    try {
      const connected = once(server, "connection");
      subscription = notifications.subscribe("work");
      await connected;
      await Promise.race([
        notifications.close(),
        delay(1000, undefined, { signal: timer.signal }).then(() => {
          assert.fail(
            "Notification shutdown remained stuck in client.connect()",
          );
        }),
      ]);
    } finally {
      timer.abort();
      subscription?.close();
      for (const socket of sockets) socket.destroy();
      await new Promise((resolve) => server.close(resolve));
    }
  },
);
