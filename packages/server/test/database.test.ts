// prepareDatabase when the database connection drops mid-run: the run fails with that
// failure instead of crashing the process through an unheard 'error' event.
import { once } from "node:events";
import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { prepareDatabase } from "../src/database.js";
import { silentLogger } from "../src/log.js";

const AUTH_OK = Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]);
const READY = Buffer.from([0x5a, 0, 0, 0, 5, 0x49]);
const commandComplete = (tag: string) => {
  const text = Buffer.from(`${tag}\0`);
  const header = Buffer.alloc(5);
  header[0] = 0x43;
  header.writeInt32BE(4 + text.length, 1);
  return Buffer.concat([header, text, READY]);
};

/** A PostgreSQL that accepts the session, then drops it at a chosen point. */
async function droppingPostgres(dropAt: "first-query" | "after-first-answer") {
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("error", () => {});
    socket.on("close", () => sockets.delete(socket));
    let started = false;
    socket.on("data", () => {
      if (!started) {
        started = true;
        socket.write(Buffer.concat([AUTH_OK, READY]));
      } else if (dropAt === "first-query") {
        socket.destroy();
      } else {
        // Answer the first statement, then drop while the client is between statements.
        socket.end(commandComplete("BEGIN"));
      }
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    url: `postgres://migrate@127.0.0.1:${(server.address() as net.AddressInfo).port}/x`,
    stop: () =>
      new Promise<void>((done) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => done());
      }),
  };
}

describe("prepareDatabase on a dropped connection", () => {
  const uncaught: unknown[] = [];
  const record = (error: unknown) => uncaught.push(error);
  let stop: (() => Promise<void>) | undefined;
  afterEach(async () => {
    process.removeListener("uncaughtException", record);
    await stop?.();
    stop = undefined;
    uncaught.length = 0;
  });

  it.each(["first-query", "after-first-answer"] as const)(
    "a connection dropped at %s fails the run without an uncaught error",
    async (dropAt) => {
      process.on("uncaughtException", record);
      const pgFake = await droppingPostgres(dropAt);
      stop = pgFake.stop;
      await expect(
        prepareDatabase(pgFake.url, { log: silentLogger }),
      ).rejects.toThrow(/Connection terminated|not queryable|ECONNRESET/);
      // Let any late 'error' emission run before checking nothing escaped.
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(uncaught).toEqual([]);
    },
  );
});
