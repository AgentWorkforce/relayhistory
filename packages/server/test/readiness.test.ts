import { once } from "node:events";
import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { databaseReadiness } from "../src/readiness.js";

/** Just enough of the PostgreSQL wire protocol to misbehave in chosen ways. */
type Mode = "blackhole" | "silent-after-connect" | "healthy" | "recovering";
const AUTH_OK = Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]);
const READY = Buffer.from([0x5a, 0, 0, 0, 5, 0x49]);
const SELECT_DONE = (() => {
  const tag = Buffer.from("SELECT 1\0");
  const header = Buffer.alloc(5);
  header[0] = 0x43;
  header.writeInt32BE(4 + tag.length, 1);
  return Buffer.concat([header, tag, READY]);
})();

async function fakePostgres(mode: Mode) {
  const sockets = new Set<net.Socket>();
  let accepted = 0;
  let closed = 0;
  const server = net.createServer((socket) => {
    accepted += 1;
    // "recovering": the first connection goes silent after connecting, later ones answer.
    const answers =
      mode === "healthy" || (mode === "recovering" && accepted > 1);
    sockets.add(socket);
    socket.on("error", () => {});
    socket.on("close", () => {
      closed += 1;
      sockets.delete(socket);
    });
    let started = false;
    socket.on("data", (chunk) => {
      if (mode === "blackhole") return;
      if (!started) {
        started = true;
        socket.write(Buffer.concat([AUTH_OK, READY]));
        return;
      }
      if (answers && chunk[0] === 0x51) socket.write(SELECT_DONE);
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = (server.address() as net.AddressInfo).port;
  return {
    url: `postgres://ready@127.0.0.1:${port}/x`,
    accepted: () => accepted,
    closed: () => closed,
    stop: () =>
      new Promise<void>((done) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => done());
      }),
  };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

describe("databaseReadiness", () => {
  let stop: (() => Promise<void>) | undefined;
  afterEach(async () => {
    await stop?.();
    stop = undefined;
  });

  it("a server that never answers: not ready within the bound, one shared check, no leaked socket", async () => {
    const pgFake = await fakePostgres("blackhole");
    stop = pgFake.stop;
    const ready = databaseReadiness(pgFake.url, 300);
    const started = Date.now();
    expect(await Promise.all(Array.from({ length: 5 }, () => ready()))).toEqual(
      [false, false, false, false, false],
    );
    expect(Date.now() - started).toBeLessThan(1_500);
    expect(pgFake.accepted()).toBe(1);
    await settle();
    expect(pgFake.closed()).toBe(1);
  });

  it("a database that stops answering after connecting: bounded, and the next probe reconnects", async () => {
    const pgFake = await fakePostgres("silent-after-connect");
    stop = pgFake.stop;
    const ready = databaseReadiness(pgFake.url, 300);
    const started = Date.now();
    expect(await ready()).toBe(false);
    expect(Date.now() - started).toBeLessThan(1_500);
    await settle();
    expect(pgFake.closed()).toBe(1);
    expect(await ready()).toBe(false);
    expect(pgFake.accepted()).toBe(2);
  });

  it("a probe right after a timeout recovers on a fresh connection", async () => {
    const pgFake = await fakePostgres("recovering");
    stop = pgFake.stop;
    const ready = databaseReadiness(pgFake.url, 300);
    expect(await ready()).toBe(false);
    // Immediately, while the timed-out attempt is still settling.
    expect(await ready()).toBe(true);
    expect(await ready()).toBe(true);
    expect(pgFake.accepted()).toBe(2);
  });

  it("a healthy database: probes reuse one connection, and close ends it", async () => {
    const pgFake = await fakePostgres("healthy");
    stop = pgFake.stop;
    const ready = databaseReadiness(pgFake.url, 300);
    expect(await ready()).toBe(true);
    expect(await ready()).toBe(true);
    expect(await ready()).toBe(true);
    expect(pgFake.accepted()).toBe(1);
    await ready.close();
    await settle();
    expect(pgFake.closed()).toBe(1);
  });

  it.skipIf(!process.env.TEST_ADMIN_DATABASE_URL)(
    "a live PostgreSQL is ready",
    async () => {
      const ready = databaseReadiness(process.env.TEST_ADMIN_DATABASE_URL!);
      expect(await ready()).toBe(true);
      expect(await ready()).toBe(true);
      await ready.close();
    },
  );
});
