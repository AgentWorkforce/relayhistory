import { once } from "node:events";
import type { Server } from "node:http";
import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { createLogger } from "../src/log.js";
import { listen } from "../src/server.js";

const ok = () => new Response("ok");

describe("listen", () => {
  const cleanup: Array<() => Promise<void>> = [];
  const uncaught: unknown[] = [];
  const record = (error: unknown) => uncaught.push(error);
  afterEach(async () => {
    process.removeListener("uncaughtException", record);
    while (cleanup.length) await cleanup.pop()!();
    uncaught.length = 0;
  });
  const closing = (server: Server | net.Server) => () =>
    new Promise<void>((done) => server.close(() => done()));

  it("logs every error after listening and keeps serving", async () => {
    process.on("uncaughtException", record);
    const lines: string[] = [];
    const server = await listen(
      ok,
      "127.0.0.1",
      0,
      createLogger((line) => lines.push(line)),
    );
    cleanup.push(closing(server));
    // Only the persistent handler remains: the startup rejection is detached.
    expect(server.listenerCount("error")).toBe(1);

    for (const n of [1, 2, 3])
      server.emit(
        "error",
        Object.assign(new Error(`accept EMFILE ${n} /home/me/secret`), {
          code: "EMFILE",
          syscall: "accept",
        }),
      );
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(uncaught).toEqual([]);
    const records = lines.map((line) => JSON.parse(line));
    expect(records).toHaveLength(3);
    for (const record of records) {
      expect(record).toMatchObject({
        level: "error",
        message: "server error",
        code: "EMFILE",
        syscall: "accept",
      });
      expect(Object.keys(record).sort()).toEqual([
        "code",
        "level",
        "message",
        "syscall",
        "time",
      ]);
    }
    expect(lines.join("")).not.toContain("secret");

    const { port } = server.address() as net.AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/`);
    expect(await response.text()).toBe("ok");
  });

  it("rejects when the port cannot be bound", async () => {
    const blocker = net.createServer();
    blocker.listen(0, "127.0.0.1");
    await once(blocker, "listening");
    cleanup.push(closing(blocker));
    const { port } = blocker.address() as net.AddressInfo;
    await expect(
      listen(
        ok,
        "127.0.0.1",
        port,
        createLogger(() => {}),
      ),
    ).rejects.toMatchObject({
      code: "EADDRINUSE",
    });
  });
});
