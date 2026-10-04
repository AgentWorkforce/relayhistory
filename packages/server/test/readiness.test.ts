import { once } from "node:events";
import net from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { databaseReadiness } from "../src/readiness.js";

describe("databaseReadiness", () => {
  // Accepts connections and never answers: a database that is stuck or unreachable
  // behind a live socket.
  let blackhole: net.Server;
  let accepted = 0;
  let url: string;
  beforeAll(async () => {
    blackhole = net.createServer((socket) => {
      accepted += 1;
      socket.on("error", () => {});
    });
    blackhole.listen(0, "127.0.0.1");
    await once(blackhole, "listening");
    url = `postgres://ready@127.0.0.1:${(blackhole.address() as net.AddressInfo).port}/x`;
  });
  afterAll(() => new Promise<void>((done) => blackhole.close(() => done())));

  it("answers not ready within its bound, and concurrent probes share one check", async () => {
    const ready = databaseReadiness(url, 300);
    const started = Date.now();
    const results = await Promise.all(Array.from({ length: 5 }, () => ready()));
    expect(results).toEqual([false, false, false, false, false]);
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(accepted).toBe(1);
    // The next probe starts a fresh check.
    expect(await ready()).toBe(false);
    expect(accepted).toBe(2);
  });

  it.skipIf(!process.env.TEST_ADMIN_DATABASE_URL)(
    "answers ready for a live database",
    async () => {
      expect(
        await databaseReadiness(process.env.TEST_ADMIN_DATABASE_URL!)(),
      ).toBe(true);
    },
  );
});
