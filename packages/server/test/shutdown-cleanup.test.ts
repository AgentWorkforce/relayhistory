// A pool.end() that fails during shutdown, before and after the cleanup deadline: the
// failure must reach close()'s caller or be ignored, never become an unhandled
// rejection, and its driver text must never be logged. Real server, real PostgreSQL.
import { randomBytes } from "node:crypto";
import pg from "pg";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { loadConfig } from "../src/config.js";
import { createLogger } from "../src/log.js";
import { CLEANUP_GRACE_MS, startServer } from "../src/server.js";

const adminUrl = process.env.TEST_ADMIN_DATABASE_URL;
const SENTINEL = "SENTINEL-driver-text password=hunter2 host=db.internal";

describe.skipIf(!adminUrl)("shutdown when closing the pool fails", () => {
  const name = `rh_shutdown_${randomBytes(4).toString("hex")}`;
  let admin: pg.Client;
  let url: string;
  const unhandled: unknown[] = [];
  const record = (reason: unknown) => unhandled.push(reason);

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: adminUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE ${name}`);
    const target = new URL(adminUrl!);
    target.pathname = `/${name}`;
    url = target.toString();
    process.on("unhandledRejection", record);
  });
  afterEach(() => {
    vi.restoreAllMocks();
    unhandled.length = 0;
  });
  afterAll(async () => {
    process.removeListener("unhandledRejection", record);
    await admin?.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin?.end();
  });

  async function started(lines: string[]) {
    return startServer(
      loadConfig({
        DATABASE_URL: url,
        HOST: "127.0.0.1",
        PORT: "0",
        RELAYHISTORY_SHUTDOWN_TIMEOUT_MS: "100",
      }),
      createLogger((line) => lines.push(line)),
    );
  }

  it("a failure before the deadline rejects close() and is not logged by the server", async () => {
    const lines: string[] = [];
    const server = await started(lines);
    const realEnd = pg.Pool.prototype.end;
    vi.spyOn(pg.Pool.prototype, "end").mockImplementation(async function (
      this: pg.Pool,
    ) {
      await (realEnd as (this: pg.Pool) => Promise<void>).call(this);
      throw Object.assign(new Error(SENTINEL), { code: "57P01" });
    });
    await expect(server.close()).rejects.toMatchObject({ code: "57P01" });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(unhandled).toEqual([]);
    expect(lines.join("\n")).not.toContain("SENTINEL");
  });

  it(
    "a failure after the deadline is handled by the race and never becomes unhandled",
    async () => {
      const lines: string[] = [];
      const server = await started(lines);
      const realEnd = pg.Pool.prototype.end;
      // Settled at the moment the injected rejection is thrown, so the test waits for
      // the rejection itself rather than for a guessed delay.
      let markRejected!: () => void;
      let threw = false;
      const rejected = new Promise<void>((resolve) => (markRejected = resolve));
      // Cleanup loses the race: pool.end() settles only after the cleanup grace, then fails.
      vi.spyOn(pg.Pool.prototype, "end").mockImplementation(async function (
        this: pg.Pool,
      ) {
        await (realEnd as (this: pg.Pool) => Promise<void>).call(this);
        await new Promise((resolve) =>
          setTimeout(resolve, CLEANUP_GRACE_MS + 300),
        );
        try {
          throw Object.assign(new Error(SENTINEL), { code: "57P01" });
        } finally {
          threw = true;
          markRejected();
        }
      });
      const closed = server.close();
      expect(await closed).toBe(false);
      // close() returned at its deadline, before the injected failure: prove the failure
      // then actually happened, and give Node the turn in which it reports a rejection
      // nobody handled.
      expect(threw).toBe(false);
      await rejected;
      expect(threw).toBe(true);
      await new Promise((resolve) => setImmediate(resolve));
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(unhandled).toEqual([]);
      expect(lines.join("\n")).toContain("shutdown cleanup timed out");
      expect(lines.join("\n")).not.toContain("SENTINEL");
    },
    CLEANUP_GRACE_MS + 10_000,
  );
});
