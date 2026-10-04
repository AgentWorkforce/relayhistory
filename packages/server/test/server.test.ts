// Process lifecycle against real PostgreSQL. Set TEST_ADMIN_DATABASE_URL (CI does).
import { randomBytes } from "node:crypto";
import http from "node:http";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";
import { openDatabase } from "../src/database.js";
import { createToken } from "../src/tokens.js";
import { silentLogger } from "../src/log.js";
import {
  CLEANUP_GRACE_MS,
  cleanupBudgetMs,
  startServer,
} from "../src/server.js";

const adminUrl = process.env.TEST_ADMIN_DATABASE_URL;

describe("cleanupBudgetMs", () => {
  it("never leaves cleanup less than the grace, even after a full drain", () => {
    expect(cleanupBudgetMs(1_000, 1_000)).toBe(CLEANUP_GRACE_MS);
    expect(cleanupBudgetMs(1_000, 9_000)).toBe(CLEANUP_GRACE_MS);
    expect(cleanupBudgetMs(60_000, 1_000)).toBe(59_000);
  });
});

describe.skipIf(!adminUrl)("server shutdown", () => {
  const name = `rh_server_life_${randomBytes(4).toString("hex")}`;
  let admin: pg.Client;
  let url: string;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: adminUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE ${name}`);
    const target = new URL(adminUrl!);
    target.pathname = `/${name}`;
    url = target.toString();
  });

  afterAll(async () => {
    await admin?.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin?.end();
  });

  it("a request held past the deadline is cut off and cleanup still completes", async () => {
    const config = loadConfig({
      DATABASE_URL: url,
      HOST: "127.0.0.1",
      PORT: "0",
      RELAYHISTORY_SHUTDOWN_TIMEOUT_MS: "200",
    });
    const server = await startServer(config, silentLogger);
    const database = openDatabase(url, { max: 1, log: silentLogger });
    const { token } = await createToken(database.db, {
      orgId: "acme",
      workspaceId: "main",
      label: "shutdown",
    });
    await database.close();
    // An upload whose body never finishes, so its request stays in flight.
    const stalledUpload = (authorization?: string) => {
      const responses: number[] = [];
      const request = http.request(
        {
          host: "127.0.0.1",
          port: server.port,
          method: "POST",
          path: "/v1/delivery/batches",
          headers: {
            ...(authorization ? { authorization } : {}),
            "content-type": "application/json",
            "transfer-encoding": "chunked",
          },
        },
        (response) => {
          responses.push(response.statusCode ?? 0);
          response.resume();
        },
      );
      request.on("error", () => {});
      request.write("{");
      return { request, responses };
    };

    let anonymous: ReturnType<typeof stalledUpload> | undefined;
    let upload: ReturnType<typeof stalledUpload> | undefined;
    let closed = false;
    try {
      // Without the token, authentication answers at once: the handler never runs.
      anonymous = stalledUpload();
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(anonymous.responses).toEqual([401]);

      // With it, the request passes authentication and the delivery handler waits on the
      // body, so it is still unanswered when shutdown starts.
      upload = stalledUpload(`Bearer ${token}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(upload.responses).toEqual([]);

      const started = Date.now();
      closed = true;
      const drained = await server.close();
      const elapsed = Date.now() - started;
      expect(drained).toBe(true);
      // Held until the 200 ms cutoff forced it closed, then cleanup within its grace.
      expect(elapsed).toBeGreaterThanOrEqual(180);
      expect(elapsed).toBeLessThan(200 + CLEANUP_GRACE_MS);
      expect(upload.responses).toEqual([]);
    } finally {
      // A failed assertion must not leave the server listening or a socket open.
      anonymous?.request.destroy();
      upload?.request.destroy();
      if (!closed) await server.close();
    }
  });
});
