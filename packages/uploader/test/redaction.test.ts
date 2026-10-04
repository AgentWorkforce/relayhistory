// A server (or a proxy in front of it) controls every byte of an error response. None
// of it may reach an error message or a log line except codes the uploader recognises.
import { describe, expect, it } from "vitest";
import { createLogger } from "../src/log.js";
import { describe as describeFailure } from "../src/failures.js";
import { upload } from "../src/uploader.js";
import {
  LIMITS,
  MemoryFeed,
  change,
  config,
  fakeServer,
  json,
  noSleep,
} from "./support.js";

const SECRET = "rth_st_SYNTHETIC-echoed-credential-Zq9";

describe("server-controlled error text", () => {
  it.each([401, 403, 400, 413, 422, 429, 503, 409])(
    "a %i whose error.code echoes a credential never reaches messages or logs",
    async (status) => {
      const lines: string[] = [];
      const log = createLogger((line) => lines.push(line));
      const fake = fakeServer(({ url }) =>
        url.endsWith("/v1/delivery/limits")
          ? json(LIMITS)
          : json(
              { error: { code: SECRET, message: `Bearer ${SECRET}` } },
              status,
            ),
      );
      let thrown: unknown;
      try {
        await upload({
          config: config(),
          log,
          feed: new MemoryFeed([change(1)]),
          fetch: fake.fetch,
          sleep: noSleep,
          retry: { attempts: 2, baseMs: 1, maxMs: 1 },
        });
      } catch (error) {
        thrown = error;
      }
      expect(thrown).toBeInstanceOf(Error);
      log.error("upload failed", describeFailure(thrown));
      expect(String((thrown as Error).message)).not.toContain(SECRET);
      expect(lines.join("\n")).not.toContain(SECRET);
    },
  );

  it("keeps recognised codes, which are what an operator acts on", async () => {
    const fake = fakeServer(({ url }) =>
      url.endsWith("/v1/delivery/limits")
        ? json(LIMITS)
        : json(
            { error: { code: "delivery_account_mismatch", message: "x" } },
            403,
          ),
    );
    await expect(
      upload({
        config: config(),
        log: createLogger(() => {}),
        feed: new MemoryFeed([change(1)]),
        fetch: fake.fetch,
        sleep: noSleep,
      }),
    ).rejects.toThrow(/accountId does not match/);
    const invalid = fakeServer(({ url }) =>
      url.endsWith("/v1/delivery/limits")
        ? json(LIMITS)
        : json({ error: { code: "invalid_delivery" } }, 400),
    );
    await expect(
      upload({
        config: config(),
        log: createLogger(() => {}),
        feed: new MemoryFeed([change(1)]),
        fetch: invalid.fetch,
        sleep: noSleep,
      }),
    ).rejects.toThrow("server refused the batch (400 invalid_delivery)");
  });

  it("logs a receipt id only in the engine's own form", async () => {
    const lines: string[] = [];
    const fake = fakeServer(({ url, body }) =>
      url.endsWith("/v1/delivery/limits")
        ? json(LIMITS)
        : json({
            protocolVersion: 1,
            receiptId: `rhr_${SECRET}`,
            batchId: body.batch.batch_id,
            acceptedRevisionIds: body.batch.records.map(
              (r: { revision_id: string }) => r.revision_id,
            ),
            unsupportedRevisionIds: [],
            acceptanceLevel: "durable",
          }),
    );
    await upload({
      config: config(),
      log: createLogger((line) => lines.push(line)),
      feed: new MemoryFeed([change(1)]),
      fetch: fake.fetch,
      sleep: noSleep,
    });
    expect(lines.join("\n")).toContain("batch accepted");
    expect(lines.join("\n")).not.toContain(SECRET);
  });
});
