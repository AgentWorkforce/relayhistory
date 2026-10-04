// The uploader names records exactly as `ai-hist export` does, on a real local store
// built through the public SDK.
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  canonicalDeliveryJson,
  exportHistory,
  getChangesPage,
  sync,
  commitChanges,
} from "ai-hist";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { parseEndpoint } from "../src/config.js";
import { silentLogger } from "../src/log.js";
import { deliveryRecord } from "../src/records.js";
import { consumerName, upload } from "../src/uploader.js";

describe("feed records match export records", () => {
  let root: string;
  let dbPath: string;
  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), "rh-upload-identity-"));
    const home = join(root, "home");
    const project = join(home, ".claude", "projects", "work-app");
    await mkdir(project, { recursive: true });
    const session = "identity-1";
    const lines = [
      {
        type: "user",
        uuid: "u1",
        sessionId: session,
        cwd: "/work/app",
        timestamp: "2026-09-30T10:00:00.000Z",
        message: { role: "user", content: "ünïcödé \u0001 prompt" },
      },
      {
        type: "assistant",
        uuid: "a1",
        parentUuid: "u1",
        sessionId: session,
        cwd: "/work/app",
        timestamp: "2026-09-30T10:00:02.000Z",
        message: {
          role: "assistant",
          model: "claude-test",
          content: [
            { type: "text", text: "Done." },
            {
              type: "tool_use",
              id: "t1",
              name: "Read",
              input: { path: "/work/app/x.ts" },
            },
          ],
        },
      },
    ];
    await writeFile(
      join(project, `${session}.jsonl`),
      `${lines.map((l) => JSON.stringify(l)).join("\n")}\n`,
    );
    const saved = process.env.HOME;
    process.env.HOME = home;
    dbPath = join(root, "history.db");
    try {
      await sync({ dbPath });
    } finally {
      if (saved === undefined) delete process.env.HOME;
      else process.env.HOME = saved;
    }
  });
  afterAll(() => rm(root, { recursive: true, force: true }));

  it("record, revision, origin, session and payload agree", async () => {
    const page = await getChangesPage({ dbPath, limit: 10_000 });
    expect(page.changes.length).toBeGreaterThan(3);
    const mapped = new Map(
      page.changes.map((c) => {
        const record = deliveryRecord(c, page.head.epoch);
        return [record.record_id, record];
      }),
    );
    const kinds = [...new Set(page.changes.map((c) => c.kind))];
    let exported = 0;
    for await (const record of exportHistory(
      {
        all_sources: true,
        sources: [],
        sessions: [],
        kinds,
        excluded_sessions: [],
      },
      { dbPath },
    )) {
      exported += 1;
      const ours = mapped.get(record.record_id);
      expect(ours, record.kind).toBeDefined();
      expect({
        ...ours,
        schema_version: 0,
        payload: canonicalDeliveryJson(ours!.payload),
      }).toEqual({
        ...record,
        schema_version: 0,
        payload: canonicalDeliveryJson(record.payload),
      });
    }
    expect(exported).toBe(page.changes.length);
  });

  it("an idle upload reports the cursor the store holds", async () => {
    // file_edit: a kind this store has none of, so every page is empty and positioned
    // at the head, as an idle run after unrelated evidence would be.
    const config = {
      endpoint: parseEndpoint("https://history.example.com"),
      token: "rth_st_unused",
      accountId: `relayhistory:${"c".repeat(64)}`,
      dbPath,
      selection: {
        all_sources: true,
        sources: [],
        sessions: [],
        kinds: ["file_edit" as const],
        excluded_sessions: [],
      },
      instanceId: "identity",
      limits: { maxRecords: 100, maxBytes: 1_048_576 },
    };
    const unreachable = (async () => {
      throw new Error("nothing should be sent");
    }) as typeof fetch;
    const summary = await upload({
      config,
      log: silentLogger,
      fetch: unreachable,
    });
    expect(summary.selected).toBe(0);
    // A commit behind the stored cursor moves nothing and returns the cursor as stored.
    const stored = await commitChanges(
      consumerName(config),
      { epoch: summary.cursor!.epoch, revision: 0 },
      { dbPath, kinds: ["file_edit"] },
    );
    expect(summary.cursor).toEqual(stored.cursor);
    expect(summary.cursor!.revision).toBeGreaterThan(0);
  });
});
