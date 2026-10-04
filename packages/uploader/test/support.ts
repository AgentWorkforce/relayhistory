import type {
  ChangesPage,
  ChangesPageOptions,
  FeedChange,
  Watermark,
} from "ai-hist";
import { parseEndpoint, type UploaderConfig } from "../src/config.js";
import type { Feed } from "../src/uploader.js";

export const ACCOUNT = `relayhistory:${"a".repeat(64)}`;

export function config(
  overrides: Partial<UploaderConfig> = {},
): UploaderConfig {
  return {
    endpoint: parseEndpoint("https://history.example.com"),
    token: "rth_st_secret-token-value",
    accountId: ACCOUNT,
    selection: {
      all_sources: false,
      sources: [],
      sessions: [{ source: "claude", session_id: "picked" }],
      kinds: ["session", "session_event"],
      excluded_sessions: [],
    },
    instanceId: "laptop",
    limits: { maxRecords: 100, maxBytes: 1_048_576 },
    ...overrides,
  };
}

export function change(
  revision: number,
  overrides: Partial<FeedChange> = {},
): FeedChange {
  const sessionId = overrides.sessionId ?? "picked";
  const op = overrides.op ?? "upsert";
  return {
    kind: "session_event",
    source: "claude",
    sourceName: "claude",
    sessionId,
    recordKey: `m-${revision}`,
    key: [
      overrides.kind ?? "session_event",
      "claude",
      sessionId,
      `m-${revision}`,
    ],
    revision,
    op,
    columns:
      op === "delete"
        ? null
        : { session_id: sessionId, text: `message ${revision}` },
    ...overrides,
  };
}

/** An in-memory change feed with named cursors, faithful to the paging contract. */
export class MemoryFeed implements Feed {
  cursors = new Map<string, number>();
  commits: Array<{ consumer: string; position: Watermark }> = [];
  constructor(
    public changes: FeedChange[],
    public epoch = "00000000000000aa",
  ) {}

  getChangesPage = async (
    options: ChangesPageOptions = {},
  ): Promise<ChangesPage> => {
    const head = Math.max(0, ...this.changes.map((c) => c.revision));
    const from =
      options.from && typeof options.from === "object"
        ? options.from.revision
        : options.from === "start"
          ? 0
          : options.consumer
            ? (this.cursors.get(options.consumer) ?? 0)
            : 0;
    const kinds = options.kinds;
    const remaining = this.changes
      .filter((c) => c.revision > from && (!kinds || kinds.includes(c.kind)))
      .sort((a, b) => a.revision - b.revision);
    const page = remaining.slice(0, options.limit ?? 1000);
    const done = page.length === remaining.length;
    // As the native drain: an exhausted read is positioned at the head, a partial one
    // at its last change.
    return {
      changes: page,
      position: {
        epoch: this.epoch,
        revision: done ? head : page.at(-1)!.revision,
      },
      head: { epoch: this.epoch, revision: head },
      done,
      consumer: options.consumer ?? null,
    };
  };

  commitChanges = async (consumer: string, position: Watermark) => {
    this.commits.push({ consumer, position });
    const revision = Math.max(
      this.cursors.get(consumer) ?? 0,
      position.revision,
    );
    this.cursors.set(consumer, revision);
    return { consumer, cursor: { epoch: this.epoch, revision } };
  };
}

type Handler = (request: {
  url: string;
  method: string;
  body: any;
  headers: Headers;
}) => Response | Promise<Response>;

/** A fetch double that records requests and answers through `handler`. */
export function fakeServer(handler: Handler) {
  const requests: Array<{
    url: string;
    method: string;
    body: any;
    headers: Headers;
  }> = [];
  const fetchImpl = (async (input: string | URL, init?: RequestInit) => {
    const request = {
      url: String(input),
      method: init?.method ?? "GET",
      body: init?.body ? JSON.parse(String(init.body)) : undefined,
      headers: new Headers(init?.headers),
    };
    requests.push(request);
    return handler(request);
  }) as typeof fetch;
  return { fetch: fetchImpl, requests };
}

export function json(
  body: unknown,
  status = 200,
  headers: Record<string, string> = {},
) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export const LIMITS = { maxRecords: 500, maxRequestBytes: 2_097_152 };

/** A receipt for exactly the batch in `body`. */
export function receiptFor(body: any) {
  return {
    protocolVersion: 1,
    receiptId: `rhr_${body.batch.batch_id}`,
    batchId: body.batch.batch_id,
    acceptedRevisionIds: body.batch.records.map((r: any) => r.revision_id),
    unsupportedRevisionIds: [],
    acceptanceLevel: "durable",
    limits: LIMITS,
  };
}

export const noSleep = async () => {};
