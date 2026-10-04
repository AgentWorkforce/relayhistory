import { Hono } from "hono";
import {
  hostContext,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import { embeddingProviderFromEnv } from "../lib/embed.js";
import { applyIngest } from "../lib/ingest.js";
import type { IngestRequest, IngestResponse } from "../lib/types.js";

const MAX_BATCH_SIZE = 1000;

function validateIngestRequest(body: IngestRequest): string | null {
  if (!body || typeof body !== "object") {
    return "Body must be an object";
  }
  if (
    !body.machine ||
    typeof body.machine.id !== "string" ||
    !body.machine.id
  ) {
    return "machine.id is required";
  }
  if (typeof body.batchId !== "string" || !body.batchId) {
    return "batchId is required";
  }
  if (!Array.isArray(body.records)) {
    return "records must be an array";
  }
  if (body.records.length > MAX_BATCH_SIZE) {
    return `records must include ${MAX_BATCH_SIZE} items or fewer`;
  }

  for (const [index, record] of body.records.entries()) {
    const recordPayload = record as unknown as Record<string, unknown>;
    if (!record || typeof record !== "object") {
      return `records[${index}] must be an object`;
    }
    if (typeof record.kind !== "string" || !record.kind) {
      return `records[${index}].kind is required`;
    }
    if (typeof record.source !== "string" || !record.source) {
      return `records[${index}].source is required`;
    }
    if (typeof record.sessionId !== "string" || !record.sessionId) {
      return `records[${index}].sessionId is required`;
    }
    if (record.kind === "session_outcome") {
      if (
        (typeof recordPayload.commitSha !== "string" ||
          !recordPayload.commitSha) &&
        (typeof recordPayload.commit_sha !== "string" ||
          !recordPayload.commit_sha)
      ) {
        return `records[${index}].commitSha is required for session_outcome records`;
      }
      continue;
    }
    if (
      recordPayload.record != null &&
      (typeof recordPayload.record !== "object" ||
        Array.isArray(recordPayload.record))
    ) {
      return `records[${index}].record must be an object when present`;
    }
  }

  return null;
}

function badRequest(c: any, message: string): Response {
  return c.json(
    {
      error: { code: "bad_request", message },
      correlationId: c.get("correlationId") ?? "",
    },
    400,
  );
}

export function createIngestRoutes<E extends HistoryEnv>(
  deps: HistoryEngineDeps<E>,
): Hono<HistoryEnv> {
  const database = (c: HistoryContext) => deps.database(hostContext<E>(c));
  const ingestRoutes = new Hono<HistoryEnv>();

  ingestRoutes.post("/ingest", requireScope("rth:sync"), async (c) => {
    const db = database(c);
    if (!db) {
      return c.json(
        {
          error: {
            code: "not_configured",
            message: "DATABASE_URL is required for hosted ingest",
          },
          correlationId: c.get("correlationId") ?? "",
        },
        503,
      );
    }

    let body: IngestRequest;
    try {
      body = await c.req.json<IngestRequest>();
    } catch {
      return badRequest(c, "Request body must be valid JSON");
    }

    const error = validateIngestRequest(body);
    if (error) {
      return badRequest(c, error);
    }
    const outcome = await applyIngest(db, getAuth(c), body, {
      embeddings: deps.embeddings?.(hostContext<E>(c)) ?? null,
    });
    const response: IngestResponse = {
      batchId: body.batchId,
      received: outcome.received,
      accepted: outcome.accepted,
      cursors: outcome.cursors,
    };

    return c.json(response);
  });

  return ingestRoutes;
}
