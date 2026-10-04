import { Hono } from "hono";
import {
  hostContext,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import {
  getSessionMetadata,
  ingestConversationTurns,
  listConversationTurns,
  type ConversationActorRole,
  type ConversationRole,
  type ConversationTurnInput,
} from "../lib/turns.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import {
  BoundedJsonError,
  MAX_JSON_BODY_BYTES,
  readBoundedJson,
} from "../lib/bounded-json.js";

const MAX_TURNS_PER_REQUEST = 1000;
const MAX_TURN_INDEX = 2_147_483_647;
const ROLES = new Set<ConversationRole>(["user", "assistant", "system"]);
const ACTOR_ROLES = new Set<ConversationActorRole>(["owner", "steerer"]);
const NATIVE_CLIS = new Set(["claude", "codex"]);

export function parseTurnRequest(
  body: unknown,
): ConversationTurnInput[] | string {
  const container =
    body && typeof body === "object" && !Array.isArray(body)
      ? (body as Record<string, unknown>)
      : null;
  const rawTurns = Array.isArray(body) ? body : container?.turns;
  const defaultSessionOwner = container?.sessionOwner;

  if (!Array.isArray(rawTurns)) {
    return "Body must be a turn array or include a turns array";
  }
  if (rawTurns.length > MAX_TURNS_PER_REQUEST) {
    return `turns must include ${MAX_TURNS_PER_REQUEST} items or fewer`;
  }

  const turns: ConversationTurnInput[] = [];
  for (const [index, raw] of rawTurns.entries()) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      return `turns[${index}] must be an object`;
    }
    const turn = raw as Record<string, unknown>;
    const sessionOwner = turn.sessionOwner ?? defaultSessionOwner;
    if (typeof sessionOwner !== "string" || sessionOwner.length === 0) {
      return `turns[${index}].sessionOwner is required`;
    }
    if (!Number.isInteger(turn.turnIndex) || Number(turn.turnIndex) < 0) {
      return `turns[${index}].turnIndex must be a non-negative integer`;
    }
    // conversation_turns.turn_index is a PostgreSQL integer.
    if (Number(turn.turnIndex) > MAX_TURN_INDEX) {
      return `turns[${index}].turnIndex must be at most ${MAX_TURN_INDEX}`;
    }
    if (
      typeof turn.role !== "string" ||
      !ROLES.has(turn.role as ConversationRole)
    ) {
      return `turns[${index}].role must be one of: user, assistant, system`;
    }
    if (typeof turn.content !== "string") {
      return `turns[${index}].content must be a string`;
    }
    if (typeof turn.actorName !== "string" || turn.actorName.length === 0) {
      return `turns[${index}].actorName is required`;
    }
    if (
      typeof turn.actorRole !== "string" ||
      !ACTOR_ROLES.has(turn.actorRole as ConversationActorRole)
    ) {
      return `turns[${index}].actorRole must be one of: owner, steerer`;
    }
    if (
      turn.metadata != null &&
      (typeof turn.metadata !== "object" || Array.isArray(turn.metadata))
    ) {
      return `turns[${index}].metadata must be an object when present`;
    }
    const metadata = (turn.metadata ?? {}) as Record<string, unknown>;
    if (
      metadata.nativeCli != null &&
      (typeof metadata.nativeCli !== "string" ||
        !NATIVE_CLIS.has(metadata.nativeCli))
    ) {
      return `turns[${index}].metadata.nativeCli must be one of: claude, codex`;
    }
    for (const key of ["nativeResumeId", "originNode"] as const) {
      if (metadata[key] != null && typeof metadata[key] !== "string") {
        return `turns[${index}].metadata.${key} must be a string when present`;
      }
    }

    const ts = parseTimestamp(turn.ts);
    if (!ts) {
      return `turns[${index}].ts must be a valid timestamp`;
    }

    turns.push({
      sessionOwner,
      turnIndex: turn.turnIndex as number,
      role: turn.role as ConversationRole,
      content: turn.content,
      actorName: turn.actorName,
      actorRole: turn.actorRole as ConversationActorRole,
      metadata,
      ts,
    });
  }

  return turns;
}

function parseTimestamp(value: unknown): Date | null {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function missingDb(c: any): Response {
  return c.json(
    {
      error: { code: "not_configured", message: "DATABASE_URL is required" },
      correlationId: c.get("correlationId") ?? "",
    },
    503,
  );
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

function payloadTooLarge(c: any): Response {
  return c.json(
    {
      error: {
        code: "payload_too_large",
        message: `Request body exceeds ${MAX_JSON_BODY_BYTES} bytes`,
      },
      correlationId: c.get("correlationId") ?? "",
    },
    413,
  );
}

export function createTurnRoutes<E extends HistoryEnv>(
  deps: HistoryEngineDeps<E>,
): Hono<HistoryEnv> {
  const database = (c: HistoryContext) => deps.database(hostContext<E>(c));
  const turnRoutes = new Hono<HistoryEnv>();

  turnRoutes.post(
    "/sessions/:sessionId/turns",
    requireScope("rth:sync"),
    async (c) => {
      const db = database(c);
      if (!db) {
        return missingDb(c);
      }

      let body: unknown;
      try {
        body = await readBoundedJson(c.req.raw, {
          maxBytes: MAX_JSON_BODY_BYTES,
        });
      } catch (error) {
        return error instanceof BoundedJsonError &&
          error.failure === "too_large"
          ? payloadTooLarge(c)
          : badRequest(c, "Request body must be valid JSON");
      }

      const parsed = parseTurnRequest(body);
      if (typeof parsed === "string") {
        return badRequest(c, parsed);
      }

      const sessionId = c.req.param("sessionId");
      const accepted = await ingestConversationTurns(
        db,
        getAuth(c),
        sessionId,
        parsed,
      );

      return c.json({
        sessionId,
        received: parsed.length,
        accepted,
        correlationId: c.get("correlationId") ?? "",
      });
    },
  );

  turnRoutes.get(
    "/sessions/:sessionId/turns",
    requireScope("rth:read"),
    async (c) => {
      const db = database(c);
      if (!db) {
        return missingDb(c);
      }

      const sessionId = c.req.param("sessionId");
      const auth = getAuth(c);
      const turns = await listConversationTurns(
        db,
        auth.orgId,
        sessionId,
        c.req.query("source") || undefined,
      );

      return c.json({
        sessionId,
        turns,
        correlationId: c.get("correlationId") ?? "",
      });
    },
  );

  turnRoutes.get(
    "/sessions/:sessionId/metadata",
    requireScope("rth:read"),
    async (c) => {
      const db = database(c);
      if (!db) {
        return missingDb(c);
      }

      const sessionId = c.req.param("sessionId");
      const auth = getAuth(c);
      const metadata = await getSessionMetadata(
        db,
        auth.orgId,
        sessionId,
        c.req.query("source") || undefined,
      );

      if (!metadata) {
        return c.json(
          {
            error: {
              code: "not_found",
              message: `No conversation turns found for session ${sessionId}`,
            },
            correlationId: c.get("correlationId") ?? "",
          },
          404,
        );
      }

      return c.json(metadata);
    },
  );

  return turnRoutes;
}
