import { selectedSessionTurns } from "./session-work-state.js";
import { and, asc, desc, eq, sql } from "drizzle-orm";
import type { HistoryDb as Db } from "../db/database.js";
import { conversationTurns } from "../db/schema.js";
import type { AuthContext } from "../env.js";
import { scrubJson, scrubText } from "./scrub.js";

export type ConversationRole = "user" | "assistant" | "system";
export type ConversationActorRole = "owner" | "steerer";

export interface ConversationTurnInput {
  sessionOwner: string;
  turnIndex: number;
  role: ConversationRole;
  content: string;
  actorName: string;
  actorRole: ConversationActorRole;
  metadata: Record<string, unknown>;
  ts: Date;
}

export interface SessionMetadata {
  nativeCli: string | null;
  nativeResumeId: string | null;
  sessionOwner: string;
  originNode: string | null;
}

/**
 * The source a turn declares for itself: `nativeCli`, else `source`, the same order
 * `conversationSource` reads them in.
 */
function explicitSource(metadata: unknown): string | null {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return null;
  }
  for (const key of ["nativeCli", "source"]) {
    const value = (metadata as Record<string, unknown>)[key];
    if (typeof value === "string" && value.length > 0) return value;
  }
  return null;
}

/**
 * Store a batch of turns under the authenticated workspace and the session's source.
 *
 * A session id is unique only within (org, workspace, source), so both are part of the
 * turn's key. The workspace is the authenticated one, never the request's. The source
 * is the turn's own `metadata.nativeCli`/`metadata.source`, else the one another turn in
 * the request names (a harness often tags only the opening turn), else the session
 * owner — which the OSS uploader set to the capture source. Resolution depends only on
 * the request, never on stored rows: an untagged upload therefore cannot land on (and
 * overwrite) a row another source wrote for the same owner, and an identical retry
 * always updates the row it first wrote.
 */
export async function ingestConversationTurns(
  db: Db,
  auth: AuthContext,
  sessionId: string,
  turns: ConversationTurnInput[],
): Promise<number> {
  if (turns.length === 0) {
    return 0;
  }

  const workspaceId = auth.workspaceId ?? "default";
  const prepared = turns.map((turn) => ({
    turnIndex: turn.turnIndex,
    fields: {
      sessionOwner: scrubText(turn.sessionOwner),
      role: turn.role,
      content: scrubText(turn.content),
      actorName: scrubText(turn.actorName),
      actorRole: turn.actorRole,
      metadata: scrubJson(turn.metadata),
      ts: turn.ts,
    },
  }));
  const requestSource =
    prepared
      .map((turn) => explicitSource(turn.fields.metadata))
      .find((source) => source !== null) ?? null;
  for (const { turnIndex, fields } of prepared) {
    const source =
      explicitSource(fields.metadata) ?? requestSource ?? fields.sessionOwner;

    await db
      .insert(conversationTurns)
      .values({
        id: crypto.randomUUID(),
        orgId: auth.orgId,
        workspaceId,
        source,
        sessionId,
        turnIndex,
        ...fields,
      })
      .onConflictDoUpdate({
        target: [
          conversationTurns.orgId,
          conversationTurns.workspaceId,
          conversationTurns.source,
          conversationTurns.sessionId,
          conversationTurns.turnIndex,
        ],
        set: fields,
      });
  }

  return turns.length;
}

export async function listConversationTurns(
  db: Db,
  orgId: string,
  sessionId: string,
  source?: string,
) {
  return (
    db
      .select({
        id: conversationTurns.id,
        sessionId: conversationTurns.sessionId,
        sessionOwner: conversationTurns.sessionOwner,
        turnIndex: conversationTurns.turnIndex,
        role: conversationTurns.role,
        content: conversationTurns.content,
        actorName: conversationTurns.actorName,
        actorRole: conversationTurns.actorRole,
        metadata: conversationTurns.metadata,
        ts: conversationTurns.ts,
      })
      .from(conversationTurns)
      .where(
        and(
          eq(conversationTurns.orgId, orgId),
          eq(conversationTurns.sessionId, sessionId),
          source
            ? selectedSessionTurns(orgId, [{ source, sessionId }])
            : undefined,
        ),
      )
      // Several workspaces or sources can hold the same turn index for one session id;
      // the rest of the key, then the row id, makes the order total and repeatable.
      // Text keys sort bytewise so the order does not depend on the database collation.
      .orderBy(
        asc(conversationTurns.turnIndex),
        sql`${conversationTurns.workspaceId} collate "C"`,
        sql`${conversationTurns.source} collate "C"`,
        asc(conversationTurns.id),
      )
  );
}

export async function getSessionMetadata(
  db: Db,
  orgId: string,
  sessionId: string,
  source?: string,
): Promise<SessionMetadata | null> {
  const rows = await db
    .select({
      sessionOwner: conversationTurns.sessionOwner,
      metadata: conversationTurns.metadata,
    })
    .from(conversationTurns)
    .where(
      and(
        eq(conversationTurns.orgId, orgId),
        eq(conversationTurns.sessionId, sessionId),
        source
          ? selectedSessionTurns(orgId, [{ source, sessionId }])
          : undefined,
      ),
    )
    .orderBy(
      desc(conversationTurns.turnIndex),
      asc(conversationTurns.workspaceId),
      asc(conversationTurns.source),
      asc(conversationTurns.id),
    );

  if (rows.length === 0) {
    return null;
  }

  return {
    nativeCli: firstMetadataString(rows, "nativeCli"),
    nativeResumeId: firstMetadataString(rows, "nativeResumeId"),
    sessionOwner: rows[0].sessionOwner,
    originNode: firstMetadataString(rows, "originNode"),
  };
}

function firstMetadataString(
  rows: Array<{ metadata: unknown }>,
  key: string,
): string | null {
  for (const row of rows) {
    if (
      !row.metadata ||
      typeof row.metadata !== "object" ||
      Array.isArray(row.metadata)
    ) {
      continue;
    }
    const value = (row.metadata as Record<string, unknown>)[key];
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }
  return null;
}
