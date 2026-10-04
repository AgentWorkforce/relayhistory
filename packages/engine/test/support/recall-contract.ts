/**
 * The client contract for the recall responses the session catalog suite reads. These
 * mirror `sessionsResponse` and `sessionCatalogResponse`, with the JSON helpers they
 * use, from `@relayhistory/cloud-client/contract` at cloud commit a69fa263. Every
 * object is a `strictObject`, so a response fails on a field the server stopped
 * sending, renamed, or added.
 */
import { z } from "zod";

export type JsonValue =
  string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

export const jsonValue: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(jsonValue),
    z.record(z.string(), jsonValue),
  ]),
);

/** An ISO-8601 instant, as every timestamp on this API is normalised to. */
export const isoTimestamp = z
  .string()
  .refine((value) => Number.isFinite(Date.parse(value)), {
    message: "expected an ISO-8601 timestamp",
  });

/** Where a session's one-line `summary` came from — see `lib/recall.ts::summaryFor`. */
export const summarySource = z.enum(["task_title", "first_user_turn"]);

/** Delivered catalog fields shared by the session list and `getSessionCatalog`. */
const sessionCatalogFields = {
  title: z.string().nullable(),
  gitBranch: z.string().nullable(),
  repoUrl: z.string().nullable(),
  initialCommit: z.string().nullable(),
  projectKey: z.string().nullable(),
  projectKeyMethod: z.string().nullable(),
  models: z.array(z.string()),
  originator: z.string().nullable(),
  agentVersion: z.string().nullable(),
  firstActivityAt: isoTimestamp.nullable(),
  lastActivityAt: isoTimestamp.nullable(),
};

/**
 * `lib/session-catalog.ts::catalogSummaries` — attached to a listed session only when
 * exactly one delivered catalog row belongs to the workspaces its events came from.
 */
export const sessionCatalogSummary = z.strictObject({
  workspaceId: z.string(),
  ...sessionCatalogFields,
  parentSessionIds: z.array(z.string()),
  childSessionCount: z.number().int(),
});

export const sessionSummary = z.strictObject({
  workState: z
    .strictObject({
      status: z.enum(["active", "idle", "finished"]),
      updatedAt: isoTimestamp,
    })
    .nullable()
    .optional(),
  sessionId: z.string(),
  userIds: z.array(z.string()).optional(),
  source: z.string(),
  projectId: z.string().nullable(),
  machineIds: z.array(z.string()),
  firstTs: isoTimestamp,
  lastTs: isoTimestamp,
  eventCount: z.number(),
  kinds: z.array(z.string()),
  models: z.array(z.string()),
  taskTitle: z.string().nullable(),
  summary: z.string().nullable(),
  summarySource: summarySource.nullable(),
  taskRefs: z.array(jsonValue),
  /** Sum over the events that carried a cost; null when none did. */
  totalCostUsdMicros: z.number().nullable(),
  totalInputTokens: z.number(),
  totalOutputTokens: z.number(),
  // Optional so this client still parses an older server that does not send them.
  totalReasoningTokens: z.number().optional(),
  totalCacheReadTokens: z.number().optional(),
  /** Every cache-write bucket summed. */
  totalCacheCreateTokens: z.number().optional(),
  /**
   * The 5-minute / 1-hour cache-write split; null unless every event that wrote cache
   * reported a split covering its whole write.
   */
  totalCacheCreate5mTokens: z.number().nullable().optional(),
  totalCacheCreate1hTokens: z.number().nullable().optional(),
  catalog: sessionCatalogSummary.optional(),
});

/** `GET /v1/sessions` */
export const sessionsResponse = z.strictObject({
  sessions: z.array(sessionSummary),
  nextCursor: z.string().nullable(),
  correlationId: z.string(),
});

export const sessionRelationship = z.strictObject({
  relationshipUid: z.string(),
  parentSessionId: z.string(),
  /** Null for unlinked evidence: a sidecar whose child session was never identified. */
  childSessionId: z.string().nullable(),
  relationship: z.string().nullable(),
  identityStatus: z.string().nullable(),
  childAgentType: z.string().nullable(),
  childAgentName: z.string().nullable(),
  childModel: z.string().nullable(),
  spawnDepth: z.number().int().nullable(),
  spawnedAt: isoTimestamp.nullable(),
});

export const sessionMarker = z.strictObject({
  markerUid: z.string(),
  kind: z.string().nullable(),
  subkind: z.string().nullable(),
  ts: isoTimestamp.nullable(),
  turnId: z.string().nullable(),
});

export const sessionCommitLink = z.strictObject({
  commitSha: z.string(),
  matchMethod: z.string(),
  repo: z.string().nullable(),
  branch: z.string().nullable(),
  /** 0..1, re-expanded from basis points. */
  confidence: z.number().nullable(),
  linkedAt: isoTimestamp.nullable(),
});

/**
 * `GET /v1/sessions/:sessionId/catalog?source=` — always narrowed to the token's
 * workspace. A session with no delivered catalog is a 404, which the client turns
 * into `null`.
 */
export const sessionCatalogResponse = z.strictObject({
  sessionId: z.string(),
  source: z.string(),
  workspaceId: z.string(),
  catalog: z
    .strictObject({
      ...sessionCatalogFields,
      firstPrompt: z.string().nullable(),
    })
    .nullable(),
  parents: z.array(sessionRelationship),
  children: z.array(sessionRelationship),
  markers: z.array(sessionMarker),
  commitLinks: z.array(sessionCommitLink),
  /** True when any list was cut at 500 entries. */
  truncated: z.boolean(),
  correlationId: z.string(),
});
