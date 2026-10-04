import {
  bigint,
  bigserial,
  date,
  unique,
  boolean,
  customType,
  doublePrecision,
  index,
  integer,
  jsonb,
  pgSchema,
  primaryKey,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

const sessionsSchema = pgSchema("sessions");

/** Immutable source snapshots and versioned model results for hosted session analysis. */
export const sessionAnalysisJobs = sessionsSchema.table(
  "session_analysis_jobs",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    kind: text("kind")
      .$type<"brief" | "analysis">()
      .notNull()
      .default("analysis"),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id"),
    sessionId: text("session_id").notNull(),
    requestedByUserId: text("requested_by_user_id").notNull(),
    sourceProvider: text("source_provider").notNull(),
    sourceFingerprint: text("source_fingerprint").notNull(),
    analysisVersion: text("analysis_version").notNull(),
    schemaVersion: integer("schema_version").notNull(),
    summaryVersion: text("summary_version").notNull(),
    modelVersion: text("model_version").notNull(),
    summaryModelVersion: text("summary_model_version").notNull(),
    source: jsonb("source").notNull(),
    status: text("status")
      .$type<"queued" | "running" | "completed" | "failed">()
      .notNull()
      .default("queued"),
    attempts: integer("attempts").notNull().default(0),
    requeues: integer("requeues").notNull().default(0),
    availableAt: timestamp("available_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    leaseToken: uuid("lease_token"),
    leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
    result: jsonb("result"),
    lastError: text("last_error"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [
    uniqueIndex("session_analysis_jobs_identity_idx").on(
      t.orgId,
      t.sessionId,
      t.kind,
      t.sourceProvider,
      t.sourceFingerprint,
      t.analysisVersion,
      t.schemaVersion,
      t.summaryVersion,
      t.modelVersion,
      t.summaryModelVersion,
    ),
    index("session_analysis_jobs_claim_idx").on(
      t.kind,
      t.status,
      t.availableAt,
      t.leaseExpiresAt,
    ),
    index("session_analysis_jobs_scope_idx").on(
      t.orgId,
      t.sessionId,
      t.createdAt,
    ),
  ],
);

/** Bumped by ingest triggers whenever a session's stored events or turns change. */
export const sessionRevisions = sessionsSchema.table(
  "session_revisions",
  {
    orgId: text("org_id").notNull(),
    sessionId: text("session_id").notNull(),
    revision: bigint("revision", { mode: "number" }).notNull().default(1),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [primaryKey({ columns: [t.orgId, t.sessionId] })],
);

/** Database-wide admission across Worker isolates, keyed by authorized organization. */
export const sessionAnalysisAdmissions = sessionsSchema.table(
  "session_analysis_admissions",
  {
    orgId: text("org_id").notNull(),
    day: date("day").notNull(),
    requests: integer("requests").notNull().default(0),
  },
  (t) => [primaryKey({ columns: [t.orgId, t.day] })],
);

/** Identity-only requests; History resolves evidence before creating a versioned job. */
export const sessionAnalysisRequests = sessionsSchema.table(
  "session_analysis_requests",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    sessionId: text("session_id").notNull(),
    requestedByUserId: text("requested_by_user_id").notNull(),
    status: text("status")
      .$type<"queued" | "running" | "resolved" | "failed">()
      .notNull()
      .default("queued"),
    attempts: integer("attempts").notNull().default(0),
    availableAt: timestamp("available_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    leaseToken: uuid("lease_token"),
    leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
    analysisJobId: uuid("analysis_job_id").references(
      () => sessionAnalysisJobs.id,
    ),
    lastError: text("last_error"),
    sourceRevision: text("source_revision"),
    briefJobId: uuid("brief_job_id").references(() => sessionAnalysisJobs.id),
    engine: text("engine"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("session_analysis_requests_claim_idx").on(
      t.status,
      t.availableAt,
      t.leaseExpiresAt,
    ),
    index("session_analysis_requests_scope_idx").on(
      t.orgId,
      t.sessionId,
      t.createdAt,
    ),
    index("session_analysis_requests_admission_idx").on(t.orgId, t.createdAt),
  ],
);

const vector = customType<{
  data: number[] | null;
  driverData: string | null;
}>({
  dataType() {
    return "vector(1536)";
  },
  toDriver(value) {
    return value == null ? null : `[${value.join(",")}]`;
  },
  fromDriver(value) {
    if (value == null || value === "") {
      return null;
    }
    const trimmed = String(value).trim().replace(/^\[/, "").replace(/\]$/, "");
    if (!trimmed) {
      return [];
    }
    return trimmed.split(",").map(Number);
  },
});

export const machines = sessionsSchema.table(
  "machines",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    machineId: text("machine_id").notNull(),
    hostname: text("hostname"),
    label: text("label"),
    os: text("os"),
    relayhistoryVersion: text("relayhistory_version"),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    cursorsJson: jsonb("cursors_json").notNull().default({}),
  },
  (t) => [primaryKey({ columns: [t.orgId, t.machineId] })],
);

export const convergenceEvents = sessionsSchema.table(
  "convergence_events",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    machineId: text("machine_id").notNull(),
    userId: text("user_id").notNull(),
    source: text("source").notNull(),
    lens: text("lens"),
    sessionId: text("session_id").notNull(),
    eventId: text("event_id").notNull(),
    kind: text("kind").notNull(),
    type: text("type").notNull(),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
    actorName: text("actor_name"),
    actorRole: text("actor_role"),
    subagentId: text("subagent_id"),
    trajectoryId: text("trajectory_id"),
    chapterId: text("chapter_id"),
    projectId: text("project_id"),
    workflowId: text("workflow_id"),
    taskRef: jsonb("task_ref").notNull().default({}),
    taskTitle: text("task_title"),
    taskDescription: text("task_description"),
    taskStatus: text("task_status"),
    content: text("content"),
    significance: text("significance"),
    // Stored as 0..10000 basis points; source schemas expose 0.0..1.0.
    confidence: integer("confidence_basis_points"),
    tags: jsonb("tags").notNull().default([]),
    model: text("model"),
    provider: text("provider"),
    inputTokens: bigint("input_tokens", { mode: "number" })
      .notNull()
      .default(0),
    outputTokens: bigint("output_tokens", { mode: "number" })
      .notNull()
      .default(0),
    reasoningTokens: bigint("reasoning_tokens", { mode: "number" })
      .notNull()
      .default(0),
    cacheReadTokens: bigint("cache_read_tokens", { mode: "number" })
      .notNull()
      .default(0),
    // Sum of every cache-write bucket, kept for compatibility. The TTL split below is
    // priced differently and is NULL when the client did not report it (0021).
    cacheCreateTokens: bigint("cache_create_tokens", { mode: "number" })
      .notNull()
      .default(0),
    cacheCreate5mTokens: bigint("cache_create_5m_tokens", { mode: "number" }),
    cacheCreate1hTokens: bigint("cache_create_1h_tokens", { mode: "number" }),
    // NULL when the client sent no cost (0021). Rows written before 0021 hold 0 there.
    costUsdMicros: bigint("cost_usd_micros", { mode: "number" }).default(0),
    toolName: text("tool_name"),
    toolStatus: text("tool_status"),
    toolCalls: jsonb("tool_calls").notNull().default([]),
    retries: integer("retries").notNull().default(0),
    durationMs: integer("duration_ms"),
    filesTouched: jsonb("files_touched").notNull().default([]),
    codeChurn: jsonb("code_churn").notNull().default({}),
    embedding: vector("embedding"),
    embeddingModel: text("embedding_model"),
    embeddingDim: integer("embedding_dim"),
    contentHash: text("content_hash"),
    generatedAt: timestamp("generated_at", { withTimezone: true }),
    embeddingSkipReason: text("embedding_skip_reason"),
    // Delivery projection (migration 0023): the origin-independent delivery record id
    // the row was projected from, and the namespace-qualified request key its usage is
    // deduplicated on. Both are NULL for rows written by the legacy ingest path.
    deliveryRecordId: text("delivery_record_id"),
    requestKey: text("request_key"),
    record: jsonb("record").notNull(),
    ingestedAt: timestamp("ingested_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    primaryKey({
      columns: [t.orgId, t.machineId, t.source, t.sessionId, t.kind, t.eventId],
    }),
    index("convergence_events_org_ts_idx").on(t.orgId, t.ts),
    index("convergence_events_org_source_ts_idx").on(t.orgId, t.source, t.ts),
    index("convergence_events_org_lens_ts_idx").on(t.orgId, t.lens, t.ts),
    index("convergence_events_org_type_ts_idx").on(t.orgId, t.type, t.ts),
    index("convergence_events_org_session_idx").on(t.orgId, t.sessionId),
    index("convergence_events_org_project_ts_idx").on(
      t.orgId,
      t.projectId,
      t.ts,
    ),
    index("convergence_events_org_task_status_ts_idx").on(
      t.orgId,
      t.taskStatus,
      t.ts,
    ),
  ],
);

// Ordered, readable session transcript used to resume work across harnesses.
// A session id is unique only within (org, workspace, source), so the turn key
// carries both (migration 0020).
export const conversationTurns = sessionsSchema.table(
  "conversation_turns",
  {
    id: uuid("id").primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull().default("default"),
    source: text("source").notNull().default(""),
    sessionId: text("session_id").notNull(),
    sessionOwner: text("session_owner").notNull(),
    turnIndex: integer("turn_index").notNull(),
    role: text("role").notNull(),
    content: text("content").notNull(),
    actorName: text("actor_name").notNull(),
    actorRole: text("actor_role").notNull(),
    metadata: jsonb("metadata").notNull().default({}),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
  },
  (t) => [
    unique("conversation_turns_scope_turn_key").on(
      t.orgId,
      t.workspaceId,
      t.source,
      t.sessionId,
      t.turnIndex,
    ),
    index("conversation_turns_org_session_ts_idx").on(
      t.orgId,
      t.sessionId,
      t.ts,
    ),
  ],
);

export const syncBatches = sessionsSchema.table(
  "sync_batches",
  {
    id: text("id").primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    machineId: text("machine_id").notNull(),
    receivedAt: timestamp("received_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    recordCount: integer("record_count").notNull().default(0),
    acceptedCount: integer("accepted_count").notNull().default(0),
    cursorsJson: jsonb("cursors_json").notNull().default({}),
  },
  (t) => [
    uniqueIndex("sync_batches_idem_idx").on(t.orgId, t.machineId, t.id),
    index("sync_batches_org_received_idx").on(t.orgId, t.receivedAt),
  ],
);

export const authSessions = sessionsSchema.table(
  "auth_sessions",
  {
    id: text("id").primaryKey(),
    tokenFamilyId: text("token_family_id").notNull(),
    subjectType: text("subject_type").notNull().default("cli"),
    userId: text("user_id").notNull(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    scopes: jsonb("scopes").notNull().default([]),
    accessTokenHash: text("access_token_hash").notNull(),
    accessTokenExpiresAt: timestamp("access_token_expires_at", {
      withTimezone: true,
    }).notNull(),
    refreshTokenHash: text("refresh_token_hash").notNull(),
    refreshTokenExpiresAt: timestamp("refresh_token_expires_at", {
      withTimezone: true,
    }).notNull(),
    label: text("label"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    lastUsedAt: timestamp("last_used_at", { withTimezone: true }),
    lastRefreshedAt: timestamp("last_refreshed_at", { withTimezone: true }),
    revokedAt: timestamp("revoked_at", { withTimezone: true }),
    revokedReason: text("revoked_reason"),
  },
  (t) => [
    uniqueIndex("auth_sessions_access_hash_idx").on(t.accessTokenHash),
    uniqueIndex("auth_sessions_refresh_hash_idx").on(t.refreshTokenHash),
    index("auth_sessions_family_idx").on(t.tokenFamilyId),
    index("auth_sessions_org_idx").on(t.orgId),
  ],
);

// --- Reflex derived layer ---
// See docs/decisions/2026-06-27-reflex-learnings-and-outcomes-layer.md.
// Derived/materialized on top of convergence_events; never the raw source of truth.

// session -> commit linkage + ship/revert (mistake) signal. Keyed per workspace:
// a session id is unique only within (org, workspace, source).
export const sessionOutcomes = sessionsSchema.table(
  "session_outcomes",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    machineId: text("machine_id").notNull(),
    userId: text("user_id").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id").notNull(),
    repo: text("repo"),
    branch: text("branch"),
    commitSha: text("commit_sha").notNull(),
    // The strongest method that linked this session to this commit: a replay under a
    // lower-confidence method never overwrites it (see ingest `upsertSessionOutcome`).
    matchMethod: text("match_method"),
    // 0..10000 basis points; same invariant as convergence_events.confidence.
    confidence: integer("confidence_basis_points"),
    // The session's project and touched files from the envelope (0021).
    projectId: text("project_id"),
    filesTouched: jsonb("files_touched").notNull().default([]),
    numstatJson: jsonb("numstat_json").notNull().default({}),
    filesJson: jsonb("files_json").notNull().default([]),
    shippedAt: timestamp("shipped_at", { withTimezone: true }),
    reverted: boolean("reverted").notNull().default(false),
    revertedBySha: text("reverted_by_sha"),
    revertedAt: timestamp("reverted_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    ingestedAt: timestamp("ingested_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    primaryKey({
      columns: [t.orgId, t.workspaceId, t.source, t.sessionId, t.commitSha],
    }),
    index("session_outcomes_org_session_idx").on(t.orgId, t.sessionId),
    index("session_outcomes_org_commit_idx").on(t.orgId, t.commitSha),
    index("session_outcomes_org_reverted_idx")
      .on(t.orgId, t.reverted)
      .where(sql`${t.reverted} = true`),
  ],
);

// Polymorphic lifecycle artifacts, keyed by the session's source + id within an org.
export const sessionLinks = sessionsSchema.table(
  "session_links",
  {
    id: bigserial("id", { mode: "bigint" }).primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id").notNull(),
    linkKind: text("link_kind").notNull(),
    linkRef: text("link_ref").notNull(),
    linkUrl: text("link_url"),
    // Keep microseconds for cursor comparisons; JS Date would truncate them.
    linkTs: timestamp("link_ts", { withTimezone: true, mode: "string" }),
    metadata: jsonb("metadata"),
    provenanceLens: text("provenance_lens").notNull(),
    confidence: integer("confidence_basis_points"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    unique().on(t.orgId, t.source, t.sessionId, t.linkKind, t.linkRef),
    index("session_links_session_idx").on(
      t.orgId,
      t.source,
      t.sessionId,
      t.linkTs.desc(),
    ),
    index("session_links_ref_idx").on(t.orgId, t.linkKind, t.linkRef),
  ],
);

// The shared brain. scope='individual' (subjectUserId set) or 'team' (null, org-wide).
export const patterns = sessionsSchema.table(
  "patterns",
  {
    // Content-addressed: stable hash of (orgId, scope, subjectUserId, kind,
    // normalized statement, projectId) so re-mining upserts instead of duplicating.
    id: text("id").primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    scope: text("scope").notNull(), // 'individual' | 'team'
    subjectUserId: text("subject_user_id"),
    projectId: text("project_id"),
    kind: text("kind").notNull(), // 'skill' | 'rule' | 'footgun' | 'mistake' | 'playbook' | 'hotspot'
    title: text("title").notNull(),
    statement: text("statement").notNull(),
    body: text("body"),
    confidence: integer("confidence_basis_points"),
    sampleSize: integer("sample_size").notNull().default(0),
    supportEventIds: jsonb("support_event_ids").notNull().default([]),
    evidence: jsonb("evidence").notNull().default({}),
    status: text("status").notNull().default("candidate"), // candidate | active | dismissed | superseded
    supersededBy: text("superseded_by"),
    embedding: vector("embedding"),
    firstObservedAt: timestamp("first_observed_at", { withTimezone: true }),
    lastObservedAt: timestamp("last_observed_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("patterns_org_scope_kind_idx").on(t.orgId, t.scope, t.kind),
    index("patterns_org_subject_idx")
      .on(t.orgId, t.subjectUserId)
      .where(sql`${t.subjectUserId} IS NOT NULL`),
    index("patterns_org_project_status_idx").on(t.orgId, t.projectId, t.status),
    index("patterns_embedding_hnsw_idx")
      .using("hnsw", sql`embedding vector_cosine_ops`)
      .where(sql`${t.embedding} IS NOT NULL`),
  ],
);

// Pair feedback loop. outcome='recurred' is the "same mistake twice" signal.
export const patternHits = sessionsSchema.table(
  "pattern_hits",
  {
    id: text("id").primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    userId: text("user_id").notNull(),
    patternId: text("pattern_id").notNull(),
    source: text("source"),
    sessionId: text("session_id"),
    eventId: text("event_id"),
    outcome: text("outcome").notNull(), // fired | accepted | dismissed | recurred
    ts: timestamp("ts", { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index("pattern_hits_org_pattern_ts_idx").on(t.orgId, t.patternId, t.ts),
    index("pattern_hits_org_outcome_ts_idx").on(t.orgId, t.outcome, t.ts),
  ],
);

// GitHub org -> RelayAuth org_id binding for team scoping (the shared brain).
export const githubOrgLinks = sessionsSchema.table(
  "github_org_links",
  {
    githubOrgId: bigint("github_org_id", { mode: "number" }).primaryKey(),
    githubOrgLogin: text("github_org_login").notNull(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    linkedByUserId: text("linked_by_user_id").notNull(),
    verified: boolean("verified").notNull().default(false),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("github_org_links_org_idx").on(t.orgId),
    uniqueIndex("github_org_links_login_idx").on(t.githubOrgLogin),
  ],
);

// --- Neighborhood memory derived tier ---
// See docs/specs/2026-09-02-neighborhood-memory.md.
// Tenancy columns mirror `patterns` (org_id + workspace_id, never from payload).

export const projectAliases = sessionsSchema.table(
  "project_aliases",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    alias: text("alias").notNull(),
    canonicalProject: text("canonical_project").notNull(),
    kind: text("kind").notNull(), // self | sibling | contract
    version: text("version"),
    approvedBy: text("approved_by"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.orgId, t.alias] }),
    index("project_aliases_org_canonical_idx").on(t.orgId, t.canonicalProject),
    index("project_aliases_org_kind_idx").on(t.orgId, t.kind),
  ],
);

export const neighborhoodClaims = sessionsSchema.table(
  "neighborhood_claims",
  {
    // Content-addressed: sha256(org, subject_project, kind, normalized statement).
    id: text("id").primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    sourceProject: text("source_project"),
    subjectProject: text("subject_project").notNull(),
    kind: text("kind").notNull(), // dependency_change | api_contract | sibling_decision | incident | repeated_mistake
    statement: text("statement").notNull(),
    body: text("body"),
    impactOnSubject: text("impact_on_subject").notNull(),
    entities: jsonb("entities").notNull().default([]),
    evidenceClass: text("evidence_class").notNull(), // asserted | observed | corroborated
    confidence: doublePrecision("confidence"),
    status: text("status").notNull().default("candidate"), // candidate | active | contested | superseded | dormant | expired | retracted
    validFrom: timestamp("valid_from", { withTimezone: true }),
    invalidAt: timestamp("invalid_at", { withTimezone: true }),
    observedFrom: timestamp("observed_from", { withTimezone: true }),
    lastConfirmedAt: timestamp("last_confirmed_at", { withTimezone: true }),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    supersedesClaimId: text("supersedes_claim_id"),
    supportEventIds: jsonb("support_event_ids").notNull().default([]),
    extractorVersion: text("extractor_version"),
    profileVersion: text("profile_version"),
    embedding: vector("embedding"),
    embeddingModel: text("embedding_model"),
    embeddingDim: integer("embedding_dim"),
    contentHash: text("content_hash"),
    generatedAt: timestamp("generated_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("neighborhood_claims_org_subject_status_idx").on(
      t.orgId,
      t.subjectProject,
      t.status,
    ),
    index("neighborhood_claims_org_kind_idx").on(t.orgId, t.kind),
    index("neighborhood_claims_org_source_idx").on(t.orgId, t.sourceProject),
    index("neighborhood_claims_embedding_hnsw_idx")
      .using("hnsw", sql`embedding vector_cosine_ops`)
      .where(sql`${t.status} IN ('active', 'contested')`),
  ],
);

export const neighborhoodEdges = sessionsSchema.table(
  "neighborhood_edges",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    srcProject: text("src_project").notNull(),
    dstProject: text("dst_project").notNull(),
    relation: text("relation").notNull(), // impacts | depends_on | broke | supersedes
    claimId: text("claim_id").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    primaryKey({
      columns: [t.orgId, t.srcProject, t.dstProject, t.relation, t.claimId],
    }),
    index("neighborhood_edges_org_src_idx").on(t.orgId, t.srcProject),
    index("neighborhood_edges_org_dst_idx").on(t.orgId, t.dstProject),
    index("neighborhood_edges_org_claim_idx").on(t.orgId, t.claimId),
  ],
);

export const memoryJobs = sessionsSchema.table(
  "memory_jobs",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    machineId: text("machine_id").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id").notNull(),
    kind: text("kind").notNull(),
    eventId: text("event_id").notNull(),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
    profileVersion: text("profile_version").notNull(),
    extractorVersion: text("extractor_version").notNull(),
    contentHash: text("content_hash").notNull(),
    state: text("state").notNull().default("pending"),
    attempts: integer("attempts").notNull().default(0),
    lastError: text("last_error"),
    completionReason: text("completion_reason"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
  },
  (t) => [
    primaryKey({
      columns: [
        t.orgId,
        t.machineId,
        t.source,
        t.sessionId,
        t.kind,
        t.eventId,
        t.profileVersion,
        t.extractorVersion,
        t.contentHash,
      ],
    }),
    index("memory_jobs_org_state_idx").on(t.orgId, t.state),
    index("memory_jobs_org_session_idx").on(t.orgId, t.sessionId),
  ],
);

export const memoryRetrievalLog = sessionsSchema.table(
  "memory_retrieval_log",
  {
    id: text("id").primaryKey(),
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    userId: text("user_id"),
    subjectProject: text("subject_project").notNull(),
    task: text("task"),
    files: jsonb("files").notNull().default([]),
    budgetTokens: integer("budget_tokens"),
    scopes: jsonb("scopes").notNull().default([]),
    asOf: timestamp("as_of", { withTimezone: true }),
    candidates: jsonb("candidates").notNull().default([]),
    scores: jsonb("scores").notNull().default([]),
    selectedIds: jsonb("selected_ids").notNull().default([]),
    tokenCount: integer("token_count"),
    packDigest: text("pack_digest"),
    memoryTruncated: integer("memory_truncated").notNull().default(0),
    feedback: jsonb("feedback"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    index("memory_retrieval_log_org_subject_idx").on(
      t.orgId,
      t.subjectProject,
      t.createdAt,
    ),
    index("memory_retrieval_log_org_digest_idx").on(t.orgId, t.packDigest),
  ],
);

// --- Daily digest narrative cache ---
// See docs/specs/2026-09-05-reflex-day-view.md §1.
// The stats/projects/epics/sessions rollup is always recomputed from convergence_events —
// this table caches only the model-written prose over it, keyed so a narrative is
// regenerated when the rollup that produced it actually changed, not on every page load.

export const dailyDigests = sessionsSchema.table(
  "daily_digests",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    // The calendar day in `tz`, e.g. "2026-09-05" — not a UTC date, and not a timestamp:
    // the row's identity is the civil day the caller asked about.
    day: text("day").notNull(),
    tz: text("tz").notNull(),
    // Exact `project_id`, or "" for the org-wide digest. The same empty-string-for-null
    // convention §2's cursor uses, so the primary key never has to treat NULL as a value.
    scopeKey: text("scope_key").notNull(),
    // Hash over the deterministic rollup that produced `narrativeText`. A fresh request
    // recomputes the rollup and its hash first; a mismatch here is what triggers
    // regeneration, not the row's age.
    statsHash: text("stats_hash").notNull(),
    narrativeText: text("narrative_text"),
    narrativeModel: text("narrative_model"),
    narrativeGeneratedAt: timestamp("narrative_generated_at", {
      withTimezone: true,
    }),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.orgId, t.day, t.tz, t.scopeKey] }),
    index("daily_digests_org_day_idx").on(t.orgId, t.day),
  ],
);

export type AuthSession = typeof authSessions.$inferSelect;

// A read request must never masquerade as a collector heartbeat.
export const probeConnections = sessionsSchema.table(
  "probe_connections",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    userId: text("user_id").notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).notNull(),
    progress: jsonb("progress"),
  },
  (t) => [primaryKey({ columns: [t.orgId, t.workspaceId, t.userId] })],
);

/** Authenticated workspace-scoped durable delivery, separate from legacy ingest. */
export const deliveryOrigins = sessionsSchema.table(
  "delivery_origins",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    originId: text("origin_id").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.orgId, table.workspaceId, table.originId] }),
  ],
);
export const deliveryRecords = sessionsSchema.table(
  "delivery_records",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    originId: text("origin_id").notNull(),
    recordId: text("record_id").notNull(),
    revisionId: text("revision_id").notNull(),
    revision: bigint("revision", { mode: "number" }).notNull(),
    digest: text("digest").notNull(),
    kind: text("kind").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id"),
    operation: text("operation").notNull(),
    payload: jsonb("payload"),
    userId: text("user_id").notNull(),
    receivedAt: timestamp("received_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    primaryKey({
      columns: [table.orgId, table.workspaceId, table.originId, table.recordId],
    }),
    index("delivery_records_lookup").on(
      table.orgId,
      table.workspaceId,
      table.kind,
      table.source,
      table.sessionId,
      table.originId,
      table.recordId,
    ),
  ],
);
export const deliveryReceipts = sessionsSchema.table(
  "delivery_receipts",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    originId: text("origin_id").notNull(),
    batchId: text("batch_id").notNull(),
    digest: text("digest").notNull(),
    receipt: jsonb("receipt").notNull(),
    receivedAt: timestamp("received_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    primaryKey({
      columns: [table.orgId, table.workspaceId, table.originId, table.batchId],
    }),
  ],
);

// Content-free, tenant-scoped evaluation cache. Policy/model and message content
// are hashed into cacheKey; failures carry a retry lease rather than a verdict.
export const sessionEvaluations = sessionsSchema.table(
  "session_evaluations",
  {
    orgId: text("org_id").notNull(),
    cacheKey: text("cache_key").notNull(),
    status: text("status"),
    confidence: integer("confidence_basis_points"),
    retryAt: timestamp("retry_at", { withTimezone: true }).notNull(),
  },
  (t) => [primaryKey({ columns: [t.orgId, t.cacheKey] })],
);

// --- Session catalog (migration 0022) ---
// Typed projections of the delivery kinds that describe sessions rather than
// activity, maintained by a trigger on delivery_records. Session ids are unique
// only within (org, workspace, source), so every key carries all three. Each row
// names the delivery record (and the origin whose upsert won) it came from.

export const sessionCatalog = sessionsSchema.table(
  "session_catalog",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id").notNull(),
    recordId: text("record_id").notNull(),
    originId: text("origin_id").notNull(),
    userId: text("user_id").notNull(),
    title: text("title"),
    cwd: text("cwd"),
    gitBranch: text("git_branch"),
    repoUrl: text("repo_url"),
    initialCommit: text("initial_commit"),
    firstPrompt: text("first_prompt"),
    models: jsonb("models"),
    originator: text("originator"),
    agentVersion: text("agent_version"),
    workspaceRoots: jsonb("workspace_roots"),
    projectKey: text("project_key"),
    projectKeyMethod: text("project_key_method"),
    discoveryState: text("discovery_state"),
    firstActivityAt: timestamp("first_activity_at", { withTimezone: true }),
    lastActivityAt: timestamp("last_activity_at", { withTimezone: true }),
    receivedAt: timestamp("received_at", { withTimezone: true }).notNull(),
  },
  (t) => [
    primaryKey({
      columns: [t.orgId, t.workspaceId, t.source, t.sessionId],
    }),
    index("session_catalog_record_idx").on(t.orgId, t.workspaceId, t.recordId),
    index("session_catalog_org_session_idx").on(t.orgId, t.source, t.sessionId),
  ],
);

// Keyed by relationship_uid under the parent, never by the child: unlinked
// evidence has no child id, and two sidecars of one parent must stay two rows.
export const sessionRelationships = sessionsSchema.table(
  "session_relationships",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    source: text("source").notNull(),
    parentSessionId: text("parent_session_id").notNull(),
    relationshipUid: text("relationship_uid").notNull(),
    recordId: text("record_id").notNull(),
    originId: text("origin_id").notNull(),
    userId: text("user_id").notNull(),
    childSessionId: text("child_session_id"),
    relationship: text("relationship"),
    identityStatus: text("identity_status"),
    childAgentType: text("child_agent_type"),
    childAgentName: text("child_agent_name"),
    childModel: text("child_model"),
    spawnDepth: integer("spawn_depth"),
    evidenceKind: text("evidence_kind"),
    childHasEvents: boolean("child_has_events"),
    spawnedAt: timestamp("spawned_at", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true }),
    receivedAt: timestamp("received_at", { withTimezone: true }).notNull(),
  },
  (t) => [
    primaryKey({
      columns: [
        t.orgId,
        t.workspaceId,
        t.source,
        t.parentSessionId,
        t.relationshipUid,
      ],
    }),
    index("session_relationships_record_idx").on(
      t.orgId,
      t.workspaceId,
      t.recordId,
    ),
    index("session_relationships_child_idx")
      .on(t.orgId, t.workspaceId, t.source, t.childSessionId)
      .where(sql`${t.childSessionId} is not null`),
  ],
);

export const sessionMarkers = sessionsSchema.table(
  "session_markers",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id").notNull(),
    markerUid: text("marker_uid").notNull(),
    recordId: text("record_id").notNull(),
    originId: text("origin_id").notNull(),
    userId: text("user_id").notNull(),
    markerKind: text("marker_kind"),
    subkind: text("subkind"),
    ts: timestamp("ts", { withTimezone: true }),
    messageId: text("message_id"),
    parentId: text("parent_id"),
    turnId: text("turn_id"),
    text: text("text"),
    payload: jsonb("payload"),
    receivedAt: timestamp("received_at", { withTimezone: true }).notNull(),
  },
  (t) => [
    primaryKey({
      columns: [t.orgId, t.workspaceId, t.source, t.sessionId, t.markerUid],
    }),
    index("session_markers_record_idx").on(t.orgId, t.workspaceId, t.recordId),
  ],
);

// Order in which each delivery row's latest live catalog upsert passed the
// per-key lock (migration 0022). Ranks remaining contenders when the projected
// one leaves a key; received_at is a transaction-start stamp and cannot.
export const deliveryCatalogAcceptance = sessionsSchema.table(
  "delivery_catalog_acceptance",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    originId: text("origin_id").notNull(),
    recordId: text("record_id").notNull(),
    acceptedSeq: bigint("accepted_seq", { mode: "number" }).notNull(),
  },
  (t) => [
    primaryKey({
      columns: [t.orgId, t.workspaceId, t.originId, t.recordId],
    }),
  ],
);

// Delivered commit links. Separate from session_outcomes, which legacy ingest also
// writes and which is keyed without match_method.
export const sessionCommitLinks = sessionsSchema.table(
  "session_commit_links",
  {
    orgId: text("org_id").notNull(),
    workspaceId: text("workspace_id").notNull(),
    source: text("source").notNull(),
    sessionId: text("session_id").notNull(),
    commitSha: text("commit_sha").notNull(),
    matchMethod: text("match_method").notNull(),
    recordId: text("record_id").notNull(),
    originId: text("origin_id").notNull(),
    userId: text("user_id").notNull(),
    repo: text("repo"),
    branch: text("branch"),
    noteRef: text("note_ref"),
    // 0..10000 basis points; same invariant as session_outcomes.confidence.
    confidence: integer("confidence_basis_points"),
    files: jsonb("files"),
    numstat: jsonb("numstat"),
    evidence: jsonb("evidence"),
    linkedAt: timestamp("linked_at", { withTimezone: true }),
    receivedAt: timestamp("received_at", { withTimezone: true }).notNull(),
  },
  (t) => [
    primaryKey({
      columns: [
        t.orgId,
        t.workspaceId,
        t.source,
        t.sessionId,
        t.commitSha,
        t.matchMethod,
      ],
    }),
    index("session_commit_links_record_idx").on(
      t.orgId,
      t.workspaceId,
      t.recordId,
    ),
    index("session_commit_links_commit_idx").on(
      t.orgId,
      t.workspaceId,
      t.commitSha,
    ),
  ],
);
