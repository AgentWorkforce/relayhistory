import { sql } from "drizzle-orm";
import type { HistoryDb as Db } from "../db/database.js";
import {
  convergenceEvents,
  machines,
  sessionOutcomes,
  syncBatches,
} from "../db/schema.js";
import type { AuthContext } from "../env.js";
import type {
  ConvergenceRecordEnvelope,
  Cursors,
  IngestRequest,
  OutcomeEnvelope,
} from "./types.js";
import {
  resolveEventEmbeddings,
  type EmbeddingProvider,
  type EventEmbedding,
} from "./embed.js";
import { scrubRecord, scrubJson, scrubText } from "./scrub.js";
import { upsertSessionLink } from "./session-links.js";

type JsonRecord = Record<string, unknown>;
type Scope = {
  orgId: string;
  workspaceId: string;
  machineId: string;
  userId: string;
};

export interface IngestOutcome {
  received: number;
  accepted: number;
  cursors: Cursors;
}

export interface IngestOptions {
  embeddings?: EmbeddingProvider | null;
}

/**
 * The CLI's version, under either name it may arrive as. `ai-hist` serializes this field
 * as `cliVersion` (see `MachineIdentity` in ai-hist-core), while the column and the API
 * contract call it `relayhistoryVersion`; reading only the latter silently dropped it on
 * every push. Prefer the canonical name so a future client that sends it wins.
 */
function machineVersion(machine: IngestRequest["machine"]): string | null {
  return machine.relayhistoryVersion ?? machine.cliVersion ?? null;
}

export async function applyIngest(
  db: Db,
  auth: AuthContext,
  body: IngestRequest,
  options: IngestOptions = {},
): Promise<IngestOutcome> {
  const workspaceId = auth.workspaceId ?? "default";
  const scope = {
    orgId: auth.orgId,
    workspaceId,
    userId: auth.userId,
    machineId: body.machine.id,
  };

  await db
    .insert(machines)
    .values({
      orgId: scope.orgId,
      workspaceId,
      machineId: scope.machineId,
      hostname: body.machine.hostname ?? null,
      label: body.machine.label ?? null,
      os: body.machine.os ?? null,
      relayhistoryVersion: machineVersion(body.machine),
      cursorsJson: body.cursors ?? {},
      lastSeenAt: new Date(),
    })
    .onConflictDoUpdate({
      target: [machines.orgId, machines.machineId],
      set: {
        workspaceId,
        // Never downgrade known identity to unknown. These fields are best-effort on the
        // client — the CLI reads `hostname` from $HOSTNAME, which launchd does not export,
        // so the scheduled push sends null while an interactive one sends the real name.
        // Clobbering on every push meant a machine's hostname was erased within one timer
        // tick of being learned, leaving an opaque machine id as its only identifier.
        // A client that genuinely reports a new value still wins; only null is ignored.
        hostname: sql`COALESCE(EXCLUDED.hostname, ${machines.hostname})`,
        label: sql`COALESCE(EXCLUDED.label, ${machines.label})`,
        os: sql`COALESCE(EXCLUDED.os, ${machines.os})`,
        relayhistoryVersion: sql`COALESCE(EXCLUDED.relayhistory_version, ${machines.relayhistoryVersion})`,
        // Cursors and freshness are always the newest push's — they describe this push,
        // not the machine.
        cursorsJson: body.cursors ?? {},
        lastSeenAt: new Date(),
      },
    });

  const queue: Array<
    | { type: "outcome"; envelope: OutcomeEnvelope }
    | { type: "event"; event: PreparedEvent }
  > = [];
  for (const envelope of body.records) {
    if (envelope.kind === "session_outcome") {
      queue.push({ type: "outcome", envelope: envelope as OutcomeEnvelope });
      continue;
    }
    const event = prepareConvergenceEvent(
      scope,
      envelope as ConvergenceRecordEnvelope,
    );
    if (event) {
      queue.push({ type: "event", event });
    }
  }

  // Scrub first, then embed the post-scrub content in bounded batches so a
  // 1,000-record ingest is not 1,000 serial provider round-trips. Never embed
  // pre-scrub text. Embeddings stay on the ingest path (spec §4.4 / §9 step 2).
  const eventItems = queue.filter(
    (item): item is { type: "event"; event: PreparedEvent } =>
      item.type === "event",
  );
  const embeddings = await resolveEventEmbeddings(
    eventItems.map((item) => ({
      content: item.event.content,
      eventId: item.event.eventId,
    })),
    options.embeddings,
  );

  let accepted = 0;
  let embedIndex = 0;
  for (const item of queue) {
    if (item.type === "outcome") {
      accepted += await upsertSessionOutcome(db, scope, item.envelope);
      continue;
    }
    const embedding = embeddings[embedIndex];
    if (!embedding) {
      throw new Error(`ingest embedding result missing at index ${embedIndex}`);
    }
    accepted += await writeConvergenceEvent(db, scope, item.event, embedding);
    // The existing taskRef contract can explicitly identify a GitHub PR. Project only
    // that unambiguous reference; a lens value alone never invents an artifact link.
    const envelope = item.event.envelope;
    const taskRef = envelope.taskRef;
    if (
      envelope.lens === "github" &&
      taskRef?.system === "github" &&
      typeof taskRef.id === "string" &&
      /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+#[1-9][0-9]*$/.test(taskRef.id) &&
      taskRef.url === `https://github.com/${taskRef.id.replace("#", "/pull/")}`
    ) {
      await upsertSessionLink(db, auth, {
        source: envelope.source,
        sessionId: envelope.sessionId,
        linkKind: "github_pr",
        linkRef: taskRef.id,
        linkUrl: `https://github.com/${taskRef.id.replace("#", "/pull/")}`,
        linkTs: item.event.fields.ts.toISOString(),
        provenanceLens: "github",
        confidence: envelope.confidence,
      });
    }

    embedIndex += 1;
  }

  await db
    .insert(syncBatches)
    .values({
      id: body.batchId,
      orgId: scope.orgId,
      workspaceId,
      machineId: scope.machineId,
      recordCount: body.records.length,
      acceptedCount: accepted,
      cursorsJson: body.cursors ?? {},
    })
    .onConflictDoNothing({
      target: [syncBatches.orgId, syncBatches.machineId, syncBatches.id],
    });

  return {
    received: body.records.length,
    accepted,
    cursors: body.cursors ?? {},
  };
}

type PreparedEvent = {
  envelope: ConvergenceRecordEnvelope;
  eventId: string;
  content: string | null;
  fields: Omit<
    typeof convergenceEvents.$inferInsert,
    | "orgId"
    | "machineId"
    | "source"
    | "sessionId"
    | "eventId"
    | "embedding"
    | "embeddingModel"
    | "embeddingDim"
    | "contentHash"
    | "generatedAt"
    | "embeddingSkipReason"
  >;
};

function prepareConvergenceEvent(
  scope: Scope,
  envelope: ConvergenceRecordEnvelope,
): PreparedEvent | null {
  const eventId = eventIdentity(envelope);
  const ts = eventTimestamp(envelope);
  if (!eventId || !ts) {
    return null;
  }

  const usage = envelope.usage ?? {};
  const rawRecord = recordValue(envelope.record);
  let record = scrubRecord(rawRecord);
  const task = recordValue(record.task);
  const taskTitle =
    envelope.taskTitle ??
    stringValue(record.taskTitle) ??
    stringValue(task.title) ??
    null;
  const taskDescription =
    envelope.taskDescription ??
    stringValue(record.taskDescription) ??
    stringValue(task.description) ??
    null;
  const taskStatus =
    envelope.taskStatus ??
    stringValue(record.taskStatus) ??
    stringValue(record.status) ??
    null;
  const content = buildReadableContent(envelope.content, {
    title: taskTitle,
    description: taskDescription,
  });
  record = withTaskContext(record, {
    title: taskTitle,
    description: taskDescription,
    status: taskStatus,
  });
  record = withDecisionContext(record, rawRecord, envelope);
  const fields = {
    workspaceId: scope.workspaceId,
    userId: scope.userId,
    kind: envelope.kind,
    lens: envelope.lens ?? stringValue(record.lens) ?? null,
    type: envelope.type ?? envelope.kind,
    ts,
    actorName: envelope.actorName ?? stringValue(record.actorName) ?? null,
    actorRole: envelope.actorRole ?? stringValue(record.actorRole) ?? null,
    subagentId: envelope.subagentId ?? stringValue(record.subagentId) ?? null,
    trajectoryId:
      envelope.trajectoryId ??
      stringValue(record.trajectoryId) ??
      trajectoryIdFromLens(envelope) ??
      null,
    chapterId: envelope.chapterId ?? stringValue(record.chapterId) ?? null,
    projectId: envelope.projectId ?? stringValue(record.projectId) ?? null,
    workflowId: envelope.workflowId ?? stringValue(record.workflowId) ?? null,
    taskRef: normalizeTaskRef(envelope.taskRef ?? record.taskRef),
    taskTitle: taskTitle ? scrubText(taskTitle) : null,
    taskDescription: taskDescription ? scrubText(taskDescription) : null,
    taskStatus: taskStatus ? scrubText(taskStatus) : null,
    content,
    significance: envelope.significance ?? null,
    confidence: toBasisPoints(envelope.confidence),
    tags: Array.isArray(envelope.tags) ? envelope.tags : [],
    model: envelope.model ?? stringValue(record.model) ?? null,
    provider: envelope.provider ?? stringValue(record.provider) ?? null,
    inputTokens: numberValue(usage.input),
    outputTokens: numberValue(usage.output),
    reasoningTokens: numberValue(usage.reasoning),
    cacheReadTokens: numberValue(usage.cacheRead),
    cacheCreateTokens:
      numberValue(usage.cacheCreate) +
      numberValue(usage.cacheCreate5m) +
      numberValue(usage.cacheCreate1h),
    ...cacheWriteSplit(usage),
    // NULL, not 0, when no cost was sent: "unreported" is not "free".
    costUsdMicros: optionalNumber(envelope.costUsdMicros),
    toolName: envelope.toolName ?? stringValue(record.toolName) ?? null,
    toolStatus: envelope.toolStatus ?? stringValue(record.toolStatus) ?? null,
    toolCalls: Array.isArray(envelope.toolCalls)
      ? scrubJson(envelope.toolCalls)
      : Array.isArray(record.toolCalls)
        ? scrubJson(record.toolCalls)
        : [],
    retries: numberValue(envelope.retries),
    durationMs: optionalInteger(envelope.durationMs),
    filesTouched: Array.isArray(envelope.filesTouched)
      ? scrubJson(envelope.filesTouched)
      : [],
    codeChurn: envelope.codeChurn ? scrubJson(envelope.codeChurn) : {},
    record,
  };

  return { envelope, eventId, content, fields };
}

async function writeConvergenceEvent(
  db: Db,
  scope: Scope,
  event: PreparedEvent,
  embedding: EventEmbedding,
): Promise<number> {
  const fields = {
    ...event.fields,
    embedding: embedding.embedding,
    embeddingModel: embedding.embeddingModel,
    embeddingDim: embedding.embeddingDim,
    contentHash: embedding.contentHash,
    generatedAt: embedding.generatedAt,
    embeddingSkipReason: embedding.embeddingSkipReason,
  };

  await db
    .insert(convergenceEvents)
    .values({
      orgId: scope.orgId,
      machineId: scope.machineId,
      source: event.envelope.source,
      sessionId: event.envelope.sessionId,
      eventId: event.eventId,
      ...fields,
    })
    .onConflictDoUpdate({
      target: [
        convergenceEvents.orgId,
        convergenceEvents.machineId,
        convergenceEvents.source,
        convergenceEvents.sessionId,
        convergenceEvents.kind,
        convergenceEvents.eventId,
      ],
      // Never blank a stored vector on replay. Outbox retries and trajectory
      // re-pushes re-upsert the same event; if the provider is down, unconfigured,
      // or returns the wrong dimension, EXCLUDED.embedding is NULL. Keep the
      // previous vector (and its provenance) when the content hash is unchanged;
      // if the content changed and we could not embed, drop the stale vector.
      // Always record this attempt's skip reason.
      set: {
        ...fields,
        embedding: sql`CASE
          WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding
          WHEN EXCLUDED.content_hash IS NOT DISTINCT FROM ${convergenceEvents.contentHash}
            THEN ${convergenceEvents.embedding}
          ELSE NULL
        END`,
        embeddingModel: sql`CASE
          WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding_model
          WHEN EXCLUDED.content_hash IS NOT DISTINCT FROM ${convergenceEvents.contentHash}
            THEN ${convergenceEvents.embeddingModel}
          ELSE NULL
        END`,
        embeddingDim: sql`CASE
          WHEN EXCLUDED.embedding IS NOT NULL THEN EXCLUDED.embedding_dim
          WHEN EXCLUDED.content_hash IS NOT DISTINCT FROM ${convergenceEvents.contentHash}
            THEN ${convergenceEvents.embeddingDim}
          ELSE NULL
        END`,
        generatedAt: sql`CASE
          WHEN EXCLUDED.generated_at IS NOT NULL THEN EXCLUDED.generated_at
          WHEN EXCLUDED.content_hash IS NOT DISTINCT FROM ${convergenceEvents.contentHash}
            THEN ${convergenceEvents.generatedAt}
          ELSE NULL
        END`,
      },
    });

  return 1;
}

async function upsertSessionOutcome(
  db: Db,
  scope: Scope,
  envelope: OutcomeEnvelope,
): Promise<number> {
  const commitSha = envelope.commitSha ?? envelope.commit_sha;
  if (!commitSha || !envelope.source || !envelope.sessionId) {
    return 0;
  }

  const filesJson =
    envelope.filesJson ?? envelope.files_json ?? envelope.files ?? [];
  const filesTouched = envelope.filesTouched ?? envelope.files_touched;
  const projectId =
    stringValue(envelope.projectId) ?? stringValue(envelope.project_id) ?? null;
  // Presence, not value, decides whether a replay may overwrite: an explicit
  // `projectId: null` / `filesTouched: []` is a correction that clears the field,
  // while an envelope that omits the property leaves the stored value alone.
  const sentProject = "projectId" in envelope || "project_id" in envelope;
  const sentFiles = "filesTouched" in envelope || "files_touched" in envelope;
  const fields = {
    machineId: scope.machineId,
    userId: scope.userId,
    repo: envelope.repo ? scrubText(envelope.repo) : null,
    branch: envelope.branch ? scrubText(envelope.branch) : null,
    matchMethod: outcomeMatchMethod(envelope),
    confidence: toBasisPoints(envelope.confidence),
    projectId: projectId ? scrubText(projectId) : null,
    filesTouched: Array.isArray(filesTouched) ? scrubJson(filesTouched) : [],
    numstatJson: recordValue(
      envelope.numstatJson ?? envelope.numstat_json ?? envelope.numstat,
    ),
    filesJson: Array.isArray(filesJson) ? scrubJson(filesJson) : [],
    shippedAt: dateValue(envelope.shippedAt ?? envelope.shipped_at),
    reverted: envelope.reverted ?? false,
    revertedBySha: envelope.revertedBySha ?? envelope.reverted_by_sha ?? null,
    revertedAt: dateValue(envelope.revertedAt ?? envelope.reverted_at),
    ingestedAt: new Date(),
  };

  await db
    .insert(sessionOutcomes)
    .values({
      orgId: scope.orgId,
      workspaceId: scope.workspaceId,
      source: envelope.source,
      sessionId: envelope.sessionId,
      commitSha,
      ...fields,
    })
    .onConflictDoUpdate({
      target: [
        sessionOutcomes.orgId,
        sessionOutcomes.workspaceId,
        sessionOutcomes.source,
        sessionOutcomes.sessionId,
        sessionOutcomes.commitSha,
      ],
      // One row per (session, commit), so the uploader's several match methods for
      // one commit compete for it. The strongest link wins: a replay or a weaker
      // method (lower confidence, or none) never overwrites the method and confidence
      // of a stronger one; on a tie the newest write wins, so a same-method replay
      // still updates. Readers (session thread, digest, reflex learn) count one row
      // per commit and need no change. The commit's own facts (numstat, files,
      // shipped/reverted) are the same whichever method found it, so they always take
      // the newest write. Project and touched files are never erased by an envelope
      // that omits them, but an explicit null / [] clears them.
      set: {
        ...fields,
        matchMethod: sql`CASE WHEN ${weakerOutcome} THEN ${sessionOutcomes.matchMethod} ELSE EXCLUDED.match_method END`,
        confidence: sql`CASE WHEN ${weakerOutcome} THEN ${sessionOutcomes.confidence} ELSE EXCLUDED.confidence_basis_points END`,
        projectId: sentProject
          ? sql`EXCLUDED.project_id`
          : sql`${sessionOutcomes.projectId}`,
        filesTouched: sentFiles
          ? sql`EXCLUDED.files_touched`
          : sql`${sessionOutcomes.filesTouched}`,
      },
    });

  return 1;
}

/** The incoming outcome links its commit less confidently than the stored row. */
const weakerOutcome = sql`COALESCE(EXCLUDED.confidence_basis_points, -1) < COALESCE(${sessionOutcomes.confidence}, -1)`;

/**
 * The Anthropic 5-minute / 1-hour cache-write split, priced differently, so it is kept
 * alongside the summed `cache_create_tokens`. NULL when the client reported neither
 * bucket. When it reported one, the other is a known zero. A reader must still check
 * that the split covers the total: an unsplit `cacheCreate` sent alongside it is part
 * of the sum but of neither bucket.
 */
function cacheWriteSplit(usage: Record<string, unknown>): {
  cacheCreate5mTokens: number | null;
  cacheCreate1hTokens: number | null;
} {
  const fiveMinute = optionalNumber(usage.cacheCreate5m);
  const oneHour = optionalNumber(usage.cacheCreate1h);
  if (fiveMinute === null && oneHour === null) {
    return { cacheCreate5mTokens: null, cacheCreate1hTokens: null };
  }
  return {
    cacheCreate5mTokens: fiveMinute ?? 0,
    cacheCreate1hTokens: oneHour ?? 0,
  };
}

function outcomeMatchMethod(envelope: OutcomeEnvelope): string | null {
  const value = envelope.matchMethod ?? envelope.match_method;
  return value ? scrubText(value) : null;
}

function eventIdentity(
  envelope: ConvergenceRecordEnvelope,
): string | undefined {
  const explicit =
    envelope.eventId ??
    envelope.messageId ??
    envelope.requestId ??
    envelope.fingerprint ??
    envelope.idFingerprint ??
    stringValue(envelope.record?.eventId) ??
    stringValue(envelope.record?.id);
  if (explicit) {
    return namespaceEventId(envelope.kind, explicit);
  }
  const scopeId = envelope.chapterId ?? envelope.trajectoryId;
  if (scopeId && Number.isInteger(envelope.eventIndex)) {
    const group = envelope.eventGroup ? `${envelope.eventGroup}:` : "";
    return `${envelope.kind}:${scopeId}:${group}${envelope.eventIndex}`;
  }
  return undefined;
}

function namespaceEventId(kind: string, eventId: string): string {
  return eventId.includes(":") ? eventId : `${kind}:${eventId}`;
}

function eventTimestamp(envelope: ConvergenceRecordEnvelope): Date | null {
  return (
    dateValue(envelope.ts) ??
    dateValue(envelope.startTs) ??
    dateValue(envelope.writtenAt)
  );
}

function trajectoryIdFromLens(
  envelope: ConvergenceRecordEnvelope,
): string | null {
  return envelope.lens === "trajectories" ? envelope.sessionId : null;
}

function withTaskContext(
  record: JsonRecord,
  task: {
    title?: string | null;
    description?: string | null;
    status?: string | null;
  },
): JsonRecord {
  const nextTask = recordValue(record.task);
  if (task.title) {
    nextTask.title = scrubText(task.title);
  }
  if (task.description) {
    nextTask.description = scrubText(task.description);
  }
  if (task.status) {
    nextTask.status = scrubText(task.status);
  }
  return Object.keys(nextTask).length > 0
    ? { ...record, task: nextTask }
    : record;
}

function withDecisionContext(
  record: JsonRecord,
  rawRecord: JsonRecord,
  envelope: ConvergenceRecordEnvelope,
): JsonRecord {
  if (envelope.kind !== "decision") {
    return record;
  }

  const rawDecision = recordValue(rawRecord.decision);
  const existingDecision = recordValue(record.decision);
  const decision: JsonRecord = { ...existingDecision };
  const chosen =
    stringValue(rawRecord.chosen) ?? stringValue(rawDecision.chosen);
  const reasoning =
    stringValue(rawRecord.reasoning) ?? stringValue(rawDecision.reasoning);
  const question =
    stringValue(rawRecord.question) ?? stringValue(rawDecision.question);
  const alternatives = Array.isArray(rawRecord.alternatives)
    ? scrubJson(rawRecord.alternatives)
    : Array.isArray(rawDecision.alternatives)
      ? scrubJson(rawDecision.alternatives)
      : undefined;

  if (question) decision.question = scrubText(question);
  if (chosen) decision.chosen = scrubText(chosen);
  if (reasoning) decision.reasoning = scrubText(reasoning);
  if (alternatives) decision.alternatives = alternatives;

  return Object.keys(decision).length > 0
    ? {
        ...record,
        decision,
      }
    : record;
}

export function buildReadableContent(
  content: unknown,
  task: { title?: string | null; description?: string | null },
): string | null {
  const lines: string[] = [];
  const body = typeof content === "string" ? content.trim() : "";
  const taskTitle = task.title?.trim();
  const taskDescription = task.description?.trim();

  if (taskTitle && !body.startsWith(`Task: ${taskTitle}`)) {
    lines.push(`Task: ${taskTitle}`);
  }
  if (
    taskDescription &&
    !body.includes(`Task description: ${taskDescription}`)
  ) {
    lines.push(`Task description: ${taskDescription}`);
  }
  if (body) {
    if (lines.length > 0) {
      lines.push("");
    }
    lines.push(body);
  }
  return lines.length > 0 ? scrubText(lines.join("\n")) : null;
}

function toBasisPoints(value: unknown): number | null {
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > 1
  ) {
    return null;
  }
  return Math.round(value * 10_000);
}

function dateValue(value: unknown): Date | null {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return new Date(value);
  }
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function numberValue(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function optionalNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function optionalInteger(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) ? value : null;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function recordValue(value: unknown): JsonRecord {
  return value && typeof value === "object" && !Array.isArray(value)
    ? scrubJson(value as JsonRecord)
    : {};
}

function normalizeTaskRef(value: unknown): JsonRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const input = value as JsonRecord;
  const output: JsonRecord = {};
  const system = stringValue(input.system);
  const id = stringValue(input.id);
  const url = stringValue(input.url);
  if (system) output.system = scrubText(system);
  if (id) output.id = scrubText(id);
  if (url) output.url = scrubText(url);
  return output;
}
