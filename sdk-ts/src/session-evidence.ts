/** Typed full-session evidence from the Rust SessionStore facade. */
import type { Source, EvidenceKind } from './sdk-common.js';
import type { ControlKind, NormalizedUsage, SessionRequest, SessionUsage, SessionUserTurn } from './contracts.js';
import { sessionStoreCall, SESSION_STORE_OPS } from './native.js';
import { evidenceIdentity, sourceOrder } from './normalization.js';

export interface EvidenceSession {
  source: Source;
  sessionId: string;
  cwd: string | null;
  gitBranch: string | null;
  firstActivityMs: number | null;
  lastActivityMs: number | null;
  firstPrompt: string | null;
  lastAssistantText: string | null;
  title: string | null;
  sessionKind: string | null;
  agentName: string | null;
  models: Array<string>;
  originator: string | null;
  agentVersion: string | null;
  repoUrl: string | null;
  initialCommit: string | null;
  workspaceRoots: Array<string>;
  projectKey: string | null;
  projectKeyMethod: string | null;
  rawPath: string | null;
  sourceStamp: string | null;
  discoveryState: 'shallow' | 'full' | 'delegated';
  locations: Array<string>;
}

export interface EvidencePrompt {
  eventUid: string | null;
  prompt: string | null;
  promptHash: string | null;
  promptBytes: number;
  project: string | null;
  timestampMs: number;
}

export interface EvidenceMessage {
  messageId: string | null;
  role: 'user' | 'assistant' | 'tool_result';
  tsMs: number;
  parentId: string | null;
  model: string | null;
  provider: string | null;
  requestId: string | null;
  providerMessageId: string | null;
  requestSpan: string | null;
  stopReason: string | null;
  turnId: string | null;
  agentVersion: string | null;
  isSidechain: boolean | null;
  isMeta: boolean | null;
  project: string | null;
  projectKey: string | null;
  cwd: string | null;
  gitBranch: string | null;
  usage: NormalizedUsage | null;
  usageError: string | null;
  blocks: Array<EvidenceBlock>;
  rawUsage?: string;
}

export interface EvidenceBlock {
  sourceOrder: number | null;
  eventUid: string;
  role: 'user' | 'assistant' | 'tool_result';
  kind: 'text' | 'thinking' | 'tool_use' | 'tool_result';
  text: string | null;
  textBytes: number | null;
  toolUseId: string | null;
  rawKind: string | null;
  control: ControlKind | null;
  tsMs: number;
}

export interface EvidenceToolCall {
  toolUseId: string;
  messageId: string | null;
  name: string;
  target: string | null;
  args: unknown | null;
  isError: boolean | null;
  tsMs: number | null;
  rawArgs?: string;
}

export interface EvidenceToolResult {
  eventUid: string;
  messageId: string | null;
  tsMs: number;
  toolUseId: string | null;
  callIndex: number | null;
  eventIndex: number | null;
  payloadBytes: number | null;
  payloadTruncated: boolean | null;
  payloadHash: string | null;
  resultStatus: string | null;
  eventSource: string | null;
  errorSignal: string | null;
  subagentSessionId: string | null;
  agentId: string | null;
  text: string | null;
  textBytes: number | null;
  rawKind: string | null;
}

export interface EvidenceFileEdit {
  toolUseId: string;
  messageId: string | null;
  filePath: string;
  toolName: string | null;
  linesAdded: number | null;
  linesRemoved: number | null;
  structuredPatch: unknown | null;
  userModified: boolean | null;
  tsMs: number | null;
  gitBranch: string | null;
  cwd: string | null;
  rawStructuredPatch?: string;
}

export interface EvidenceMarker {
  markerUid: string;
  tsMs: number | null;
  messageId: string | null;
  parentId: string | null;
  turnId: string | null;
  kind: string;
  subkind: string | null;
  text: string | null;
  payload: unknown | null;
  usageSnapshot?: Record<string, unknown> | null;
  /** Exact JSON, including counters outside JavaScript's safe integer range. */
  rawUsageSnapshot?: string;
  rawPayload?: string;
}

export interface EvidenceRelationship {
  side: 'parent' | 'child' | 'continuity';
  relationship: string;
  relationshipUid: string;
  parentSessionId: string;
  childSessionId: string | null;
  identityStatus: string;
  childAgentType: string | null;
  childAgentName: string | null;
  childModel: string | null;
  spawnDepth: number | null;
  evidenceKind: string;
  evidenceLocator: string | null;
  evidenceRef: string | null;
  childHasEvents: boolean;
  spawnedAtMs: number | null;
  createdMs: number;
  originSessionId: string | null;
}

export interface EvidenceDiagnostic {
  code: string;
  message: string;
  subject: string | null;
}

export interface SessionEvidence {
  session: EvidenceSession;
  prompts: Array<EvidencePrompt>;
  messages: Array<EvidenceMessage>;
  toolCalls: Array<EvidenceToolCall>;
  toolResults: Array<EvidenceToolResult>;
  fileEdits: Array<EvidenceFileEdit>;
  markers: Array<EvidenceMarker>;
  relationships: Array<EvidenceRelationship>;
  requests: Array<SessionRequest>;
  usage: SessionUsage | null;
  userTurns: Array<SessionUserTurn>;
  coverage: Array<EvidenceKind>;
  loaded: Array<EvidenceKind>;
  includeText: boolean;
  diagnostics: Array<EvidenceDiagnostic>;
}

export interface SessionEvidenceOptions {
  dbPath?: string;
  /** Exclude transcript text while retaining hashes and byte measurements. */
  includeText?: boolean;
  /** Omit to read all evidence kinds; an empty array reads metadata only. */
  kinds?: readonly EvidenceKind[];
}

/** Convert only a record's field names. Provider JSON remains opaque. */
function record(value: unknown): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value as Record<string, unknown>)
    .map(([key, item]) => [key.replace(/_([a-z])/g, (_, letter: string) => letter.toUpperCase()), item]));
}

function normalizeEvidence(value: Record<string, unknown>): SessionEvidence {
  const result = record(value);
  result.session = record(value.session);
  for (const field of ['prompts', 'tool_calls', 'tool_results', 'file_edits', 'markers', 'relationships', 'diagnostics']) {
    result[field.replace(/_([a-z])/g, (_, letter: string) => letter.toUpperCase())] =
      (value[field] as unknown[]).map(record);
  }
  result.prompts = (value.prompts as Record<string, unknown>[]).map((prompt) => ({
    ...record(prompt), eventUid: prompt.event_uid ?? null,
  }));
  result.messages = (value.messages as Record<string, unknown>[]).map((message) => ({
    ...record(message), blocks: (message.blocks as Record<string, unknown>[]).map((block) => ({
      ...record(block), sourceOrder: sourceOrder(block.source_order),
    })),
  }));
  // Requests, usage and user turns already use their paged camelCase wire shapes.
  return result as unknown as SessionEvidence;
}

/**
 * Read the stored evidence for one session on a single database snapshot.
 * Returns null for an unknown session or missing database. This does not hydrate
 * provider files. Usage is provider-reported; no cost is computed. Opaque tool
 * arguments, patches and marker payloads retain the provider's original keys.
 */
export async function getSessionEvidence(
  source: Source, sessionId: string, options: SessionEvidenceOptions = {},
): Promise<SessionEvidence | null> {
  evidenceIdentity(source, sessionId, 'getSessionEvidence');
  const result = await sessionStoreCall(SESSION_STORE_OPS.session, { ...options, source, sessionId });
  return result.evidence === null ? null : normalizeEvidence(result.evidence as Record<string, unknown>);
}
