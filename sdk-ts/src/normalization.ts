import {
  SESSION_CATALOG_CONTRACT_VERSION,
  SESSION_HYDRATION_CONTRACT_VERSION,
  SESSION_RELATIONSHIP_CONTRACT_VERSION,
  SESSION_EVIDENCE_CONTRACT_VERSION,
  SOURCES,
  Source,
  CatalogSource,
  CATALOG_SOURCES,
  isCatalogSource,
  SessionScope,
  SessionLocation,
  NativeContractMismatchError,
  InvalidArgumentError,
  SessionSourceUnavailableError,
  SessionSourceMismatchError,
  HydrationUnsupportedError,
  HydrationFailedError,
} from './sdk-common.js';

import type {
  HistoryEntry,
  ListOptions,
  SearchOptions,
  SessionOptions,
  CatalogCursor,
  CatalogSession,
  ListCatalogOptions,
  SessionCatalogPage,
  DiscoveryDiagnostic,
  ProviderDiscoverySummary,
  DiscoveryCounters,
  SourceExemption,
  SourceConnectorOptions,
  DiscoverSessionsOptions,
  DiscoverResult,
  SessionRef,
  HydrateSessionOptions,
  HydrationDiagnostic,
  HydrateSessionResult,
  SessionEvent,
  SessionUserTurn,
  SessionUserTurnBlock,
  ProjectKeyMethod,
  EventCursor,
  EventsPageOptions,
  SessionEventsPage,
  RelationshipType,
  IdentityStatus,
  StableChildIdentity,
  SessionRelationship,
  RelationshipCapabilities,
  RelationshipDiagnostic,
  GetSessionRelationshipsOptions,
  SessionRelationships,
  SessionTreeNode,
  GetSessionTreeOptions,
  SessionTree,
  RelationshipCursor,
  SessionChildrenPage,
  GetSessionChildrenPageOptions,
  SessionDescendantsOptions,
  DescendantEventsOptions,
  JsonValue,
  SessionToolCall,
  SessionFileEdit,
  EvidenceCursor,
  EvidenceCursorInput,
  EvidencePageOptions,
  SessionToolCallsPage,
  SessionFileEditsPage,
  Stats,
  StatsOptions,
  SyncOptions,
  SyncResult,
} from './contracts.js';
export type UnknownRecord = Record<string, unknown>;

export const RELATIONSHIP_TYPES: readonly string[] = ['delegated'];
export const DEFAULT_TREE_MAX_DEPTH = 32;
export const MAX_TREE_MAX_DEPTH = 64;
export const DEFAULT_TREE_MAX_NODES = 1_000;
export const MAX_TREE_MAX_NODES = 10_000;

export function nullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

export function historyEntry(value: UnknownRecord): HistoryEntry {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: nullableString(value.sessionId),
    project: nullableString(value.project),
    prompt: String(value.prompt),
    timestampMs: Number(value.timestampMs),
    locations: Array.isArray(value.locations)
      ? value.locations.filter(
          (location): location is SessionLocation => location === 'local' || location === 'remote',
        )
      : [],
  };
}

export function catalogCursor(value: unknown): CatalogCursor | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return {
    lastActivityMs: typeof row.lastActivityMs === 'number' ? row.lastActivityMs : null,
    source: String(row.source),
    sessionId: String(row.sessionId),
  };
}

/**
 * Narrow a native `projectKeyMethod` to the documented set.
 *
 * An unrecognized value becomes null rather than being passed through: a
 * consumer branching on `'remote'` must not be handed a fourth case it has
 * never seen, and a silently widened union is exactly the shape a contract
 * mismatch takes.
 */
export function projectKeyMethod(value: unknown): ProjectKeyMethod | null {
  return value === 'remote' || value === 'path' || value === 'inherited' ? value : null;
}

export function catalogSession(value: UnknownRecord): CatalogSession {
  return {
    source: String(value.source) as CatalogSource,
    sessionId: String(value.sessionId),
    cwd: nullableString(value.cwd),
    gitBranch: nullableString(value.gitBranch),
    firstActivityMs: typeof value.firstActivityMs === 'number' ? value.firstActivityMs : null,
    lastActivityMs: typeof value.lastActivityMs === 'number' ? value.lastActivityMs : null,
    firstPrompt: nullableString(value.firstPrompt),
    lastAssistantText: nullableString(value.lastAssistantText),
    models: Array.isArray(value.models) ? value.models.map(String) : [],
    originator: nullableString(value.originator),
    agentVersion: nullableString(value.agentVersion),
    repoUrl: nullableString(value.repoUrl),
    initialCommit: nullableString(value.initialCommit),
    workspaceRoots: Array.isArray(value.workspaceRoots) ? value.workspaceRoots.map(String) : [],
    rawPath: nullableString(value.rawPath),
    sourceStamp: nullableString(value.sourceStamp),
    discoveryState: value.discoveryState === 'shallow' ? 'shallow' : 'full',
    projectKey: nullableString(value.projectKey),
    projectKeyMethod: projectKeyMethod(value.projectKeyMethod),
    fromCache: value.fromCache === true,
    locations: Array.isArray(value.locations)
      ? value.locations.filter(
          (location): location is SessionLocation => location === 'local' || location === 'remote',
        )
      : [],
  };
}

export function validateNativeLocation(value: unknown): SessionLocation {
  if (value === 'local' || value === 'remote') return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid session location: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function validateNativeScope(value: unknown): SessionScope {
  if (value === 'local' || value === 'remote' || value === 'all') return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid session scope: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function assertCatalogContract(value: number): void {
  if (value !== SESSION_CATALOG_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects catalog contract ${SESSION_CATALOG_CONTRACT_VERSION}, but native returned ${value}.`,
      'CATALOG_CONTRACT_MISMATCH',
    );
  }
}

export function tokenUsage(raw: unknown): Record<string, unknown> | null {
  if (typeof raw !== 'string') return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

export function sessionEvent(value: UnknownRecord): SessionEvent {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: String(value.sessionId),
    project: nullableString(value.project),
    projectKey: nullableString(value.projectKey),
    cwd: nullableString(value.cwd),
    gitBranch: nullableString(value.gitBranch),
    messageId: nullableString(value.messageId),
    parentId: nullableString(value.parentId),
    tsMs: Number(value.tsMs),
    role: String(value.role) as SessionEvent['role'],
    kind: String(value.kind) as SessionEvent['kind'],
    text: nullableString(value.text),
    model: nullableString(value.model),
    tokenUsage: tokenUsage(value.tokenJson),
    eventUid: String(value.eventUid),
    toolUseId: nullableString(value.toolUseId),
    payloadBytes: nullableNumber(value.payloadBytes),
    payloadTruncated: nullableBoolean(value.payloadTruncated),
    payloadHash: nullableString(value.payloadHash),
    callIndex: nullableNumber(value.callIndex),
    eventIndex: nullableNumber(value.eventIndex),
    resultStatus: nullableString(value.resultStatus) as SessionEvent['resultStatus'],
    eventSource: nullableString(value.eventSource) as SessionEvent['eventSource'],
    errorSignal: nullableString(value.errorSignal) as SessionEvent['errorSignal'],
    subagentSessionId: nullableString(value.subagentSessionId),
    agentId: nullableString(value.agentId),
  };
}

export function sessionUserTurnBlock(value: UnknownRecord): SessionUserTurnBlock {
  return {
    kind: value.kind === 'tool_result' ? 'tool_result' : 'text',
    toolUseId: nullableString(value.toolUseId),
    byteLen: Number(value.byteLen),
    isError: nullableBoolean(value.isError),
  };
}

export function sessionUserTurn(value: UnknownRecord): SessionUserTurn {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: String(value.sessionId),
    messageId: nullableString(value.messageId),
    tsMs: Number(value.tsMs),
    blocks: Array.isArray(value.blocks)
      ? (value.blocks as UnknownRecord[]).map(sessionUserTurnBlock)
      : [],
  };
}

export function assertRelationshipContract(value: number): void {
  if (value !== SESSION_RELATIONSHIP_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects relationship contract ${SESSION_RELATIONSHIP_CONTRACT_VERSION}, but native returned ${value}.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
}

/**
 * Provider JSON is stored as the raw string the provider wrote. A row whose
 * string cannot be parsed still belongs in its page, so parsing yields null
 * and the caller reads the raw companion field instead of losing the row.
 */
export function parseStoredJson(raw: unknown): JsonValue | null {
  if (typeof raw !== 'string') return null;
  try {
    return JSON.parse(raw) as JsonValue;
  } catch {
    return null;
  }
}

export function nullableNumber(value: unknown): number | null {
  return typeof value === 'number' ? value : null;
}

export function nullableBoolean(value: unknown): boolean | null {
  return typeof value === 'boolean' ? value : null;
}

export function sessionToolCall(value: UnknownRecord): SessionToolCall {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: String(value.sessionId),
    messageId: nullableString(value.messageId),
    toolUseId: String(value.toolUseId),
    name: String(value.name),
    target: nullableString(value.target),
    args: parseStoredJson(value.argsJson),
    argsJson: nullableString(value.argsJson),
    isError: nullableBoolean(value.isError),
    tsMs: nullableNumber(value.tsMs),
  };
}

export function sessionFileEdit(value: UnknownRecord): SessionFileEdit {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: String(value.sessionId),
    messageId: nullableString(value.messageId),
    toolUseId: String(value.toolUseId),
    filePath: String(value.filePath),
    toolName: nullableString(value.toolName),
    linesAdded: nullableNumber(value.linesAdded),
    linesRemoved: nullableNumber(value.linesRemoved),
    structuredPatch: parseStoredJson(value.structuredPatchJson),
    structuredPatchJson: nullableString(value.structuredPatchJson),
    userModified: nullableBoolean(value.userModified),
    tsMs: nullableNumber(value.tsMs),
    gitBranch: nullableString(value.gitBranch),
    cwd: nullableString(value.cwd),
  };
}

export function evidenceCursor(value: unknown): EvidenceCursor | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return { tsMs: nullableNumber(row.tsMs), id: Number(row.id) };
}

/**
 * The native boundary reads an absent `tsMs` as "inside the undated tail" but
 * cannot convert an explicit `null` into its integer field, so the cursor this
 * SDK hands back — `tsMs: null` for an undated row — has to be normalized
 * before it goes back in. The catalog cursor is normalized the same way.
 */
export function nativeEvidenceCursor(after: EvidenceCursorInput | undefined): object | undefined {
  return after ? { ...after, tsMs: after.tsMs ?? undefined } : undefined;
}

export function assertEvidenceContract(value: number): void {
  if (value !== SESSION_EVIDENCE_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects session evidence contract ${SESSION_EVIDENCE_CONTRACT_VERSION}, but native returned ${value}.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
}

export function catalogSource(value: unknown): CatalogSource {
  if (isCatalogSource(value)) return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid catalog source: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function relationshipType(value: unknown): RelationshipType {
  if (typeof value === 'string' && RELATIONSHIP_TYPES.includes(value))
    return value as RelationshipType;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid relationship type: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function identityStatus(value: unknown): IdentityStatus {
  if (value === 'observed' || value === 'unlinked') return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid relationship identity status: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function stableChildIdentity(value: unknown): StableChildIdentity {
  if (value === 'always' || value === 'sometimes' || value === 'never') return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid stable child identity: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function relationship(value: UnknownRecord): SessionRelationship {
  return {
    source: catalogSource(value.source),
    parentSessionId: String(value.parentSessionId),
    childSessionId: nullableString(value.childSessionId),
    relationship: relationshipType(value.relationship),
    identityStatus: identityStatus(value.identityStatus),
    childAgentType: nullableString(value.childAgentType),
    childAgentName: nullableString(value.childAgentName),
    childModel: nullableString(value.childModel),
    spawnDepth: typeof value.spawnDepth === 'number' ? value.spawnDepth : null,
    evidenceKind: String(value.evidenceKind),
    evidenceLocator: nullableString(value.evidenceLocator),
    evidenceRef: nullableString(value.evidenceRef),
    childHasEvents: value.childHasEvents === true,
    spawnedAtMs: typeof value.spawnedAtMs === 'number' ? value.spawnedAtMs : null,
    createdMs: Number(value.createdMs),
    relationshipUid: String(value.relationshipUid),
  };
}

export function relationships(value: unknown): SessionRelationship[] {
  return Array.isArray(value) ? (value as UnknownRecord[]).map(relationship) : [];
}

export function relationshipCapabilities(value: unknown): RelationshipCapabilities {
  const row = (value ?? {}) as UnknownRecord;
  return {
    source: catalogSource(row.source),
    stableChildIdentity: stableChildIdentity(row.stableChildIdentity),
    recordsAgentType: row.recordsAgentType === true,
    recordsSpawnTime: row.recordsSpawnTime === true,
    recordsEvidenceLocator: row.recordsEvidenceLocator === true,
  };
}

export function relationshipDiagnostics(value: unknown): RelationshipDiagnostic[] {
  return Array.isArray(value)
    ? (value as UnknownRecord[]).map((item) => ({
        code: String(item.code),
        message: String(item.message),
        relationshipUid: nullableString(item.relationshipUid),
      }))
    : [];
}

export function treeNode(value: UnknownRecord): SessionTreeNode {
  return {
    source: catalogSource(value.source),
    sessionId: String(value.sessionId),
    depth: Number(value.depth),
    parentSessionId: nullableString(value.parentSessionId),
    relationship:
      value.relationship && typeof value.relationship === 'object'
        ? relationship(value.relationship as UnknownRecord)
        : null,
    childCount: Number(value.childCount),
    hasEvents: value.hasEvents === true,
    truncated: value.truncated === true,
  };
}

export function relationshipCursor(value: unknown): RelationshipCursor | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return {
    spawnedAtMs: typeof row.spawnedAtMs === 'number' ? row.spawnedAtMs : null,
    relationshipUid: String(row.relationshipUid),
  };
}

export function validateSessionRef(
  options: GetSessionRelationshipsOptions,
  operation: string,
): void {
  if (!options || typeof options !== 'object') {
    throw new InvalidArgumentError(`${operation} options are required`, 'INVALID_ARGUMENT');
  }
  if (!CATALOG_SOURCES.includes(options.source)) {
    throw new InvalidArgumentError(
      `invalid catalog source: ${String(options.source)}`,
      'INVALID_ARGUMENT',
    );
  }
  if (typeof options.sessionId !== 'string' || options.sessionId.trim() === '') {
    throw new InvalidArgumentError('sessionId must not be empty', 'INVALID_ARGUMENT');
  }
}

/**
 * Both halves of an evidence page's identity, checked before the call.
 *
 * `source` is half of the identity here, not a filter that narrows a wider
 * result, so an id this build has no provider for cannot mean "no rows" — it
 * means the caller named something that does not exist, and an empty page
 * would report that as an empty session. `hydrateSession` rejects the same
 * mistake for the same reason.
 */
export function evidenceIdentity(source: unknown, sessionId: unknown, operation: string): void {
  if (typeof source !== 'string' || source.trim() === '') {
    throw new InvalidArgumentError(`${operation} requires a source`, 'INVALID_ARGUMENT');
  }
  // Membership is asked of the trimmed id: whether a *padded* identity is
  // acceptable is a separate question, and the native layer already answers it
  // with its own wording for both halves.
  if (!(SOURCES as readonly string[]).includes(source.trim())) {
    throw new InvalidArgumentError(
      `invalid source: ${source} (expected one of ${SOURCES.join(', ')})`,
      'INVALID_ARGUMENT',
    );
  }
  if (typeof sessionId !== 'string' || sessionId.trim() === '') {
    throw new InvalidArgumentError(`${operation} requires a sessionId`, 'INVALID_ARGUMENT');
  }
}

/**
 * Optimization profile of the loaded native addon: 'release', 'debug', or
 * 'unknown' for an addon predating the probe. Performance measurements are
 * only meaningful against 'release'.
 */

export function normalizeHydration(value: UnknownRecord): HydrateSessionResult {
  const contractVersion = Number(value.contractVersion);
  if (contractVersion !== SESSION_HYDRATION_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects hydration contract ${SESSION_HYDRATION_CONTRACT_VERSION}, but native returned ${contractVersion}.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
  if (
    !['hydrated', 'updated', 'unchanged', 'capability_limited'].includes(String(value.status)) ||
    !['full', 'partial', 'shallow_only'].includes(String(value.capability)) ||
    !['shallow', 'full'].includes(String(value.discoveryState))
  ) {
    throw new NativeContractMismatchError(
      'ai-hist-native returned an invalid hydration result.',
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
  const indexed = (value.indexedThrough ?? {}) as UnknownRecord;
  const evidence = (value.evidence ?? {}) as UnknownRecord;
  return {
    contractVersion,
    source: String(value.source) as CatalogSource,
    sessionId: String(value.sessionId),
    status: String(value.status) as HydrateSessionResult['status'],
    capability: String(value.capability) as HydrateSessionResult['capability'],
    discoveryState: String(value.discoveryState) as HydrateSessionResult['discoveryState'],
    presence: value.presence === 'remote' ? 'remote' : 'local',
    indexedThrough: {
      sourceStamp: nullableString(indexed.sourceStamp),
      lastEventAtMs: typeof indexed.lastEventAtMs === 'number' ? indexed.lastEventAtMs : null,
    },
    evidence: {
      prompts: Number(evidence.prompts),
      events: Number(evidence.events),
      toolCalls: Number(evidence.toolCalls),
      fileEdits: Number(evidence.fileEdits),
      relatedSessions: Number(evidence.relatedSessions),
    },
    relatedSessionIds: Array.isArray(value.relatedSessionIds)
      ? value.relatedSessionIds.map(String)
      : [],
    diagnostics: Array.isArray(value.diagnostics)
      ? (value.diagnostics as UnknownRecord[]).map((item) => ({
          code: String(item.code),
          message: String(item.message),
          durationMs: typeof item.durationMs === 'number' ? item.durationMs : null,
          sourceBytes: typeof item.sourceBytes === 'number' ? item.sourceBytes : null,
          recordsParsed: typeof item.recordsParsed === 'number' ? item.recordsParsed : null,
        }))
      : [],
  };
}
