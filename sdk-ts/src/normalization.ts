import {
  SESSION_CATALOG_CONTRACT_VERSION,
  SESSION_HYDRATION_CONTRACT_VERSION,
  SESSION_RELATIONSHIP_CONTRACT_VERSION,
  SESSION_EVIDENCE_CONTRACT_VERSION,
  SESSION_USAGE_CONTRACT_VERSION,
  SOURCES,
  EVIDENCE_KINDS,
  EvidenceKind,
  FULL_SESSION_KINDS,
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
  NormalizedUsage,
  UsageAccounting,
  UsageDiagnostic,
  RequestKeySource,
  SessionRequest,
  RequestCursor,
  SessionFileEditsPage,
  Stats,
  StatsOptions,
  SyncOptions,
  SyncResult,
} from './contracts.js';
export type UnknownRecord = Record<string, unknown>;

/** Delegation kinds: one session started another thread of work. */
export const DELEGATION_RELATIONSHIP_TYPES: readonly string[] = ['delegated', 'materialized_local'];
/** Continuity kinds: one conversation carrying on as another. */
export const CONTINUITY_RELATIONSHIP_TYPES: readonly string[] = ['continuation', 'fork', 'resume'];
export const RELATIONSHIP_TYPES: readonly string[] = [
  ...DELEGATION_RELATIONSHIP_TYPES,
  ...CONTINUITY_RELATIONSHIP_TYPES,
];
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
    provider: nullableString(value.provider),
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
    requestId: nullableString(value.requestId),
    stopReason: nullableString(value.stopReason),
    agentVersion: nullableString(value.agentVersion),
    isSidechain: nullableBoolean(value.isSidechain),
    isMeta: nullableBoolean(value.isMeta),
    turnId: nullableString(value.turnId),
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
    precedingMessageId: nullableString(value.precedingMessageId),
    followingMessageId: nullableString(value.followingMessageId),
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

const USAGE_ACCOUNTING: readonly string[] = [
  'per-request', 'per-message', 'cumulative-delta', 'context-proxy', 'mixed',
];
const USAGE_DIAGNOSTICS: readonly string[] = [
  'ambiguous-usage-copies', 'unnormalizable-usage', 'ambiguous-model',
  'unresolved-request-identity', 'partial-cache-write-split',
  'partial-reported-cost', 'count-not-representable',
];

export function assertUsageContract(value: number): void {
  if (value !== SESSION_USAGE_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects session usage contract ${SESSION_USAGE_CONTRACT_VERSION}, but native returned ${value}.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
}

/**
 * An out-of-contract label reaches the caller as a contract mismatch rather
 * than as a lie about the shape of the typed API — the same rule the
 * relationship enums follow.
 */
export function usageAccounting(value: unknown): UsageAccounting {
  if (typeof value === 'string' && USAGE_ACCOUNTING.includes(value)) return value as UsageAccounting;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid usage accounting mode: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function requestKeySource(value: unknown): RequestKeySource {
  if (
    value === 'request-id' ||
    value === 'provider-message-id' ||
    value === 'request-span' ||
    value === 'record-id'
  ) {
    return value;
  }
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid request key source: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

export function usageDiagnostics(value: unknown): UsageDiagnostic[] {
  if (!Array.isArray(value)) return [];
  return value.map((entry) => {
    if (typeof entry === 'string' && USAGE_DIAGNOSTICS.includes(entry)) return entry as UsageDiagnostic;
    throw new NativeContractMismatchError(
      `ai-hist-native returned an invalid usage diagnostic: ${JSON.stringify(entry)}. Reinstall matching ai-hist packages.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  });
}

export function normalizedUsage(value: unknown): NormalizedUsage | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return {
    inputTokens: Number(row.inputTokens),
    outputTokens: Number(row.outputTokens),
    reasoningTokens: nullableNumber(row.reasoningTokens),
    cacheReadTokens: Number(row.cacheReadTokens),
    cacheWriteTokens: Number(row.cacheWriteTokens),
    cacheWrite5mTokens: nullableNumber(row.cacheWrite5mTokens),
    cacheWrite1hTokens: nullableNumber(row.cacheWrite1hTokens),
    providerTotalTokens: nullableNumber(row.providerTotalTokens),
    reportedCostUsd: nullableNumber(row.reportedCostUsd),
    accounting: usageAccounting(row.accounting),
    hasInputTokens: Boolean(row.hasInputTokens),
    hasOutputTokens: Boolean(row.hasOutputTokens),
    hasReasoningTokens: Boolean(row.hasReasoningTokens),
    hasCacheReadTokens: Boolean(row.hasCacheReadTokens),
    hasCacheWriteTokens: Boolean(row.hasCacheWriteTokens),
  };
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

export function sessionRequest(value: UnknownRecord): SessionRequest {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: String(value.sessionId),
    requestKey: String(value.requestKey),
    requestKeySource: requestKeySource(value.requestKeySource),
    messageIds: stringList(value.messageIds),
    model: nullableString(value.model),
    provider: nullableString(value.provider),
    firstTsMs: Number(value.firstTsMs),
    lastTsMs: Number(value.lastTsMs),
    usage: normalizedUsage(value.usage),
    usageError: nullableString(value.usageError),
    toolUseIds: stringList(value.toolUseIds),
    hasThinking: Boolean(value.hasThinking),
    eventCount: Number(value.eventCount),
    diagnostics: usageDiagnostics(value.diagnostics),
  };
}

export function requestCursor(value: unknown): RequestCursor | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return { tsMs: Number(row.tsMs), id: Number(row.id) };
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
    originSessionId: nullableString(value.originSessionId),
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

/**
 * Validate the reported coverage rather than cast it: an unknown kind is a
 * native contract mismatch, not a value to hand a caller that will branch on it.
 */
function hydrationCoverage(value: unknown): EvidenceKind[] {
  // Every contract-3 result carries `coverage`, including an empty one for a
  // listing-only connector. Defaulting an absent field to `[]` would let a
  // malformed result through and silently drop the coverage a merge needs, so
  // absent is a contract violation rather than "covers nothing".
  if (!Array.isArray(value)) {
    throw new NativeContractMismatchError(
      'ai-hist-native returned a hydration result without a coverage list.',
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
  return value.map((kind) => {
    if (!(EVIDENCE_KINDS as readonly string[]).includes(String(kind))) {
      throw new NativeContractMismatchError(
        `ai-hist-native returned an unknown evidence kind: ${String(kind)}.`,
        'NATIVE_CONTRACT_MISMATCH',
      );
    }
    return String(kind) as EvidenceKind;
  });
}

/**
 * The capability a merged hydration result is entitled to claim.
 *
 * `capability` is *defined* by `coverage` -- `full` exactly when every kind in
 * `FULL_SESSION_KINDS` is covered -- so a merge has to recompute it. Two
 * connectors that complement each other can cover all five kinds while neither
 * is `full` alone, and carrying an input's `partial` through would rank
 * complete merged evidence below a single full result. `shallow_only` survives
 * only when nothing was covered and nothing claimed otherwise: a `partial`
 * over empty coverage would imply some kind was indexed.
 */
export function mergedHydrationCapability(
  coverage: readonly EvidenceKind[],
  parts: readonly HydrateSessionResult[],
): HydrateSessionResult['capability'] {
  if (missingHydrationCoverage(coverage).length === 0) return 'full';
  if (coverage.length === 0 && parts.every((part) => part.capability === 'shallow_only'))
    return 'shallow_only';
  return 'partial';
}

/** The `FULL_SESSION_KINDS` a coverage set leaves out, in canonical order. */
export function missingHydrationCoverage(coverage: readonly EvidenceKind[]): EvidenceKind[] {
  return FULL_SESSION_KINDS.filter((kind) => !coverage.includes(kind));
}

/**
 * The capability a coverage set entitles a single result to claim — the same
 * rule the Rust producer applies (`capability_for` in `hydrate.rs`). Covering
 * nothing is `shallow_only`, not `partial`: `partial` implies some kind was
 * indexed.
 *
 * A *merge* is a different question and uses {@link mergedHydrationCapability},
 * which additionally keeps `shallow_only` only when every input claimed it.
 */
export function expectedHydrationCapability(
  coverage: readonly EvidenceKind[],
): HydrateSessionResult['capability'] {
  if (missingHydrationCoverage(coverage).length === 0) return 'full';
  return coverage.length === 0 ? 'shallow_only' : 'partial';
}

/**
 * Fold one more presence's hydration result into the running one.
 *
 * Evidence counts take the maximum and coverage takes the union, because the
 * merged result reports what *either* presence indexed; the remaining
 * scalar fields come from the presence that performed the strongest work;
 * equal statuses select the richer presence. This keeps `status`, `presence`
 * and `indexedThrough` describing the same acquisition.
 */
export function combineHydration(
  previous: HydrateSessionResult | undefined,
  next: HydrateSessionResult,
): HydrateSessionResult {
  if (!previous) return next;
  const rank = { full: 2, partial: 1, shallow_only: 0 };
  const best = rank[next.capability] > rank[previous.capability] ? next : previous;
  // A presence that performed work must not disappear behind an unchanged
  // presence chosen for its capability. A first hydration outranks an update,
  // which outranks a repeat; a capability-limited result did no indexing.
  const statusRank = { hydrated: 3, updated: 2, unchanged: 1, capability_limited: 0 };
  const selected = statusRank[next.status] > statusRank[previous.status] ? next
    : statusRank[next.status] < statusRank[previous.status] ? previous : best;
  // Taking only the winner's coverage would understate a merge whose other
  // half indexed a kind the winner does not.
  const coverage = EVIDENCE_KINDS.filter(
    (kind) => previous.coverage.includes(kind) || next.coverage.includes(kind),
  );
  const missing = missingHydrationCoverage(coverage);
  // Each part's partial-coverage diagnostic describes only that part, so once
  // the union is formed they are stale: concatenating them would leave a
  // merged `full` result carrying a note naming kinds it does cover. They are
  // reconciled into at most one, recomputed from the union. Every other
  // diagnostic is per-presence fact and survives untouched.
  const diagnostics = [...previous.diagnostics, ...next.diagnostics].filter(
    (item) => item.code !== 'HYDRATION_PARTIAL_COVERAGE',
  );
  if (missing.length > 0) {
    // States what the merge covers and no more. Only the producing side knows
    // *why* a kind is absent -- a provider that cannot record it, or a request
    // that declined it with `includeRelated: false` -- and that reason does not
    // survive a union of presences that may have had different ones. Inferring
    // provider inability from reduced coverage would make this text false for
    // two relationship-capable presences merged under `includeRelated: false`.
    diagnostics.push({
      code: 'HYDRATION_PARTIAL_COVERAGE',
      message: `merged hydration covers ${coverage.join(', ') || 'no evidence kinds'}; `
        + `it does not cover ${missing.join(', ')}`,
      durationMs: null,
      sourceBytes: null,
      recordsParsed: null,
    });
  }
  return {
    ...selected,
    // Derived from the union rather than carried off `best`, which is spread
    // above: an individual `partial` no longer describes the merged coverage.
    capability: mergedHydrationCapability(coverage, [previous, next]),
    evidence: {
      prompts: Math.max(previous.evidence.prompts, next.evidence.prompts),
      events: Math.max(previous.evidence.events, next.evidence.events),
      toolCalls: Math.max(previous.evidence.toolCalls, next.evidence.toolCalls),
      fileEdits: Math.max(previous.evidence.fileEdits, next.evidence.fileEdits),
      relatedSessions: Math.max(previous.evidence.relatedSessions, next.evidence.relatedSessions),
    },
    coverage,
    relatedSessionIds: [...new Set([...previous.relatedSessionIds, ...next.relatedSessionIds])],
    diagnostics,
  };
}

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
  const coverage = hydrationCoverage(value.coverage);
  // Contract 3 *defines* `capability` from `coverage`, so it is re-derived and
  // compared rather than spot-checked. Only rejecting an unsupported `full`
  // would still admit the mirror-image defects -- a `partial` that covers
  // everything, or a `shallow_only` that covered something -- and those are
  // not harmless understatements: `combineHydration` ranks the parts of a
  // merge by their reported capability before recomputing, so an under-reported
  // result loses the `best` selection and with it the top-level fields the
  // merge carries over.
  const expected = expectedHydrationCapability(coverage);
  if (String(value.capability) !== expected) {
    throw new NativeContractMismatchError(
      `ai-hist-native reported hydration capability ${String(value.capability)}, `
        + `which is inconsistent with its coverage [${coverage.join(', ')}] (expected ${expected}).`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
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
    coverage,
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
