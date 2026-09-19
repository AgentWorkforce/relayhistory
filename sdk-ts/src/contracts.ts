import type { HistoryPluginRegistry } from './delivery-plugins.js';
import type { Source, CatalogSource, SessionScope, SessionLocation } from './sdk-common.js';
export interface HistoryEntry {
  id: number;
  source: Source;
  sessionId: string | null;
  project: string | null;
  prompt: string;
  timestampMs: number;
  locations: SessionLocation[];
}

export interface ListOptions {
  dbPath?: string;
  scope?: SessionScope;
  source?: Source;
  project?: string;
  tag?: string;
  beforeMs?: number;
  limit?: number;
}

export interface SearchOptions extends ListOptions {
  rawFts?: boolean;
}

export interface SessionOptions {
  dbPath?: string;
  source?: Source;
  tag?: string;
}

export interface CatalogCursor {
  lastActivityMs: number | null;
  source: string;
  sessionId: string;
}

export interface CatalogSession {
  source: CatalogSource;
  sessionId: string;
  cwd: string | null;
  gitBranch: string | null;
  firstActivityMs: number | null;
  lastActivityMs: number | null;
  firstPrompt: string | null;
  lastAssistantText: string | null;
  models: string[];
  originator: string | null;
  agentVersion: string | null;
  repoUrl: string | null;
  initialCommit: string | null;
  workspaceRoots: string[];
  rawPath: string | null;
  sourceStamp: string | null;
  discoveryState: 'shallow' | 'full';
  fromCache: boolean;
  locations: SessionLocation[];
}

export interface ListCatalogOptions {
  dbPath?: string;
  scope?: SessionScope;
  sources?: CatalogSource[];
  limit?: number;
  beforeMs?: number;
  after?: CatalogCursor;
}

export interface SessionCatalogPage {
  contractVersion: number;
  scope: SessionScope;
  sessions: CatalogSession[];
  nextCursor: CatalogCursor | null;
}

export interface DiscoveryDiagnostic {
  source: string;
  locator: string | null;
  error: string;
}

export interface ProviderDiscoverySummary {
  source: string;
  candidates: number;
  discovered: number;
  skippedUnchanged: number;
  failed: boolean;
}

export interface DiscoveryCounters {
  candidatesEnumerated: number;
  shallowReads: number;
  skippedUnchanged: number;
  filesOpened: number;
  bytesRead: number;
  providerQueries: number;
  recordsInspected: number;
}

export interface SourceExemption {
  source: string;
  reason: string;
}

/** Explicit remote acquisition selection; cached reads never consult connectors. */
export interface SourceConnectorOptions {
  /** Per-connector discovery or complete snapshot budget. Default 300000 ms;
   * integer from 1 to 3600000. Cancellation stops the active helper as well. */
  acquisitionTimeoutMs?: number;
  /** Explicitly configured source plugins. Installation alone never enables them. */
  plugins?: HistoryPluginRegistry;
  signal?: AbortSignal;
  /** Omit for provider-only defaults; [] disables remote acquisition. Local scope
   * never probes remote connectors. Commercial connectors require explicit IDs.
   * Built-ins: claude-web, codex-cloud, cloud, relaycast (sync only). */
  sourceConnectors?: string[];
}

export interface DiscoverSessionsOptions extends SourceConnectorOptions {
  dbPath?: string;
  scope?: SessionScope;
  sources?: CatalogSource[];
  limit?: number;
}

export interface DiscoverResult {
  contractVersion: number;
  scope: SessionScope;
  /** Connector locations that actually executed. `scope` records the ask;
   * this records what ran — an `all` request executes remote connectors only
   * where one is configured on this machine. */
  locationsRun: SessionLocation[];
  sessions: CatalogSession[];
  discovered: number;
  skippedUnchanged: number;
  providers: ProviderDiscoverySummary[];
  exemptSources: SourceExemption[];
  diagnostics: DiscoveryDiagnostic[];
  counters: DiscoveryCounters;
}

export interface SessionRef {
  source: CatalogSource;
  sessionId: string;
  scope?: SessionScope;
}

export interface HydrateSessionOptions extends SessionRef, SourceConnectorOptions {
  dbPath?: string;
  includeRelated?: boolean;
}

export interface HydrationDiagnostic {
  code: string;
  message: string;
  durationMs: number | null;
  sourceBytes: number | null;
  recordsParsed: number | null;
}

export interface HydrateSessionResult {
  contractVersion: number;
  source: CatalogSource;
  sessionId: string;
  status: 'hydrated' | 'updated' | 'unchanged' | 'capability_limited';
  capability: 'full' | 'partial' | 'shallow_only';
  discoveryState: 'shallow' | 'full';
  presence: SessionLocation;
  indexedThrough: {
    sourceStamp: string | null;
    lastEventAtMs: number | null;
  };
  evidence: {
    prompts: number;
    events: number;
    toolCalls: number;
    fileEdits: number;
    relatedSessions: number;
  };
  relatedSessionIds: string[];
  diagnostics: HydrationDiagnostic[];
}

export interface SessionEvent {
  id: number;
  source: Source;
  sessionId: string;
  project: string | null;
  cwd: string | null;
  gitBranch: string | null;
  messageId: string | null;
  parentId: string | null;
  tsMs: number;
  role: 'user' | 'assistant' | 'tool_result';
  kind: 'text' | 'thinking' | 'tool_use' | 'tool_result';
  text: string | null;
  model: string | null;
  tokenUsage: Record<string, unknown> | null;
  eventUid: string;
}

export interface EventCursor {
  tsMs: number;
  id: number;
}

export interface EventsPageOptions {
  dbPath?: string;
  source?: Source;
  limit?: number;
  after?: EventCursor;
}

export interface SessionEventsPage {
  events: SessionEvent[];
  nextCursor: EventCursor | null;
}

export type RelationshipType = 'delegated';
export type IdentityStatus = 'observed' | 'unlinked';
export type StableChildIdentity = 'always' | 'sometimes' | 'never';

/**
 * One observed delegation edge. `childSessionId` is null when the provider
 * recorded the delegation but no stable child identity, in which case
 * `identityStatus` is `unlinked` and the child's output stays attributed to
 * the parent.
 */
export interface SessionRelationship {
  source: CatalogSource;
  parentSessionId: string;
  childSessionId: string | null;
  relationship: RelationshipType;
  identityStatus: IdentityStatus;
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
  relationshipUid: string;
}

/** What a provider is able to record about its own delegations. */
export interface RelationshipCapabilities {
  source: CatalogSource;
  stableChildIdentity: StableChildIdentity;
  recordsAgentType: boolean;
  recordsSpawnTime: boolean;
  recordsEvidenceLocator: boolean;
}

export interface RelationshipDiagnostic {
  code: string;
  message: string;
  relationshipUid: string | null;
}

export interface GetSessionRelationshipsOptions {
  source: CatalogSource;
  sessionId: string;
  dbPath?: string;
}

export interface SessionRelationships {
  contractVersion: number;
  source: CatalogSource;
  sessionId: string;
  /** Edges where this session is the delegating parent. */
  asParent: SessionRelationship[];
  /** Edges where this session is the delegated child. */
  asChild: SessionRelationship[];
  capabilities: RelationshipCapabilities;
  diagnostics: RelationshipDiagnostic[];
}

export interface SessionTreeNode {
  source: CatalogSource;
  sessionId: string;
  depth: number;
  parentSessionId: string | null;
  /** The edge that reached this node; null for the root. */
  relationship: SessionRelationship | null;
  childCount: number;
  hasEvents: boolean;
  /** Children exist but were not expanded (depth/node budget, or a cycle). */
  truncated: boolean;
}

export interface GetSessionTreeOptions extends GetSessionRelationshipsOptions {
  /** Default 32, maximum 64. */
  maxDepth?: number;
  /** Default 1000, maximum 10000. */
  maxNodes?: number;
}

export interface SessionTree {
  contractVersion: number;
  source: CatalogSource;
  rootSessionId: string;
  /** Pre-order and deterministic; `nodes[0]` is the root when it exists. */
  nodes: SessionTreeNode[];
  /** Related evidence at any depth with no stable child identity. */
  unlinked: SessionRelationship[];
  capabilities: RelationshipCapabilities;
  diagnostics: RelationshipDiagnostic[];
  truncated: boolean;
  maxDepthReached: number;
}

export interface RelationshipCursor {
  spawnedAtMs: number | null;
  relationshipUid: string;
}

export interface SessionChildrenPage {
  children: SessionRelationship[];
  nextCursor: RelationshipCursor | null;
}

export interface GetSessionChildrenPageOptions extends GetSessionRelationshipsOptions {
  /** Default 100, maximum 1000. */
  limit?: number;
  after?: RelationshipCursor;
}

export interface SessionDescendantsOptions extends GetSessionRelationshipsOptions {
  maxDepth?: number;
  /** Includes the root. Default 1000, maximum 10000. */
  maxNodes?: number;
  pageLimit?: number;
}

export interface DescendantEventsOptions {
  source: CatalogSource;
  dbPath?: string;
  /** Events read per session, not a total for the iteration. */
  limit?: number;
  maxDepth?: number;
  /** Includes the root. Default 1000, maximum 10000. */
  maxNodes?: number;
  /** Defaults to true. */
  includeRoot?: boolean;
}

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface SessionToolCall {
  id: number;
  source: Source;
  sessionId: string;
  messageId: string | null;
  toolUseId: string;
  name: string;
  target: string | null;
  /** Parsed provider arguments, or null when absent or unparseable. */
  args: JsonValue | null;
  /** The stored argument string exactly as indexed, parseable or not. */
  argsJson: string | null;
  isError: boolean | null;
  tsMs: number | null;
}

export interface SessionFileEdit {
  id: number;
  source: Source;
  sessionId: string;
  messageId: string | null;
  toolUseId: string;
  filePath: string;
  toolName: string | null;
  linesAdded: number | null;
  linesRemoved: number | null;
  /** Parsed provider patch, or null when absent or unparseable. */
  structuredPatch: JsonValue | null;
  /** The stored patch string exactly as indexed, parseable or not. */
  structuredPatchJson: string | null;
  userModified: boolean | null;
  tsMs: number | null;
  gitBranch: string | null;
  cwd: string | null;
}

/**
 * Continuation for tool call and file edit pages. `tsMs` is nullable because
 * both records may be indexed without a timestamp and are ordered last.
 */
export interface EvidenceCursor {
  tsMs: number | null;
  id: number;
}

/**
 * A cursor on the way back in. An emitted cursor is always accepted, and so is
 * one whose `tsMs` was dropped by a transport that omits nulls: absent and
 * `null` both mean "already inside the undated tail".
 */
export interface EvidenceCursorInput {
  tsMs?: number | null;
  id: number;
}

export interface EvidencePageOptions {
  dbPath?: string;
  limit?: number;
  after?: EvidenceCursorInput;
}

export interface SessionToolCallsPage {
  contractVersion: number;
  source: Source;
  sessionId: string;
  toolCalls: SessionToolCall[];
  nextCursor: EvidenceCursor | null;
}

export interface SessionFileEditsPage {
  contractVersion: number;
  source: Source;
  sessionId: string;
  fileEdits: SessionFileEdit[];
  nextCursor: EvidenceCursor | null;
}

/** How a source accounts for the usage one stored record stands for. */
export type UsageAccounting =
  | 'per-request'
  | 'per-message'
  | 'cumulative-delta'
  | 'context-proxy'
  /** A session summary spanning more than one accounting mode. */
  | 'mixed';

/**
 * Where a request's grouping key came from.
 *
 * `request-id` and `provider-message-id` are identities the provider gave the
 * API call. `record-id` is not: it is the stored event's own id, and for a
 * source that writes one call as several records a key built from it can be
 * finer than one row per request.
 */
export type RequestKeySource = 'request-id' | 'provider-message-id' | 'record-id';

/** Why a request's usage is absent, or narrower than it looks. */
export type UsageDiagnostic =
  | 'ambiguous-usage-copies'
  | 'unnormalizable-usage'
  | 'ambiguous-model'
  /** No provider request identity was captured, so rows may be per record. */
  | 'unresolved-request-identity'
  /** Only some contributing requests reported the cache-write TTL split. */
  | 'partial-cache-write-split'
  /** Only some contributing requests carried a cost. */
  | 'partial-reported-cost'
  /** A count exceeded `Number.MAX_SAFE_INTEGER` and was not rounded to fit. */
  | 'count-not-representable';

/**
 * Usage in provider-neutral terms. `inputTokens` always excludes cache reads,
 * whatever the provider's own convention was.
 *
 * A `null` count means the provider did not report it, which is a different
 * fact from a reported zero — the `has*` flags are what tell them apart.
 * `providerTotalTokens` is what the provider wrote and is never recomputed
 * from the parts, and `reportedCostUsd` appears only when the source data
 * carried a cost. Nothing here is priced or estimated.
 */
export interface NormalizedUsage {
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number | null;
  cacheReadTokens: number;
  /** Total cache-write tokens across every TTL bucket. */
  cacheWriteTokens: number;
  /** Claude's `cache_creation.ephemeral_5m_input_tokens`, when split. */
  cacheWrite5mTokens: number | null;
  /** Claude's `cache_creation.ephemeral_1h_input_tokens`, when split. */
  cacheWrite1hTokens: number | null;
  providerTotalTokens: number | null;
  reportedCostUsd: number | null;
  accounting: UsageAccounting;
  // Every count above is a safe integer. A value JavaScript cannot represent
  // exactly is refused at the native boundary — reported as
  // `count-not-representable` with the usage absent — rather than rounded
  // into something that looks like a measurement.
  hasInputTokens: boolean;
  hasOutputTokens: boolean;
  hasReasoningTokens: boolean;
  hasCacheReadTokens: boolean;
  hasCacheWriteTokens: boolean;
}

/** One model request, with its usage normalized. */
export interface SessionRequest {
  id: number;
  source: Source;
  sessionId: string;
  /** The provider request id when the store has one, else the message id. */
  requestKey: string;
  requestKeySource: RequestKeySource;
  /**
   * Every event message id this request collapsed. More than one means the
   * provider split the request across records — Claude's per-content-block
   * layout.
   */
  messageIds: string[];
  model: string | null;
  /** The provider behind the model, when a source records one. Never inferred. */
  provider: string | null;
  firstTsMs: number;
  lastTsMs: number;
  /** Null when the request carried no usage evidence, or none that could be trusted. */
  usage: NormalizedUsage | null;
  /** The stable normalization error code when usage could not be read. */
  usageError: string | null;
  toolUseIds: string[];
  hasThinking: boolean;
  /** How many session event rows this one request collapsed. */
  eventCount: number;
  diagnostics: UsageDiagnostic[];
}

/**
 * Continuation for a request page. Requests inside one session routinely
 * share a timestamp, so `id` is part of the cursor.
 */
export interface RequestCursor {
  tsMs: number;
  id: number;
}

export interface RequestPageOptions {
  dbPath?: string;
  limit?: number;
  after?: RequestCursor;
}

export interface SessionRequestsPage {
  contractVersion: number;
  source: Source;
  sessionId: string;
  requests: SessionRequest[];
  nextCursor: RequestCursor | null;
}

export interface SessionUsageOptions {
  dbPath?: string;
}

/**
 * One session's usage rollup.
 *
 * `usage` is null whenever the totals are not established — no request
 * carried usage, every request's usage was rejected, the totals overflowed,
 * or the requests are not known to be one per API call. It is never zeroed,
 * because zero is a claim.
 *
 * The summary itself is still returned in all of those cases, with the
 * request counts, models, timestamps and `diagnostics` intact: a session
 * whose usage is unreadable and a session that does not exist are different
 * answers, and only the second one is nothing.
 */
export interface SessionUsage {
  contractVersion: number;
  source: Source;
  sessionId: string;
  usage: NormalizedUsage | null;
  /** Requests that contributed to `usage`. */
  requestCount: number;
  /** Requests seen, including ones with no usage. */
  totalRequestCount: number;
  /** Every accounting mode present; more than one means the totals mix units. */
  accounting: UsageAccounting[];
  models: string[];
  firstTsMs: number | null;
  lastTsMs: number | null;
  /** Everything that kept a request out of the totals, or narrows them. */
  diagnostics: UsageDiagnostic[];
  /** The totals exceeded what can be represented and must not be used. */
  overflowed: boolean;
}

export interface Stats {
  scope: SessionScope;
  total: number;
  bySource: Partial<Record<Source, number>>;
  byProject: Array<{ project: string; count: number }>;
  firstTimestampMs: number | null;
  lastTimestampMs: number | null;
}

export interface StatsOptions {
  dbPath?: string;
  scope?: SessionScope;
  tag?: string;
}
export interface SyncOptions extends SourceConnectorOptions {
  dbPath?: string;
  scope?: SessionScope;
}
export interface SyncResult {
  databasePath: string;
  scope: SessionScope;
  completed: boolean;
  diagnostics?: DiscoveryDiagnostic[];
}
