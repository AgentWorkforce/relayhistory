import type { HistoryPluginRegistry } from './delivery-plugins.js';
import type {
  Source, CatalogSource, EvidenceKind, SessionScope, SessionLocation,
} from './sdk-common.js';
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

/**
 * How a `projectKey` was resolved.
 *
 * - `remote` — canonicalized `origin` remote; comparable across machines.
 * - `path` — no remote resolved, so the key is the working directory and is
 *   only meaningful on the machine that produced it.
 * - `inherited` — adopted from the delegating parent session.
 */
export type ProjectKeyMethod = 'remote' | 'path' | 'inherited';

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
  /**
   * Canonical project identity: the `origin` remote canonicalized to
   * `host/owner/repo`, or the working directory when no remote resolves.
   * Group by this, not by `cwd` — two checkouts of one repository share it.
   */
  projectKey: string | null;
  /** How `projectKey` was arrived at. `path` keys are machine-local. */
  projectKeyMethod: ProjectKeyMethod | null;
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
  /** Exact canonical project key; not a prefix and not a path search. */
  projectKey?: string;
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
  /**
   * `full` only when every kind in `FULL_SESSION_KINDS` appears in
   * {@link HydrateSessionResult.coverage}. Derived from the provider's
   * declared coverage, never asserted by the local path.
   */
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
  /**
   * Bytes read from provider files by this hydration. Zero for `unchanged`,
   * and about the size of the append when a live transcript grew - the counter
   * a watch loop reads to tell "the tail grew" from "the whole file was
   * re-read".
   *
   * When a hydration draws on more than one source - the local reader plus one
   * or more connectors - this is the **sum across every source that
   * contributed**, not the figure from whichever one won the capability rank.
   * A total that reported one source's bytes would let a caller watch a real
   * read go by as a zero.
   */
  bytesRead: number;
  /**
   * The evidence kinds this hydration could have indexed, in canonical order.
   * A zero count for a covered kind means the session has none of it; a kind
   * absent from this list means no parser on this path ever looked, and a
   * `HYDRATION_PARTIAL_COVERAGE` diagnostic names the ones that are missing.
   */
  coverage: EvidenceKind[];
  relatedSessionIds: string[];
  diagnostics: HydrationDiagnostic[];
}

export interface SessionEvent {
  id: number;
  source: Source;
  sessionId: string;
  project: string | null;
  /** Canonical project identity, denormalized from the owning session. */
  projectKey: string | null;
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
  /**
   * The upstream inference provider, when the harness records one of its own
   * (OpenCode's `providerID`). Null for harnesses that do not name one — it is
   * never inferred from `model`.
  */
  provider: string | null;
  eventUid: string;
  /**
   * Per-tool-result fidelity. Null on every row that is not a tool result,
   * and on a tool-result row whose provider does not record that fact — the
   * absence is the answer, never a stand-in zero or a guessed status.
   */
  toolUseId: string | null;
  /** Raw UTF-8 byte length of the provider's result payload. */
  payloadBytes: number | null;
  /** True when the harness had already truncated the payload. */
  payloadTruncated: boolean | null;
  /** First 16 hex characters of the payload's sha256. */
  payloadHash: string | null;
  /** n-th result recorded for this `toolUseId`, from zero. */
  callIndex: number | null;
  /** Position of this result in the transcript's tool-result order. */
  eventIndex: number | null;
  resultStatus: ToolResultStatus | null;
  eventSource: ToolResultEventSource | null;
  /** Which provider signal set the error, when one did. */
  errorSignal: ToolResultErrorSignal | null;
  subagentSessionId: string | null;
  agentId: string | null;
  /**
   * Per-message facts the provider recorded on the envelope, stored as it
   * wrote them. `stopReason` is the verbatim wire string, never a normalized
   * enum, and stays null while a turn is still in flight. `isSidechain` and
   * `isMeta` are null when the provider did not say either way, which is not
   * the same as false.
   */
  requestId: string | null;
  stopReason: string | null;
  agentVersion: string | null;
  isSidechain: boolean | null;
  isMeta: boolean | null;
  turnId: string | null;
}

export type ToolResultStatus = 'running' | 'completed' | 'errored' | 'cancelled' | 'unknown';

export type ToolResultEventSource = 'tool_result' | 'subagent_notification' | 'function_call_output';

export type ToolResultErrorSignal =
  | 'tool_result.is_error'
  | 'exit_code'
  | 'patch_apply'
  | 'mcp_err'
  | 'subagent_status';

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

/** One session started another thread of work. */
export type DelegationRelationshipType = 'delegated' | 'materialized_local';
/** One conversation carrying on as another, rather than delegating. */
export type ContinuityRelationshipType = 'continuation' | 'fork' | 'resume';
export type RelationshipType = DelegationRelationshipType | ContinuityRelationshipType;
export type IdentityStatus = 'observed' | 'unlinked';
export type StableChildIdentity = 'always' | 'sometimes' | 'never';

/**
 * One observed edge. `childSessionId` is null when the provider recorded the
 * relationship but no stable child identity, in which case `identityStatus`
 * is `unlinked` and the child's output stays attributed to the parent — which
 * is also how two branches sharing one provider session id are recorded.
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
  /**
   * The conversation a fork or continuation came from, when the provider
   * named one distinct from `parentSessionId`. Null for delegation.
   */
  originSessionId: string | null;
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
  /** Delegation edges where this session is the delegating parent. */
  asParent: SessionRelationship[];
  /** Delegation edges where this session is the delegated child. */
  asChild: SessionRelationship[];
  /**
   * Continuity edges touching this session in either direction: the resumes,
   * forks and continuations that are not delegation. Kept out of `asParent`
   * and `asChild` so a delegation-only consumer reads exactly what it read
   * before continuity existed.
   */
  continuity: SessionRelationship[];
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
  /**
   * Which edges the walk follows. Omitted means delegation only, which is
   * what every caller got before continuity existed; naming continuity kinds
   * expands from an origin to its resumed, continued, or forked descendants.
   */
  relationshipKinds?: RelationshipType[];
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
  /** Omitted means delegation only. See `GetSessionTreeOptions`. */
  relationshipKinds?: RelationshipType[];
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

/**
 * One record a provider wrote about a session that the normalized event model
 * cannot carry: a compaction or summary boundary, a provider `system` row, a
 * non-text content block, an agent lifecycle event. A marker is deliberately
 * not an event — it has no role, and its `tsMs` is nullable because a
 * provider may record that something happened without recording when.
 */
export interface SessionMarker {
  id: number;
  source: Source;
  sessionId: string;
  /** Deterministic per record, so a re-parse updates the row in place. */
  markerUid: string;
  tsMs: number | null;
  messageId: string | null;
  parentId: string | null;
  turnId: string | null;
  /**
   * Classified vocabulary (`compaction_boundary`, `summary`,
   * `subagent_notification`, ...). A record type no classifier knows is
   * `unknown`, with the provider-native type kept verbatim in `subkind`.
   */
  kind: string;
  subkind: string | null;
  /** The provider's own readable text for this marker, when it wrote one. */
  text: string | null;
  /** Parsed bounded payload projection, or null when absent or unparseable. */
  payload: JsonValue | null;
  /** The stored payload string exactly as indexed, parseable or not. */
  payloadJson: string | null;
}

/** Markers page on the evidence keyset: `(tsMs IS NULL, tsMs, id)`. */
export interface SessionMarkersPage {
  contractVersion: number;
  source: Source;
  sessionId: string;
  markers: SessionMarker[];
  nextCursor: EvidenceCursor | null;
}

/**
 * What one provider's local parser can record, answered from RelayHistory's
 * own capability tables rather than from any database — so it is correct
 * before a first sync and for a database that does not exist yet.
 *
 * `evidenceKinds` is the `coverage` a hydration of this source reports;
 * `fullCoverage` is whether that hydration can ever say `full`.
 * `relationships` is the same table `getSessionRelationships` returns as
 * `capabilities`.
 */
export interface SourceCapabilities {
  /** The contract whose `EvidenceKind` vocabulary `evidenceKinds` uses. */
  hydrationContractVersion: number;
  /** The contract `relationships` is spelled in. */
  relationshipContractVersion: number;
  source: CatalogSource;
  evidenceKinds: EvidenceKind[];
  /** The `FULL_SESSION_KINDS` the parser does not produce, in canonical order. */
  missingEvidenceKinds: EvidenceKind[];
  fullCoverage: boolean;
  relationships: RelationshipCapabilities;
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
 * API call. `request-span` is one the provider implied rather than named: a
 * source that reports a cumulative usage snapshot after each call ends a
 * request with every snapshot, so the span between two of them is one call.
 * `record-id` is not an API identity at all: it is the stored event's own id,
 * and for a source that writes one call as several records a key built from it
 * can be finer than one row per request.
 */
export type RequestKeySource =
  | 'request-id'
  | 'provider-message-id'
  | 'request-span'
  | 'record-id';

/** Why a request's usage is absent, or narrower than it looks. */
export type UsageDiagnostic =
  | 'ambiguous-usage-copies'
  | 'unnormalizable-usage'
  | 'ambiguous-model'
  | 'ambiguous-provider'
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

/**
 * One block inside a user turn. `approxTokens` is deliberately absent: every
 * estimate available here is a bytes-per-token heuristic, and a heuristic
 * served alongside measured values is indistinguishable from one at the call
 * site. Bring a tokenizer and apply it to `byteLen`.
 */
export interface SessionUserTurnBlock {
  kind: 'text' | 'tool_result';
  toolUseId: string | null;
  /** Measured payload bytes when recorded, else the stored text's UTF-8 length. */
  byteLen: number;
  /**
   * Whether the result is known not to have succeeded. `true` for a
   * `resultStatus` of `errored` or `cancelled` — both terminal, both stated
   * by the provider — and `false` for `completed`.
   *
   * `null` means the outcome is not known *yet* (`running`, `unknown`, or a
   * row indexed before the status existed). It does not mean "not an error",
   * so a consumer that treats it as a success is reading a missing fact as a
   * measured one. Read `resultStatus` from the event to tell a cancellation
   * from a failure.
   */
  isError: boolean | null;
}

/** One user-side message and the ordered blocks it carried. */
export interface SessionUserTurn {
  /** Row id of the turn's first event; the cursor's tiebreaker. */
  id: number;
  source: Source;
  sessionId: string;
  messageId: string | null;
  /**
   * The nearest messages recorded either side of this turn, whichever side of
   * the conversation each came from — normally the assistant message the human
   * answered, and the one their prompt drew. `null` only when the session
   * recorded no named message on that side. An event the provider left
   * unnamed is passed over rather than nulling the field: it is not a message
   * you could reference, while the named message behind it still borders this
   * turn. A later block of this same turn is never its own neighbour.
   */
  precedingMessageId: string | null;
  followingMessageId: string | null;
  tsMs: number;
  blocks: SessionUserTurnBlock[];
}

export interface UserTurnsPageOptions {
  dbPath?: string;
  limit?: number;
  after?: EventCursor;
}

export interface SessionUserTurnsPage {
  contractVersion: number;
  source: Source;
  sessionId: string;
  userTurns: SessionUserTurn[];
  nextCursor: EventCursor | null;
}

export interface Stats {
  scope: SessionScope;
  total: number;
  bySource: Partial<Record<Source, number>>;
  byProject: Array<{ project: string; count: number }>;
  /**
   * Which key `byProject` is bucketed by. `project_key` merges two checkouts
   * of one repository; `cwd` is the historical per-directory grouping. Read
   * it — the same database gives different counts under each.
   */
  groupedBy: ProjectGrouping;
  firstTimestampMs: number | null;
  lastTimestampMs: number | null;
}

export type ProjectGrouping = 'project_key' | 'cwd';

export interface StatsOptions {
  dbPath?: string;
  scope?: SessionScope;
  tag?: string;
  /** Bucket `byProject` by working directory instead of project key. */
  byCwd?: boolean;
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
