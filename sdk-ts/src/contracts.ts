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
