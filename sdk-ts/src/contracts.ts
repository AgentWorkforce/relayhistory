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
  eventUid: string;
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
   * rolls a resumed or forked conversation up to its origin.
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
