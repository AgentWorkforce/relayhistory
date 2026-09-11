/**
 * RelayHistory's public TypeScript API.
 *
 * Every production operation crosses one mandatory Node-API boundary into the
 * Rust engine. This module owns only input defaults, object normalization,
 * pagination ergonomics, and stable JavaScript errors.
 */

import { homedir } from 'node:os';
import { join } from 'node:path';

export const NATIVE_CONTRACT_VERSION = 10;
export const SESSION_CATALOG_CONTRACT_VERSION = 3;
export const SESSION_HYDRATION_CONTRACT_VERSION = 2;
export const SESSION_RELATIONSHIP_CONTRACT_VERSION = 1;
export const SESSION_EVIDENCE_CONTRACT_VERSION = 1;

/**
 * The provider ids this build knows, as a value. `Source` is derived from it
 * so the runtime checks below and the compile-time type cannot drift apart,
 * and it is the same list the MCP server's `SOURCE` enum publishes.
 */
export const SOURCES = Object.freeze([
  'claude',
  'codex',
  'cursor',
  'grok',
  'relay',
  'trajectory',
  'opencode',
] as const);

export type Source = (typeof SOURCES)[number];
export type CatalogSource = Exclude<Source, 'trajectory'>;
export const CATALOG_SOURCES: readonly CatalogSource[] = Object.freeze(
  SOURCES.filter((source): source is CatalogSource => source !== 'trajectory'),
);

const SOURCE_SET: ReadonlySet<string> = new Set(SOURCES);
const CATALOG_SOURCE_SET: ReadonlySet<string> = new Set(CATALOG_SOURCES);

/** Return whether an unknown value is a source recognized by this SDK build. */
export function isSource(value: unknown): value is Source {
  return typeof value === 'string' && SOURCE_SET.has(value);
}

/** Return whether an unknown value can identify a session catalog entry. */
export function isCatalogSource(value: unknown): value is CatalogSource {
  return typeof value === 'string' && CATALOG_SOURCE_SET.has(value);
}

export type SessionScope = 'local' | 'remote' | 'all';
export type SessionLocation = Exclude<SessionScope, 'all'>;

export class RelayHistoryError extends Error {
  constructor(message: string, readonly code: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

export class UnsupportedPlatformError extends RelayHistoryError {}
export class NativePackageMissingError extends RelayHistoryError {}
export class NativeLoadError extends RelayHistoryError {}
export class NativeContractMismatchError extends RelayHistoryError {}
export class DatabaseOpenError extends RelayHistoryError {}
export class InvalidArgumentError extends RelayHistoryError {}
export class UnsupportedOperationError extends RelayHistoryError {}
export class SessionNotFoundError extends RelayHistoryError {}
export class SessionSourceUnavailableError extends RelayHistoryError {}
export class SessionSourceMismatchError extends RelayHistoryError {}
export class HydrationUnsupportedError extends RelayHistoryError {}
export class HydrationFailedError extends RelayHistoryError {}
export class ConnectorNotConfiguredError extends RelayHistoryError {}
export class AuthenticationExpiredError extends RelayHistoryError {}
export class EvidencePartialError extends RelayHistoryError {}
export class ConnectorFailureError extends RelayHistoryError {}

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

export interface SourceExemption { source: string; reason: string }

export interface DiscoverSessionsOptions {
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

export interface HydrateSessionOptions extends SessionRef {
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

export interface EventCursor { tsMs: number; id: number }

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

export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

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
export interface EvidenceCursor { tsMs: number | null; id: number }

/**
 * A cursor on the way back in. An emitted cursor is always accepted, and so is
 * one whose `tsMs` was dropped by a transport that omits nulls: absent and
 * `null` both mean "already inside the undated tail".
 */
export interface EvidenceCursorInput { tsMs?: number | null; id: number }

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
  firstTimestampMs: number | null;
  lastTimestampMs: number | null;
}

export interface StatsOptions { dbPath?: string; scope?: SessionScope; tag?: string }
export interface SyncOptions { dbPath?: string; scope?: SessionScope }
export interface SyncResult { databasePath: string; scope: SessionScope; completed: boolean }

type UnknownRecord = Record<string, unknown>;

interface NativeBinding {
  accessToken(baseUrl?: string): Promise<string>;
  replay(sessionId: string, options: ReplayOptions): Promise<ReplayResult>;
  createShareableTrace(sessionId: string, visibility: string, source?: string, baseUrl?: string): Promise<string>;
  installGitHooks(optionsJson: string, node: string, sdkUrl: string): Promise<string>;
  linkGitCommit(optionsJson: string): Promise<string>;
  cloudLoadAuth(baseUrl?: string): Promise<RelayhistoryAuth | null>;
  cloudValidateExchangeBaseUrl(baseUrl?: string): Promise<void>;
  cloudLogin(options: object): Promise<RelayhistoryAuth>;
  enableCloud(options: object): Promise<CloudPushResult>;
  pushCloud(options: object): Promise<CloudPushResult>;
  nativeContractVersion(): number;
  nativeBuildProfile?(): string;
  search(query: string, options?: object): Promise<UnknownRecord[]>;
  recent(options?: object): Promise<UnknownRecord[]>;
  getSession(sessionId: string, options?: object): Promise<UnknownRecord[]>;
  getSessionEventsPage(sessionId: string, options?: object): Promise<UnknownRecord>;
  getSessionToolCallsPage(source: string, sessionId: string, options?: object): Promise<UnknownRecord>;
  getSessionFileEditsPage(source: string, sessionId: string, options?: object): Promise<UnknownRecord>;
  stats(options?: object): Promise<UnknownRecord>;
  listSessionCatalog(options?: object): Promise<UnknownRecord[]>;
  listSessionCatalogPage(options?: object): Promise<UnknownRecord>;
  discoverSessions(options?: object): Promise<UnknownRecord>;
  hydrateSession(options: object): Promise<UnknownRecord>;
  getSessionRelationships(options: object): Promise<UnknownRecord>;
  getSessionTree(options: object): Promise<UnknownRecord>;
  getSessionChildrenPage(options: object): Promise<UnknownRecord>;
  sync(options?: object): Promise<UnknownRecord>;
}

const RELATIONSHIP_TYPES: readonly string[] = ['delegated'];
const DEFAULT_TREE_MAX_DEPTH = 32;
const MAX_TREE_MAX_DEPTH = 64;
const DEFAULT_TREE_MAX_NODES = 1_000;
const MAX_TREE_MAX_NODES = 10_000;

const SUPPORTED_PLATFORMS = new Set([
  'darwin-arm64', 'darwin-x64',
  'linux-arm64-gnu', 'linux-arm64-musl',
  'linux-x64-gnu', 'linux-x64-musl',
  'win32-x64-msvc',
]);

function linuxLibc(): 'gnu' | 'musl' {
  const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
  return report?.header?.glibcVersionRuntime ? 'gnu' : 'musl';
}

export function runtimePlatform(): string {
  if (process.platform === 'linux') return `linux-${process.arch}-${linuxLibc()}`;
  if (process.platform === 'win32') return `win32-${process.arch}-msvc`;
  return `${process.platform}-${process.arch}`;
}

let nativePromise: Promise<NativeBinding> | null = null;

export function validateNativeContract(actual: number): void {
  if (actual !== NATIVE_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist requires native contract ${NATIVE_CONTRACT_VERSION}, but ai-hist-native provides ${actual}. Reinstall matching versions.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
}

async function loadNative(): Promise<NativeBinding> {
  if (nativePromise) return nativePromise;
  nativePromise = (async () => {
    const platform = runtimePlatform();
    if (!SUPPORTED_PLATFORMS.has(platform)) {
      throw new UnsupportedPlatformError(
        `RelayHistory has no native build for ${platform}. Supported platforms: ${[...SUPPORTED_PLATFORMS].join(', ')}.`,
        'UNSUPPORTED_PLATFORM',
      );
    }
    let loaded: unknown;
    try {
      // Kept as a variable so TypeScript does not require native build-time
      // declarations; npm installs this mandatory production dependency.
      const packageName = 'ai-hist-native';
      loaded = await import(packageName);
    } catch (cause) {
      const error = cause as NodeJS.ErrnoException;
      if (error.code === 'ERR_MODULE_NOT_FOUND' || error.code === 'MODULE_NOT_FOUND') {
        throw new NativePackageMissingError(
          `RelayHistory supports ${platform}, but its native package is missing. Reinstall ai-hist with optional dependencies enabled.`,
          'NATIVE_PACKAGE_MISSING',
          { cause },
        );
      }
      throw new NativeLoadError(
        `RelayHistory's native package for ${platform} failed to load: ${error.message}`,
        'NATIVE_LOAD_FAILED',
        { cause },
      );
    }
    const binding = ((loaded as { default?: unknown }).default ?? loaded) as NativeBinding;
    if (typeof binding.nativeContractVersion !== 'function') {
      throw new NativeContractMismatchError(
        'The installed ai-hist-native package does not expose a contract version. Reinstall matching ai-hist packages.',
        'NATIVE_CONTRACT_MISMATCH',
      );
    }
    validateNativeContract(binding.nativeContractVersion());
    return binding;
  })();
  return nativePromise.catch((error) => {
    nativePromise = null;
    throw error;
  });
}

function nativeMessage(error: unknown): { code: string; message: string } | null {
  const message = error instanceof Error ? error.message : String(error);
  const match = message.match(/RELAYHISTORY_NATIVE::([A-Z_]+)::([\s\S]*)/);
  return match ? { code: match[1], message: match[2] } : null;
}

async function nativeCall<T>(call: (binding: NativeBinding) => Promise<T>): Promise<T> {
  try {
    return await call(await loadNative());
  } catch (error) {
    if (error instanceof RelayHistoryError) throw error;
    const native = nativeMessage(error);
    if (!native) throw new NativeLoadError(String(error), 'NATIVE_CALL_FAILED', { cause: error });
    if (native.code === 'DATABASE_OPEN_FAILED') {
      throw new DatabaseOpenError(native.message, native.code, { cause: error });
    }
    if (native.code === 'INVALID_ARGUMENT') {
      throw new InvalidArgumentError(native.message, native.code, { cause: error });
    }
    if (native.code === 'UNSUPPORTED_OPERATION') {
      throw new UnsupportedOperationError(native.message, native.code, { cause: error });
    }
    if (native.code === 'SESSION_NOT_FOUND') {
      throw new SessionNotFoundError(native.message, native.code, { cause: error });
    }
    if (native.code === 'SESSION_SOURCE_UNAVAILABLE') {
      throw new SessionSourceUnavailableError(native.message, native.code, { cause: error });
    }
    if (native.code === 'SESSION_SOURCE_MISMATCH') {
      throw new SessionSourceMismatchError(native.message, native.code, { cause: error });
    }
    if (native.code === 'HYDRATION_UNSUPPORTED') {
      throw new HydrationUnsupportedError(native.message, native.code, { cause: error });
    }
    if (native.code === 'HYDRATION_FAILED') {
      throw new HydrationFailedError(native.message, native.code, { cause: error });
    }
    if (native.code === 'CONNECTOR_NOT_CONFIGURED') {
      throw new ConnectorNotConfiguredError(native.message, native.code, { cause: error });
    }
    if (native.code === 'AUTHENTICATION_EXPIRED') {
      throw new AuthenticationExpiredError(native.message, native.code, { cause: error });
    }
    if (native.code === 'EVIDENCE_PARTIAL') {
      throw new EvidencePartialError(native.message, native.code, { cause: error });
    }
    if (native.code === 'CONNECTOR_FAILURE') {
      throw new ConnectorFailureError(native.message, native.code, { cause: error });
    }
    throw new RelayHistoryError(native.message, native.code, { cause: error });
  }
}

function nullableString(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function historyEntry(value: UnknownRecord): HistoryEntry {
  return {
    id: Number(value.id),
    source: String(value.source) as Source,
    sessionId: nullableString(value.sessionId),
    project: nullableString(value.project),
    prompt: String(value.prompt),
    timestampMs: Number(value.timestampMs),
    locations: Array.isArray(value.locations)
      ? value.locations.filter((location): location is SessionLocation => location === 'local' || location === 'remote')
      : [],
  };
}

function catalogCursor(value: unknown): CatalogCursor | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return {
    lastActivityMs: typeof row.lastActivityMs === 'number' ? row.lastActivityMs : null,
    source: String(row.source),
    sessionId: String(row.sessionId),
  };
}

function catalogSession(value: UnknownRecord): CatalogSession {
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
    fromCache: value.fromCache === true,
    locations: Array.isArray(value.locations)
      ? value.locations.filter((location): location is SessionLocation => location === 'local' || location === 'remote')
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

function assertCatalogContract(value: number): void {
  if (value !== SESSION_CATALOG_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects catalog contract ${SESSION_CATALOG_CONTRACT_VERSION}, but native returned ${value}.`,
      'CATALOG_CONTRACT_MISMATCH',
    );
  }
}

function tokenUsage(raw: unknown): Record<string, unknown> | null {
  if (typeof raw !== 'string') return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

function sessionEvent(value: UnknownRecord): SessionEvent {
  return {
    id: Number(value.id), source: String(value.source) as Source, sessionId: String(value.sessionId),
    project: nullableString(value.project), cwd: nullableString(value.cwd), gitBranch: nullableString(value.gitBranch),
    messageId: nullableString(value.messageId), parentId: nullableString(value.parentId), tsMs: Number(value.tsMs),
    role: String(value.role) as SessionEvent['role'], kind: String(value.kind) as SessionEvent['kind'],
    text: nullableString(value.text), model: nullableString(value.model), tokenUsage: tokenUsage(value.tokenJson),
    eventUid: String(value.eventUid),
  };
}

function assertRelationshipContract(value: number): void {
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

function nullableNumber(value: unknown): number | null {
  return typeof value === 'number' ? value : null;
}

function nullableBoolean(value: unknown): boolean | null {
  return typeof value === 'boolean' ? value : null;
}

function sessionToolCall(value: UnknownRecord): SessionToolCall {
  return {
    id: Number(value.id), source: String(value.source) as Source, sessionId: String(value.sessionId),
    messageId: nullableString(value.messageId), toolUseId: String(value.toolUseId),
    name: String(value.name), target: nullableString(value.target),
    args: parseStoredJson(value.argsJson), argsJson: nullableString(value.argsJson),
    isError: nullableBoolean(value.isError), tsMs: nullableNumber(value.tsMs),
  };
}

function sessionFileEdit(value: UnknownRecord): SessionFileEdit {
  return {
    id: Number(value.id), source: String(value.source) as Source, sessionId: String(value.sessionId),
    messageId: nullableString(value.messageId), toolUseId: String(value.toolUseId),
    filePath: String(value.filePath), toolName: nullableString(value.toolName),
    linesAdded: nullableNumber(value.linesAdded), linesRemoved: nullableNumber(value.linesRemoved),
    structuredPatch: parseStoredJson(value.structuredPatchJson),
    structuredPatchJson: nullableString(value.structuredPatchJson),
    userModified: nullableBoolean(value.userModified), tsMs: nullableNumber(value.tsMs),
    gitBranch: nullableString(value.gitBranch), cwd: nullableString(value.cwd),
  };
}

function evidenceCursor(value: unknown): EvidenceCursor | null {
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
function nativeEvidenceCursor(after: EvidenceCursorInput | undefined): object | undefined {
  return after ? { ...after, tsMs: after.tsMs ?? undefined } : undefined;
}

function assertEvidenceContract(value: number): void {
  if (value !== SESSION_EVIDENCE_CONTRACT_VERSION) {
    throw new NativeContractMismatchError(
      `ai-hist expects session evidence contract ${SESSION_EVIDENCE_CONTRACT_VERSION}, but native returned ${value}.`,
      'NATIVE_CONTRACT_MISMATCH',
    );
  }
}

function catalogSource(value: unknown): CatalogSource {
  if (isCatalogSource(value)) return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid catalog source: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

function relationshipType(value: unknown): RelationshipType {
  if (typeof value === 'string' && RELATIONSHIP_TYPES.includes(value)) return value as RelationshipType;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid relationship type: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

function identityStatus(value: unknown): IdentityStatus {
  if (value === 'observed' || value === 'unlinked') return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid relationship identity status: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

function stableChildIdentity(value: unknown): StableChildIdentity {
  if (value === 'always' || value === 'sometimes' || value === 'never') return value;
  throw new NativeContractMismatchError(
    `ai-hist-native returned an invalid stable child identity: ${JSON.stringify(value)}. Reinstall matching ai-hist packages.`,
    'NATIVE_CONTRACT_MISMATCH',
  );
}

function relationship(value: UnknownRecord): SessionRelationship {
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

function relationships(value: unknown): SessionRelationship[] {
  return Array.isArray(value) ? (value as UnknownRecord[]).map(relationship) : [];
}

function relationshipCapabilities(value: unknown): RelationshipCapabilities {
  const row = (value ?? {}) as UnknownRecord;
  return {
    source: catalogSource(row.source),
    stableChildIdentity: stableChildIdentity(row.stableChildIdentity),
    recordsAgentType: row.recordsAgentType === true,
    recordsSpawnTime: row.recordsSpawnTime === true,
    recordsEvidenceLocator: row.recordsEvidenceLocator === true,
  };
}

function relationshipDiagnostics(value: unknown): RelationshipDiagnostic[] {
  return Array.isArray(value) ? (value as UnknownRecord[]).map((item) => ({
    code: String(item.code),
    message: String(item.message),
    relationshipUid: nullableString(item.relationshipUid),
  })) : [];
}

function treeNode(value: UnknownRecord): SessionTreeNode {
  return {
    source: catalogSource(value.source),
    sessionId: String(value.sessionId),
    depth: Number(value.depth),
    parentSessionId: nullableString(value.parentSessionId),
    relationship: value.relationship && typeof value.relationship === 'object'
      ? relationship(value.relationship as UnknownRecord)
      : null,
    childCount: Number(value.childCount),
    hasEvents: value.hasEvents === true,
    truncated: value.truncated === true,
  };
}

function relationshipCursor(value: unknown): RelationshipCursor | null {
  if (!value || typeof value !== 'object') return null;
  const row = value as UnknownRecord;
  return {
    spawnedAtMs: typeof row.spawnedAtMs === 'number' ? row.spawnedAtMs : null,
    relationshipUid: String(row.relationshipUid),
  };
}

function validateSessionRef(options: GetSessionRelationshipsOptions, operation: string): void {
  if (!options || typeof options !== 'object') {
    throw new InvalidArgumentError(`${operation} options are required`, 'INVALID_ARGUMENT');
  }
  if (!CATALOG_SOURCES.includes(options.source)) {
    throw new InvalidArgumentError(`invalid catalog source: ${String(options.source)}`, 'INVALID_ARGUMENT');
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
function evidenceIdentity(source: unknown, sessionId: unknown, operation: string): void {
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

export function defaultDbPath(): string {
  if (process.env.AI_HIST_DB !== undefined) return process.env.AI_HIST_DB;
  if (process.env.XDG_DATA_HOME !== undefined) {
    return join(process.env.XDG_DATA_HOME, 'ai-hist', 'ai-history.db');
  }
  return join(homedir(), '.local', 'share', 'ai-hist', 'ai-history.db');
}

/**
 * Optimization profile of the loaded native addon: 'release', 'debug', or
 * 'unknown' for an addon predating the probe. Performance measurements are
 * only meaningful against 'release'.
 */
export async function nativeBuildProfile(): Promise<string> {
  return nativeCall(async (native) => native.nativeBuildProfile?.() ?? 'unknown');
}

export async function search(query: string, options: SearchOptions = {}): Promise<HistoryEntry[]> {
  return nativeCall(async (native) => (await native.search(query, { ...options, scope: options.scope ?? 'local' })).map(historyEntry));
}

export async function recent(options: ListOptions = {}): Promise<HistoryEntry[]> {
  return nativeCall(async (native) => (await native.recent({ ...options, scope: options.scope ?? 'local' })).map(historyEntry));
}

export async function getSession(sessionId: string, options: SessionOptions = {}): Promise<HistoryEntry[]> {
  return nativeCall(async (native) => (await native.getSession(sessionId, options)).map(historyEntry));
}

export async function listSessionCatalog(options: ListCatalogOptions = {}): Promise<CatalogSession[]> {
  return (await listSessionCatalogPage(options)).sessions;
}

export async function listSessionCatalogPage(options: ListCatalogOptions = {}): Promise<SessionCatalogPage> {
  return nativeCall(async (native) => {
    const page = await native.listSessionCatalogPage({ ...options, scope: options.scope ?? 'local', after: options.after ? {
      ...options.after,
      lastActivityMs: options.after.lastActivityMs ?? undefined,
    } : undefined });
    const contractVersion = Number(page.contractVersion);
    assertCatalogContract(contractVersion);
    return {
      contractVersion,
      scope: validateNativeScope(page.scope),
      sessions: Array.isArray(page.sessions) ? (page.sessions as UnknownRecord[]).map(catalogSession) : [],
      nextCursor: catalogCursor(page.nextCursor),
    };
  });
}

export async function discoverSessions(options: DiscoverSessionsOptions = {}): Promise<DiscoverResult> {
  return nativeCall(async (native) => {
    const result = await native.discoverSessions({ ...options, scope: options.scope ?? 'local' });
    const contractVersion = Number(result.contractVersion);
    assertCatalogContract(contractVersion);
    return {
      contractVersion,
      scope: validateNativeScope(result.scope),
      locationsRun: Array.isArray(result.locationsRun)
        ? (result.locationsRun as unknown[]).map(validateNativeLocation)
        : [],
      sessions: Array.isArray(result.sessions) ? (result.sessions as UnknownRecord[]).map(catalogSession) : [],
      discovered: Number(result.discovered),
      skippedUnchanged: Number(result.skippedUnchanged),
      providers: (result.providers as ProviderDiscoverySummary[]) ?? [],
      exemptSources: (result.exemptSources as SourceExemption[]) ?? [],
      diagnostics: Array.isArray(result.diagnostics) ? (result.diagnostics as UnknownRecord[]).map((item) => ({
        source: String(item.source), locator: nullableString(item.locator), error: String(item.error),
      })) : [],
      counters: result.counters as unknown as DiscoveryCounters,
    };
  });
}

export async function hydrateSession(options: HydrateSessionOptions): Promise<HydrateSessionResult> {
  if (!options || typeof options !== 'object') {
    throw new InvalidArgumentError('hydrateSession options are required', 'INVALID_ARGUMENT');
  }
  if (!CATALOG_SOURCES.includes(options.source)) {
    throw new InvalidArgumentError(`invalid catalog source: ${String(options.source)}`, 'INVALID_ARGUMENT');
  }
  if (typeof options.sessionId !== 'string' || options.sessionId.trim() === '') {
    throw new InvalidArgumentError('sessionId must not be empty', 'INVALID_ARGUMENT');
  }
  return nativeCall(async (native) => {
    const value = await native.hydrateSession({
      ...options,
      scope: options.scope ?? 'local',
      includeRelated: options.includeRelated ?? true,
    });
    const contractVersion = Number(value.contractVersion);
    if (contractVersion !== SESSION_HYDRATION_CONTRACT_VERSION) {
      throw new NativeContractMismatchError(
        `ai-hist expects hydration contract ${SESSION_HYDRATION_CONTRACT_VERSION}, but native returned ${contractVersion}.`,
        'NATIVE_CONTRACT_MISMATCH',
      );
    }
    if (!['hydrated', 'updated', 'unchanged', 'capability_limited'].includes(String(value.status))
      || !['full', 'partial', 'shallow_only'].includes(String(value.capability))
      || !['shallow', 'full'].includes(String(value.discoveryState))) {
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
      relatedSessionIds: Array.isArray(value.relatedSessionIds) ? value.relatedSessionIds.map(String) : [],
      diagnostics: Array.isArray(value.diagnostics) ? (value.diagnostics as UnknownRecord[]).map((item) => ({
        code: String(item.code),
        message: String(item.message),
        durationMs: typeof item.durationMs === 'number' ? item.durationMs : null,
        sourceBytes: typeof item.sourceBytes === 'number' ? item.sourceBytes : null,
        recordsParsed: typeof item.recordsParsed === 'number' ? item.recordsParsed : null,
      })) : [],
    };
  });
}

export async function discoverAndList(options: DiscoverSessionsOptions & ListCatalogOptions = {}): Promise<SessionCatalogPage> {
  await discoverSessions(options);
  return listSessionCatalogPage(options);
}

export async function getSessionEventsPage(sessionId: string, options: EventsPageOptions = {}): Promise<SessionEventsPage> {
  return nativeCall(async (native) => {
    const page = await native.getSessionEventsPage(sessionId, options);
    return {
      events: Array.isArray(page.events) ? (page.events as UnknownRecord[]).map(sessionEvent) : [],
      nextCursor: page.nextCursor && typeof page.nextCursor === 'object'
        ? { tsMs: Number((page.nextCursor as UnknownRecord).tsMs), id: Number((page.nextCursor as UnknownRecord).id) }
        : null,
    };
  });
}

export async function* sessionEvents(sessionId: string, options: Omit<EventsPageOptions, 'after'> = {}): AsyncGenerator<SessionEvent> {
  let after: EventCursor | undefined;
  do {
    const page = await getSessionEventsPage(sessionId, { ...options, after });
    for (const event of page.events) yield event;
    after = page.nextCursor ?? undefined;
  } while (after);
}

export async function getSessionEvents(sessionId: string, options: Omit<EventsPageOptions, 'after'> = {}): Promise<SessionEvent[]> {
  const events: SessionEvent[] = [];
  for await (const event of sessionEvents(sessionId, options)) events.push(event);
  return events;
}

/**
 * Direct delegation relationships for one session, in both directions. A
 * missing database returns an empty, well-formed result whose `capabilities`
 * still describe what the provider is able to record.
 */
export async function getSessionRelationships(options: GetSessionRelationshipsOptions): Promise<SessionRelationships> {
  validateSessionRef(options, 'getSessionRelationships');
  return nativeCall(async (native) => {
    const value = await native.getSessionRelationships({
      source: options.source, sessionId: options.sessionId, dbPath: options.dbPath,
    });
    const contractVersion = Number(value.contractVersion);
    assertRelationshipContract(contractVersion);
    return {
      contractVersion,
      source: String(value.source) as CatalogSource,
      sessionId: String(value.sessionId),
      asParent: relationships(value.asParent),
      asChild: relationships(value.asChild),
      capabilities: relationshipCapabilities(value.capabilities),
      diagnostics: relationshipDiagnostics(value.diagnostics),
    };
  });
}

export async function getSessionToolCallsPage(
  source: Source, sessionId: string, options: EvidencePageOptions = {},
): Promise<SessionToolCallsPage> {
  evidenceIdentity(source, sessionId, 'getSessionToolCallsPage');
  return nativeCall(async (native) => {
    const page = await native.getSessionToolCallsPage(source, sessionId, {
      ...options, after: nativeEvidenceCursor(options.after),
    });
    assertEvidenceContract(Number(page.contractVersion));
    return {
      contractVersion: Number(page.contractVersion),
      source: String(page.source) as Source,
      sessionId: String(page.sessionId),
      toolCalls: Array.isArray(page.toolCalls) ? (page.toolCalls as UnknownRecord[]).map(sessionToolCall) : [],
      nextCursor: evidenceCursor(page.nextCursor),
    };
  });
}

/**
 * The complete descendant delegation tree for one session: pre-order,
 * cycle-safe, and bounded by `maxDepth` and `maxNodes`. Child events keep
 * their own session identity and are never flattened into the root.
 */
export async function getSessionTree(options: GetSessionTreeOptions): Promise<SessionTree> {
  validateSessionRef(options, 'getSessionTree');
  return nativeCall(async (native) => {
    const value = await native.getSessionTree({
      source: options.source, sessionId: options.sessionId, dbPath: options.dbPath,
      maxDepth: options.maxDepth, maxNodes: options.maxNodes,
    });
    const contractVersion = Number(value.contractVersion);
    assertRelationshipContract(contractVersion);
    return {
      contractVersion,
      source: String(value.source) as CatalogSource,
      rootSessionId: String(value.rootSessionId),
      nodes: Array.isArray(value.nodes) ? (value.nodes as UnknownRecord[]).map(treeNode) : [],
      unlinked: relationships(value.unlinked),
      capabilities: relationshipCapabilities(value.capabilities),
      diagnostics: relationshipDiagnostics(value.diagnostics),
      truncated: value.truncated === true,
      maxDepthReached: Number(value.maxDepthReached),
    };
  });
}

export async function* sessionToolCalls(
  source: Source, sessionId: string, options: Omit<EvidencePageOptions, 'after'> = {},
): AsyncGenerator<SessionToolCall> {
  let after: EvidenceCursor | undefined;
  do {
    const page = await getSessionToolCallsPage(source, sessionId, { ...options, after });
    for (const call of page.toolCalls) yield call;
    after = page.nextCursor ?? undefined;
  } while (after);
}

export async function getSessionToolCalls(
  source: Source, sessionId: string, options: Omit<EvidencePageOptions, 'after'> = {},
): Promise<SessionToolCall[]> {
  const calls: SessionToolCall[] = [];
  for await (const call of sessionToolCalls(source, sessionId, options)) calls.push(call);
  return calls;
}

export async function getSessionFileEditsPage(
  source: Source, sessionId: string, options: EvidencePageOptions = {},
): Promise<SessionFileEditsPage> {
  evidenceIdentity(source, sessionId, 'getSessionFileEditsPage');
  return nativeCall(async (native) => {
    const page = await native.getSessionFileEditsPage(source, sessionId, {
      ...options, after: nativeEvidenceCursor(options.after),
    });
    assertEvidenceContract(Number(page.contractVersion));
    return {
      contractVersion: Number(page.contractVersion),
      source: String(page.source) as Source,
      sessionId: String(page.sessionId),
      fileEdits: Array.isArray(page.fileEdits) ? (page.fileEdits as UnknownRecord[]).map(sessionFileEdit) : [],
      nextCursor: evidenceCursor(page.nextCursor),
    };
  });
}

/**
 * One bounded page of a session's direct children, in the same total order
 * the tree traversal uses: `(spawnedAtMs, relationshipUid)`, nulls last.
 */
export async function getSessionChildrenPage(options: GetSessionChildrenPageOptions): Promise<SessionChildrenPage> {
  validateSessionRef(options, 'getSessionChildrenPage');
  return nativeCall(async (native) => {
    const page = await native.getSessionChildrenPage({
      source: options.source, sessionId: options.sessionId, dbPath: options.dbPath, limit: options.limit,
      after: options.after ? {
        ...options.after,
        spawnedAtMs: options.after.spawnedAtMs ?? undefined,
      } : undefined,
    });
    return {
      children: relationships(page.children),
      nextCursor: relationshipCursor(page.nextCursor),
    };
  });
}

/**
 * Lazily walks a session's descendants breadth-first over the paged children
 * primitive, with a bounded node queue instead of materializing a large tree.
 * Unlinked evidence has no traversable identity and is skipped; use
 * `getSessionTree` when you need it.
 */
export async function* sessionDescendants(options: SessionDescendantsOptions): AsyncGenerator<SessionTreeNode> {
  validateSessionRef(options, 'sessionDescendants');
  const maxDepth = Math.min(Math.max(Math.trunc(options.maxDepth ?? DEFAULT_TREE_MAX_DEPTH), 1), MAX_TREE_MAX_DEPTH);
  const requestedMaxNodes = options.maxNodes ?? DEFAULT_TREE_MAX_NODES;
  if (!Number.isFinite(requestedMaxNodes)) {
    throw new InvalidArgumentError('maxNodes must be a finite number', 'INVALID_ARGUMENT');
  }
  const maxNodes = Math.min(Math.max(Math.trunc(requestedMaxNodes), 1), MAX_TREE_MAX_NODES);
  const request = { source: options.source, dbPath: options.dbPath };
  const visited = new Set<string>([options.sessionId]);
  // Each walked session's parent, so a repeated edge can be told apart: back
  // into this branch's own ancestry it is a cycle, anywhere else it is a
  // diamond, which leaves nothing unexplored. `getSessionTree` draws the same
  // line, and the two must not disagree about what `truncated` means.
  const parentOf = new Map<string, string | null>([[options.sessionId, null]]);
  const isAncestor = (from: string, candidate: string): boolean => {
    let current: string | null | undefined = from;
    while (current !== null && current !== undefined) {
      if (current === candidate) return true;
      current = parentOf.get(current) ?? null;
    }
    return false;
  };
  let frontier: SessionTreeNode[] = [{
    source: options.source, sessionId: options.sessionId, depth: 0, parentSessionId: null,
    relationship: null, childCount: 0, hasEvents: false, truncated: false,
  }];
  while (frontier.length > 0) {
    const next: SessionTreeNode[] = [];
    for (const node of frontier) {
      const expand = node.depth < maxDepth;
      let after: RelationshipCursor | undefined;
      do {
        const page = await getSessionChildrenPage({
          ...request, sessionId: node.sessionId, limit: options.pageLimit, after,
        });
        for (const edge of page.children) {
          if (edge.identityStatus !== 'observed' || edge.childSessionId === null) continue;
          node.childCount += 1;
          if (!expand || isAncestor(node.sessionId, edge.childSessionId)) {
            node.truncated = true;
            continue;
          }
          if (visited.has(edge.childSessionId)) continue;
          if (visited.size >= maxNodes) {
            node.truncated = true;
            continue;
          }
          visited.add(edge.childSessionId);
          parentOf.set(edge.childSessionId, node.sessionId);
          next.push({
            source: edge.source, sessionId: edge.childSessionId, depth: node.depth + 1,
            parentSessionId: node.sessionId, relationship: edge, childCount: 0,
            hasEvents: edge.childHasEvents, truncated: false,
          });
        }
        after = page.nextCursor ?? undefined;
      } while (after);
      if (node.depth > 0) yield node;
    }
    frontier = next;
  }
}

/**
 * The root session's events followed by each descendant's events, in
 * descendant traversal order. Every yielded event keeps the `sessionId` of the
 * session that actually produced it: a child's event is never rewritten as a
 * parent's.
 */
export async function* sessionEventsIncludingDescendants(
  options: DescendantEventsOptions & { sessionId: string },
): AsyncGenerator<SessionEvent> {
  validateSessionRef(options, 'sessionEventsIncludingDescendants');
  const events = { dbPath: options.dbPath, source: options.source, limit: options.limit };
  if (options.includeRoot !== false) {
    yield* sessionEvents(options.sessionId, events);
  }
  for await (const node of sessionDescendants({
    source: options.source, sessionId: options.sessionId, dbPath: options.dbPath, maxDepth: options.maxDepth,
    maxNodes: options.maxNodes,
  })) {
    if (!node.hasEvents) continue;
    yield* sessionEvents(node.sessionId, events);
  }
}

export async function* sessionFileEdits(
  source: Source, sessionId: string, options: Omit<EvidencePageOptions, 'after'> = {},
): AsyncGenerator<SessionFileEdit> {
  let after: EvidenceCursor | undefined;
  do {
    const page = await getSessionFileEditsPage(source, sessionId, { ...options, after });
    for (const edit of page.fileEdits) yield edit;
    after = page.nextCursor ?? undefined;
  } while (after);
}

export async function getSessionFileEdits(
  source: Source, sessionId: string, options: Omit<EvidencePageOptions, 'after'> = {},
): Promise<SessionFileEdit[]> {
  const edits: SessionFileEdit[] = [];
  for await (const edit of sessionFileEdits(source, sessionId, options)) edits.push(edit);
  return edits;
}

export async function stats(options: StatsOptions = {}): Promise<Stats> {
  const scope = options.scope ?? 'local';
  
  // For 'remote' scope, require authentication
  if (scope === 'remote') {
    await ensureRemoteAuthentication(scope);
  }
  
  // For 'all' scope, fall back to 'local' if remote authentication fails
  let effectiveScope = scope;
  if (scope === 'all') {
    try {
      await ensureRemoteAuthentication('remote');
    } catch (error) {
      // If remote authentication fails, use local scope instead
      effectiveScope = 'local';
    }
  }
  
  return nativeCall(async (native) => {
    const result = await native.stats({ ...options, scope: effectiveScope });
    const bySource: Partial<Record<Source, number>> = {};
    for (const item of (result.bySource as UnknownRecord[] | undefined) ?? []) {
      bySource[String(item.source) as Source] = Number(item.count);
    }
    return {
      scope: validateNativeScope(result.scope),
      total: Number(result.total), bySource,
      byProject: ((result.byProject as UnknownRecord[] | undefined) ?? []).map((item) => ({ project: String(item.project), count: Number(item.count) })),
      firstTimestampMs: typeof result.firstTimestampMs === 'number' ? result.firstTimestampMs : null,
      lastTimestampMs: typeof result.lastTimestampMs === 'number' ? result.lastTimestampMs : null,
    };
  });
}

export async function sync(options: SyncOptions = {}): Promise<SyncResult> {
  return nativeCall(async (native) => {
    const result = await native.sync({ ...options, scope: options.scope ?? 'local' });
    return {
      databasePath: String(result.databasePath),
      scope: validateNativeScope(result.scope),
      completed: result.completed === true,
    };
  });
}

export interface FormatSessionRowOptions {
  /** Opt in to ANSI colour. Default false for logs and SDK consumers. */
  color?: boolean;
  nowMs?: number;
  maxPromptLength?: number;
}

/** Format a catalog session for a terminal without reading providers or the DB. */
export function formatSessionRow(session: CatalogSession, options: FormatSessionRowOptions = {}): string {
  if (options.nowMs !== undefined && !Number.isFinite(options.nowMs)) {
    throw new InvalidArgumentError('nowMs must be finite', 'INVALID_ARGUMENT');
  }
  const max = options.maxPromptLength ?? 96;
  if (!Number.isInteger(max) || max < 1 || max > 1000) {
    throw new InvalidArgumentError('maxPromptLength must be an integer between 1 and 1000', 'INVALID_ARGUMENT');
  }
  const clean = (value: string) => value.replace(/[\u0000-\u001f\u007f-\u009f]/g, ' ').replace(/\s+/g, ' ').trim();
  const icons: Record<Source, string> = {
    claude: '✦', codex: '◇', cursor: '▸', grok: '◉', relay: '↔', opencode: '⌘', trajectory: '↗',
  };
  const ageMs = session.lastActivityMs === null ? null : Math.max(0, (options.nowMs ?? Date.now()) - session.lastActivityMs);
  const age = ageMs === null ? 'unknown age' : ageMs < 60_000 ? 'just now'
    : ageMs < 3_600_000 ? `${Math.floor(ageMs / 60_000)}m ago`
    : ageMs < 86_400_000 ? `${Math.floor(ageMs / 3_600_000)}h ago`
    : `${Math.floor(ageMs / 86_400_000)}d ago`;
  const paint = (text: string, code: number) => options.color ? `\u001b[${code}m${text}\u001b[0m` : text;
  const prompt = Array.from(clean(session.firstPrompt ?? '(no prompt)'));
  const preview = prompt.length > max ? `${prompt.slice(0, max).join('')}…` : prompt.join('');
  const locations = session.locations.length ? ` [${session.locations.join(',')}]` : '';
  return `${paint(`${icons[session.source] ?? '•'} [${clean(session.source)}]`, 36)} ${paint(age, 2)}${locations}  `
    + `${clean(session.sessionId)}  ${session.cwd ? `${clean(session.cwd)}  ` : ''}${preview}`;
}

export interface BootstrapLocalOptions {
  dbPath?: string;
  /** Maximum sessions to discover and index on first use. Default 20. */
  limit?: number;
}

export interface BootstrapLocalResult {
  status: 'ready' | 'empty' | 'partial';
  alreadyIndexed: boolean;
  indexedPrompts: number;
  hydratedSessions: number;
  discovery: DiscoverResult | null;
  diagnostics: Array<{ source: string; sessionId: string; code: string; message: string }>;
}

/**
 * Prepare a first local search with the native discovery and hydration APIs.
 * Existing searchable databases are left alone; use sync() for a full refresh.
 * Discovery is bounded and related sessions are excluded from first-touch work.
 */
export async function bootstrapLocal(options: BootstrapLocalOptions = {}): Promise<BootstrapLocalResult> {
  const limit = options.limit ?? 20;
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new InvalidArgumentError('bootstrap limit must be an integer between 1 and 1000', 'INVALID_ARGUMENT');
  }
  const local = { dbPath: options.dbPath, scope: 'local' as const };
  const existing = await stats(local);
  if (existing.total > 0) {
    return { status: 'ready', alreadyIndexed: true, indexedPrompts: existing.total,
      hydratedSessions: 0, discovery: null, diagnostics: [] };
  }
  const discovery = await discoverSessions({ ...local, limit });
  const diagnostics: BootstrapLocalResult['diagnostics'] = [];
  let hydratedSessions = 0;
  for (const session of discovery.sessions.slice(0, limit)) {
    try {
      const result = await hydrateSession({ ...local, source: session.source,
        sessionId: session.sessionId, includeRelated: false });
      hydratedSessions++;
      if (result.capability !== 'full') {
        diagnostics.push({ source: session.source, sessionId: session.sessionId,
          code: 'CAPABILITY_LIMITED', message: `Provider exposes ${result.capability} evidence` });
      }
    } catch (error) {
      if (!(error instanceof SessionSourceUnavailableError || error instanceof SessionSourceMismatchError
        || error instanceof HydrationUnsupportedError || error instanceof HydrationFailedError)) throw error;
      diagnostics.push({ source: session.source, sessionId: session.sessionId,
        code: error.code, message: error.message });
    }
  }
  const indexed = await stats(local);
  const partial = diagnostics.length > 0 || discovery.diagnostics.length > 0
    || discovery.providers.some((provider) => provider.failed);
  return { status: partial ? 'partial' : indexed.total > 0 ? 'ready' : 'empty',
    alreadyIndexed: false, indexedPrompts: indexed.total, hydratedSessions, discovery, diagnostics };
}

/**
 * `ready` — the store answers queries. `empty` — bootstrap ran and this machine
 * has no local coding-agent history at all. `unbuilt` — bootstrap was declined,
 * so nothing is indexed yet and that is not the same as having no history.
 * `skipped` — the caller is not reading the local store.
 */
export type LocalStoreStatus = 'ready' | 'empty' | 'unbuilt' | 'skipped';

export interface LocalStoreReadiness {
  status: LocalStoreStatus;
  indexedPrompts: number;
  /** The bootstrap that ran on this call, or null when none did. */
  bootstrap: BootstrapLocalResult | null;
}

export interface EnsureLocalStoreOptions {
  dbPath?: string;
  /** Only `local` and `all` read the local database; `remote` skips the check. */
  scope?: SessionScope;
  /** False for `--no-bootstrap`: report the store as it stands, build nothing. */
  bootstrap?: boolean;
}

/**
 * The single first-use decision behind every local read. Callers must not each
 * choose whether to bootstrap: a fresh install has to answer the same way
 * whichever command the user happens to type first, and a store that was never
 * built has to be distinguishable from one that holds no match.
 */
export async function ensureLocalStore(options: EnsureLocalStoreOptions = {}): Promise<LocalStoreReadiness> {
  const { dbPath, scope = 'local' } = options;
  if (scope === 'remote') return { status: 'skipped', indexedPrompts: 0, bootstrap: null };
  if (options.bootstrap === false) {
    const existing = await stats({ dbPath, scope: 'local' });
    return { status: existing.total > 0 ? 'ready' : 'unbuilt', indexedPrompts: existing.total, bootstrap: null };
  }
  const bootstrap = await bootstrapLocal({ dbPath });
  // `partial` reports indexing diagnostics, not emptiness: whether the store can
  // answer is decided by the prompt count alone.
  return {
    status: bootstrap.indexedPrompts > 0 ? 'ready' : 'empty',
    indexedPrompts: bootstrap.indexedPrompts,
    bootstrap,
  };
}

export function resumeCommand(entry: Pick<HistoryEntry, 'source' | 'sessionId' | 'project' | 'locations'>): string | null {
  if (!entry.sessionId) return null;
  if (entry.locations.length > 0 && !entry.locations.includes('local')) return null;
  const resume = (() => {
    if (entry.source === 'claude') return `claude --resume ${shellQuote(entry.sessionId)}`;
    if (entry.source === 'codex') return `codex resume ${shellQuote(entry.sessionId)}`;
    if (entry.source === 'cursor') return `cursor-agent --resume=${shellQuote(entry.sessionId)}`;
    if (entry.source === 'grok') return `grok resume ${shellQuote(entry.sessionId)}`;
    return null;
  })();
  return resume && entry.project ? `cd ${shellQuote(entry.project)} && ${resume}` : resume;
}

function shellQuote(value: string): string {
  return /^[A-Za-z0-9._:/-]+$/.test(value) ? value : `'${value.replace(/'/g, `'"'"'`)}'`;
}

export interface RelayhistoryAuth { baseUrl: string; accessToken: string; refreshToken?: string }
export type LoginCloudResult = { ok: true; auth: RelayhistoryAuth } | { ok: false; error: string };
export interface CloudPushResult { baseUrl: string; sent: number; accepted: number; syncSkipped: boolean }
/** Return a secret service token with at least 60 seconds of validity. Rust
 * selects the stage, refreshes if needed and atomically saves rotated tokens. */
export async function accessToken(options: { baseUrl?: string } = {}): Promise<string> {
  return nativeCall((native) => native.accessToken(options.baseUrl));
}

export interface ReplayOptions {
  baseUrl?: string;
  /** Page size, not a total cap; Rust fetches every page in server order. */
  limit?: number;
  maxContent?: number;
  /** Return the raw event array serialized as JSON instead of readable text. */
  json?: boolean;
  /** Atomically save in Rust after all pages succeed; transcript is then null. */
  out?: string;
}
export interface ReplayResult { eventCount: number; transcript: string | null; outputPath: string | null }

/** Fetch a cloud transcript without opening or importing into the local DB. */
export async function replay(sessionId: string, options: ReplayOptions = {}): Promise<ReplayResult> {
  for (const key of ['limit', 'maxContent'] as const) {
    const value = options[key];
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff)) {
      throw new InvalidArgumentError(`${key} must be an integer between 0 and 4294967295`, 'INVALID_ARGUMENT');
    }
  }
  const result = await nativeCall((native) => native.replay(sessionId, options));
  return { eventCount: result.eventCount, transcript: result.transcript ?? null, outputPath: result.outputPath ?? null };
}

export interface CloudOptions { dbPath?: string; baseUrl?: string; relayAccessToken?: string; label?: string }
export interface EnableCloudOptions extends CloudOptions {
  /** Keep syncing until stop() is called. Defaults to true. */
  watch?: boolean;
  intervalMs?: number;
  onPush?: (result: CloudPushResult) => void;
  onError?: (error: unknown) => void;
}
export interface CloudHandle extends CloudPushResult { stop(): Promise<void> }

/** Both SDK consumers and the engine use the same stage-scoped Rust auth store. */
export async function loadStoredRelayhistoryAuth(baseUrl?: string): Promise<RelayhistoryAuth | null> {
  return nativeCall((native) => native.cloudLoadAuth(baseUrl));
}

/**
 * Remote stats would otherwise return exit 0 with an empty org store (#126).
 * Connector-based acquisition keeps native UNSUPPORTED_OPERATION semantics.
 */
async function ensureRemoteAuthentication(scope: SessionScope): Promise<void> {
  if (scope !== 'remote') return;

  try {
    const auth = await loadStoredRelayhistoryAuth();
    if (!auth) {
      throw new RelayHistoryError(
        'not authenticated for remote scope — run `ai-hist login` first',
        'CLOUD_AUTH_FAILED'
      );
    }
  } catch (error) {
    if (error instanceof RelayHistoryError) {
      throw error;
    }
    throw new RelayHistoryError(
      'not authenticated for remote scope — run `ai-hist login` first',
      'CLOUD_AUTH_FAILED'
    );
  }
}

/** Refuse to forward an SDK-obtained Agent Relay bearer to an untrusted stage. */
export async function validateCloudExchangeBaseUrl(baseUrl?: string): Promise<void> {
  return nativeCall((native) => native.cloudValidateExchangeBaseUrl(baseUrl));
}

export interface LoginOptions {
  baseUrl?: string;
  relayAccessToken?: string;
  label?: string;
}

/** Exchange a supplied Agent Relay Cloud bearer for a RelayHistory session. */
export async function login(options: LoginOptions = {}): Promise<RelayhistoryAuth> {
  return nativeCall((native) => native.cloudLogin({
    baseUrl: options.baseUrl,
    relayAccessToken: options.relayAccessToken,
    label: options.label,
  }));
}

export async function loginCloud(relayAccessToken: string, options: { baseUrl?: string; label?: string } = {}): Promise<LoginCloudResult> {
  try {
    const auth = await login({ ...options, relayAccessToken });
    return { ok: true, auth };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

export async function pushCloud(options: CloudOptions = {}): Promise<CloudPushResult> {
  return nativeCall((native) => native.pushCloud(options));
}

/** Device login, service-token exchange and first push run in Rust. The optional
 * loop is host orchestration only: no token, refresh, stage or cursor logic in JS.
 * The loop remains in this process; await stop() before shutdown. */
export async function enableCloud(options: EnableCloudOptions = {}): Promise<CloudHandle> {
  const intervalMs = options.intervalMs ?? 60_000;
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 1_000 || intervalMs > 2_147_483_647) {
    throw new InvalidArgumentError('intervalMs must be an integer between 1000 and 2147483647', 'INVALID_ARGUMENT');
  }
  const first = await nativeCall((native) => native.enableCloud(options));
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let pending: Promise<void> = Promise.resolve();
  const schedule = () => {
    if (stopped || options.watch === false) return;
    timer = setTimeout(() => {
      pending = (async () => {
        try {
          const result = await pushCloud({ dbPath: options.dbPath, baseUrl: first.baseUrl });
          options.onPush?.(result);
        } catch (error) {
          if (options.onError) options.onError(error);
          else process.stderr.write(`ai-hist cloud push failed: ${error instanceof Error ? error.message : String(error)}\n`);
        } finally { schedule(); }
      })();
    }, intervalMs);
  };
  schedule();
  return { ...first, async stop() { stopped = true; clearTimeout(timer); await pending; } };
}

export interface GitHookOptions { repo: string; sessionId: string; source?: string; dbPath?: string; prUrl?: string }
/** Install a local post-commit recorder for an explicit session. prUrl identifies
 * an existing GitHub PR; linkage is uploaded by the next cloud push. */
export async function installGitHooks(options: GitHookOptions): Promise<{ hookPath: string; prUrl: string | null }> {
  const result = await nativeCall((native) => native.installGitHooks(JSON.stringify({ ...options, dbPath: options.dbPath ?? defaultDbPath() }), process.execPath, import.meta.url));
  return JSON.parse(result) as { hookPath: string; prUrl: string | null };
}
export async function linkGitCommit(options: GitHookOptions): Promise<{ commitSha: string }> {
  const commitSha = await nativeCall((native) => native.linkGitCommit(JSON.stringify({ ...options, dbPath: options.dbPath ?? defaultDbPath() })));
  return { commitSha };
}

export type TraceVisibility = 'public' | 'private' | 'direct-link';
export interface ShareableTrace { url: string; visibility: TraceVisibility; eventCount: number }
/** Share the already-pushed, frozen server snapshot of a session. */
export async function createShareableTrace(sessionId: string, options: { visibility: TraceVisibility; source?: string; baseUrl?: string }): Promise<ShareableTrace> {
  return nativeCall(async (native) => JSON.parse(await native.createShareableTrace(sessionId, options.visibility, options.source, options.baseUrl)) as ShareableTrace);
}
