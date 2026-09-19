import { normalizeHydration } from './normalization.js';
import {
  discoverSourcePlugins,
  hydrateSourcePlugin,
  getSourceObservation,
  camelSourceResult,
  sourceAcquisitionTimeout,
  throwIfSourceAborted,
} from './source-plugins.js';
/**
 * RelayHistory's public TypeScript API.
 *
 * Every production operation crosses one mandatory Node-API boundary into the
 * Rust engine. This module owns only input defaults, object normalization,
 * re-exports of local contracts.
 */

import { nativeCall } from './native.js';
import {
  SESSION_CATALOG_CONTRACT_VERSION,
  SESSION_HYDRATION_CONTRACT_VERSION,
  SESSION_RELATIONSHIP_CONTRACT_VERSION,
  SESSION_EVIDENCE_CONTRACT_VERSION,
  SOURCES,
  defaultDbPath,
  Source,
  CatalogSource,
  CATALOG_SOURCES,
  isCatalogSource,
  SessionScope,
  SessionLocation,
  NativeContractMismatchError,
  InvalidArgumentError,
  RelayHistoryError,
  ConnectorNotConfiguredError,
  SessionNotFoundError,
  SessionSourceUnavailableError,
  SessionSourceMismatchError,
  HydrationUnsupportedError,
  HydrationFailedError,
} from './sdk-common.js';

export * from './sdk-common.js';
export * from './delivery.js';
export * from './history-export.js';
export { NATIVE_CONTRACT_VERSION, runtimePlatform, validateNativeContract } from './native.js';
export * from './git.js';

export * from './contracts.js';
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

import {
  UnknownRecord,
  RELATIONSHIP_TYPES,
  DEFAULT_TREE_MAX_DEPTH,
  MAX_TREE_MAX_DEPTH,
  DEFAULT_TREE_MAX_NODES,
  MAX_TREE_MAX_NODES,
  nullableString,
  historyEntry,
  catalogCursor,
  catalogSession,
  validateNativeLocation,
  validateNativeScope,
  assertCatalogContract,
  tokenUsage,
  sessionEvent,
  assertRelationshipContract,
  parseStoredJson,
  nullableNumber,
  nullableBoolean,
  sessionToolCall,
  sessionFileEdit,
  evidenceCursor,
  nativeEvidenceCursor,
  assertEvidenceContract,
  catalogSource,
  relationshipType,
  identityStatus,
  stableChildIdentity,
  relationship,
  relationships,
  relationshipCapabilities,
  relationshipDiagnostics,
  treeNode,
  relationshipCursor,
  validateSessionRef,
  evidenceIdentity,
  combineHydration,
} from './normalization.js';
export { validateNativeLocation, validateNativeScope, parseStoredJson } from './normalization.js';

export async function nativeBuildProfile(): Promise<string> {
  return nativeCall(async (native) => native.nativeBuildProfile?.() ?? 'unknown');
}

export async function search(query: string, options: SearchOptions = {}): Promise<HistoryEntry[]> {
  const scope = options.scope ?? 'local';
  return nativeCall(async (native) =>
    (await native.search(query, { ...options, scope })).map(historyEntry),
  );
}

export async function recent(options: ListOptions = {}): Promise<HistoryEntry[]> {
  const scope = options.scope ?? 'local';
  return nativeCall(async (native) =>
    (await native.recent({ ...options, scope })).map(historyEntry),
  );
}

export async function getSession(
  sessionId: string,
  options: SessionOptions = {},
): Promise<HistoryEntry[]> {
  return nativeCall(async (native) =>
    (await native.getSession(sessionId, options)).map(historyEntry),
  );
}

export async function listSessionCatalog(
  options: ListCatalogOptions = {},
): Promise<CatalogSession[]> {
  return (await listSessionCatalogPage(options)).sessions;
}

export async function listSessionCatalogPage(
  options: ListCatalogOptions = {},
): Promise<SessionCatalogPage> {
  const scope = options.scope ?? 'local';
  return nativeCall(async (native) => {
    const page = await native.listSessionCatalogPage({
      ...options,
      scope,
      after: options.after
        ? {
            ...options.after,
            lastActivityMs: options.after.lastActivityMs ?? undefined,
          }
        : undefined,
    });
    const contractVersion = Number(page.contractVersion);
    assertCatalogContract(contractVersion);
    return {
      contractVersion,
      scope: validateNativeScope(page.scope),
      sessions: Array.isArray(page.sessions)
        ? (page.sessions as UnknownRecord[]).map(catalogSession)
        : [],
      nextCursor: catalogCursor(page.nextCursor),
    };
  });
}

export async function discoverSessions(
  options: DiscoverSessionsOptions = {},
): Promise<DiscoverResult> {
  validateAcquisition(options);
  const sourceConnectors = validateSourceConnectors(options.sourceConnectors);
  if (options.plugins && options.scope !== undefined && options.scope !== 'local') {
    const selected = options.plugins.sourceConnectors(options.sourceConnectors);
    const local =
      options.scope === 'all'
        ? await discoverSessions({
            ...options,
            plugins: undefined,
            scope: 'local',
            sourceConnectors: [],
          })
        : null;
    if (!selected.length && local) return { ...local, scope: 'all' };
    const failures: DiscoveryDiagnostic[] = [];
    let sourceFailure: RelayHistoryError | undefined;
    const runs = await discoverSourcePlugins(options.plugins, {
      ...options,
      onUnavailable: (source, error) => {
        sourceFailure ??= error;
        failures.push({ source, locator: null, error: `${error.code}: ${error.message}` });
      },
    });
    if (!runs.length && !local)
      throw sourceFailure ?? new ConnectorNotConfiguredError(
        'No selected source plugin is available',
        'CONNECTOR_NOT_CONFIGURED',
      );
    const page = await listSessionCatalogPage({ ...options, scope: options.scope });
    return {
      contractVersion: SESSION_CATALOG_CONTRACT_VERSION,
      scope: options.scope,
      locationsRun: [
        ...(local?.locationsRun ?? []),
        ...new Set(runs.map((run) => run.connector.location)),
      ],
      sessions: page.sessions,
      discovered:
        (local?.discovered ?? 0) +
        runs.reduce(
          (sum, run) => sum + Number(run.summary.discovered ?? run.observations.length),
          0,
        ),
      skippedUnchanged:
        (local?.skippedUnchanged ?? 0) +
        runs.reduce((sum, run) => sum + Number(run.summary.skipped_unchanged ?? 0), 0),
      providers: [
        ...(local?.providers ?? []),
        ...runs.map((run) => ({
          source: run.connector.id,
          candidates: run.observations.length,
          discovered: Number(run.summary.discovered ?? run.observations.length),
          skippedUnchanged: Number(run.summary.skipped_unchanged ?? 0),
          failed: false,
        })),
      ],
      exemptSources: local?.exemptSources ?? [],
      diagnostics: [...(local?.diagnostics ?? []), ...failures],
      counters: {
        candidatesEnumerated:
          (local?.counters.candidatesEnumerated ?? 0) +
          runs.reduce((sum, run) => sum + run.observations.length, 0),
        shallowReads: local?.counters.shallowReads ?? 0,
        skippedUnchanged: local?.counters.skippedUnchanged ?? 0,
        filesOpened: local?.counters.filesOpened ?? 0,
        bytesRead: local?.counters.bytesRead ?? 0,
        providerQueries: (local?.counters.providerQueries ?? 0) + runs.length,
        recordsInspected: local?.counters.recordsInspected ?? 0,
      },
    };
  }
  return nativeCall(async (native) => {
    const result = await native.discoverSessions({
      ...options,
      sourceConnectors,
      scope: options.scope ?? 'local',
    });
    const contractVersion = Number(result.contractVersion);
    assertCatalogContract(contractVersion);
    return {
      contractVersion,
      scope: validateNativeScope(result.scope),
      locationsRun: Array.isArray(result.locationsRun)
        ? (result.locationsRun as unknown[]).map(validateNativeLocation)
        : [],
      sessions: Array.isArray(result.sessions)
        ? (result.sessions as UnknownRecord[]).map(catalogSession)
        : [],
      discovered: Number(result.discovered),
      skippedUnchanged: Number(result.skippedUnchanged),
      providers: (result.providers as ProviderDiscoverySummary[]) ?? [],
      exemptSources: (result.exemptSources as SourceExemption[]) ?? [],
      diagnostics: Array.isArray(result.diagnostics)
        ? (result.diagnostics as UnknownRecord[]).map((item) => ({
            source: String(item.source),
            locator: nullableString(item.locator),
            error: String(item.error),
          }))
        : [],
      counters: result.counters as unknown as DiscoveryCounters,
    };
  });
}

export async function hydrateSession(
  options: HydrateSessionOptions,
): Promise<HydrateSessionResult> {
  if (!options || typeof options !== 'object') {
    throw new InvalidArgumentError('hydrateSession options are required', 'INVALID_ARGUMENT');
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
  validateAcquisition(options);
  const sourceConnectors = validateSourceConnectors(options.sourceConnectors);
  if (options.plugins && options.scope !== undefined && options.scope !== 'local') {
    const selected = options.plugins
      .sourceConnectors(sourceConnectors)
      .filter((source) => source.supportedSources.includes(options.source));
    const failures: HydrationDiagnostic[] = [];
    let sourceFailure: RelayHistoryError | undefined;
    let result: HydrateSessionResult | undefined;
    if (options.scope === 'all') {
      try {
        result = await hydrateSession({
          ...options,
          plugins: undefined,
          scope: 'local',
          sourceConnectors: [],
        });
      } catch (error) {
        if (
          !(error instanceof SessionNotFoundError || error instanceof SessionSourceUnavailableError)
        )
          throw error;
      }
    }
    if (!selected.length && result) return result;
    for (const connector of selected) {
      try {
        let state = await getSourceObservation(
          {
            source: options.source,
            session_id: options.sessionId,
            location: connector.location,
            connector_id: connector.id,
            connector_instance: connector.instanceId,
          },
          options,
        );
        if (!state.observation) {
          await discoverSourcePlugins(options.plugins, {
            ...options,
            sourceConnectors: [`${connector.id}:${connector.instanceId}`],
            sources: [options.source],
            sessionId: options.sessionId,
          });
          state = await getSourceObservation(
            {
              source: options.source,
              session_id: options.sessionId,
              location: connector.location,
              connector_id: connector.id,
              connector_instance: connector.instanceId,
            },
            options,
          );
        }
        if (state.observation)
          result = combineHydration(
            result,
            normalizeHydration(
              camelSourceResult(await hydrateSourcePlugin(connector, options, options)),
            ),
          );
      } catch (error) {
        throwIfSourceAborted(options.signal);
        preserveStorageFailure(error);
        if (error instanceof RelayHistoryError) sourceFailure ??= error;
        failures.push({
          code: error instanceof RelayHistoryError ? error.code : 'SOURCE_PLUGIN_UNAVAILABLE',
          message: `${connector.id}:${connector.instanceId}: source acquisition failed`,
          durationMs: null,
          sourceBytes: null,
          recordsParsed: null,
        });
      }
    }
    if (!result)
      throw sourceFailure ?? new SessionSourceUnavailableError(
        'No selected plugin observed this session',
        'SESSION_SOURCE_UNAVAILABLE',
      );
    return { ...result, diagnostics: [...result.diagnostics, ...failures] };
  }
  return nativeCall(async (native) => {
    const value = await native.hydrateSession({
      ...options,
      sourceConnectors,
      scope: options.scope ?? 'local',
      includeRelated: options.includeRelated ?? true,
    });
    return normalizeHydration(value);
  });
}

export async function discoverAndList(
  options: DiscoverSessionsOptions & ListCatalogOptions = {},
): Promise<SessionCatalogPage> {
  await discoverSessions(options);
  return listSessionCatalogPage(options);
}

export async function getSessionEventsPage(
  sessionId: string,
  options: EventsPageOptions = {},
): Promise<SessionEventsPage> {
  return nativeCall(async (native) => {
    const page = await native.getSessionEventsPage(sessionId, options);
    return {
      events: Array.isArray(page.events) ? (page.events as UnknownRecord[]).map(sessionEvent) : [],
      nextCursor:
        page.nextCursor && typeof page.nextCursor === 'object'
          ? {
              tsMs: Number((page.nextCursor as UnknownRecord).tsMs),
              id: Number((page.nextCursor as UnknownRecord).id),
            }
          : null,
    };
  });
}

/**
 * Direct delegation relationships for one session, in both directions. A
 * missing database returns an empty, well-formed result whose `capabilities`
 * still describe what the provider is able to record.
 */
export async function getSessionRelationships(
  options: GetSessionRelationshipsOptions,
): Promise<SessionRelationships> {
  validateSessionRef(options, 'getSessionRelationships');
  return nativeCall(async (native) => {
    const value = await native.getSessionRelationships({
      source: options.source,
      sessionId: options.sessionId,
      dbPath: options.dbPath,
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
  source: Source,
  sessionId: string,
  options: EvidencePageOptions = {},
): Promise<SessionToolCallsPage> {
  evidenceIdentity(source, sessionId, 'getSessionToolCallsPage');
  return nativeCall(async (native) => {
    const page = await native.getSessionToolCallsPage(source, sessionId, {
      ...options,
      after: nativeEvidenceCursor(options.after),
    });
    assertEvidenceContract(Number(page.contractVersion));
    return {
      contractVersion: Number(page.contractVersion),
      source: String(page.source) as Source,
      sessionId: String(page.sessionId),
      toolCalls: Array.isArray(page.toolCalls)
        ? (page.toolCalls as UnknownRecord[]).map(sessionToolCall)
        : [],
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
      source: options.source,
      sessionId: options.sessionId,
      dbPath: options.dbPath,
      maxDepth: options.maxDepth,
      maxNodes: options.maxNodes,
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

export async function getSessionFileEditsPage(
  source: Source,
  sessionId: string,
  options: EvidencePageOptions = {},
): Promise<SessionFileEditsPage> {
  evidenceIdentity(source, sessionId, 'getSessionFileEditsPage');
  return nativeCall(async (native) => {
    const page = await native.getSessionFileEditsPage(source, sessionId, {
      ...options,
      after: nativeEvidenceCursor(options.after),
    });
    assertEvidenceContract(Number(page.contractVersion));
    return {
      contractVersion: Number(page.contractVersion),
      source: String(page.source) as Source,
      sessionId: String(page.sessionId),
      fileEdits: Array.isArray(page.fileEdits)
        ? (page.fileEdits as UnknownRecord[]).map(sessionFileEdit)
        : [],
      nextCursor: evidenceCursor(page.nextCursor),
    };
  });
}

/**
 * One bounded page of a session's direct children, in the same total order
 * the tree traversal uses: `(spawnedAtMs, relationshipUid)`, nulls last.
 */
export async function getSessionChildrenPage(
  options: GetSessionChildrenPageOptions,
): Promise<SessionChildrenPage> {
  validateSessionRef(options, 'getSessionChildrenPage');
  return nativeCall(async (native) => {
    const page = await native.getSessionChildrenPage({
      source: options.source,
      sessionId: options.sessionId,
      dbPath: options.dbPath,
      limit: options.limit,
      after: options.after
        ? {
            ...options.after,
            spawnedAtMs: options.after.spawnedAtMs ?? undefined,
          }
        : undefined,
    });
    return {
      children: relationships(page.children),
      nextCursor: relationshipCursor(page.nextCursor),
    };
  });
}

export async function stats(options: StatsOptions = {}): Promise<Stats> {
  const scope = options.scope ?? 'local';
  return nativeCall(async (native) => {
    const result = await native.stats({ ...options, scope });
    const bySource: Partial<Record<Source, number>> = {};
    for (const item of (result.bySource as UnknownRecord[] | undefined) ?? []) {
      bySource[String(item.source) as Source] = Number(item.count);
    }
    return {
      scope: validateNativeScope(result.scope),
      total: Number(result.total),
      bySource,
      byProject: ((result.byProject as UnknownRecord[] | undefined) ?? []).map((item) => ({
        project: String(item.project),
        count: Number(item.count),
      })),
      firstTimestampMs:
        typeof result.firstTimestampMs === 'number' ? result.firstTimestampMs : null,
      lastTimestampMs: typeof result.lastTimestampMs === 'number' ? result.lastTimestampMs : null,
    };
  });
}

export async function sync(options: SyncOptions = {}): Promise<SyncResult> {
  validateAcquisition(options);
  const sourceConnectors = validateSourceConnectors(options.sourceConnectors);
  if (options.plugins && options.scope !== undefined && options.scope !== 'local') {
    const selected = options.plugins.sourceConnectors(sourceConnectors);
    const local =
      options.scope === 'all'
        ? await sync({ ...options, plugins: undefined, scope: 'local', sourceConnectors: [] })
        : null;
    const diagnostics: DiscoveryDiagnostic[] = [];
    let sourceFailure: RelayHistoryError | undefined;
    const runs = selected.length
      ? await discoverSourcePlugins(options.plugins, {
          ...options,
          onUnavailable: (source, error) => {
            sourceFailure ??= error;
            diagnostics.push({ source, locator: null, error: `${error.code}: ${error.message}` });
          },
        })
      : [];
    if (!runs.length && !local)
      throw sourceFailure ?? new ConnectorNotConfiguredError(
        'No selected source plugin is available',
        'CONNECTOR_NOT_CONFIGURED',
      );
    for (const run of runs)
      for (const row of run.observations) {
        try {
          await hydrateSourcePlugin(
            run.connector,
            { source: row.source, sessionId: row.session_id },
            options,
          );
        } catch (error) {
          throwIfSourceAborted(options.signal);
          preserveStorageFailure(error);
          diagnostics.push({
            source: `${run.connector.id}:${run.connector.instanceId}`,
            locator: null,
            error: `${error instanceof RelayHistoryError ? error.code : 'SOURCE_PLUGIN_UNAVAILABLE'}: Source plugin acquisition failed`,
          });
        }
      }
    return {
      databasePath: local?.databasePath ?? options.dbPath ?? defaultDbPath(),
      scope: options.scope,
      completed: diagnostics.length === 0,
      diagnostics,
    };
  }
  return nativeCall(async (native) => {
    const result = await native.sync({
      ...options,
      sourceConnectors,
      scope: options.scope ?? 'local',
    });
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
export function formatSessionRow(
  session: CatalogSession,
  options: FormatSessionRowOptions = {},
): string {
  if (options.nowMs !== undefined && !Number.isFinite(options.nowMs)) {
    throw new InvalidArgumentError('nowMs must be finite', 'INVALID_ARGUMENT');
  }
  const max = options.maxPromptLength ?? 96;
  if (!Number.isInteger(max) || max < 1 || max > 1000) {
    throw new InvalidArgumentError(
      'maxPromptLength must be an integer between 1 and 1000',
      'INVALID_ARGUMENT',
    );
  }
  const clean = (value: string) =>
    value
      .replace(/[\u0000-\u001f\u007f-\u009f]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  const icons: Record<Source, string> = {
    claude: '✦',
    codex: '◇',
    cursor: '▸',
    grok: '◉',
    relay: '↔',
    opencode: '⌘',
    trajectory: '↗',
  };
  const ageMs =
    session.lastActivityMs === null
      ? null
      : Math.max(0, (options.nowMs ?? Date.now()) - session.lastActivityMs);
  const age =
    ageMs === null
      ? 'unknown age'
      : ageMs < 60_000
        ? 'just now'
        : ageMs < 3_600_000
          ? `${Math.floor(ageMs / 60_000)}m ago`
          : ageMs < 86_400_000
            ? `${Math.floor(ageMs / 3_600_000)}h ago`
            : `${Math.floor(ageMs / 86_400_000)}d ago`;
  const paint = (text: string, code: number) =>
    options.color ? `\u001b[${code}m${text}\u001b[0m` : text;
  const prompt = Array.from(clean(session.firstPrompt ?? '(no prompt)'));
  const preview = prompt.length > max ? `${prompt.slice(0, max).join('')}…` : prompt.join('');
  const locations = session.locations.length ? ` [${session.locations.join(',')}]` : '';
  return (
    `${paint(`${icons[session.source] ?? '•'} [${clean(session.source)}]`, 36)} ${paint(age, 2)}${locations}  ` +
    `${clean(session.sessionId)}  ${session.cwd ? `${clean(session.cwd)}  ` : ''}${preview}`
  );
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
export async function bootstrapLocal(
  options: BootstrapLocalOptions = {},
): Promise<BootstrapLocalResult> {
  const limit = options.limit ?? 20;
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
    throw new InvalidArgumentError(
      'bootstrap limit must be an integer between 1 and 1000',
      'INVALID_ARGUMENT',
    );
  }
  const local = { dbPath: options.dbPath, scope: 'local' as const };
  const existing = await stats(local);
  if (existing.total > 0) {
    return {
      status: 'ready',
      alreadyIndexed: true,
      indexedPrompts: existing.total,
      hydratedSessions: 0,
      discovery: null,
      diagnostics: [],
    };
  }
  const discovery = await discoverSessions({ ...local, limit });
  const diagnostics: BootstrapLocalResult['diagnostics'] = [];
  let hydratedSessions = 0;
  for (const session of discovery.sessions.slice(0, limit)) {
    try {
      const result = await hydrateSession({
        ...local,
        source: session.source,
        sessionId: session.sessionId,
        includeRelated: false,
      });
      hydratedSessions++;
      if (result.capability !== 'full') {
        diagnostics.push({
          source: session.source,
          sessionId: session.sessionId,
          code: 'CAPABILITY_LIMITED',
          message: `Provider exposes ${result.capability} evidence`,
        });
      }
    } catch (error) {
      if (
        !(
          error instanceof SessionSourceUnavailableError ||
          error instanceof SessionSourceMismatchError ||
          error instanceof HydrationUnsupportedError ||
          error instanceof HydrationFailedError
        )
      )
        throw error;
      diagnostics.push({
        source: session.source,
        sessionId: session.sessionId,
        code: error.code,
        message: error.message,
      });
    }
  }
  const indexed = await stats(local);
  const partial =
    diagnostics.length > 0 ||
    discovery.diagnostics.length > 0 ||
    discovery.providers.some((provider) => provider.failed);
  return {
    status: partial ? 'partial' : indexed.total > 0 ? 'ready' : 'empty',
    alreadyIndexed: false,
    indexedPrompts: indexed.total,
    hydratedSessions,
    discovery,
    diagnostics,
  };
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
export async function ensureLocalStore(
  options: EnsureLocalStoreOptions = {},
): Promise<LocalStoreReadiness> {
  const { dbPath, scope = 'local' } = options;
  if (scope === 'remote') return { status: 'skipped', indexedPrompts: 0, bootstrap: null };
  if (options.bootstrap === false) {
    const existing = await stats({ dbPath, scope: 'local' });
    return {
      status: existing.total > 0 ? 'ready' : 'unbuilt',
      indexedPrompts: existing.total,
      bootstrap: null,
    };
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

export function resumeCommand(
  entry: Pick<HistoryEntry, 'source' | 'sessionId' | 'project' | 'locations'>,
): string | null {
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

function preserveStorageFailure(error: unknown): void {
  if (
    error instanceof RelayHistoryError &&
    [
      'SOURCE_REVISION_CONFLICT',
      'DELIVERY_RETENTION_LIMIT',
      'SOURCE_INTAKE_FAILED',
      'DATABASE_OPEN_FAILED',
      'NATIVE_CALL_FAILED',
      'NATIVE_LOAD_FAILED',
      'NATIVE_CONTRACT_MISMATCH',
    ].includes(error.code)
  )
    throw error;
}
function validateAcquisition(options: {
  scope?: SessionScope;
  sources?: CatalogSource[];
  limit?: number;
  acquisitionTimeoutMs?: number;
}): void {
  if (!options || typeof options !== 'object')
    throw new InvalidArgumentError('acquisition options are required', 'INVALID_ARGUMENT');
  sourceAcquisitionTimeout(options.acquisitionTimeoutMs);
  if (options.scope !== undefined && !['local', 'remote', 'all'].includes(options.scope))
    throw new InvalidArgumentError('scope must be local, remote, or all', 'INVALID_ARGUMENT');
  if (
    options.sources !== undefined &&
    (!Array.isArray(options.sources) ||
      options.sources.some((source) => !CATALOG_SOURCES.includes(source)))
  )
    throw new InvalidArgumentError(
      'sources must contain supported catalog sources',
      'INVALID_ARGUMENT',
    );
  if (
    options.limit !== undefined &&
    (!Number.isInteger(options.limit) || options.limit < 1 || options.limit > 10000)
  )
    throw new InvalidArgumentError(
      'acquisition limit must be an integer from 1 to 10000',
      'INVALID_ARGUMENT',
    );
}
function validateSourceConnectors(value: unknown): string[] | undefined {
  if (value === undefined) return undefined;
  if (
    !Array.isArray(value) ||
    [...value].some((id) => typeof id !== 'string' || !id || id.trim() !== id)
  ) {
    throw new InvalidArgumentError(
      'sourceConnectors must be an array of nonempty, unpadded connector IDs',
      'INVALID_ARGUMENT',
    );
  }
  if (new Set(value).size !== value.length) {
    throw new InvalidArgumentError(
      'sourceConnectors must not contain duplicate IDs',
      'INVALID_ARGUMENT',
    );
  }
  return [...value];
}
