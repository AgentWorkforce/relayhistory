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
} from './normalization.js';
import {
  getSessionEventsPage,
  getSessionToolCallsPage,
  getSessionFileEditsPage,
  getSessionChildrenPage,
} from './operations.js';

export async function* sessionEvents(
  sessionId: string,
  options: Omit<EventsPageOptions, 'after'> = {},
): AsyncGenerator<SessionEvent> {
  let after: EventCursor | undefined;
  do {
    const page = await getSessionEventsPage(sessionId, { ...options, after });
    for (const event of page.events) yield event;
    after = page.nextCursor ?? undefined;
  } while (after);
}

export async function getSessionEvents(
  sessionId: string,
  options: Omit<EventsPageOptions, 'after'> = {},
): Promise<SessionEvent[]> {
  const events: SessionEvent[] = [];
  for await (const event of sessionEvents(sessionId, options)) events.push(event);
  return events;
}

export async function* sessionToolCalls(
  source: Source,
  sessionId: string,
  options: Omit<EvidencePageOptions, 'after'> = {},
): AsyncGenerator<SessionToolCall> {
  let after: EvidenceCursor | undefined;
  do {
    const page = await getSessionToolCallsPage(source, sessionId, { ...options, after });
    for (const call of page.toolCalls) yield call;
    after = page.nextCursor ?? undefined;
  } while (after);
}

export async function getSessionToolCalls(
  source: Source,
  sessionId: string,
  options: Omit<EvidencePageOptions, 'after'> = {},
): Promise<SessionToolCall[]> {
  const calls: SessionToolCall[] = [];
  for await (const call of sessionToolCalls(source, sessionId, options)) calls.push(call);
  return calls;
}

/**
 * Lazily walks a session's descendants breadth-first over the paged children
 * primitive, with a bounded node queue instead of materializing a large tree.
 * Unlinked evidence has no traversable identity and is skipped; use
 * `getSessionTree` when you need it.
 */
export async function* sessionDescendants(
  options: SessionDescendantsOptions,
): AsyncGenerator<SessionTreeNode> {
  validateSessionRef(options, 'sessionDescendants');
  const maxDepth = Math.min(
    Math.max(Math.trunc(options.maxDepth ?? DEFAULT_TREE_MAX_DEPTH), 1),
    MAX_TREE_MAX_DEPTH,
  );
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
  let frontier: SessionTreeNode[] = [
    {
      source: options.source,
      sessionId: options.sessionId,
      depth: 0,
      parentSessionId: null,
      relationship: null,
      childCount: 0,
      hasEvents: false,
      truncated: false,
    },
  ];
  while (frontier.length > 0) {
    const next: SessionTreeNode[] = [];
    for (const node of frontier) {
      const expand = node.depth < maxDepth;
      let after: RelationshipCursor | undefined;
      do {
        const page = await getSessionChildrenPage({
          ...request,
          sessionId: node.sessionId,
          limit: options.pageLimit,
          after,
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
            source: edge.source,
            sessionId: edge.childSessionId,
            depth: node.depth + 1,
            parentSessionId: node.sessionId,
            relationship: edge,
            childCount: 0,
            hasEvents: edge.childHasEvents,
            truncated: false,
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
    source: options.source,
    sessionId: options.sessionId,
    dbPath: options.dbPath,
    maxDepth: options.maxDepth,
    maxNodes: options.maxNodes,
  })) {
    if (!node.hasEvents) continue;
    yield* sessionEvents(node.sessionId, events);
  }
}

export async function* sessionFileEdits(
  source: Source,
  sessionId: string,
  options: Omit<EvidencePageOptions, 'after'> = {},
): AsyncGenerator<SessionFileEdit> {
  let after: EvidenceCursor | undefined;
  do {
    const page = await getSessionFileEditsPage(source, sessionId, { ...options, after });
    for (const edit of page.fileEdits) yield edit;
    after = page.nextCursor ?? undefined;
  } while (after);
}

export async function getSessionFileEdits(
  source: Source,
  sessionId: string,
  options: Omit<EvidencePageOptions, 'after'> = {},
): Promise<SessionFileEdit[]> {
  const edits: SessionFileEdit[] = [];
  for await (const edit of sessionFileEdits(source, sessionId, options)) edits.push(edit);
  return edits;
}
