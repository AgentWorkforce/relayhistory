import type { HistoryPluginRegistry } from './delivery-plugins.js';
import type {
  CatalogSession,
  CatalogCursor,
  EventCursor,
  EvidenceCursorInput,
  HistoryEntry,
  SessionEvent,
  SessionFileEdit,
  SessionToolCall,
} from './contracts.js';
import {
  CATALOG_SOURCES,
  ConnectorNotConfiguredError,
  InvalidArgumentError,
  RelayHistoryError,
  SessionNotFoundError,
  SessionSourceUnavailableError,
  type CatalogSource,
} from './sdk-common.js';
import {
  discoverSessions,
  getSession,
  getSessionEventsPage,
  getSessionFileEditsPage,
  getSessionToolCallsPage,
  hydrateSession,
  listSessionCatalogPage,
} from './operations.js';
import { discoverSourcePlugins } from './source-plugins.js';

export const HANDOFF_CONTRACT_VERSION = 1;
export const MAX_HANDOFF_INTENT_CHARS = 4000;
const HANDOFF_DISCOVERY_ATTEMPTS = 2;
const HANDOFF_INTENT_TRUNCATION_MARKER = '…';

function continuationIntent(
  source: CatalogSource,
  sessionId: string,
  originalIntent: string,
): string {
  const prefix = `Resume this handoff: call resume_handoff(source=${source}, session_id=${sessionId}) via the ai-hist MCP, then continue: `;
  const prefixLength = Array.from(prefix).length;
  if (prefixLength >= MAX_HANDOFF_INTENT_CHARS) {
    throw new InvalidArgumentError(
      `handoff resume instruction must be shorter than ${MAX_HANDOFF_INTENT_CHARS} characters`,
      'INVALID_ARGUMENT',
    );
  }
  const originalCharacters = Array.from(originalIntent);
  const availableCharacters = MAX_HANDOFF_INTENT_CHARS - prefixLength;
  if (originalCharacters.length <= availableCharacters) return prefix + originalIntent;
  return prefix + originalCharacters
    .slice(0, availableCharacters - 1)
    .join('') + HANDOFF_INTENT_TRUNCATION_MARKER;
}

/** The exact pointer carried in a Relaycast delivery whose metadata kind is `handoff`. */
export interface HandoffPointer {
  source: CatalogSource;
  session_id: string;
  intent: string;
  origin_agent: string;
  origin_user: string;
}

export interface CreateHandoffOptions {
  dbPath?: string;
  /** Primarily for embedded hosts and deterministic tests. */
  env?: NodeJS.ProcessEnv;
}

export interface HandoffPromptCursor {
  timestampMs: number;
  id: number;
}

export interface ResumeHandoffCursor {
  prompt?: HandoffPromptCursor;
  events?: EventCursor;
  toolCalls?: EvidenceCursorInput;
  fileEdits?: EvidenceCursorInput;
}

export interface ResumeHandoffOptions {
  dbPath?: string;
  plugins?: HistoryPluginRegistry;
  acquisitionTimeoutMs?: number;
  /** Per-evidence-kind page size. Default 200, maximum 1000. */
  limit?: number;
  cursor?: ResumeHandoffCursor;
}

export interface ResumeHandoffResult {
  contract_version: typeof HANDOFF_CONTRACT_VERSION;
  source: CatalogSource;
  session_id: string;
  session: CatalogSession | null;
  hydration: Awaited<ReturnType<typeof hydrateSession>>;
  prompts: HistoryEntry[];
  events: SessionEvent[];
  tool_calls: SessionToolCall[];
  file_edits: SessionFileEdit[];
  next_cursor: ResumeHandoffCursor | null;
}

interface SessionCandidate {
  source: CatalogSource;
  sessionId: string;
}

function nonempty(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function catalogSource(value: string | undefined): CatalogSource | undefined {
  return CATALOG_SOURCES.includes(value as CatalogSource) ? value as CatalogSource : undefined;
}

/** Harness-owned session IDs inherited by an MCP subprocess. Values are never guessed. */
export function currentSessionCandidates(env: NodeJS.ProcessEnv = process.env): SessionCandidate[] {
  const explicitSource = catalogSource(nonempty(env.AI_HIST_CURRENT_SOURCE));
  const explicitSession = nonempty(env.AI_HIST_CURRENT_SESSION_ID);
  const candidates: Array<SessionCandidate | undefined> = [
    explicitSource && explicitSession
      ? { source: explicitSource, sessionId: explicitSession }
      : undefined,
    (nonempty(env.CODEX_THREAD_ID) ?? nonempty(env.CODEX_SESSION_ID))
      ? { source: 'codex', sessionId: (nonempty(env.CODEX_THREAD_ID) ?? nonempty(env.CODEX_SESSION_ID))! }
      : undefined,
    nonempty(env.CLAUDE_CODE_SESSION_ID)
      ? { source: 'claude', sessionId: nonempty(env.CLAUDE_CODE_SESSION_ID)! }
      : undefined,
    (nonempty(env.CURSOR_AGENT_SESSION_ID) ?? nonempty(env.CURSOR_SESSION_ID))
      ? { source: 'cursor', sessionId: (nonempty(env.CURSOR_AGENT_SESSION_ID) ?? nonempty(env.CURSOR_SESSION_ID))! }
      : undefined,
    nonempty(env.GROK_SESSION_ID)
      ? { source: 'grok', sessionId: nonempty(env.GROK_SESSION_ID)! }
      : undefined,
    nonempty(env.OPENCODE_SESSION_ID)
      ? { source: 'opencode', sessionId: nonempty(env.OPENCODE_SESSION_ID)! }
      : undefined,
  ];
  const seen = new Set<string>();
  return candidates.filter((candidate): candidate is SessionCandidate => {
    if (!candidate) return false;
    const key = `${candidate.source}\0${candidate.sessionId}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function catalogMatches(
  candidates: readonly SessionCandidate[],
  dbPath?: string,
  scope: 'local' | 'all' = 'local',
): Promise<CatalogSession[]> {
  if (!candidates.length) return [];
  const candidateKeys = new Set(
    candidates.map((candidate) => `${candidate.source}\0${candidate.sessionId}`),
  );
  const matches = new Map<string, CatalogSession>();
  const seenCursors = new Set<string>();
  let after: CatalogCursor | undefined;
  do {
    const page = await listSessionCatalogPage({
      dbPath,
      scope,
      sources: [...new Set(candidates.map((candidate) => candidate.source))],
      limit: 1000,
      after,
    });
    for (const session of page.sessions) {
      const key = `${session.source}\0${session.sessionId}`;
      if (candidateKeys.has(key)) matches.set(key, session);
    }
    if (matches.size === candidateKeys.size || !page.nextCursor) break;
    const cursorKey = JSON.stringify(page.nextCursor);
    if (seenCursors.has(cursorKey)) {
      throw new RelayHistoryError(
        'Session catalog pagination repeated a cursor',
        'CATALOG_CURSOR_REPEATED',
      );
    }
    seenCursors.add(cursorKey);
    after = page.nextCursor;
  } while (after);
  return candidates.flatMap((candidate) => {
    const match = matches.get(`${candidate.source}\0${candidate.sessionId}`);
    return match ? [match] : [];
  });
}

/** Build a pointer for the invoking harness's own indexed session. */
export async function createHandoff(
  intent: string,
  options: CreateHandoffOptions = {},
): Promise<HandoffPointer> {
  const normalizedIntent = nonempty(intent);
  if (!normalizedIntent)
    throw new InvalidArgumentError('handoff intent must not be empty', 'INVALID_ARGUMENT');
  if (Array.from(normalizedIntent).length > MAX_HANDOFF_INTENT_CHARS)
    throw new InvalidArgumentError(
      `handoff intent must not exceed ${MAX_HANDOFF_INTENT_CHARS} characters`,
      'INVALID_ARGUMENT',
    );
  const env = options.env ?? process.env;
  const candidates = currentSessionCandidates(env);
  if (!candidates.length) {
    throw new RelayHistoryError(
      'The current harness did not expose a session identity; set AI_HIST_CURRENT_SOURCE and AI_HIST_CURRENT_SESSION_ID',
      'CURRENT_SESSION_UNAVAILABLE',
    );
  }
  let matches = await catalogMatches(candidates, options.dbPath);
  if (!matches.length) {
    await discoverSessions({
      dbPath: options.dbPath,
      scope: 'local',
      sources: [...new Set(candidates.map((candidate) => candidate.source))],
    });
    matches = await catalogMatches(candidates, options.dbPath);
  }
  const session = matches[0];
  if (!session) {
    throw new RelayHistoryError(
      'The current harness session was not found in the local session catalog',
      'CURRENT_SESSION_NOT_FOUND',
    );
  }
  const intentWithResumeInstruction = continuationIntent(
    session.source,
    session.sessionId,
    normalizedIntent,
  );
  return {
    source: session.source,
    session_id: session.sessionId,
    intent: intentWithResumeInstruction,
    origin_agent:
      nonempty(env.AI_HIST_ORIGIN_AGENT) ??
      nonempty(env.AGENT_RELAY_AGENT_NAME) ??
      nonempty(session.originator ?? undefined) ??
      session.source,
    origin_user:
      nonempty(env.AI_HIST_ORIGIN_USER) ??
      nonempty(env.AGENT_RELAY_USER_ID) ??
      nonempty(env.AGENT_RELAY_USER_EMAIL) ??
      nonempty(env.USER ?? env.USERNAME) ??
      'unknown',
  };
}

function validateIdentity(source: CatalogSource, sessionId: string): void {
  if (!CATALOG_SOURCES.includes(source))
    throw new InvalidArgumentError(`invalid catalog source: ${String(source)}`, 'INVALID_ARGUMENT');
  if (!nonempty(sessionId))
    throw new InvalidArgumentError('sessionId must not be empty', 'INVALID_ARGUMENT');
}

function validateLimit(limit: number | undefined): number {
  const value = limit ?? 200;
  if (!Number.isInteger(value) || value < 1 || value > 1000)
    throw new InvalidArgumentError('limit must be an integer from 1 to 1000', 'INVALID_ARGUMENT');
  return value;
}

function afterPrompt(entry: HistoryEntry, cursor: HandoffPromptCursor | undefined): boolean {
  return !cursor || entry.timestampMs > cursor.timestampMs ||
    (entry.timestampMs === cursor.timestampMs && entry.id > cursor.id);
}

/**
 * Hydrate a same-workspace session, then compose one bounded continuation page
 * containing every evidence class needed by a coding agent.
 */
export async function resumeHandoff(
  source: CatalogSource,
  sessionId: string,
  options: ResumeHandoffOptions = {},
): Promise<ResumeHandoffResult> {
  validateIdentity(source, sessionId);
  const limit = validateLimit(options.limit);
  let hydration: Awaited<ReturnType<typeof hydrateSession>>;
  try {
    const sourceConnectors = ['cloud'];
    if (options.plugins) {
      // Refresh the identity-addressed observation on every resume. A cached
      // observation may belong to the account that was authenticated before a
      // workspace switch, and hydrateSession otherwise reuses it without
      // rediscovery.
      let refreshed = false;
      let discoveryFailure: RelayHistoryError | undefined;
      for (let attempt = 0; attempt < HANDOFF_DISCOVERY_ATTEMPTS; attempt += 1) {
        const attemptFailures: RelayHistoryError[] = [];
        const refresh = await discoverSourcePlugins(options.plugins, {
          dbPath: options.dbPath,
          sourceConnectors,
          sources: [source],
          sessionId,
          acquisitionTimeoutMs: options.acquisitionTimeoutMs,
          onUnavailable: (_connector, error) => { attemptFailures.push(error); },
        });
        refreshed = refresh.some((run) => run.observations.some(
          (observation) => observation.source === source && observation.session_id === sessionId,
        ));
        if (refreshed) break;
        const terminalFailure = attemptFailures.find(
          (error) => !['CONNECTOR_FAILURE', 'SOURCE_ACQUISITION_TIMEOUT'].includes(error.code),
        );
        if (terminalFailure) throw terminalFailure;
        if (!attemptFailures.length) {
          // A complete targeted refresh is authoritative. It also prevents
          // hydrateSession from reusing an observation cached for an older
          // authenticated workspace.
          throw new SessionNotFoundError(
            'Source session was not found in the authenticated workspace',
            'SESSION_NOT_FOUND',
          );
        }
        discoveryFailure = attemptFailures[0];
      }
      if (!refreshed) {
        // A partial refresh cannot prove either presence or absence. Do not
        // fall back to cached authorization from a connector that failed;
        // surface a retryable acquisition error instead.
        throw new RelayHistoryError(
          'The authenticated workspace could not be checked for the handoff session',
          'HANDOFF_SOURCE_UNAVAILABLE',
          { cause: discoveryFailure },
        );
      }
    }
    hydration = await hydrateSession({
      source,
      sessionId,
      dbPath: options.dbPath,
      // A handoff is a workspace pointer. Remote acquisition must authorize it;
      // a coincidentally matching local session may never satisfy the load.
      scope: 'remote',
      includeRelated: true,
      plugins: options.plugins,
      // The standard connector is the workspace-scoped RelayHistory source.
      // Other provider connectors represent personal accounts and cannot
      // authorize a teammate handoff.
      sourceConnectors,
      acquisitionTimeoutMs: options.acquisitionTimeoutMs,
    });
  } catch (error) {
    if (
      error instanceof SessionNotFoundError ||
      error instanceof SessionSourceUnavailableError
    ) {
      throw new RelayHistoryError(
        'The handoff session is unavailable in the authenticated workspace; cross-workspace and cross-organization handoffs are not supported',
        'HANDOFF_WORKSPACE_MISMATCH',
        { cause: error },
      );
    }
    if (error instanceof ConnectorNotConfiguredError) {
      throw new RelayHistoryError(
        'Remote handoff acquisition is not configured; configure the workspace cloud source connector',
        'HANDOFF_CONNECTOR_NOT_CONFIGURED',
        { cause: error },
      );
    }
    throw error;
  }

  const [allPrompts, eventsPage, toolCallsPage, fileEditsPage, catalogMatchesResult] = await Promise.all([
    getSession(sessionId, { source, dbPath: options.dbPath }),
    getSessionEventsPage(sessionId, {
      source,
      dbPath: options.dbPath,
      limit,
      after: options.cursor?.events,
    }),
    getSessionToolCallsPage(source, sessionId, {
      dbPath: options.dbPath,
      limit,
      after: options.cursor?.toolCalls,
    }),
    getSessionFileEditsPage(source, sessionId, {
      dbPath: options.dbPath,
      limit,
      after: options.cursor?.fileEdits,
    }),
    catalogMatches([{ source, sessionId }], options.dbPath, 'all'),
  ]);
  const promptCandidates = allPrompts
    .filter((entry) => afterPrompt(entry, options.cursor?.prompt))
    .sort((left, right) => left.timestampMs - right.timestampMs || left.id - right.id);
  const prompts = promptCandidates.slice(0, limit);
  const lastPrompt = prompts.at(-1);
  const lastEvent = eventsPage.events.at(-1);
  const lastToolCall = toolCallsPage.toolCalls.at(-1);
  const lastFileEdit = fileEditsPage.fileEdits.at(-1);
  const hasMore = promptCandidates.length > prompts.length || eventsPage.nextCursor !== null ||
    toolCallsPage.nextCursor !== null || fileEditsPage.nextCursor !== null;
  const promptCheckpoint = lastPrompt
    ? { timestampMs: lastPrompt.timestampMs, id: lastPrompt.id }
    : options.cursor?.prompt;
  const eventCheckpoint = eventsPage.nextCursor ??
    (lastEvent ? { tsMs: lastEvent.tsMs, id: lastEvent.id } : options.cursor?.events);
  const toolCallCheckpoint = toolCallsPage.nextCursor ??
    (lastToolCall ? { tsMs: lastToolCall.tsMs, id: lastToolCall.id } : options.cursor?.toolCalls);
  const fileEditCheckpoint = fileEditsPage.nextCursor ??
    (lastFileEdit ? { tsMs: lastFileEdit.tsMs, id: lastFileEdit.id } : options.cursor?.fileEdits);
  // A composite cursor checkpoints every stream whenever any stream continues;
  // otherwise a shorter stream would restart and duplicate evidence on page 2.
  const nextCursor: ResumeHandoffCursor | null = hasMore ? {
    ...(promptCheckpoint ? { prompt: promptCheckpoint } : {}),
    ...(eventCheckpoint ? { events: eventCheckpoint } : {}),
    ...(toolCallCheckpoint ? { toolCalls: toolCallCheckpoint } : {}),
    ...(fileEditCheckpoint ? { fileEdits: fileEditCheckpoint } : {}),
  } : null;
  return {
    contract_version: HANDOFF_CONTRACT_VERSION,
    source,
    session_id: sessionId,
    session: catalogMatchesResult[0] ?? null,
    hydration,
    prompts,
    events: eventsPage.events,
    tool_calls: toolCallsPage.toolCalls,
    file_edits: fileEditsPage.fileEdits,
    next_cursor: nextCursor,
  };
}
