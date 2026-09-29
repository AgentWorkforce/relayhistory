#!/usr/bin/env node

/** Thin MCP adapters over the public `ai-hist` TypeScript SDK. */
import { readFileSync } from 'node:fs';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import {
  discoverSessions, getSession, getSessionEventsPage, getSessionFileEditsPage,
  getSessionMarkersPage, getSessionRelationships, getSessionRequestsPage, getSessionToolCallsPage,
  getSessionTree, getSessionUsage, getSourceCapabilities, hydrateSession,
  listSessionCatalogPage, recent, search, stats, sync, createHandoff, resumeHandoff,
  MAX_HANDOFF_INTENT_CHARS,
} from './index.js';

import type { HistoryPluginRegistry } from './index.js';
import { loadHistoryApplicationConfig } from './delivery-cli.js';

const READ = { readOnlyHint: true, idempotentHint: true, openWorldHint: false } as const;
// Acquisition can reach provider services when a remote scope is requested
// (claude.ai/code web sessions, Codex cloud tasks), so it is open-world.
const ACQUIRE = { readOnlyHint: false, idempotentHint: true, openWorldHint: true } as const;
const LOCAL_ACQUIRE = { readOnlyHint: false, idempotentHint: true, openWorldHint: false } as const;
const SOURCE = z.enum(['claude', 'codex', 'cursor', 'grok', 'relay', 'trajectory', 'opencode']);
const CATALOG_SOURCE = z.enum(['claude', 'codex', 'cursor', 'grok', 'relay', 'opencode']);
const SESSION_SCOPE = z.enum(['local', 'remote', 'all']);
const SINCE_MS = z.number().int().optional().describe('Inclusive lower bound on timestampMs.');
const UNTIL_MS = z.number().int().optional().describe('Inclusive upper bound on timestampMs.');
const HISTORY_AFTER = z.object({
  timestampMs: z.number().int(), id: z.number().int(), matchSource: z.enum(['history', 'session_event']).optional(),
}).optional().describe('Continue strictly after this row: the last row\'s timestampMs, id and (for search) matchSource.');
const SOURCE_CONNECTORS = z.array(z.string().min(1)).optional().describe('Explicit configured source-plugin IDs; [] disables remote acquisition.');
const packageVersion = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
).version as string;

let configuredSources: HistoryPluginRegistry | undefined;
const server = new McpServer(
  { name: 'ai-hist', version: packageVersion },
  { capabilities: { tools: {} } },
);

function result(value: unknown) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(value, null, 2) }] };
}

async function call(operation: () => Promise<unknown>) {
  try { return result(await operation()); }
  catch (error) {
    const value = error as { code?: string; message?: string };
    return { content: [{ type: 'text' as const, text: `${value.code ?? 'ERROR'}: ${value.message ?? String(error)}` }], isError: true };
  }
}

server.tool('search_history',
  'Full-text search of already-indexed user prompts and session events (assistant text, tool calls and results), '
  + 'newest first; the same contract as `ai-hist search`. By default each word is matched as a literal token and all '
  + 'must match; a leading - excludes a word. A query containing AND, OR, NOT, a trailing * or a "quoted phrase" is '
  + 'passed to SQLite FTS5 as written, and raw_fts: true always does so. Each match carries matchSource '
  + '(history or session_event), role and kind; id is unique only within matchSource. To page, pass the last '
  + 'match\'s timestampMs, id and matchSource as after.', {
  query: z.string(), source: SOURCE.optional(), project: z.string().optional(), tag: z.string().optional(),
  scope: SESSION_SCOPE.optional().default('local'),
  role: z.enum(['all', 'user', 'assistant', 'prompt']).optional().default('all')
    .describe('all: prompts and every event; user: prompts and user events; assistant: assistant events; prompt: prompts only.'),
  raw_fts: z.boolean().optional().default(false)
    .describe('Pass the query to SQLite FTS5 verbatim; a malformed expression is an error.'),
  since_ms: SINCE_MS, until_ms: UNTIL_MS, after: HISTORY_AFTER,
  limit: z.number().int().min(1).max(1000).optional().default(20),
}, READ, ({ query, source, project, tag, scope, role, raw_fts, since_ms, until_ms, after, limit }) => call(() => search(query, {
  source, project, tag, scope, role, rawFts: raw_fts, sinceMs: since_ms, untilMs: until_ms, after, limit,
})));

server.tool('recent_history', 'List recent already-indexed prompts, newest first by (timestampMs, id). '
  + 'To page, pass the last row\'s timestampMs and id as after.', {
  source: SOURCE.optional(), project: z.string().optional(), tag: z.string().optional(),
  scope: SESSION_SCOPE.optional().default('local'),
  n: z.number().int().min(1).max(1000).optional().default(20),
  before_ms: z.number().int().optional().describe('Deprecated: exclusive, so it skips rows tied on the timestamp. Use after.'),
  since_ms: SINCE_MS, until_ms: UNTIL_MS, after: HISTORY_AFTER,
}, READ, ({ source, project, tag, scope, n, before_ms, since_ms, until_ms, after }) => call(() => recent({
  source, project, tag, scope, limit: n, beforeMs: before_ms, sinceMs: since_ms, untilMs: until_ms, after,
})));

server.tool('list_sessions', 'Cache-only indexed session catalog listing. This never discovers or syncs.', {
  sources: z.array(CATALOG_SOURCE).optional(), limit: z.number().int().min(1).max(1000).optional().default(20),
  scope: SESSION_SCOPE.optional().default('local'),
  before_ms: z.number().int().optional(),
  after: z.object({ lastActivityMs: z.number().int().nullable().optional(), source: z.string(), sessionId: z.string() }).optional(),
}, READ, ({ sources, scope, limit, before_ms, after }) => call(() => listSessionCatalogPage({
  sources, scope, limit, beforeMs: before_ms,
  after: after ? { ...after, lastActivityMs: after.lastActivityMs ?? null } : undefined,
})));

server.tool('discover_sessions', 'Explicit shallow provider discovery. Updates only the session catalog.', {
  sources: z.array(CATALOG_SOURCE).optional(), limit: z.number().int().min(1).max(10000).optional(),
  scope: SESSION_SCOPE.optional().default('local'),
  source_connectors: SOURCE_CONNECTORS,
  acquisition_timeout_ms: z.number().int().min(1).max(3600000).optional(),
}, ACQUIRE, ({ sources, scope, limit, source_connectors, acquisition_timeout_ms }) => call(() => discoverSessions({ sources, scope, limit, sourceConnectors: source_connectors, acquisitionTimeoutMs: acquisition_timeout_ms, plugins: configuredSources })));

server.tool('hydrate_session',
  'Index one cataloged session as fully as its provider allows, without global sync. '
  + '`capability` is computed from `coverage`, the evidence kinds that provider\'s parser produces: '
  + 'prompt-only providers return `partial` with a HYDRATION_PARTIAL_COVERAGE diagnostic naming what is absent, '
  + 'never `full`.', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
  scope: SESSION_SCOPE.optional().default('local'),
  include_related: z.boolean().optional().default(true),
  source_connectors: SOURCE_CONNECTORS,
  acquisition_timeout_ms: z.number().int().min(1).max(3600000).optional(),
}, ACQUIRE, ({ source, session_id, scope, include_related, source_connectors, acquisition_timeout_ms }) => call(() => hydrateSession({
  source, sessionId: session_id, scope, includeRelated: include_related, sourceConnectors: source_connectors, acquisitionTimeoutMs: acquisition_timeout_ms, plugins: configuredSources,
})));

server.tool('get_session', 'Get indexed prompts for one session.', {
  session_id: z.string(), source: SOURCE.optional(), tag: z.string().optional(),
}, READ, ({ session_id, source, tag }) => call(() => getSession(session_id, { source, tag })));

server.tool('get_session_events', 'Get one bounded page of normalized events.', {
  session_id: z.string(), source: SOURCE.optional(), limit: z.number().int().min(1).max(1000).optional().default(200),
  after: z.object({ tsMs: z.number().int(), id: z.number().int() }).optional(),
}, READ, ({ session_id, source, limit, after }) => call(() => getSessionEventsPage(session_id, { source, limit, after })));

const RELATIONSHIP_KIND = z.enum(['delegated', 'materialized_local', 'continuation', 'fork', 'resume']);

server.tool('get_session_relationships',
  'Direct relationships for one session: delegation in both directions (as parent and as child), plus the continuity edges (resume, fork, continuation) that connect it to the conversation it came from.', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
}, READ, ({ source, session_id }) => call(() => getSessionRelationships({ source, sessionId: session_id })));

server.tool('get_session_tree',
  'Complete descendant tree for one session, with cycle protection and deterministic ordering. Follows delegation edges by default; pass relationship_kinds to also include resumed, continued, or forked descendants of the root. Child events are not flattened into the parent.', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
  max_depth: z.number().int().min(1).max(64).optional(),
  max_nodes: z.number().int().min(1).max(10000).optional(),
  relationship_kinds: z.array(RELATIONSHIP_KIND).min(1).optional(),
}, READ, ({ source, session_id, max_depth, max_nodes, relationship_kinds }) => call(() => getSessionTree({
  source, sessionId: session_id, maxDepth: max_depth, maxNodes: max_nodes,
  relationshipKinds: relationship_kinds,
})));

const EVIDENCE_CURSOR = z.object({ tsMs: z.number().int().nullable().optional(), id: z.number().int() });

server.tool('get_session_tool_calls', 'Get one bounded page of recorded tool calls for one session.', {
  source: SOURCE, session_id: z.string().min(1),
  limit: z.number().int().min(1).max(1000).optional().default(200),
  after: EVIDENCE_CURSOR.optional(),
}, READ, ({ source, session_id, limit, after }) => call(() => getSessionToolCallsPage(source, session_id, { limit, after })));

server.tool('get_session_file_edits', 'Get one bounded page of recorded file edits for one session.', {
  source: SOURCE, session_id: z.string().min(1),
  limit: z.number().int().min(1).max(1000).optional().default(200),
  after: EVIDENCE_CURSOR.optional(),
}, READ, ({ source, session_id, limit, after }) => call(() => getSessionFileEditsPage(source, session_id, { limit, after })));

server.tool('create_handoff', 'Create a Relaycast handoff pointer for the caller\'s current session. Its single intent field is a self-describing resume prompt; send that exact intent as the delivery text and the pointer as metadata kind="handoff". Never inline the transcript.', {
  intent: z.string().min(1).max(MAX_HANDOFF_INTENT_CHARS),
}, LOCAL_ACQUIRE, ({ intent }) => call(() => createHandoff(intent)));

const HANDOFF_CURSOR = z.object({
  prompt: z.object({ timestampMs: z.number().int(), id: z.number().int() }).optional(),
  events: z.object({ tsMs: z.number().int(), id: z.number().int() }).optional(),
  tool_calls: EVIDENCE_CURSOR.optional(),
  file_edits: EVIDENCE_CURSOR.optional(),
});

server.tool('resume_handoff', 'Auto-resume a same-workspace handoff in one call: acquire the session and compose prompts, normalized events, tool calls, and file edits. Cross-workspace and cross-organization handoffs are rejected.', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
  limit: z.number().int().min(1).max(1000).optional().default(200),
  cursor: HANDOFF_CURSOR.optional(),
  acquisition_timeout_ms: z.number().int().min(1).max(3600000).optional(),
}, ACQUIRE, ({ source, session_id, limit, cursor, acquisition_timeout_ms }) => call(async () => {
  const resumed = await resumeHandoff(source, session_id, {
    limit,
    cursor: cursor ? {
      prompt: cursor.prompt,
      events: cursor.events,
      toolCalls: cursor.tool_calls,
      fileEdits: cursor.file_edits,
    } : undefined,
    acquisitionTimeoutMs: acquisition_timeout_ms,
    plugins: configuredSources,
  });
  const next = resumed.next_cursor;
  return {
    ...resumed,
    next_cursor: next ? {
      ...(next.prompt ? { prompt: next.prompt } : {}),
      ...(next.events ? { events: next.events } : {}),
      ...(next.toolCalls ? { tool_calls: next.toolCalls } : {}),
      ...(next.fileEdits ? { file_edits: next.fileEdits } : {}),
    } : null,
  };
}));

server.tool('get_session_markers', 'Get one bounded page of a session\'s markers: the records a provider wrote that are not transcript events, such as compaction and summary boundaries, provider system rows, non-text content blocks and agent lifecycle events. `kind` is the classified vocabulary and `subkind` the provider-native type; an unclassified record is kind `unknown`. `payload_json` is a bounded projection, never an image or document\'s bytes. Undated markers page last.', {
  source: SOURCE, session_id: z.string().min(1),
  limit: z.number().int().min(1).max(1000).optional().default(200),
  after: EVIDENCE_CURSOR.optional(),
}, READ, ({ source, session_id, limit, after }) => call(() => getSessionMarkersPage(source, session_id, { limit, after })));

server.tool('get_source_capabilities', 'What one provider\'s parser can record, from RelayHistory\'s own capability tables rather than any database: the evidence kinds a hydration of that source covers (and which of the full set it cannot), and what its records establish about delegation. Answers the same before a first sync.', {
  source: CATALOG_SOURCE,
}, READ, ({ source }) => call(() => getSourceCapabilities(source)));

const REQUEST_CURSOR = z.object({ tsMs: z.number().int(), id: z.number().int() });

server.tool('get_session_requests', 'Get one bounded page of a session\'s model requests, with usage normalized. One row per API request: Claude\'s per-content-block copies of message.usage are collapsed, so these can be summed where raw events cannot.', {
  source: SOURCE, session_id: z.string().min(1),
  limit: z.number().int().min(1).max(1000).optional().default(200),
  after: REQUEST_CURSOR.optional(),
}, READ, ({ source, session_id, limit, after }) => call(() => getSessionRequestsPage(source, session_id, { limit, after })));

server.tool('get_session_usage', 'Provider-neutral token usage rollup for one session. Null usage means no usage evidence, never an assumed zero; cost appears only when the source data carried one.', {
  source: SOURCE, session_id: z.string().min(1),
}, READ, ({ source, session_id }) => call(() => getSessionUsage(source, session_id)));

server.tool('history_stats', 'Statistics for already-indexed RelayHistory data.', {
  scope: SESSION_SCOPE.optional().default('local'),
  tag: z.string().optional(),
}, READ, ({ scope, tag }) => call(() => stats({ scope, tag })));

server.tool('sync', 'Explicit full provider ingestion into RelayHistory.', {
  scope: SESSION_SCOPE.optional().default('local'),
  source_connectors: SOURCE_CONNECTORS,
  acquisition_timeout_ms: z.number().int().min(1).max(3600000).optional(),
}, ACQUIRE, ({ scope, source_connectors, acquisition_timeout_ms }) => call(() => sync({ scope, sourceConnectors: source_connectors, acquisitionTimeoutMs: acquisition_timeout_ms, plugins: configuredSources })));

// Only an explicitly named config may load installed modules, and only their
// source connectors are used. No package scan or implicit enablement at startup.
if (process.env.AI_HIST_PLUGIN_CONFIG) {
  configuredSources = (await loadHistoryApplicationConfig(process.env.AI_HIST_PLUGIN_CONFIG)).registry;
}
await server.connect(new StdioServerTransport());
