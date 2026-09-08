#!/usr/bin/env node

/** Thin MCP adapters over the public `ai-hist` TypeScript SDK. */
import { readFileSync } from 'node:fs';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import {
  discoverSessions, getSession, getSessionEventsPage, getSessionFileEditsPage,
  getSessionRelationships, getSessionToolCallsPage, getSessionTree, hydrateSession,
  listSessionCatalogPage, recent, search, stats, sync,
} from './index.js';
import { getSessionThread } from './cloud-client.js';

const READ = { readOnlyHint: true, idempotentHint: true, openWorldHint: false } as const;
// A lifecycle thread is served by the cloud recall API and never cached, so it
// reads nothing locally and reaches the network on every call.
const CLOUD_READ = { readOnlyHint: true, idempotentHint: true, openWorldHint: true } as const;
// Acquisition can reach provider services when a remote scope is requested
// (claude.ai/code web sessions, Codex cloud tasks), so it is open-world.
const ACQUIRE = { readOnlyHint: false, idempotentHint: true, openWorldHint: true } as const;
// Targeted hydration indexes local provider evidence only.
const LOCAL_WRITE = { readOnlyHint: false, idempotentHint: true, openWorldHint: false } as const;
const SOURCE = z.enum(['claude', 'codex', 'cursor', 'grok', 'relay', 'trajectory', 'opencode']);
const CATALOG_SOURCE = z.enum(['claude', 'codex', 'cursor', 'grok', 'relay', 'opencode']);
const SESSION_SCOPE = z.enum(['local', 'remote', 'all']);
const packageVersion = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
).version as string;

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

server.tool('search_history', 'Search already-indexed RelayHistory prompts.', {
  query: z.string(), source: SOURCE.optional(), project: z.string().optional(), tag: z.string().optional(),
  scope: SESSION_SCOPE.optional().default('local'),
  limit: z.number().int().min(1).max(1000).optional().default(20),
}, READ, ({ query, source, project, tag, scope, limit }) => call(() => search(query, { source, project, tag, scope, limit })));

server.tool('recent_history', 'List recent already-indexed history.', {
  source: SOURCE.optional(), project: z.string().optional(), tag: z.string().optional(),
  scope: SESSION_SCOPE.optional().default('local'),
  n: z.number().int().min(1).max(1000).optional().default(20),
  before_ms: z.number().int().optional(),
}, READ, ({ source, project, tag, scope, n, before_ms }) => call(() => recent({ source, project, tag, scope, limit: n, beforeMs: before_ms })));

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
}, ACQUIRE, (args) => call(() => discoverSessions(args)));

server.tool('hydrate_session', 'Fully index one cataloged session without global sync.', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
  scope: SESSION_SCOPE.optional().default('local'),
  include_related: z.boolean().optional().default(true),
}, LOCAL_WRITE, ({ source, session_id, scope, include_related }) => call(() => hydrateSession({
  source, sessionId: session_id, scope, includeRelated: include_related,
})));

server.tool('get_session', 'Get indexed prompts for one session.', {
  session_id: z.string(), source: SOURCE.optional(), tag: z.string().optional(),
}, READ, ({ session_id, source, tag }) => call(() => getSession(session_id, { source, tag })));

server.tool('get_session_events', 'Get one bounded page of normalized events.', {
  session_id: z.string(), source: SOURCE.optional(), limit: z.number().int().min(1).max(1000).optional().default(200),
  after: z.object({ tsMs: z.number().int(), id: z.number().int() }).optional(),
}, READ, ({ session_id, source, limit, after }) => call(() => getSessionEventsPage(session_id, { source, limit, after })));

server.tool('get_session_relationships',
  'Direct delegation relationships for one session, in both directions (as parent and as child).', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
}, READ, ({ source, session_id }) => call(() => getSessionRelationships({ source, sessionId: session_id })));

server.tool('get_session_tree',
  'Complete descendant delegation tree for one session, with cycle protection and deterministic ordering. Child events are not flattened into the parent.', {
  source: CATALOG_SOURCE,
  session_id: z.string().min(1),
  max_depth: z.number().int().min(1).max(64).optional(),
  max_nodes: z.number().int().min(1).max(10000).optional(),
}, READ, ({ source, session_id, max_depth, max_nodes }) => call(() => getSessionTree({
  source, sessionId: session_id, maxDepth: max_depth, maxNodes: max_nodes,
})));

// `get_session_tree` is the subagent fan-out; `get_session_thread` is the
// lifecycle fan-out. They are complementary and an agent may call both.
// The thread is cloud-only and never cached: it changes as PRs and incidents
// land, so every call fetches. Tenancy is derived from the token server-side,
// which is why there is no org parameter to pass.
server.tool('get_session_thread',
  'Lifecycle thread for one session from the RelayHistory cloud: shipped commits plus linked PRs, reviews, incidents, tickets, Slack threads, hotfixes and follow-up sessions. Known link kinds are github_pr, pr_review, commit, sentry_event, incident, zendesk_ticket, slack_thread, hotfix and followup_session. Requires a stored cloud session; complements get_session_tree.', {
  source: SOURCE,
  session_id: z.string().min(1),
  kinds: z.array(z.string().min(1)).max(50).optional(),
  since: z.string().min(1).optional(),
  cursor: z.string().min(1).optional(),
}, CLOUD_READ, ({ source, session_id, kinds, since, cursor }) => call(() => getSessionThread({
  source, sessionId: session_id, kinds, since, cursor,
})));

// Tool calls and file edits are keyed by (source, session_id): a session id
// alone can name two sessions from two providers. A cursor's `tsMs` may be
// null or absent -- both mean "already inside the undated tail" -- and reaches
// the SDK exactly as the client sent it.
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

server.tool('history_stats', 'Statistics for already-indexed RelayHistory data.', {
  scope: SESSION_SCOPE.optional().default('local'),
  tag: z.string().optional(),
}, READ, ({ scope, tag }) => call(() => stats({ scope, tag })));

server.tool('sync', 'Explicit full provider ingestion into RelayHistory.', {
  scope: SESSION_SCOPE.optional().default('local'),
}, ACQUIRE, ({ scope }) => call(() => sync({ scope })));

await server.connect(new StdioServerTransport());
