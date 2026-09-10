#!/usr/bin/env node

import { readFile } from 'node:fs/promises';

import {
  discoverSessions, ensureLocalStore, formatSessionRow, getSession, getSessionEventsPage, getSessionFileEditsPage,
  getSessionRelationships, getSessionToolCallsPage, getSessionTree, hydrateSession,
  listSessionCatalogPage, loadStoredRelayhistoryAuth, login, recent, resumeCommand, search, stats, sync,
  enableCloud, accessToken, replay,
  validateCloudExchangeBaseUrl,
  type CatalogCursor, type EvidenceCursor, type HistoryEntry, type LocalStoreReadiness,
  type SessionFileEditsPage, type SessionRelationship, type SessionScope, type SessionToolCallsPage,
} from './index.js';
import { prepareCloudSessionForEnableCloud } from './cloud-preflight.js';

type Parsed = { positional: string[]; flags: Map<string, Array<string | true>> };

type PackageMetadata = { version?: string };

const BOOLEAN_FLAGS = new Set(['all', 'fts', 'json', 'local', 'no-bootstrap', 'no-related', 'no-warning', 'once', 'pretty', 'remote', 'version']);
const VALUE_FLAGS = new Set([
  'base-url', 'interval', 'label', 'max-content', 'out', 'after', 'after-ms', 'after-session-id', 'after-source', 'before-ms', 'db', 'limit',
  'max-depth', 'max-nodes', 'project', 'source', 'tag', 'token', 'tokens',
]);
const KNOWN_FLAGS = new Set([...BOOLEAN_FLAGS, ...VALUE_FLAGS]);

function versionTriple(value: string): [number, number, number] | null {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(value);
  return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null;
}

function newerVersion(current: string, latest: string): boolean {
  const left = versionTriple(current);
  const right = versionTriple(latest);
  if (!left || !right) return false;
  for (let index = 0; index < left.length; index++) {
    if (right[index] !== left[index]) return right[index] > left[index];
  }
  return false;
}

async function packageVersion(): Promise<string> {
  const contents = await readFile(new URL('../package.json', import.meta.url), 'utf8');
  return (JSON.parse(contents) as PackageMetadata).version ?? 'unknown';
}

async function maybePrintUpdateNotice(current: string, args: string[]): Promise<void> {
  const optOut = process.env.RELAYHISTORY_NO_UPDATE_CHECK;
  if (!process.stderr.isTTY || args.includes('--no-warning') || (optOut && optOut !== '0')) return;
  try {
    const response = await fetch('https://registry.npmjs.org/ai-hist/latest', {
      signal: AbortSignal.timeout(3_000),
      headers: { accept: 'application/json' },
    });
    if (!response.ok) return;
    const latest = (await response.json()) as PackageMetadata;
    if (!latest.version || !newerVersion(current, latest.version)) return;
    process.stderr.write(
      `\nA new version of ai-hist is available: ${current} -> ${latest.version}\n` +
      'Update with:\n  npm install --global ai-hist@latest\n' +
      '(pass --no-warning or set RELAYHISTORY_NO_UPDATE_CHECK=1 to hide this notice)\n',
    );
  } catch {
    // Version checks are best-effort; --version stays useful while offline.
  }
}

function parse(argv: string[]): Parsed {
  const positional: string[] = [];
  const flags = new Map<string, Array<string | true>>();
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '-V') {
      flags.set('version', [...(flags.get('version') ?? []), true]);
      continue;
    }
    if (!arg.startsWith('--')) { positional.push(arg); continue; }
    const [name, inline] = arg.slice(2).split('=', 2);
    if (!KNOWN_FLAGS.has(name)) usage(`unknown option '--${name}'`);
    if (inline !== undefined && BOOLEAN_FLAGS.has(name)) usage(`--${name} does not take a value`);
    if (inline !== undefined) {
      if (inline === '') usage(`--${name} requires a value`);
      flags.set(name, [...(flags.get(name) ?? []), inline]);
      continue;
    }
    if (BOOLEAN_FLAGS.has(name)) {
      const next = argv[i + 1];
      if (next === 'true' || next === 'false') usage(`--${name} does not take a value`);
      flags.set(name, [...(flags.get(name) ?? []), true]);
      continue;
    }
    const next = argv[i + 1];
    if (next && !next.startsWith('-')) { flags.set(name, [...(flags.get(name) ?? []), next]); i++; }
    else usage(`--${name} requires a value`);
  }
  return { positional, flags };
}

function validateFlags(args: Parsed, command: string, allowed: readonly string[]): void {
  const permitted = new Set([...allowed, 'no-warning']);
  for (const name of args.flags.keys()) {
    if (!permitted.has(name)) usage(`${command} does not accept --${name}`);
  }
}

function textFlag(args: Parsed, name: string): string | undefined {
  const value = args.flags.get(name)?.at(-1);
  return typeof value === 'string' ? value : undefined;
}

async function relayAccessTokenForCloudCommand(
  command: 'login' | 'enable-cloud',
  rawArgs: readonly string[],
  args: Parsed,
  reuseStoredAuth: boolean,
): Promise<string | undefined> {
  const explicitToken = textFlag(args, 'token');
  if (explicitToken !== undefined) return explicitToken;
  const baseUrl = textFlag(args, 'base-url');
  if (reuseStoredAuth && await loadStoredRelayhistoryAuth(baseUrl)) return undefined;
  const preparedToken = await prepareCloudSessionForEnableCloud(command, rawArgs, process.env);
  if (preparedToken !== null) await validateCloudExchangeBaseUrl(baseUrl);
  return preparedToken ?? undefined;
}

function textFlags(args: Parsed, name: string): string[] {
  return (args.flags.get(name) ?? []).filter((value): value is string => typeof value === 'string');
}

function numberFlag(args: Parsed, name: string): number | undefined {
  const value = textFlag(args, name);
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`--${name} must be a number`);
  return parsed;
}

function scopeFlag(args: Parsed): SessionScope {
  const selected = (['local', 'remote', 'all'] as const).filter((scope) => args.flags.has(scope));
  for (const scope of selected) {
    if (args.flags.get(scope)?.some((value) => value !== true)) {
      usage(`--${scope} does not take a value`);
    }
  }
  if (selected.length > 1) usage('--local, --remote, and --all are mutually exclusive');
  return selected[0] ?? 'local';
}

function rejectScopeFlag(args: Parsed, command: string): void {
  if (['local', 'remote', 'all'].some((scope) => args.flags.has(scope))) {
    usage(`${command} addresses a session by identity and does not accept --local, --remote, or --all`);
  }
}

function rejectSurplusPositionals(values: string[], command: string): void {
  if (values.length > 0) usage(`${command} does not accept positional argument '${values[0]}'`);
}

function common(args: Parsed) {
  return {
    dbPath: textFlag(args, 'db'),
    scope: scopeFlag(args),
    source: textFlag(args, 'source') as never,
    project: textFlag(args, 'project'),
    tag: textFlag(args, 'tag'),
    limit: numberFlag(args, 'limit'),
    beforeMs: numberFlag(args, 'before-ms'),
  };
}

function snakeCase(key: string): string {
  return key.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
}

// Parsed provider payloads are data, not RelayHistory field names: their own
// keys must reach stdout exactly as the provider wrote them.
const OPAQUE_JSON_KEYS = new Set(['args', 'structuredPatch']);

function wireValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(wireValue);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value)
    .map(([key, item]) => [snakeCase(key), OPAQUE_JSON_KEYS.has(key) ? item : wireValue(item)]));
}

function humanLine(value: unknown): string {
  if (!value || typeof value !== 'object') return String(value);
  const row = value as Record<string, unknown>;
  const locations = Array.isArray(row.locations) ? `[${row.locations.join(',')}]` : '';
  return [row.timestampMs ?? row.lastActivityMs ?? '', row.source ?? '', locations, row.sessionId ?? '', row.project ?? row.cwd ?? '', row.prompt ?? row.firstPrompt ?? '']
    .filter((item) => item !== '' && item != null)
    .join('  ');
}

function output(value: unknown, json: boolean): void {
  if (json) {
    process.stdout.write(`${JSON.stringify(wireValue(value))}\n`);
  } else if (Array.isArray(value)) {
    process.stdout.write(value.length ? `${value.map(humanLine).join('\n')}\n` : 'No results.\n');
  } else if (typeof value === 'object' && value !== null) {
    const record = value as Record<string, unknown>;
    if (Array.isArray(record.sessions)) {
      process.stdout.write(record.sessions.length ? `${record.sessions.map(humanLine).join('\n')}\n` : 'No sessions in the catalog.\n');
      if (record.nextCursor) process.stdout.write(`more available: --after '${JSON.stringify(record.nextCursor)}'\n`);
    } else {
      process.stdout.write(`${Object.entries(record).map(([key, item]) => `${key}: ${typeof item === 'object' ? JSON.stringify(item) : String(item)}`).join('\n')}\n`);
    }
  } else {
    process.stdout.write(`${String(value)}\n`);
  }
}

function usage(message?: string): never {
  if (message) process.stderr.write(`ai-hist: ${message}\n\n`);
  process.stderr.write(`Usage:
  ai-hist [--no-bootstrap] [--db PATH] [--json]
  ai-hist enable-cloud [--base-url URL] [--token TOKEN] [--db PATH] [--interval SECONDS] [--once] [--json]
  ai-hist sessions list [--pretty] [--local | --remote | --all] [--source SOURCE]... [--limit N] [--before-ms MS] [--after JSON | --after-source SOURCE --after-session-id ID [--after-ms MS]] [--json]
  ai-hist sessions discover [--local | --remote | --all] [--source SOURCE] [--limit N] [--json]
  ai-hist sessions hydrate SOURCE SESSION_ID [--local | --remote | --all] [--no-related] [--db PATH] [--json]
  ai-hist sessions relationships SOURCE SESSION_ID [--db PATH] [--json]
  ai-hist sessions tree SOURCE SESSION_ID [--max-depth N] [--max-nodes N] [--db PATH] [--json]
  ai-hist sessions tools SOURCE SESSION_ID [--limit N] [--after JSON] [--db PATH] [--json]
  ai-hist sessions edits SOURCE SESSION_ID [--limit N] [--after JSON] [--db PATH] [--json]
  ai-hist search QUERY... [--local | --remote | --all] [--source SOURCE] [--project PATH] [--limit N] [--json]
  ai-hist recent [N] [--local | --remote | --all] [--source SOURCE] [--project PATH] [--json]
  ai-hist session SESSION_ID [--source SOURCE] [--json]
  ai-hist events SESSION_ID [--source SOURCE] [--limit N] [--after JSON] [--json]
  ai-hist resume QUERY... [--local | --remote | --all] [--db PATH] [--fts] [--json]
  ai-hist pack QUERY... [--local | --remote | --all] [--source SOURCE] [--project PATH] [--tag TAG] [--limit N] [--tokens N] [--db PATH] [--fts] [--json]
  ai-hist login [--base-url URL] [--token TOKEN] [--label LABEL] [--json]
  ai-hist token [--base-url URL]
  ai-hist replay SESSION_ID [--base-url URL] [--limit N] [--max-content N] [--json] [--out PATH]
  ai-hist stats [--local | --remote | --all] [--json]
  ai-hist sync [--local | --remote | --all] [--db PATH] [--json]

Every command that reads local history indexes it on first use; pass
--no-bootstrap to answer from the store exactly as it stands.
`);
  process.exit(2);
}

function cursorFlag<T>(args: Parsed): T | undefined {
  const raw = textFlag(args, 'after');
  return raw ? JSON.parse(raw) as T : undefined;
}

function catalogCursorFlag(args: Parsed): CatalogCursor | undefined {
  const encoded = cursorFlag<CatalogCursor>(args);
  if (encoded) return encoded;
  const source = textFlag(args, 'after-source');
  const sessionId = textFlag(args, 'after-session-id');
  if (!source && !sessionId) return undefined;
  if (!source || !sessionId) throw new Error('--after-source and --after-session-id must be used together');
  return { lastActivityMs: numberFlag(args, 'after-ms') ?? null, source, sessionId };
}

// Human output prints the SDK cursor and --json prints its snake_case wire
// form, so --after accepts either spelling and a printed cursor can be fed
// back unedited.
function evidenceCursorFlag(args: Parsed): EvidenceCursor | undefined {
  const raw = cursorFlag<{ tsMs?: unknown; ts_ms?: unknown; id?: unknown }>(args);
  if (raw === undefined) return undefined;
  if (!raw || typeof raw !== 'object' || !Number.isInteger(raw.id)) {
    throw new Error('--after must be a JSON cursor with an integer id');
  }
  // An undated cursor spells its timestamp `null` or leaves it out. Any other
  // non-integer timestamp is a malformed cursor: reading it as null instead
  // would place the page inside the undated tail and silently drop every dated
  // record after it.
  const tsMs = raw.tsMs ?? raw.ts_ms ?? null;
  if (tsMs !== null && !Number.isInteger(tsMs)) {
    throw new Error('--after cursor ts_ms must be an integer or null');
  }
  return { tsMs: tsMs as number | null, id: raw.id as number };
}

function outputDiscovery(value: Awaited<ReturnType<typeof discoverSessions>>, json: boolean): void {
  if (!json) {
    for (const session of value.sessions) process.stdout.write(`${humanLine(session)}\n`);
    process.stdout.write(
      `${value.sessions.length} session(s): ${value.discovered} discovered, ${value.skippedUnchanged} unchanged ` +
      `(${value.counters.filesOpened} file(s) opened, ${value.counters.shallowReads} shallow read(s)); ` +
      `requested scope: ${value.scope}, connector locations run: ${value.locationsRun.length > 0 ? value.locationsRun.join(', ') : 'none'}\n`,
    );
    return;
  }
  for (const session of value.sessions) output({ type: 'session', ...session }, true);
  for (const diagnostic of value.diagnostics) output({ type: 'diagnostic', ...diagnostic }, true);
  const { sessions: _sessions, diagnostics: _diagnostics, ...summary } = value;
  const providers = Object.fromEntries(summary.providers.map(({ source, ...provider }) => [source, provider]));
  output({ type: 'summary', ...summary, providers }, true);
}

function continuationNotice(cursor: EvidenceCursor | null): void {
  if (cursor) process.stdout.write(`more available: --after '${JSON.stringify(cursor)}'\n`);
}

// Human rows are positional, so every column is always printed: an absent
// value is `-` rather than a dropped field that would shift the columns after
// it, and an uncounted line delta is `?` rather than a fabricated 0.
function outputToolCalls(page: SessionToolCallsPage, json: boolean): void {
  if (json) {
    output(page, true);
    return;
  }
  if (page.toolCalls.length === 0) {
    process.stdout.write('No tool calls.\n');
    return;
  }
  for (const call of page.toolCalls) {
    process.stdout.write([
      call.tsMs ?? '-', call.source, call.toolUseId, call.name,
      call.target ?? '-', call.isError === true ? '(error)' : '-',
    ].join('  ').concat('\n'));
  }
  continuationNotice(page.nextCursor);
}

function outputFileEdits(page: SessionFileEditsPage, json: boolean): void {
  if (json) {
    output(page, true);
    return;
  }
  if (page.fileEdits.length === 0) {
    process.stdout.write('No file edits.\n');
    return;
  }
  for (const edit of page.fileEdits) {
    process.stdout.write([
      edit.tsMs ?? '-', edit.source, edit.toolUseId, edit.toolName ?? '-', edit.filePath,
      `+${edit.linesAdded ?? '?'}/-${edit.linesRemoved ?? '?'}`,
    ].join('  ').concat('\n'));
  }
  continuationNotice(page.nextCursor);
}

function outputHydration(value: Awaited<ReturnType<typeof hydrateSession>>, json: boolean): void {
  if (json) {
    output(value, true);
    return;
  }
  process.stdout.write(`${value.source}/${value.sessionId}: ${value.status}\n`);
  process.stdout.write(
    `evidence: ${value.evidence.prompts} prompt(s), ${value.evidence.events} event(s), ` +
    `${value.evidence.toolCalls} tool call(s), ${value.evidence.fileEdits} file edit(s)\n`,
  );
  if (value.relatedSessionIds.length) {
    process.stdout.write(`related sessions: ${value.relatedSessionIds.join(', ')}\n`);
  }
  for (const diagnostic of value.diagnostics) {
    process.stdout.write(`${diagnostic.code}: ${diagnostic.message}\n`);
  }
}

function relationshipLine(direction: 'child' | 'parent', row: SessionRelationship): string {
  const identity = direction === 'child' ? row.childSessionId ?? '(unlinked)' : row.parentSessionId;
  return [
    direction, identity, row.relationship, row.childAgentType ?? '-', row.spawnedAtMs ?? '-',
    `events=${row.childHasEvents ? 'yes' : 'no'}`, `identity=${row.identityStatus}`,
    row.evidenceLocator ?? row.evidenceKind,
  ].join('  ');
}

function outputRelationships(value: Awaited<ReturnType<typeof getSessionRelationships>>, json: boolean): void {
  if (json) {
    output(value, true);
    return;
  }
  const total = value.asParent.length + value.asChild.length;
  process.stdout.write(total === 0
    ? `${value.source}/${value.sessionId}: no delegation relationships.\n`
    : `${value.source}/${value.sessionId}: ${value.asParent.length} child relationship(s), ` +
      `${value.asChild.length} parent relationship(s)\n`);
  for (const row of value.asParent) process.stdout.write(`${relationshipLine('child', row)}\n`);
  for (const row of value.asChild) process.stdout.write(`${relationshipLine('parent', row)}\n`);
  process.stdout.write(`capability: stable child identity = ${value.capabilities.stableChildIdentity}\n`);
  for (const diagnostic of value.diagnostics) {
    process.stdout.write(`${diagnostic.code}: ${diagnostic.message}\n`);
  }
}

// Mirrors the Rust CLI's `Local.timestamp_millis_opt(ms).format("%Y-%m-%d %H:%M")`:
// the machine's local timezone, minute precision, zero-padded.
function formatLocalMinute(epochMs: number): string {
  const date = new Date(epochMs);
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function queryPositionals(subcommand: string | undefined, rest: string[], command: string): string[] {
  const query = [subcommand, ...rest].filter((value): value is string => value !== undefined);
  if (query.length === 0) usage(`${command} requires a query`);
  return query;
}

async function runResume(args: Parsed, subcommand: string | undefined, rest: string[], json: boolean): Promise<void> {
  const query = queryPositionals(subcommand, rest, 'resume');
  // Matches the Rust CLI: search is capped to the single best match, then that
  // one row is checked for a usable session id rather than scanning further.
  const rows = await search(query.join(' '), {
    dbPath: textFlag(args, 'db'), scope: scopeFlag(args), rawFts: args.flags.has('fts'), limit: 1,
  });
  const entry = rows.find((row) => row.sessionId);
  if (!entry) throw new Error('No session found');
  const cmd = resumeCommand(entry);
  // A session with no local presence is only "unavailable", not an error: the
  // Rust CLI's JSON output always succeeds and explains itself with this
  // field rather than failing, matching that contract here.
  const locallyAvailable = entry.locations.length === 0 || entry.locations.includes('local');
  if (json) {
    output({
      ...entry,
      resumeCmd: cmd,
      scope: scopeFlag(args),
      ...(locallyAvailable ? {} : {
        resumeUnavailableReason: 'session is remote-only; materialize it locally before resuming',
      }),
    }, true);
    return;
  }
  if (cmd) {
    process.stdout.write(`${cmd}\n`);
    return;
  }
  if (!locallyAvailable) {
    throw new Error(
      `Session ${entry.sessionId} is remote-only and cannot be resumed locally; materialize it locally first.`,
    );
  }
  throw new Error(`No resume command available for source '${entry.source}'`);
}

function nonNegativeIntFlag(args: Parsed, name: string): number | undefined {
  const value = textFlag(args, name);
  if (value === undefined) return undefined;
  if (!/^\d+$/.test(value)) throw new Error(`--${name} must be a non-negative integer`);
  return Number(value);
}

// Rust's `.chars().take(limit)` walks Unicode scalar values, not UTF-16 code
// units — a plain `.slice()`/`.length` here could split a surrogate pair for
// non-BMP characters (e.g. most emoji). Array.from() iterates by code point,
// matching that semantics.
function takeCodePoints(text: string, limit: number): { truncated: boolean; text: string } {
  const points = Array.from(text);
  if (points.length <= limit) return { truncated: false, text };
  return { truncated: true, text: points.slice(0, limit).join('') };
}

async function runPack(args: Parsed, subcommand: string | undefined, rest: string[], json: boolean): Promise<void> {
  const query = queryPositionals(subcommand, rest, 'pack');
  const queryStr = query.join(' ');
  const tokens = nonNegativeIntFlag(args, 'tokens') ?? 0;
  // The native Pack command defaults its own limit to 10, distinct from
  // search()'s general-purpose default of 20 — match Pack specifically.
  const rows = await search(queryStr, {
    ...common(args), limit: numberFlag(args, 'limit') ?? 10, rawFts: args.flags.has('fts'),
  });
  if (rows.length === 0) {
    if (json) {
      output({ query: queryStr, entries: [] }, true);
    } else {
      process.stdout.write('No results.\n');
    }
    process.exitCode = 1;
    return;
  }
  const charsBudget = tokens > 0 ? tokens * 4 : undefined;
  const generatedMs = Date.now();
  if (json) {
    const entries = rows.map((entry) => {
      const prompt = charsBudget ? takeCodePoints(entry.prompt, charsBudget).text : entry.prompt;
      return { ...entry, prompt, resumeCmd: resumeCommand(entry) };
    });
    output({ query: queryStr, generatedMs, tokenBudget: tokens, entries }, true);
    return;
  }
  process.stdout.write(`=== ai-hist pack: "${queryStr}" | ${formatLocalMinute(generatedMs)} | ${rows.length} entries ===\n\n`);
  rows.forEach((entry: HistoryEntry, index: number) => {
    const project = entry.project ? `  ${entry.project}` : '';
    let text = entry.prompt.replace(/\n/g, ' ');
    if (charsBudget) {
      const capped = takeCodePoints(text, charsBudget);
      if (capped.truncated) text = `${capped.text}...`;
    }
    process.stdout.write(
      `[${index + 1}/${rows.length}] #${entry.id}  ${formatLocalMinute(entry.timestampMs)}  ${entry.source}${project}\n`,
    );
    process.stdout.write(`      ${text}\n`);
    if (entry.sessionId) {
      const cmd = resumeCommand(entry);
      if (cmd) {
        process.stdout.write(`      Resume: ${cmd}\n`);
      } else {
        const short = entry.sessionId.length > 16 ? `${entry.sessionId.slice(0, 16)}...` : entry.sessionId;
        process.stdout.write(`      Session: ${short}\n`);
      }
    }
    process.stdout.write('\n');
  });
}

function outputTree(value: Awaited<ReturnType<typeof getSessionTree>>, json: boolean): void {
  if (json) {
    output(value, true);
    return;
  }
  for (const node of value.nodes) {
    const edge = node.relationship
      ? `  [${[node.relationship.relationship, node.relationship.childAgentType].filter(Boolean).join(' ')}]` +
        `  events=${node.hasEvents ? 'yes' : 'no'}`
      : '';
    process.stdout.write(`${'  '.repeat(node.depth)}${node.sessionId}${edge}${node.truncated ? '  …' : ''}\n`);
  }
  process.stdout.write(
    `${Math.max(value.nodes.length - 1, 0)} descendant(s), max depth ${value.maxDepthReached}\n`,
  );
  for (const row of value.unlinked) {
    process.stdout.write(`unlinked evidence: ${row.evidenceKind} ${row.evidenceLocator ?? ''}`.trimEnd() + '\n');
  }
  for (const diagnostic of value.diagnostics) {
    process.stdout.write(`${diagnostic.code}: ${diagnostic.message}\n`);
  }
  if (value.truncated) process.stdout.write('truncated: node/depth budget reached\n');
}

interface CommandSpec {
  /** Name used in flag- and argument-rejection messages. */
  name: string;
  allowed: readonly string[];
  /** Positional arguments after the command words: [minimum, maximum]. */
  positionals: readonly [number, number | null];
  /** Named in the usage error when fewer than the minimum are given. */
  requires?: string;
  /** Answers from the local database, so it takes the shared first-use bootstrap. */
  readsLocalStore?: boolean;
  /** Addresses a session by identity; --local/--remote/--all are meaningless. */
  rejectsScope?: boolean;
  /** Remaining command-line checks, run before any store work. */
  validate?: (args: Parsed) => void;
}

function validateInterval(args: Parsed): void {
  // --interval is seconds; validate before converting so the error names the
  // unit the caller actually typed rather than a millisecond bound.
  const seconds = numberFlag(args, 'interval') ?? 60;
  if (!Number.isSafeInteger(seconds) || seconds < 1 || seconds > 2_147_483) {
    usage('--interval must be a whole number of seconds between 1 and 2147483');
  }
}

// The whole dispatch surface in one table. `readsLocalStore` is the decision
// that used to live only in the bare-invocation branch: keeping it here is what
// stops `search` and `ai-hist` disagreeing about whether a store exists.
const COMMANDS = new Map<string, CommandSpec>([
  ['', { name: 'ai-hist', positionals: [0, 0], allowed: ['db', 'json'], readsLocalStore: true }],
  ['login', { name: 'login', positionals: [0, 0],
    allowed: ['base-url', 'json', 'label', 'token'],
    validate: (args: Parsed) => {
      if (textFlag(args, 'token') && !textFlag(args, 'base-url')) usage('login requires --base-url with --token');
    } }],
  ['token', { name: 'token', positionals: [0, 0], allowed: ['base-url'] }],
  ['replay', { name: 'replay', positionals: [1, 1], requires: 'replay requires SESSION_ID',
    allowed: ['base-url', 'limit', 'max-content', 'json', 'out'] }],
  ['enable-cloud', { name: 'enable-cloud', positionals: [0, 0], validate: validateInterval,
    allowed: ['base-url', 'db', 'interval', 'once', 'json', 'token'] }],
  ['sessions list', { name: 'sessions list', positionals: [0, 0], readsLocalStore: true,
    validate: (args) => {
      if (args.flags.has('json') && args.flags.has('pretty')) usage('--pretty and --json are mutually exclusive');
    },
    allowed: [
      'after', 'after-ms', 'after-session-id', 'after-source', 'all', 'before-ms', 'db',
      'json', 'limit', 'local', 'pretty', 'remote', 'source',
    ] }],
  ['sessions discover', { name: 'sessions discover', positionals: [0, 0],
    allowed: ['all', 'db', 'json', 'limit', 'local', 'remote', 'source'] }],
  ['sessions hydrate', { name: 'sessions hydrate', positionals: [2, 2], readsLocalStore: true,
    requires: 'sessions hydrate requires SOURCE and SESSION_ID',
    allowed: ['all', 'db', 'json', 'local', 'no-related', 'remote'] }],
  ['sessions relationships', { name: 'sessions relationships', positionals: [2, 2], readsLocalStore: true,
    rejectsScope: true, requires: 'sessions relationships requires SOURCE and SESSION_ID',
    allowed: ['all', 'db', 'json', 'local', 'remote'] }],
  ['sessions tree', { name: 'sessions tree', positionals: [2, 2], readsLocalStore: true, rejectsScope: true,
    requires: 'sessions tree requires SOURCE and SESSION_ID',
    allowed: ['all', 'db', 'json', 'local', 'max-depth', 'max-nodes', 'remote'] }],
  ['sessions tools', { name: 'sessions tools', positionals: [2, 2], readsLocalStore: true,
    requires: 'sessions tools requires SOURCE and SESSION_ID', allowed: ['after', 'db', 'json', 'limit'] }],
  ['sessions edits', { name: 'sessions edits', positionals: [2, 2], readsLocalStore: true,
    requires: 'sessions edits requires SOURCE and SESSION_ID', allowed: ['after', 'db', 'json', 'limit'] }],
  ['search', { name: 'search', positionals: [1, null], requires: 'search requires a query', readsLocalStore: true,
    allowed: ['all', 'before-ms', 'db', 'fts', 'json', 'limit', 'local', 'project', 'remote', 'source', 'tag'] }],
  ['recent', { name: 'recent', positionals: [0, 1], readsLocalStore: true,
    validate: (args) => {
      const count = args.positional[1];
      if (count !== undefined && !Number.isFinite(Number(count))) {
        usage(`recent count must be a number (got '${count}')`);
      }
    },
    allowed: ['all', 'before-ms', 'db', 'json', 'limit', 'local', 'project', 'remote', 'source', 'tag'] }],
  ['session', { name: 'session', positionals: [1, 1], requires: 'session requires SESSION_ID',
    readsLocalStore: true, rejectsScope: true,
    allowed: ['all', 'db', 'json', 'local', 'remote', 'source', 'tag'] }],
  ['events', { name: 'events', positionals: [1, 1], requires: 'events requires SESSION_ID',
    readsLocalStore: true, rejectsScope: true,
    allowed: ['after', 'all', 'db', 'json', 'limit', 'local', 'remote', 'source'] }],
  ['resume', { name: 'resume', positionals: [1, null], requires: 'resume requires a query', readsLocalStore: true,
    allowed: ['all', 'db', 'fts', 'json', 'local', 'remote'] }],
  ['pack', { name: 'pack', positionals: [1, null], requires: 'pack requires a query', readsLocalStore: true,
    validate: (args) => { nonNegativeIntFlag(args, 'tokens'); },
    allowed: ['all', 'db', 'fts', 'json', 'limit', 'local', 'project', 'remote', 'source', 'tag', 'tokens'] }],
  ['stats', { name: 'stats', positionals: [0, 0], readsLocalStore: true,
    allowed: ['all', 'db', 'json', 'local', 'remote', 'tag'] }],
  // sync and `sessions discover` build the store rather than read it, so they
  // do not bootstrap first; running them is itself the remedy for an empty one.
  ['sync', { name: 'sync', positionals: [0, 0], allowed: ['all', 'db', 'json', 'local', 'remote'] }],
]);

/** Command words consumed before the positional arguments start. */
function commandWords(command: string | undefined): number {
  if (command === undefined) return 0;
  return command === 'sessions' ? 2 : 1;
}

function commandSpec(command: string | undefined, subcommand: string | undefined): CommandSpec | undefined {
  if (command === undefined) return COMMANDS.get('');
  if (command === 'sessions') return subcommand ? COMMANDS.get(`sessions ${subcommand}`) : undefined;
  return COMMANDS.get(command);
}

// A store that was never built and a store holding no match are different
// answers, and `No results.` is only true of the second. Wording and exit code
// separate them; the query is not run against a store that cannot hold one.
function reportUnusableStore(readiness: LocalStoreReadiness, json: boolean): boolean {
  if (readiness.status === 'ready' || readiness.status === 'skipped') return false;
  const message = readiness.status === 'unbuilt'
    ? 'No local index yet: run ai-hist (or ai-hist sync) to build one.'
    : 'No searchable local sessions found. Start a coding-agent session, then run ai-hist again.';
  if (json) output({ status: readiness.status, indexedPrompts: readiness.indexedPrompts, message }, true);
  else process.stdout.write(`${message}\n`);
  process.exitCode = 1;
  return true;
}

async function main(): Promise<void> {
  const rawArgs = process.argv.slice(2);
  const versionArgs = rawArgs.filter((arg) => arg !== '--no-warning');
  if (versionArgs.length === 1 && (versionArgs[0] === '--version' || versionArgs[0] === '-V')) {
    const version = await packageVersion();
    process.stdout.write(`ai-hist ${version}\n`);
    await maybePrintUpdateNotice(version, rawArgs);
    return;
  }
  const args = parse(rawArgs);
  const [command, subcommand, ...rest] = args.positional;
  const json = args.flags.has('json');

  const spec = commandSpec(command, subcommand);
  if (!spec) usage();
  // The whole command line is checked before any work: a line that is going to
  // be rejected must not first spend a first-run bootstrap only to exit 2.
  validateFlags(args, spec.name, spec.readsLocalStore ? [...spec.allowed, 'no-bootstrap'] : spec.allowed);
  if (spec.rejectsScope) rejectScopeFlag(args, spec.name);
  const tail = args.positional.slice(commandWords(command));
  const [least, most] = spec.positionals;
  if (tail.length < least) usage(spec.requires);
  if (most !== null && tail.length > most) rejectSurplusPositionals(tail.slice(most), spec.name);
  spec.validate?.(args);
  const intervalSeconds = numberFlag(args, 'interval') ?? 60;
  const sessionSource = command === 'sessions' ? tail[0] : undefined;
  const sessionId = command === 'sessions' ? tail[1] : undefined;
  const recentFallback = command === 'recent' && tail.length > 0 ? Number(tail[0]) : undefined;
  let readiness: LocalStoreReadiness | null = null;
  if (spec.readsLocalStore) {
    readiness = await ensureLocalStore({
      dbPath: textFlag(args, 'db'), scope: scopeFlag(args), bootstrap: !args.flags.has('no-bootstrap'),
    });
    if (readiness.bootstrap?.status === 'partial') {
      process.stderr.write('Some local sessions could not be fully indexed; run ai-hist --json for diagnostics.\n');
    }
    // The bare invocation reports the store's condition as its result and
    // succeeds either way. Every command that asks the store a question refuses
    // to answer out of one that cannot hold an answer.
    if (command !== undefined && reportUnusableStore(readiness, json)) return;
  }

  if (command === undefined) {
    // --no-bootstrap answers from the catalog as it stands; the bootstrap path
    // reports what it just indexed.
    if (!readiness?.bootstrap) {
      output(await listSessionCatalogPage({ dbPath: textFlag(args, 'db') }), json);
      return;
    }
    if (json) output(readiness.bootstrap, true);
    else {
      process.stdout.write(readiness.indexedPrompts > 0
        ? `Ready: ${readiness.indexedPrompts} indexed prompt(s). Search with: ai-hist search "your query"\n`
        : 'No searchable local sessions found. Start a coding-agent session, then run ai-hist again.\n');
    }
    return;
  }
  if (command === 'login') {
    const relayAccessToken = await relayAccessTokenForCloudCommand(command, rawArgs, args, false);
    const auth = await login({
      baseUrl: textFlag(args, 'base-url'),
      relayAccessToken,
      label: textFlag(args, 'label'),
    });
    if (json) output({ ok: true, baseUrl: auth.baseUrl }, true);
    else process.stdout.write(`Logged in to ${auth.baseUrl} (session stored).\n`);
    return;
  }
  if (command === 'token') {
    const token = await accessToken({ baseUrl: textFlag(args, 'base-url') });
    if (process.stdout.isTTY) process.stderr.write('Warning: this access token is a secret and will remain in terminal scrollback.\n');
    process.stdout.write(`${token}\n`);
    return;
  }
  if (command === 'replay') {
    const result = await replay(subcommand, {
      baseUrl: textFlag(args, 'base-url'), limit: nonNegativeIntFlag(args, 'limit'),
      maxContent: nonNegativeIntFlag(args, 'max-content'), json, out: textFlag(args, 'out'),
    });
    if (result.transcript !== null) process.stdout.write(result.transcript);
    return;
  }
  if (command === 'enable-cloud') {
    const relayAccessToken = await relayAccessTokenForCloudCommand(command, rawArgs, args, true);
    const handle = await enableCloud({
      baseUrl: textFlag(args, 'base-url'), dbPath: textFlag(args, 'db'),
      relayAccessToken,
      intervalMs: intervalSeconds * 1000,
      watch: !args.flags.has('once'),
      onPush: (result) => output(result, json),
    });
    const { stop, ...result } = handle;
    output(result, json);
    if (!args.flags.has('once')) {
      const shutdown = () => { void stop(); };
      process.once('SIGINT', shutdown);
      process.once('SIGTERM', shutdown);
    }
    return;
  }

  if (command === 'sessions' && subcommand === 'list') {
    const sources = textFlags(args, 'source');
    const page = await listSessionCatalogPage({
      dbPath: textFlag(args, 'db'), scope: scopeFlag(args), sources: sources.length ? sources as never : undefined,
      limit: numberFlag(args, 'limit'), beforeMs: numberFlag(args, 'before-ms'),
      after: catalogCursorFlag(args),
    });
    if (args.flags.has('pretty')) {
      const color = Boolean(process.stdout.isTTY) && process.env.NO_COLOR === undefined;
      process.stdout.write(page.sessions.length
        ? `${page.sessions.map((session) => formatSessionRow(session, { color })).join('\n')}\n`
        : 'No sessions in the catalog.\n');
      if (page.nextCursor) process.stdout.write(`more available: --after '${JSON.stringify(page.nextCursor)}'\n`);
    } else output(page, json);
    return;
  }
  if (command === 'sessions' && subcommand === 'discover') {
    const sources = textFlags(args, 'source');
    outputDiscovery(await discoverSessions({
      dbPath: textFlag(args, 'db'), scope: scopeFlag(args), sources: sources.length ? sources as never : undefined,
      limit: numberFlag(args, 'limit'),
    }), json);
    return;
  }
  if (command === 'sessions' && subcommand === 'hydrate') {
    outputHydration(await hydrateSession({
      source: sessionSource as never,
      sessionId: sessionId!,
      scope: scopeFlag(args),
      dbPath: textFlag(args, 'db'),
      includeRelated: !args.flags.has('no-related'),
    }), json);
    return;
  }
  if (command === 'sessions' && subcommand === 'relationships') {
    outputRelationships(await getSessionRelationships({
      source: sessionSource as never, sessionId: sessionId!, dbPath: textFlag(args, 'db'),
    }), json);
    return;
  }
  if (command === 'sessions' && subcommand === 'tree') {
    outputTree(await getSessionTree({
      source: sessionSource as never, sessionId: sessionId!, dbPath: textFlag(args, 'db'),
      maxDepth: numberFlag(args, 'max-depth'), maxNodes: numberFlag(args, 'max-nodes'),
    }), json);
    return;
  }
  if (command === 'sessions' && (subcommand === 'tools' || subcommand === 'edits')) {
    const name = `sessions ${subcommand}`;
    const options = {
      dbPath: textFlag(args, 'db'),
      limit: numberFlag(args, 'limit'),
      after: evidenceCursorFlag(args),
    };
    if (subcommand === 'tools') {
      outputToolCalls(await getSessionToolCallsPage(sessionSource as never, sessionId!, options), json);
    } else {
      outputFileEdits(await getSessionFileEditsPage(sessionSource as never, sessionId!, options), json);
    }
    return;
  }
  if (command === 'search') {
    output(await search([subcommand, ...rest].join(' '), { ...common(args), rawFts: args.flags.has('fts') }), json);
    return;
  }
  if (command === 'recent') {
    output(await recent({ ...common(args), limit: numberFlag(args, 'limit') ?? recentFallback }), json);
    return;
  }
  if (command === 'session') {
    output(await getSession(subcommand, { dbPath: textFlag(args, 'db'), source: textFlag(args, 'source') as never, tag: textFlag(args, 'tag') }), json);
    return;
  }
  if (command === 'events') {
    output(await getSessionEventsPage(subcommand, {
      dbPath: textFlag(args, 'db'), source: textFlag(args, 'source') as never,
      limit: numberFlag(args, 'limit'), after: cursorFlag(args),
    }), json);
    return;
  }
  if (command === 'resume') {
    await runResume(args, subcommand, rest, json);
    return;
  }
  if (command === 'pack') {
    await runPack(args, subcommand, rest, json);
    return;
  }
  if (command === 'stats') {
    output(await stats({ dbPath: textFlag(args, 'db'), scope: scopeFlag(args), tag: textFlag(args, 'tag') }), json);
    return;
  }
  if (command === 'sync') {
    output(await sync({ dbPath: textFlag(args, 'db'), scope: scopeFlag(args) }), json);
    return;
  }
  usage();
}

main().catch((error: unknown) => {
  const value = error as { code?: string; message?: string };
  process.stderr.write(`ai-hist: ${value.code ? `${value.code}: ` : ''}${value.message ?? String(error)}\n`);
  process.exitCode = 1;
});
