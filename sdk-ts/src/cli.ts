#!/usr/bin/env node

import { realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import type { Writable } from 'node:stream';
import { pathToFileURL } from 'node:url';

import {
  discoverSessions, ensureLocalStore, formatSessionRow, getSession, getSessionEventsPage, getSessionFileEditsPage,
  getSessionRelationships, getSessionToolCallsPage, getSessionTree, hydrateSession,
  listSessionCatalogPage, recent, resumeCommand, search, stats, sync,
  type CatalogCursor, type EvidenceCursor, type HistoryEntry, type LocalStoreReadiness,
  type SessionFileEditsPage, type SessionRelationship, type SessionScope, type SessionToolCallsPage,
} from './index.js';
import { runDeliveryCommand, runHistoryExportCommand, loadHistoryApplicationConfig } from './delivery-cli.js';

type Parsed = { positional: string[]; flags: Map<string, Array<string | true>> };

/**
 * Where a run's output goes.
 *
 * The bin writes to the real streams; a host that mounts this CLI (see
 * `relay-cli.ts`) supplies its own sink. Nothing below `runCli` touches
 * `process.stdout`/`process.stderr` directly, so the same dispatch serves both.
 */
export interface CliIo {
  stdout(chunk: string): void;
  stderr(chunk: string): void;
}

/**
 * A run that is over: usage errors and `--help`, which used to call
 * `process.exit`.
 *
 * `usage()` is reached from argument parsing several frames down and is typed
 * `never`, so returning a code from it is not available. Carrying the text on
 * the throw keeps those call sites unchanged and leaves `runCli` the only place
 * that decides what reaches `io`.
 */
class CliExit extends Error {
  constructor(readonly exitCode: number, readonly stdout: string, readonly stderr: string) {
    super(`ai-hist exited with ${exitCode}`);
    this.name = 'CliExit';
  }
}

type PackageMetadata = { version?: string };

export const BOOLEAN_FLAGS = new Set(['all', 'by-cwd', 'fts', 'help', 'json', 'local', 'no-bootstrap', 'no-related', 'no-source-connectors', 'no-warning', 'once', 'pretty', 'remote', 'version']);
export const VALUE_FLAGS = new Set([
  'config', 'job', 'selection', 'poll-ms', 'timeout-ms', 'base-url', 'interval', 'label', 'max-content', 'out', 'after', 'after-ms', 'after-session-id', 'after-source', 'before-ms', 'db', 'limit',
  'max-depth', 'max-nodes', 'config', 'source-connector', 'project', 'source', 'tag', 'token', 'tokens',
  // Documented in the usage text and read by `sessions discover`, `sessions
  // hydrate` and `sync`, but absent here, so `parse` rejected it as unknown.
  'acquisition-timeout-ms',
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

/**
 * True when this module was started as the `ai-hist` program.
 *
 * `relay-cli.ts` imports this file for `runCli`; without this guard that import
 * would run `main()` against the host's own `process.argv`.
 */
function isBinEntrypoint(): boolean {
  const entry = process.argv[1];
  if (entry === undefined) return false;
  try {
    return pathToFileURL(realpathSync(entry)).href === import.meta.url;
  } catch {
    return false;
  }
}

async function packageVersion(): Promise<string> {
  const contents = await readFile(new URL('../package.json', import.meta.url), 'utf8');
  return (JSON.parse(contents) as PackageMetadata).version ?? 'unknown';
}

async function maybePrintUpdateNotice(io: CliIo, current: string, args: string[]): Promise<void> {
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
    io.stderr(
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

function textFlags(args: Parsed, name: string): string[] {
  return (args.flags.get(name) ?? []).filter((value): value is string => typeof value === 'string');
}

function sourceConnectorFlags(args: Parsed): string[] | undefined {
  if (args.flags.has('no-source-connectors')) {
    if (args.flags.has('source-connector')) usage('--no-source-connectors and --source-connector are mutually exclusive');
    return [];
  }
  return args.flags.has('source-connector') ? textFlags(args, 'source-connector') : undefined;
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

export function humanLine(value: unknown): string {
  if (!value || typeof value !== 'object') return String(value);
  const row = value as Record<string, unknown>;
  const locations = Array.isArray(row.locations) ? `[${row.locations.join(',')}]` : '';
  return [row.timestampMs ?? row.lastActivityMs ?? '', row.source ?? '', locations, row.sessionId ?? '', row.project ?? row.cwd ?? '', row.prompt ?? row.firstPrompt ?? '']
    .filter((item) => item !== '' && item != null)
    .join('  ');
}

export function output(io: CliIo, value: unknown, json: boolean): void {
  if (json) {
    io.stdout(`${JSON.stringify(wireValue(value))}\n`);
  } else if (Array.isArray(value)) {
    io.stdout(value.length ? `${value.map(humanLine).join('\n')}\n` : 'No results.\n');
  } else if (typeof value === 'object' && value !== null) {
    const record = value as Record<string, unknown>;
    if (Array.isArray(record.sessions)) {
      io.stdout(record.sessions.length ? `${record.sessions.map(humanLine).join('\n')}\n` : 'No sessions in the catalog.\n');
      if (record.nextCursor) io.stdout(`more available: --after '${JSON.stringify(record.nextCursor)}'\n`);
    } else {
      io.stdout(`${Object.entries(record).map(([key, item]) => `${key}: ${typeof item === 'object' ? JSON.stringify(item) : String(item)}`).join('\n')}\n`);
    }
  } else {
    io.stdout(`${String(value)}\n`);
  }
}

/** The one usage text. It was duplicated verbatim in `showHelp` and `usage`. */
const USAGE_TEXT = `Usage:
  ai-hist [--no-bootstrap] [--db PATH] [--json] [--help]
  ai-hist sessions list [--pretty] [--local | --remote | --all] [--source SOURCE]... [--limit N] [--before-ms MS] [--after JSON | --after-source SOURCE --after-session-id ID [--after-ms MS]] [--json]
  ai-hist sessions discover [--local | --remote | --all] [--source-connector ID | --no-source-connectors] [--acquisition-timeout-ms N] [--source SOURCE] [--limit N] [--json]
  ai-hist sessions hydrate SOURCE SESSION_ID [--local | --remote | --all] [--source-connector ID | --no-source-connectors] [--acquisition-timeout-ms N] [--no-related] [--db PATH] [--json]
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
  ai-hist stats [--local | --remote | --all] [--json]
  ai-hist export --selection FILE [--out FILE] [--db PATH]
  ai-hist delivery enable|drain|run --config FILE [--job ID] [--db PATH]
  ai-hist delivery status|pause|resume|retry|cancel [--job ID] [--db PATH]
  ai-hist plugin COMMAND --config FILE -- [ARGS...]
  ai-hist sync [--local | --remote | --all] [--source-connector ID | --no-source-connectors] [--acquisition-timeout-ms N] [--db PATH] [--json]

Every command that reads local history indexes it on first use; pass
--no-bootstrap to answer from the store exactly as it stands.
`;

function showHelp(): never {
  throw new CliExit(0, USAGE_TEXT, '');
}

function usage(message?: string): never {
  throw new CliExit(2, '', `${message ? `ai-hist: ${message}\n\n` : ''}${USAGE_TEXT}`);
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

function outputDiscovery(io: CliIo, value: Awaited<ReturnType<typeof discoverSessions>>, json: boolean): void {
  if (!json) {
    for (const session of value.sessions) io.stdout(`${humanLine(session)}\n`);
    io.stdout(
      `${value.sessions.length} session(s): ${value.discovered} discovered, ${value.skippedUnchanged} unchanged ` +
      `(${value.counters.filesOpened} file(s) opened, ${value.counters.shallowReads} shallow read(s)); ` +
      `requested scope: ${value.scope}, connector locations run: ${value.locationsRun.length > 0 ? value.locationsRun.join(', ') : 'none'}\n`,
    );
    return;
  }
  for (const session of value.sessions) output(io, { type: 'session', ...session }, true);
  for (const diagnostic of value.diagnostics) output(io, { type: 'diagnostic', ...diagnostic }, true);
  const { sessions: _sessions, diagnostics: _diagnostics, ...summary } = value;
  const providers = Object.fromEntries(summary.providers.map(({ source, ...provider }) => [source, provider]));
  output(io, { type: 'summary', ...summary, providers }, true);
}

function continuationNotice(io: CliIo, cursor: EvidenceCursor | null): void {
  if (cursor) io.stdout(`more available: --after '${JSON.stringify(cursor)}'\n`);
}

// Human rows are positional, so every column is always printed: an absent
// value is `-` rather than a dropped field that would shift the columns after
// it, and an uncounted line delta is `?` rather than a fabricated 0.
function outputToolCalls(io: CliIo, page: SessionToolCallsPage, json: boolean): void {
  if (json) {
    output(io, page, true);
    return;
  }
  if (page.toolCalls.length === 0) {
    io.stdout('No tool calls.\n');
    return;
  }
  for (const call of page.toolCalls) {
    io.stdout([
      call.tsMs ?? '-', call.source, call.toolUseId, call.name,
      call.target ?? '-', call.isError === true ? '(error)' : '-',
    ].join('  ').concat('\n'));
  }
  continuationNotice(io, page.nextCursor);
}

function outputFileEdits(io: CliIo, page: SessionFileEditsPage, json: boolean): void {
  if (json) {
    output(io, page, true);
    return;
  }
  if (page.fileEdits.length === 0) {
    io.stdout('No file edits.\n');
    return;
  }
  for (const edit of page.fileEdits) {
    io.stdout([
      edit.tsMs ?? '-', edit.source, edit.toolUseId, edit.toolName ?? '-', edit.filePath,
      `+${edit.linesAdded ?? '?'}/-${edit.linesRemoved ?? '?'}`,
    ].join('  ').concat('\n'));
  }
  continuationNotice(io, page.nextCursor);
}

function outputHydration(io: CliIo, value: Awaited<ReturnType<typeof hydrateSession>>, json: boolean): void {
  if (json) {
    output(io, value, true);
    return;
  }
  io.stdout(`${value.source}/${value.sessionId}: ${value.status}\n`);
  io.stdout(
    `capability: ${value.capability} (coverage: ${value.coverage.join(', ') || 'none'})\n`,
  );
  io.stdout(
    `evidence: ${value.evidence.prompts} prompt(s), ${value.evidence.events} event(s), ` +
    `${value.evidence.toolCalls} tool call(s), ${value.evidence.fileEdits} file edit(s)\n`,
  );
  if (value.relatedSessionIds.length) {
    io.stdout(`related sessions: ${value.relatedSessionIds.join(', ')}\n`);
  }
  for (const diagnostic of value.diagnostics) {
    io.stdout(`${diagnostic.code}: ${diagnostic.message}\n`);
  }
}

/**
 * One relationship row, rendered from the point of view of the session that
 * was asked about.
 *
 * A continuity row is returned for whichever end of it the caller named, so
 * naming its child end and printing `childSessionId` printed the session back
 * at itself. `queried` is the session the row was read for, and the line shows
 * the *other* end: the origin when this session is the branch, the branch when
 * this session is the origin.
 */
function relationshipLine(
  direction: 'child' | 'parent' | 'continuity',
  row: SessionRelationship,
  queried?: string,
): string {
  const other = row.childSessionId === queried ? row.parentSessionId : row.childSessionId;
  const identity = direction === 'parent'
    ? row.parentSessionId
    : (direction === 'continuity' ? other : row.childSessionId) ?? '(unlinked)';
  return [
    direction, identity, row.relationship, row.childAgentType ?? '-', row.spawnedAtMs ?? '-',
    `events=${row.childHasEvents ? 'yes' : 'no'}`, `identity=${row.identityStatus}`,
    row.evidenceLocator ?? row.evidenceKind,
  ].join('  ');
}

function outputRelationships(io: CliIo, value: Awaited<ReturnType<typeof getSessionRelationships>>, json: boolean): void {
  if (json) {
    output(io, value, true);
    return;
  }
  const total = value.asParent.length + value.asChild.length;
  io.stdout(total === 0
    ? `${value.source}/${value.sessionId}: no delegation relationships.\n`
    : `${value.source}/${value.sessionId}: ${value.asParent.length} child relationship(s), ` +
      `${value.asChild.length} parent relationship(s)\n`);
  for (const row of value.asParent) io.stdout(`${relationshipLine('child', row)}\n`);
  for (const row of value.asChild) io.stdout(`${relationshipLine('parent', row)}\n`);
  if (value.continuity.length > 0) {
    io.stdout(`${value.continuity.length} continuity relationship(s)\n`);
    for (const row of value.continuity) {
      io.stdout(
        `${relationshipLine('continuity', row, value.sessionId)}  origin=${row.originSessionId ?? '-'}\n`,
      );
    }
  }
  io.stdout(`capability: stable child identity = ${value.capabilities.stableChildIdentity}\n`);
  for (const diagnostic of value.diagnostics) {
    io.stdout(`${diagnostic.code}: ${diagnostic.message}\n`);
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

async function runResume(io: CliIo, args: Parsed, subcommand: string | undefined, rest: string[], json: boolean): Promise<number> {
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
    output(io, {
      ...entry,
      resumeCmd: cmd,
      scope: scopeFlag(args),
      ...(locallyAvailable ? {} : {
        resumeUnavailableReason: 'session is remote-only; materialize it locally before resuming',
      }),
    }, true);
    return 0;
  }
  if (cmd) {
    io.stdout(`${cmd}\n`);
    return 0;
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

async function runPack(io: CliIo, args: Parsed, subcommand: string | undefined, rest: string[], json: boolean): Promise<number> {
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
      output(io, { query: queryStr, entries: [] }, true);
    } else {
      io.stdout('No results.\n');
    }
    return 1;
  }
  const charsBudget = tokens > 0 ? tokens * 4 : undefined;
  const generatedMs = Date.now();
  if (json) {
    const entries = rows.map((entry) => {
      const prompt = charsBudget ? takeCodePoints(entry.prompt, charsBudget).text : entry.prompt;
      return { ...entry, prompt, resumeCmd: resumeCommand(entry) };
    });
    output(io, { query: queryStr, generatedMs, tokenBudget: tokens, entries }, true);
    return 0;
  }
  io.stdout(`=== ai-hist pack: "${queryStr}" | ${formatLocalMinute(generatedMs)} | ${rows.length} entries ===\n\n`);
  rows.forEach((entry: HistoryEntry, index: number) => {
    const project = entry.project ? `  ${entry.project}` : '';
    let text = entry.prompt.replace(/\n/g, ' ');
    if (charsBudget) {
      const capped = takeCodePoints(text, charsBudget);
      if (capped.truncated) text = `${capped.text}...`;
    }
    io.stdout(
      `[${index + 1}/${rows.length}] #${entry.id}  ${formatLocalMinute(entry.timestampMs)}  ${entry.source}${project}\n`,
    );
    io.stdout(`      ${text}\n`);
    if (entry.sessionId) {
      const cmd = resumeCommand(entry);
      if (cmd) {
        io.stdout(`      Resume: ${cmd}\n`);
      } else {
        const short = entry.sessionId.length > 16 ? `${entry.sessionId.slice(0, 16)}...` : entry.sessionId;
        io.stdout(`      Session: ${short}\n`);
      }
    }
    io.stdout('\n');
  });
  return 0;
}

function outputTree(io: CliIo, value: Awaited<ReturnType<typeof getSessionTree>>, json: boolean): void {
  if (json) {
    output(io, value, true);
    return;
  }
  for (const node of value.nodes) {
    const edge = node.relationship
      ? `  [${[node.relationship.relationship, node.relationship.childAgentType].filter(Boolean).join(' ')}]` +
        `  events=${node.hasEvents ? 'yes' : 'no'}`
      : '';
    io.stdout(`${'  '.repeat(node.depth)}${node.sessionId}${edge}${node.truncated ? '  …' : ''}\n`);
  }
  io.stdout(
    `${Math.max(value.nodes.length - 1, 0)} descendant(s), max depth ${value.maxDepthReached}\n`,
  );
  for (const row of value.unlinked) {
    io.stdout(`unlinked evidence: ${row.evidenceKind} ${row.evidenceLocator ?? ''}`.trimEnd() + '\n');
  }
  for (const diagnostic of value.diagnostics) {
    io.stdout(`${diagnostic.code}: ${diagnostic.message}\n`);
  }
  if (value.truncated) io.stdout('truncated: node/depth budget reached\n');
}

/** One positional argument, as the mounted help renders it. */
export interface CommandArgSpec {
  name: string;
  description: string;
  required: boolean;
  variadic?: boolean;
}

export interface CommandSpec {
  /** Name used in flag- and argument-rejection messages. */
  name: string;
  /**
   * One line for the mounted help.
   *
   * A host renders its own help from this table rather than forwarding
   * `--help`, so an empty description is a user-visible hole, not an internal
   * detail.
   */
  description: string;
  /**
   * Path under the mounted group, or `null` for a command the group does not
   * expose. `['sessions', 'list']` here is `ai-hist sessions list`, reached as
   * `agent-relay sessions list`.
   */
  surface: readonly string[] | null;
  /** Extra spellings the mounted group accepts for `surface`. */
  surfaceAliases?: readonly string[];
  /**
   * The positionals named for help. `positionals` below stays the authority on
   * how many are accepted; the drift test asserts the two agree.
   */
  args?: readonly CommandArgSpec[];
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
  /**
   * Reads `RunCliOptions.signal`, so the bin claims the process's signals for
   * it. Left off, the bin leaves `SIGINT`/`SIGTERM` at their default: a
   * listener suppresses Node's own termination, so claiming them for a command
   * that never looks at the signal swallows the user's first Ctrl-C.
   */
  cancellable?: true;
}

function validateInterval(args: Parsed): void {
  // --interval is seconds; validate before converting so the error names the
  // unit the caller actually typed rather than a millisecond bound.
  const seconds = numberFlag(args, 'interval') ?? 60;
  if (!Number.isSafeInteger(seconds) || seconds < 1 || seconds > 2_147_483) {
    usage('--interval must be a whole number of seconds between 1 and 2147483');
  }
}

/**
 * Every option a command may list in `allowed`, spelled the way help shows it.
 *
 * This is the only place a flag's user-facing text lives. The drift test
 * asserts it covers exactly the flags the command table allows, and that each
 * one's arity matches `BOOLEAN_FLAGS`/`VALUE_FLAGS` — so a flag that `parse`
 * cannot accept can never reach the mounted help.
 */
export const FLAG_SPECS: Record<string, { flags: string; description: string }> = {
  'acquisition-timeout-ms': { flags: '--acquisition-timeout-ms <ms>', description: 'Budget for remote acquisition, in milliseconds.' },
  after: { flags: '--after <json>', description: 'Continue from a printed cursor.' },
  'after-ms': { flags: '--after-ms <ms>', description: 'Cursor timestamp, with --after-source and --after-session-id.' },
  'after-session-id': { flags: '--after-session-id <id>', description: 'Cursor session id, with --after-source.' },
  'after-source': { flags: '--after-source <source>', description: 'Cursor source, with --after-session-id.' },
  all: { flags: '--all', description: 'Read local and remote history.' },
  'before-ms': { flags: '--before-ms <ms>', description: 'Only entries older than this epoch-millisecond timestamp.' },
  config: { flags: '--config <file>', description: 'History application config file.' },
  db: { flags: '--db <path>', description: 'History database to read or write.' },
  fts: { flags: '--fts', description: 'Treat the query as raw SQLite full-text syntax.' },
  job: { flags: '--job <id>', description: 'Act on one delivery job.' },
  json: { flags: '--json', description: 'Emit JSON instead of human-readable text.' },
  limit: { flags: '--limit <n>', description: 'Maximum rows to return.' },
  local: { flags: '--local', description: 'Read only local history (the default).' },
  'max-depth': { flags: '--max-depth <n>', description: 'Stop walking below this depth.' },
  'max-nodes': { flags: '--max-nodes <n>', description: 'Stop after this many nodes.' },
  'no-bootstrap': { flags: '--no-bootstrap', description: 'Answer from the store as it stands, without first-use indexing.' },
  'no-related': { flags: '--no-related', description: 'Do not hydrate related sessions.' },
  'no-source-connectors': { flags: '--no-source-connectors', description: 'Disable remote acquisition entirely.' },
  out: { flags: '--out <file>', description: 'Write to this file instead of standard output.' },
  'poll-ms': { flags: '--poll-ms <ms>', description: 'Delivery poll interval, in milliseconds.' },
  pretty: { flags: '--pretty', description: 'Render aligned, colourized rows.' },
  'by-cwd': { flags: '--by-cwd', description: 'Group projects by working directory instead of canonical project key.' },
  project: { flags: '--project <value>', description: 'Restrict to one project: a canonical project key for `sessions list`, a project path elsewhere.' },
  remote: { flags: '--remote', description: 'Read only remote history.' },
  selection: { flags: '--selection <file>', description: 'Export selection file.' },
  source: { flags: '--source <source>', description: 'Restrict to one coding-agent source.' },
  'source-connector': { flags: '--source-connector <id>', description: 'Run this remote connector; repeatable.' },
  tag: { flags: '--tag <tag>', description: 'Restrict to entries carrying this tag.' },
  'timeout-ms': { flags: '--timeout-ms <ms>', description: 'Per-request delivery timeout, in milliseconds.' },
  tokens: { flags: '--tokens <n>', description: 'Approximate token budget for the packed output.' },
};

/** Options the host owns, so they never reach a mounted command's help. */
export const HOST_OWNED_FLAGS = new Set(['help', 'no-warning']);

// The whole dispatch surface in one table. `readsLocalStore` is the decision
// that used to live only in the bare-invocation branch: keeping it here is what
// stops `search` and `ai-hist` disagreeing about whether a store exists.
export const COMMANDS = new Map<string, CommandSpec>([
  // The bare invocation reports the store's condition; a mounted group shows
  // help instead, so `list` is its explicit equivalent there.
  ['', { name: 'ai-hist', description: 'Report local history readiness and list the catalogue.', surface: null,
    positionals: [0, 0], allowed: ['db', 'json', 'help'], readsLocalStore: true }],
  ['export', { name: 'export', description: 'Export selected history as NDJSON.', surface: ['export'],
    positionals: [0, 0], allowed: ['db', 'selection', 'out'] }],
  // A config-driven extension hook that needs `-- ARGS` passthrough, not a
  // user-facing verb: it stays on the bin and off the mounted tree.
  ['plugin', { name: 'plugin', description: 'Run a configured history plugin command.', surface: null,
    positionals: [1, null], allowed: ['config'], requires: 'plugin requires a command name' }],
  ...(Object.entries({
    enable: 'Create the delivery job declared in the config file.',
    status: 'Report delivery job status and retention.',
    drain: 'Deliver everything currently queued, then stop.',
    run: 'Run the delivery loop until it is cancelled.',
    pause: 'Stop a delivery job from making progress.',
    resume: 'Let a paused delivery job make progress again.',
    retry: 'Clear a delivery job\'s failure and try it again.',
    cancel: 'Abandon a delivery job.',
  }) as Array<[string, string]>).map(([action, description]): [string, CommandSpec] => [`delivery ${action}`, {
    name: `delivery ${action}`, description, surface: ['delivery', action],
    positionals: [0, 0], allowed: ['db', 'config', 'job', 'poll-ms', 'timeout-ms'],
    cancellable: true,
  }]),
  ['sessions list', { name: 'sessions list', description: 'List indexed sessions from the catalogue.',
    surface: ['list'], positionals: [0, 0], readsLocalStore: true,
    validate: (args) => {
      if (args.flags.has('json') && args.flags.has('pretty')) usage('--pretty and --json are mutually exclusive');
    },
    allowed: [
      'after', 'after-ms', 'after-session-id', 'after-source', 'all', 'before-ms', 'db',
      'json', 'limit', 'local', 'pretty', 'project', 'remote', 'source',
    ] }],
  ['sessions discover', { name: 'sessions discover', description: 'Find coding-agent sessions and index the new ones.',
    surface: ['discover'], positionals: [0, 0],
    validate: (args) => { sourceConnectorFlags(args); },
    allowed: ['all', 'db', 'json', 'limit', 'local', 'remote', 'source', 'config', 'source-connector', 'no-source-connectors', 'acquisition-timeout-ms'] }],
  ['sessions hydrate', { name: 'sessions hydrate', description: 'Index one session\'s full evidence.',
    surface: ['hydrate'], positionals: [2, 2], args: [{ name: 'source', description: 'Coding-agent source, e.g. claude or codex.', required: true }, { name: 'session-id', description: 'Session identifier.', required: true }],
    requires: 'sessions hydrate requires SOURCE and SESSION_ID',
    validate: (args) => { sourceConnectorFlags(args); },
    allowed: ['all', 'db', 'json', 'local', 'no-bootstrap', 'no-related', 'remote', 'config', 'source-connector', 'no-source-connectors', 'acquisition-timeout-ms'] }],
  ['sessions relationships', { name: 'sessions relationships', description: 'Show a session\'s parent and child delegations.',
    surface: ['relationships'], positionals: [2, 2], args: [{ name: 'source', description: 'Coding-agent source, e.g. claude or codex.', required: true }, { name: 'session-id', description: 'Session identifier.', required: true }], readsLocalStore: true,
    rejectsScope: true, requires: 'sessions relationships requires SOURCE and SESSION_ID',
    allowed: ['all', 'db', 'json', 'local', 'remote'] }],
  ['sessions tree', { name: 'sessions tree', description: 'Print a session\'s delegation tree.',
    surface: ['tree'], positionals: [2, 2], args: [{ name: 'source', description: 'Coding-agent source, e.g. claude or codex.', required: true }, { name: 'session-id', description: 'Session identifier.', required: true }], readsLocalStore: true, rejectsScope: true,
    requires: 'sessions tree requires SOURCE and SESSION_ID',
    allowed: ['all', 'db', 'json', 'local', 'max-depth', 'max-nodes', 'remote'] }],
  ['sessions tools', { name: 'sessions tools', description: 'Page through a session\'s tool calls.',
    surface: ['tools'], positionals: [2, 2], args: [{ name: 'source', description: 'Coding-agent source, e.g. claude or codex.', required: true }, { name: 'session-id', description: 'Session identifier.', required: true }], readsLocalStore: true,
    requires: 'sessions tools requires SOURCE and SESSION_ID', allowed: ['after', 'db', 'json', 'limit'] }],
  ['sessions edits', { name: 'sessions edits', description: 'Page through a session\'s file edits.',
    surface: ['edits'], positionals: [2, 2], args: [{ name: 'source', description: 'Coding-agent source, e.g. claude or codex.', required: true }, { name: 'session-id', description: 'Session identifier.', required: true }], readsLocalStore: true,
    requires: 'sessions edits requires SOURCE and SESSION_ID', allowed: ['after', 'db', 'json', 'limit'] }],
  ['search', { name: 'search', description: 'Search indexed prompts.', surface: ['search'],
    positionals: [1, null], args: [{ name: 'query', description: 'Search terms.', required: true, variadic: true }],
    requires: 'search requires a query', readsLocalStore: true,
    allowed: ['all', 'before-ms', 'db', 'fts', 'json', 'limit', 'local', 'project', 'remote', 'source', 'tag'] }],
  ['recent', { name: 'recent', description: 'Show the most recent prompts.', surface: ['recent'],
    positionals: [0, 1], args: [{ name: 'count', description: 'How many to show.', required: false }],
    readsLocalStore: true,
    validate: (args) => {
      const count = args.positional[1];
      if (count !== undefined && !Number.isFinite(Number(count))) {
        usage(`recent count must be a number (got '${count}')`);
      }
    },
    allowed: ['all', 'before-ms', 'db', 'json', 'limit', 'local', 'project', 'remote', 'source', 'tag'] }],
  // `agent-relay sessions session ID` reads badly, so the mounted spelling is
  // `show`; the original name stays as an alias for existing muscle memory.
  ['session', { name: 'session', description: 'Show one session.', surface: ['show'], surfaceAliases: ['session'],
    positionals: [1, 1], args: [{ name: 'session-id', description: 'Session identifier.', required: true }], requires: 'session requires SESSION_ID',
    readsLocalStore: true, rejectsScope: true,
    allowed: ['all', 'db', 'json', 'local', 'remote', 'source', 'tag'] }],
  ['events', { name: 'events', description: 'Page through one session\'s events.', surface: ['events'],
    positionals: [1, 1], args: [{ name: 'session-id', description: 'Session identifier.', required: true }], requires: 'events requires SESSION_ID',
    readsLocalStore: true, rejectsScope: true,
    allowed: ['after', 'all', 'db', 'json', 'limit', 'local', 'remote', 'source'] }],
  ['resume', { name: 'resume', description: 'Print the command that resumes the best-matching session.',
    surface: ['resume'], positionals: [1, null],
    args: [{ name: 'query', description: 'Search terms identifying the session.', required: true, variadic: true }],
    requires: 'resume requires a query', readsLocalStore: true,
    allowed: ['all', 'db', 'fts', 'json', 'local', 'remote'] }],
  ['pack', { name: 'pack', description: 'Pack matching history into a context block.', surface: ['pack'],
    positionals: [1, null], args: [{ name: 'query', description: 'Search terms.', required: true, variadic: true }],
    requires: 'pack requires a query', readsLocalStore: true,
    validate: (args) => { nonNegativeIntFlag(args, 'tokens'); },
    allowed: ['all', 'db', 'fts', 'json', 'limit', 'local', 'project', 'remote', 'source', 'tag', 'tokens'] }],
  ['stats', { name: 'stats', description: 'Summarize what the history store holds.', surface: ['stats'],
    positionals: [0, 0], readsLocalStore: true,
    allowed: ['all', 'by-cwd', 'db', 'json', 'local', 'remote', 'tag'] }],
  // sync and `sessions discover` build the store rather than read it, so they
  // do not bootstrap first; running them is itself the remedy for an empty one.
  ['sync', { name: 'sync', description: 'Index new sessions from every configured source.', surface: ['sync'],
    positionals: [0, 0], validate: (args) => { sourceConnectorFlags(args); },
    allowed: ['all', 'db', 'json', 'local', 'remote', 'config', 'source-connector', 'no-source-connectors', 'acquisition-timeout-ms'] }],
]);

/** Command words consumed before the positional arguments start. */
function commandWords(command: string | undefined): number {
  if (command === undefined) return 0;
  return command === 'sessions' || command === 'delivery' ? 2 : 1;
}

function commandSpec(command: string | undefined, subcommand: string | undefined): CommandSpec | undefined {
  if (command === undefined) return COMMANDS.get('');
  if (command === 'sessions' || command === 'delivery') return subcommand ? COMMANDS.get(`${command} ${subcommand}`) : undefined;
  return COMMANDS.get(command);
}

/**
 * Whether this command line routes to a command that reads the cancellation
 * signal.
 *
 * The bin asks before installing a `SIGINT`/`SIGTERM` handler. A listener
 * replaces Node's default termination, so a handler on an invocation that
 * never reads the signal swallows the first shutdown request: the user presses
 * Ctrl-C, nothing happens, and they press it again. Resolved from the same
 * `COMMANDS` table `dispatch` resolves against, so the two cannot disagree
 * about which commands those are.
 */
export function usesCancellation(argv: readonly string[]): boolean {
  // `plugin -- ARGS` passes its tail to a plugin verbatim, and `plugin` is not
  // cancellable, so stopping at the separator can only ever read less.
  const boundary = argv.indexOf('--');
  const core = [...(boundary < 0 ? argv : argv.slice(0, boundary))];
  try {
    const { positional } = parse(core.map((arg) => arg === '-h' ? '--help' : arg));
    return commandSpec(positional[0], positional[1])?.cancellable === true;
  } catch {
    // An argv `parse` refuses is a usage error `dispatch` is about to report.
    // It runs no command, so it reads no signal.
    return false;
  }
}

function unknownCommandMessage(command: string | undefined, subcommand: string | undefined): string {
  if (command === undefined) return 'invalid usage';
  if (command === 'sessions' || command === 'delivery') {
    if (subcommand === undefined) return `${command} requires a subcommand`;
    return `unknown ${command} subcommand '${subcommand}'`;
  }
  return `unknown command '${command}'`;
}

// A store that was never built and a store holding no match are different
// answers, and `No results.` is only true of the second. Wording and exit code
// separate them; the query is not run against a store that cannot hold one.
/** Cached remote evidence can answer an all-scope query even when local history is empty. */
function skipUnusableStoreGate(spec: CommandSpec, scope: SessionScope): boolean {
  if (scope !== 'all') return false;
  return spec.name === 'stats'
    || spec.name === 'sessions list'
    || spec.name === 'search'
    || spec.name === 'recent';
}

function reportUnusableStore(io: CliIo, readiness: LocalStoreReadiness, json: boolean): boolean {
  if (readiness.status === 'ready' || readiness.status === 'skipped') return false;
  const message = readiness.status === 'unbuilt'
    ? 'No local index yet: run ai-hist (or ai-hist sync) to build one.'
    : 'No searchable local sessions found. Start a coding-agent session, then run ai-hist again.';
  if (json) output(io, { status: readiness.status, indexedPrompts: readiness.indexedPrompts, message }, true);
  else io.stdout(`${message}\n`);
  return true;
}

/**
 * One invocation, start to finish, with no process state touched.
 *
 * This is the whole of what `main()` used to be. Every exit that was a
 * `process.exit`/`process.exitCode` is now a returned code, and every write is
 * routed through `io`, so a host can mount the same dispatch.
 */
async function dispatch(argv: readonly string[], io: CliIo, options: RunCliOptions): Promise<number> {
  const rawArgs = [...argv];
  const versionArgs = rawArgs.filter((arg) => arg !== '--no-warning');
  if (versionArgs.length === 1 && (versionArgs[0] === '--version' || versionArgs[0] === '-V')) {
    const version = await packageVersion();
    io.stdout(`ai-hist ${version}\n`);
    // A mounted host prints its own version and must not make a network call
    // on the user's behalf, so the registry check is the bin's alone.
    if (options.updateNotice) await maybePrintUpdateNotice(io, version, rawArgs);
    return 0;
  }
  const boundary = rawArgs.indexOf('--');
  const beforeBoundary = boundary < 0 ? rawArgs : rawArgs.slice(0, boundary);
  const separator = boundary >= 0 && parse(beforeBoundary).positional[0] === 'plugin' ? boundary : -1;
  const pluginArgs = separator < 0 ? [] : rawArgs.slice(separator + 1);
  const coreArgs = separator < 0 ? rawArgs : rawArgs.slice(0, separator);
  const args = parse(coreArgs.map((arg) => arg === '-h' ? '--help' : arg));
  const [command, subcommand, ...rest] = args.positional;
  const json = args.flags.has('json');

  // Handle help before command resolution to prevent unknown command errors
  if (args.flags.has('help') && command === undefined) {
    showHelp();
  }
  if (command === 'help' && subcommand === undefined && rest.length === 0) {
    showHelp();
  }
  if ((command === 'sessions' || command === 'delivery') && subcommand === undefined && args.flags.has('help')) {
    showHelp();
  }

  const spec = commandSpec(command, subcommand);
  if (!spec) usage(unknownCommandMessage(command, subcommand));

  if (args.flags.has('help') && spec.allowed.includes('help')) {
    showHelp();
  }

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
  const scope = scopeFlag(args);
  if (command === 'delivery') {
    return runDeliveryCommand(subcommand!, io, { dbPath: textFlag(args, 'db'), configPath: textFlag(args, 'config'),
      jobId: textFlag(args, 'job'), pollIntervalMs: numberFlag(args, 'poll-ms'), requestTimeoutMs: numberFlag(args, 'timeout-ms'),
      signal: options.signal });
  }
  if (command === 'export') {
    const selectionPath = textFlag(args, 'selection');
    if (!selectionPath) usage('export requires --selection FILE');
    await runHistoryExportCommand({ dbPath: textFlag(args, 'db'), selectionPath, outputPath: textFlag(args, 'out') },
      options.stdoutStream);
    return 0;
  }
  if (command === 'plugin') {
    const configPath = textFlag(args, 'config');
    if (!configPath) usage('plugin requires --config FILE');
    const { registry } = await loadHistoryApplicationConfig(configPath);
    const operation = registry.command(tail[0]);
    if (!operation) usage('configured plugin command not found');
    output(io, await operation.run([...tail.slice(1), ...pluginArgs]), true);
    return 0;
  }
  const acquisitionPlugins = ['sync','sessions'].includes(command ?? '') && textFlag(args,'config') ? (await loadHistoryApplicationConfig(textFlag(args,'config')!)).registry : undefined;
  let readiness: LocalStoreReadiness | null = null;
  if (spec.readsLocalStore) {
    readiness = await ensureLocalStore({
      dbPath: textFlag(args, 'db'), scope, bootstrap: !args.flags.has('no-bootstrap'),
    });
    if (readiness.bootstrap?.status === 'partial') {
      io.stderr('Some local sessions could not be fully indexed; run ai-hist --json for diagnostics.\n');
    }
    // The bare invocation reports the store's condition as its result and
    // succeeds either way. Every command that asks the store a question refuses
    // to answer out of one that cannot hold an answer.
    if (command !== undefined && !skipUnusableStoreGate(spec, scope) && reportUnusableStore(io, readiness, json)) return 1;
  }

  if (command === undefined) {
    // --no-bootstrap answers from the catalog as it stands; the bootstrap path
    // reports what it just indexed.
    if (!readiness?.bootstrap) {
      output(io, await listSessionCatalogPage({ dbPath: textFlag(args, 'db') }), json);
      return 0;
    }
    if (json) output(io, readiness.bootstrap, true);
    else {
      io.stdout(readiness.indexedPrompts > 0
        ? `Ready: ${readiness.indexedPrompts} indexed prompt(s). Search with: ai-hist search "your query"\n`
        : 'No searchable local sessions found. Start a coding-agent session, then run ai-hist again.\n');
    }
    return 0;
  }
  if (command === 'sessions' && subcommand === 'list') {
    const sources = textFlags(args, 'source');
    const page = await listSessionCatalogPage({
      dbPath: textFlag(args, 'db'), scope: scopeFlag(args), sources: sources.length ? sources as never : undefined,
      limit: numberFlag(args, 'limit'), beforeMs: numberFlag(args, 'before-ms'),
      after: catalogCursorFlag(args), projectKey: textFlag(args, 'project'),
    });
    if (args.flags.has('pretty')) {
      const color = options.color && process.env.NO_COLOR === undefined;
      io.stdout(page.sessions.length
        ? `${page.sessions.map((session) => formatSessionRow(session, { color })).join('\n')}\n`
        : 'No sessions in the catalog.\n');
      if (page.nextCursor) io.stdout(`more available: --after '${JSON.stringify(page.nextCursor)}'\n`);
    } else output(io, page, json);
    return 0;
  }
  if (command === 'sessions' && subcommand === 'discover') {
    const sources = textFlags(args, 'source');
    outputDiscovery(io, await discoverSessions({
      sourceConnectors: sourceConnectorFlags(args), acquisitionTimeoutMs: numberFlag(args, 'acquisition-timeout-ms'), plugins: acquisitionPlugins,
      dbPath: textFlag(args, 'db'), scope: scopeFlag(args), sources: sources.length ? sources as never : undefined,
      limit: numberFlag(args, 'limit'),
    }), json);
    return 0;
  }
  if (command === 'sessions' && subcommand === 'hydrate') {
    outputHydration(io, await hydrateSession({
      sourceConnectors: sourceConnectorFlags(args), acquisitionTimeoutMs: numberFlag(args, 'acquisition-timeout-ms'), plugins: acquisitionPlugins,
      source: sessionSource as never,
      sessionId: sessionId!,
      scope: scopeFlag(args),
      dbPath: textFlag(args, 'db'),
      includeRelated: !args.flags.has('no-related'),
    }), json);
    return 0;
  }
  if (command === 'sessions' && subcommand === 'relationships') {
    outputRelationships(io, await getSessionRelationships({
      source: sessionSource as never, sessionId: sessionId!, dbPath: textFlag(args, 'db'),
    }), json);
    return 0;
  }
  if (command === 'sessions' && subcommand === 'tree') {
    outputTree(io, await getSessionTree({
      source: sessionSource as never, sessionId: sessionId!, dbPath: textFlag(args, 'db'),
      maxDepth: numberFlag(args, 'max-depth'), maxNodes: numberFlag(args, 'max-nodes'),
    }), json);
    return 0;
  }
  if (command === 'sessions' && (subcommand === 'tools' || subcommand === 'edits')) {
    const name = `sessions ${subcommand}`;
    const options = {
      dbPath: textFlag(args, 'db'),
      limit: numberFlag(args, 'limit'),
      after: evidenceCursorFlag(args),
    };
    if (subcommand === 'tools') {
      outputToolCalls(io, await getSessionToolCallsPage(sessionSource as never, sessionId!, options), json);
    } else {
      outputFileEdits(io, await getSessionFileEditsPage(sessionSource as never, sessionId!, options), json);
    }
    return 0;
  }
  if (command === 'search') {
    output(io, await search([subcommand, ...rest].join(' '), { ...common(args), rawFts: args.flags.has('fts') }), json);
    return 0;
  }
  if (command === 'recent') {
    output(io, await recent({ ...common(args), limit: numberFlag(args, 'limit') ?? recentFallback }), json);
    return 0;
  }
  if (command === 'session') {
    output(io, await getSession(subcommand, { dbPath: textFlag(args, 'db'), source: textFlag(args, 'source') as never, tag: textFlag(args, 'tag') }), json);
    return 0;
  }
  if (command === 'events') {
    output(io, await getSessionEventsPage(subcommand, {
      dbPath: textFlag(args, 'db'), source: textFlag(args, 'source') as never,
      limit: numberFlag(args, 'limit'), after: cursorFlag(args),
    }), json);
    return 0;
  }
  if (command === 'resume') {
    return runResume(io, args, subcommand, rest, json);
  }
  if (command === 'pack') {
    return runPack(io, args, subcommand, rest, json);
  }
  if (command === 'stats') {
    output(io, await stats({ dbPath: textFlag(args, 'db'), scope: scopeFlag(args), tag: textFlag(args, 'tag'), byCwd: args.flags.has('by-cwd') || undefined }), json);
    return 0;
  }
  if (command === 'sync') {
    output(io, await sync({ dbPath: textFlag(args, 'db'), scope: scopeFlag(args), sourceConnectors: sourceConnectorFlags(args), acquisitionTimeoutMs: numberFlag(args, 'acquisition-timeout-ms'), plugins: acquisitionPlugins }), json);
    return 0;
  }
  usage();
}

/** Knobs the bin owns and a mounted host does not. */
export interface RunCliOptions {
  /**
   * Cancels a long-running `delivery drain`/`delivery run`.
   *
   * The signal handlers that produce it belong to whoever owns the process:
   * the bin installs them, a host passes its own, and `dispatch` installs none.
   */
  signal?: AbortSignal;
  /** Check npm for a newer release on `--version`. The bin only. */
  updateNotice?: boolean;
  /** Colourize `sessions list --pretty`. Defaults to stdout being a TTY. */
  color?: boolean;
  /**
   * Where `export` writes when no `--out` is given.
   *
   * NDJSON export is unbounded, so it needs a real stream with backpressure
   * rather than the unbuffered `io.stdout` callback.
   */
  stdoutStream?: Writable;
}

/**
 * Run one `ai-hist` command line and resolve to its exit code.
 *
 * Never calls `process.exit`, never writes to `process.stdout`/`process.stderr`
 * and never installs a signal handler: the caller owns all three. `argv` is the
 * arguments after the program name.
 */
export async function runCli(argv: readonly string[], io: CliIo, options: RunCliOptions = {}): Promise<number> {
  try {
    return await dispatch(argv, io, options);
  } catch (error: unknown) {
    if (error instanceof CliExit) {
      if (error.stdout) io.stdout(error.stdout);
      if (error.stderr) io.stderr(error.stderr);
      return error.exitCode;
    }
    const value = error as { code?: string; message?: string };
    io.stderr(`ai-hist: ${value.code ? `${value.code}: ` : ''}${value.message ?? String(error)}\n`);
    return 1;
  }
}

/**
 * The `ai-hist` binary: the only place that owns process state.
 *
 * Signal handling lives here rather than in the delivery command so that
 * `runCli` stays free of global handlers for hosts that mount it. It is also
 * claimed only for the commands that read it: every other invocation keeps
 * Node's default `SIGINT`/`SIGTERM` behaviour, so Ctrl-C ends it the first
 * time it is pressed.
 */
async function main(): Promise<void> {
  const io: CliIo = {
    stdout: (chunk) => void process.stdout.write(chunk),
    stderr: (chunk) => void process.stderr.write(chunk),
  };
  const argv = process.argv.slice(2);
  const cancellable = usesCancellation(argv);
  const abort = new AbortController();
  const stop = (): void => abort.abort();
  if (cancellable) {
    process.once('SIGINT', stop);
    process.once('SIGTERM', stop);
  }
  try {
    process.exitCode = await runCli(argv, io, {
      signal: cancellable ? abort.signal : undefined,
      updateNotice: true,
      color: Boolean(process.stdout.isTTY),
      stdoutStream: process.stdout,
    });
  } finally {
    if (cancellable) {
      process.removeListener('SIGINT', stop);
      process.removeListener('SIGTERM', stop);
    }
  }
}

if (isBinEntrypoint()) void main();
