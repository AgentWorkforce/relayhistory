/**
 * TypeScript SDK for reading the ai-hist SQLite database.
 *
 * Backed by sql.js (WASM SQLite) so the package has zero native build
 * requirements — works in Electron, Node, and browser contexts without
 * needing electron-rebuild. The SDK reads the same file the Python CLI
 * maintains (default `~/.local/share/ai-hist/ai-history.db`, or
 * `$AI_HIST_DB`); the Python tool stays the canonical sync engine.
 *
 * Trade-off vs better-sqlite3: sql.js loads the whole DB file into
 * memory. Fine for the ai-hist scale (tens of thousands of rows, MBs
 * of data); revisit if anyone hits millions.
 */

import initSqlJs, { type Database, type SqlJsStatic } from 'sql.js';
import { readFile, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { scanLocalSources, LOCAL_SOURCE_PATHS } from './jsonl-sources.js';

export type Source = 'claude' | 'codex' | 'cursor' | 'relay';

export interface HistoryEntry {
  id: number;
  source: Source;
  sessionId: string | null;
  project: string | null;
  prompt: string;
  timestampMs: number;
}

export interface SessionSummary {
  sessionId: string;
  source: Source;
  project: string | null;
  firstPrompt: string;
  lastActivityMs: number;
  firstActivityMs: number;
  promptCount: number;
}

export interface ListOptions {
  source?: Source;
  project?: string;
  /** Default 50. */
  limit?: number;
  /**
   * Paginate older than this timestamp (exclusive). Use the
   * `lastActivityMs` / `timestampMs` of the last item from the previous page.
   */
  beforeMs?: number;
}

export type SearchOptions = ListOptions;

export interface Stats {
  total: number;
  bySource: Partial<Record<Source, number>>;
  byProject: Array<{ project: string; count: number }>;
  firstTimestampMs: number | null;
  lastTimestampMs: number | null;
}

/** Resolve the SQLite path the Python CLI writes to. */
export function defaultDbPath(): string {
  const fromEnv = process.env.AI_HIST_DB;
  if (fromEnv && fromEnv.trim().length > 0) return fromEnv;
  return join(homedir(), '.local', 'share', 'ai-hist', 'ai-history.db');
}

let _sqlPromise: Promise<SqlJsStatic> | null = null;
function getSqlJs(): Promise<SqlJsStatic> {
  if (!_sqlPromise) {
    _sqlPromise = initSqlJs();
  }
  return _sqlPromise;
}

export interface OpenOptions {
  /** Override the SQLite path (default: `$AI_HIST_DB` or `~/.local/share/ai-hist/ai-history.db`). */
  dbPath?: string;
  /**
   * What to do when the SQLite DB is missing:
   *   - `'jsonl'` (default): scan local Claude/Codex/Cursor history files
   *     directly into an in-memory SQLite — works without the Python
   *     `ai-hist sync` tool installed.
   *   - `'error'`: throw with an install hint (legacy 0.1.x behavior).
   */
  fallback?: 'jsonl' | 'error';
}

export interface OpenSourceInfo {
  /** `'sqlite'` when the on-disk DB was used, `'jsonl'` when the fallback scan was. */
  kind: 'sqlite' | 'jsonl';
  /** SQLite DB path or, in jsonl mode, the paths that were scanned. */
  path: string;
}

/**
 * Open an `AiHist` reader. Async because sql.js initializes its WASM
 * runtime lazily and the DB file is read asynchronously so the host
 * process's event loop isn't blocked.
 *
 * Each call snapshots the data; to pick up later writes, call `reload()`
 * (or open a fresh instance).
 */
export async function openAiHist(opts: OpenOptions = {}): Promise<AiHist> {
  const dbPath = opts.dbPath ?? defaultDbPath();
  const fallback = opts.fallback ?? 'jsonl';

  const SQL = await getSqlJs();

  // Fast path: SQLite written by `ai-hist sync`.
  if (await pathExists(dbPath)) {
    const fileBuffer = await readFile(dbPath);
    const db = new SQL.Database(fileBuffer);
    return new AiHist(db, { kind: 'sqlite', path: dbPath });
  }

  if (fallback === 'error') {
    throw new Error(
      `ai-hist database not found at ${dbPath}. Run \`ai-hist sync\` first ` +
        `(see https://github.com/AgentWorkforce/ai-hist).`,
    );
  }

  // Fallback: scan local source files (Claude/Codex/Cursor) directly.
  // No Python dependency; uses the same parsers documented in the Python
  // CLI's source. Yields control to the event loop between sources so a
  // large local history doesn't freeze the host.
  const db = new SQL.Database();
  db.run(`CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    UNIQUE(source, timestamp_ms, prompt)
  )`);
  db.run('CREATE INDEX idx_history_timestamp ON history (timestamp_ms DESC)');
  db.run('CREATE INDEX idx_history_session ON history (session_id)');

  // scanLocalSources is async with yields between sources so the event
  // loop stays responsive while we scan many MB of JSONL.
  const rows = await scanLocalSources();

  const insert = db.prepare(
    'INSERT OR IGNORE INTO history (source, session_id, project, prompt, timestamp_ms) VALUES (?, ?, ?, ?, ?)',
  );
  try {
    db.exec('BEGIN');
    for (const row of rows) {
      insert.run([row.source, row.sessionId, row.project, row.prompt, row.timestampMs]);
    }
    db.exec('COMMIT');
  } finally {
    insert.free();
  }
  const scannedPaths = `${LOCAL_SOURCE_PATHS.claude}, ${LOCAL_SOURCE_PATHS.codex}, ${LOCAL_SOURCE_PATHS.cursorRoot}`;
  return new AiHist(db, { kind: 'jsonl', path: scannedPaths });
}

async function pathExists(p: string): Promise<boolean> {
  try {
    await stat(p);
    return true;
  } catch {
    return false;
  }
}

interface RawHistoryRow {
  id: number;
  source: string;
  session_id: string | null;
  project: string | null;
  prompt: string;
  timestamp_ms: number;
}

function rowToEntry(row: RawHistoryRow): HistoryEntry {
  return {
    id: row.id,
    source: row.source as Source,
    sessionId: row.session_id,
    project: row.project,
    prompt: row.prompt,
    timestampMs: row.timestamp_ms,
  };
}

function buildFilters(opts: ListOptions): { sql: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];
  if (opts.source) {
    clauses.push('source = ?');
    params.push(opts.source);
  }
  if (opts.project) {
    clauses.push('project = ?');
    params.push(opts.project);
  }
  if (typeof opts.beforeMs === 'number') {
    clauses.push('timestamp_ms < ?');
    params.push(opts.beforeMs);
  }
  return {
    sql: clauses.length > 0 ? ` AND ${clauses.join(' AND ')}` : '',
    params,
  };
}

function runQuery<T>(db: Database, sql: string, params: unknown[]): T[] {
  const stmt = db.prepare(sql);
  try {
    stmt.bind(params as never[]);
    const rows: T[] = [];
    while (stmt.step()) {
      rows.push(stmt.getAsObject() as T);
    }
    return rows;
  } finally {
    stmt.free();
  }
}

export class AiHist {
  private readonly db: Database;
  private readonly _source: OpenSourceInfo;
  private closed = false;

  /** @internal — use `openAiHist(...)` to construct. */
  constructor(db: Database, source: OpenSourceInfo) {
    this.db = db;
    this._source = source;
  }

  /**
   * Path the data came from. SQLite mode: the .db path. JSONL fallback
   * mode: a comma-separated list of the scanned source paths.
   */
  get dbPath(): string {
    return this._source.path;
  }

  /** Which data path was used: `'sqlite'` (Python tool) or `'jsonl'` (fallback). */
  get sourceKind(): 'sqlite' | 'jsonl' {
    return this._source.kind;
  }

  close(): void {
    if (this.closed) return;
    this.db.close();
    this.closed = true;
  }

  /** Most recent prompts, newest first. */
  recent(opts: ListOptions = {}): HistoryEntry[] {
    const limit = opts.limit ?? 50;
    const { sql, params } = buildFilters(opts);
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms
       FROM history
       WHERE 1=1${sql}
       ORDER BY timestamp_ms DESC
       LIMIT ?`,
      [...params, limit],
    ).map(rowToEntry);
  }

  /**
   * Group history into sessions, ordered by last activity (newest first).
   * Sessions without a `session_id` are skipped.
   */
  listSessions(opts: ListOptions = {}): SessionSummary[] {
    const limit = opts.limit ?? 50;
    const { sql, params } = buildFilters(opts);
    const rows = runQuery<{
      session_id: string;
      source: string;
      project: string | null;
      first_prompt: string;
      first_activity_ms: number;
      last_activity_ms: number;
      prompt_count: number;
    }>(
      this.db,
      `WITH filtered AS (
        SELECT id, source, session_id, project, prompt, timestamp_ms
        FROM history
        WHERE session_id IS NOT NULL AND session_id != ''${sql}
      )
      SELECT
        session_id,
        source,
        project,
        prompt_count,
        first_activity_ms,
        last_activity_ms,
        (SELECT prompt FROM filtered f2
         WHERE f2.session_id = grouped.session_id
         ORDER BY f2.timestamp_ms ASC LIMIT 1) AS first_prompt
      FROM (
        SELECT
          session_id,
          source,
          project,
          COUNT(*) AS prompt_count,
          MIN(timestamp_ms) AS first_activity_ms,
          MAX(timestamp_ms) AS last_activity_ms
        FROM filtered
        GROUP BY session_id, source, project
      ) AS grouped
      ORDER BY last_activity_ms DESC
      LIMIT ?`,
      [...params, limit],
    );
    return rows.map((row) => ({
      sessionId: row.session_id,
      source: row.source as Source,
      project: row.project,
      firstPrompt: row.first_prompt,
      lastActivityMs: row.last_activity_ms,
      firstActivityMs: row.first_activity_ms,
      promptCount: row.prompt_count,
    }));
  }

  /** All prompts in a session, ordered oldest → newest. */
  getSession(sessionId: string): HistoryEntry[] {
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms
       FROM history
       WHERE session_id = ?
       ORDER BY timestamp_ms ASC`,
      [sessionId],
    ).map(rowToEntry);
  }

  /**
   * Substring search across prompt + project, case-insensitive, recent
   * matches first. The Python CLI uses FTS5; this SDK uses LIKE because
   * sql.js's default WASM build doesn't ship the FTS5 module. Plenty fast
   * for the ai-hist scale (~tens of thousands of rows); revisit if a
   * future consumer needs phrase/boolean queries.
   *
   * The query is matched literally — `%` and `_` are escaped so users can
   * search for them. Empty queries return an empty array.
   */
  search(query: string, opts: SearchOptions = {}): HistoryEntry[] {
    const trimmed = query.trim();
    if (!trimmed) return [];
    const limit = opts.limit ?? 50;
    const escaped = trimmed.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
    const pattern = `%${escaped}%`;
    const clauses: string[] = [
      `(LOWER(prompt) LIKE LOWER(?) ESCAPE '\\' OR LOWER(COALESCE(project, '')) LIKE LOWER(?) ESCAPE '\\')`,
    ];
    const params: unknown[] = [pattern, pattern];
    if (opts.source) {
      clauses.push('source = ?');
      params.push(opts.source);
    }
    if (opts.project) {
      clauses.push('project = ?');
      params.push(opts.project);
    }
    if (typeof opts.beforeMs === 'number') {
      clauses.push('timestamp_ms < ?');
      params.push(opts.beforeMs);
    }
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms
       FROM history
       WHERE ${clauses.join(' AND ')}
       ORDER BY timestamp_ms DESC
       LIMIT ?`,
      [...params, limit],
    ).map(rowToEntry);
  }

  /** Counts + date range, mirroring `ai-hist stats`. */
  stats(): Stats {
    const total =
      runQuery<{ c: number }>(this.db, 'SELECT COUNT(*) AS c FROM history', [])[0]?.c ?? 0;
    const bySourceRows = runQuery<{ source: string; c: number }>(
      this.db,
      'SELECT source, COUNT(*) AS c FROM history GROUP BY source',
      [],
    );
    const bySource: Partial<Record<Source, number>> = {};
    for (const row of bySourceRows) {
      bySource[row.source as Source] = row.c;
    }
    const byProject = runQuery<{ project: string; c: number }>(
      this.db,
      `SELECT project, COUNT(*) AS c FROM history
       WHERE project IS NOT NULL AND project != ''
       GROUP BY project ORDER BY c DESC LIMIT 10`,
      [],
    ).map((row) => ({ project: row.project, count: row.c }));
    const range = runQuery<{ mn: number | null; mx: number | null }>(
      this.db,
      'SELECT MIN(timestamp_ms) AS mn, MAX(timestamp_ms) AS mx FROM history',
      [],
    )[0];
    return {
      total,
      bySource,
      byProject,
      firstTimestampMs: range?.mn ?? null,
      lastTimestampMs: range?.mx ?? null,
    };
  }
}

/**
 * Resume command for an entry/session, matching what `ai-hist show` prints.
 * Returns `null` for sources that don't have a resume affordance (relay).
 */
export function resumeCommand(
  entry: Pick<HistoryEntry, 'source' | 'sessionId' | 'project'>,
): string | null {
  if (!entry.sessionId) return null;
  switch (entry.source) {
    case 'claude':
      return entry.project
        ? `cd ${shellQuote(entry.project)} && claude --resume ${shellQuote(entry.sessionId)}`
        : `claude --resume ${shellQuote(entry.sessionId)}`;
    case 'codex':
      return `codex resume ${shellQuote(entry.sessionId)}`;
    case 'cursor':
      return entry.project
        ? `cd ${shellQuote(entry.project)} && cursor-agent --resume=${shellQuote(entry.sessionId)}`
        : `cursor-agent --resume=${shellQuote(entry.sessionId)}`;
    case 'relay':
      return null;
    default:
      return null;
  }
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_\-./]+$/.test(value)) return value;
  return `'${value.replace(/'/g, `'\\''`)}'`;
}
