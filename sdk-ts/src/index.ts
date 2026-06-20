/**
 * TypeScript SDK for reading the ai-hist SQLite database.
 *
 * Backed by sql.js (WASM SQLite) so the package has zero native build
 * requirements — works in Electron, Node, and browser contexts without
 * needing electron-rebuild. The SDK reads the same file the Python CLI
 * maintains (default `~/.local/share/ai-hist/ai-history.db`, or
 * `$AI_HIST_DB`); during the Rust cutover, full sync still routes through the
 * legacy Python fallback.
 *
 * Trade-off vs better-sqlite3: sql.js loads the whole DB file into
 * memory. Fine for the ai-hist scale (tens of thousands of rows, MBs
 * of data); revisit if anyone hits millions.
 */

import initSqlJs, { type Database, type SqlJsStatic } from 'sql.js';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { writeFileSync } from 'node:fs';
import { execFile } from 'node:child_process';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { promisify } from 'node:util';
import { scanLocalSources, LOCAL_SOURCE_PATHS } from './jsonl-sources.js';
import {
  scanLocalTrajectories,
  trajectoryRootDescription,
  type TrajectoryDecision,
  type TrajectoryRetrospective,
} from './trajectory-sources.js';

const execFileAsync = promisify(execFile);

export type Source = 'claude' | 'codex' | 'cursor' | 'relay' | 'trajectory' | 'opencode';

export interface HistoryEntry {
  id: number;
  source: Source;
  sessionId: string | null;
  project: string | null;
  prompt: string;
  timestampMs: number;
  gitBranch: string | null;
}

export interface SessionMeta {
  sessionId: string;
  source: Source;
  cwd: string | null;
  gitBranch: string | null;
  firstActivityMs: number | null;
  lastActivityMs: number | null;
  lastAssistantText: string | null;
  rawPath: string | null;
}

export interface HandoffCandidate {
  sessionId: string;
  source: Source;
  cwd: string | null;
  gitBranch: string | null;
  firstActivityMs: number | null;
  lastActivityMs: number | null;
  promptCount: number;
  goal: string;
  lastState: string;
  lastAssistantText: string | null;
  filesTouched: string[];
  resumeCommand: string | null;
  warmStartCommand: string;
  confidence: number;
}

export interface Tag {
  name: string;
  displayName: string;
  color: string | null;
  sessionCount: number;
  firstTaggedMs: number | null;
  lastTaggedMs: number | null;
}

export interface TaggedSession {
  source: Source;
  sessionId: string;
  project: string | null;
  entryCount: number;
  lastActivityMs: number | null;
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
  tag?: string;
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

export interface TrajectoryEntry {
  id: string;
  version: number | null;
  personaId: string | null;
  projectId: string | null;
  task: {
    title: string | null;
    description: string | null;
  };
  status: string | null;
  startedAt: string | null;
  completedAt: string | null;
  decisions: TrajectoryDecision[];
  retrospective: TrajectoryRetrospective;
  searchText: string;
  path: string | null;
  updatedMs: number;
  timestampMs: number;
}

export interface TrajectorySearchOptions {
  /** Default 20. */
  limit?: number;
  /** Filter to a trajectory projectId or path. */
  project?: string;
}

export interface GetHandoffOptions {
  /** Substring match against session cwd. */
  repo?: string;
  /** Substring match against git_branch. */
  branch?: string;
  /** Filter to a specific CLI source. */
  source?: 'claude' | 'codex';
  /** Max candidates to return. Default 3. */
  limit?: number;
}

/** Resolve the SQLite path the Python CLI writes to. */
export function defaultDbPath(): string {
  const fromEnv = process.env.AI_HIST_DB;
  if (fromEnv && fromEnv.trim().length > 0) return fromEnv;
  return join(homedir(), '.local', 'share', 'ai-hist', 'ai-history.db');
}

function defaultOpenCodeDbPath(): string {
  return process.env.OPENCODE_DB && process.env.OPENCODE_DB.trim().length > 0
    ? process.env.OPENCODE_DB
    : join(homedir(), '.local', 'share', 'opencode', 'opencode.db');
}

let _sqlPromise: Promise<SqlJsStatic> | null = null;
function getSqlJs(): Promise<SqlJsStatic> {
  if (!_sqlPromise) {
    _sqlPromise = initSqlJs();
  }
  return _sqlPromise;
}

function ensureSessionsSchema(db: Database): void {
  db.run(`CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT NOT NULL,
    source TEXT NOT NULL,
    cwd TEXT,
    git_branch TEXT,
    first_activity_ms INTEGER,
    last_activity_ms INTEGER,
    last_assistant_text TEXT,
    raw_path TEXT,
    parser_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (session_id, source)
  )`);
  db.run('CREATE INDEX IF NOT EXISTS idx_sessions_cwd ON sessions(cwd)');
  db.run('CREATE INDEX IF NOT EXISTS idx_sessions_branch ON sessions(git_branch)');
  db.run('CREATE INDEX IF NOT EXISTS idx_sessions_last ON sessions(last_activity_ms DESC)');
}

function ensureTrajectorySchema(db: Database): void {
  db.run(`CREATE TABLE IF NOT EXISTS trajectories (
    id TEXT PRIMARY KEY,
    version INTEGER,
    persona_id TEXT,
    project_id TEXT,
    task_title TEXT,
    task_description TEXT,
    status TEXT,
    started_at TEXT,
    completed_at TEXT,
    decisions_json TEXT NOT NULL,
    retrospective_json TEXT NOT NULL,
    search_text TEXT NOT NULL,
    path TEXT,
    updated_ms INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL
  )`);
  db.run('CREATE INDEX IF NOT EXISTS idx_trajectories_timestamp ON trajectories(timestamp_ms DESC)');
  db.run('CREATE INDEX IF NOT EXISTS idx_trajectories_project ON trajectories(project_id)');
}

function ensureTagSchema(db: Database): void {
  db.run(`CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    color TEXT,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
  )`);
  db.run(`CREATE TABLE IF NOT EXISTS session_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    created_ms INTEGER NOT NULL,
    UNIQUE(source, session_id, tag_id)
  )`);
  db.run('CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name)');
  db.run('CREATE INDEX IF NOT EXISTS idx_session_tags_session ON session_tags(source, session_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_session_tags_tag ON session_tags(tag_id)');
}

export interface OpenOptions {
  /** Override the SQLite path (default: `$AI_HIST_DB` or `~/.local/share/ai-hist/ai-history.db`). */
  dbPath?: string;
  /**
   * Restrict all reads to one project directory/path. Exact matches and
   * child paths are included, so a scope of `/repo` also includes `/repo/pkg`.
   */
  projectScope?: string;
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
    ensureTrajectorySchema(db);
    ensureTagSchema(db);
    ensureSessionsSchema(db);
    // Add git_branch to history if missing (pre-handoff DBs lack this column).
    try { db.run('ALTER TABLE history ADD COLUMN git_branch TEXT'); } catch { /* already exists */ }
    // The Python CLI's schema doesn't create `idx_history_session` or
    // `idx_history_timestamp`. Without them, listSessions degrades to
    // an O(sessions × rows) full table scan and freezes the WASM
    // single-threaded JS engine for tens of seconds on real-sized DBs.
    // The DB is opened read-only over a buffer, but sql.js still lets
    // us run `CREATE INDEX` against the in-memory copy — the index
    // lives only for this session's lifetime and is rebuilt each
    // `openAiHist` call. Fast (~30ms on 35K rows).
    db.run('CREATE INDEX IF NOT EXISTS idx_history_session ON history(session_id)');
    db.run('CREATE INDEX IF NOT EXISTS idx_history_timestamp ON history(timestamp_ms DESC)');
    return new AiHist(db, { kind: 'sqlite', path: dbPath }, { projectScope: opts.projectScope });
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
    git_branch TEXT,
    UNIQUE(source, timestamp_ms, prompt)
  )`);
  db.run('CREATE INDEX idx_history_timestamp ON history (timestamp_ms DESC)');
  db.run('CREATE INDEX idx_history_session ON history (session_id)');
  ensureTrajectorySchema(db);
  ensureTagSchema(db);
  ensureSessionsSchema(db);

  // scanLocalSources is async with yields between sources so the event
  // loop stays responsive while we scan many MB of JSONL.
  const rows = await scanLocalSources();
  const openCodeRows = await scanOpenCode(SQL);
  const trajectories = await scanLocalTrajectories();

  const insert = db.prepare(
    'INSERT OR IGNORE INTO history (source, session_id, project, prompt, timestamp_ms, git_branch) VALUES (?, ?, ?, ?, ?, ?)',
  );
  const insertTrajectory = db.prepare(
    `INSERT OR REPLACE INTO trajectories
     (id, version, persona_id, project_id, task_title, task_description, status,
      started_at, completed_at, decisions_json, retrospective_json, search_text,
      path, updated_ms, timestamp_ms)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  try {
    db.exec('BEGIN');
    for (const row of rows) {
      insert.run([row.source, row.sessionId, row.project, row.prompt, row.timestampMs, row.gitBranch]);
    }
    for (const row of openCodeRows) {
      insert.run(['opencode', row.sessionId, row.project, row.prompt, row.timestampMs, null]);
    }
    for (const trajectory of trajectories) {
      insertTrajectory.run([
        trajectory.id,
        trajectory.version,
        trajectory.personaId,
        trajectory.projectId,
        trajectory.task.title,
        trajectory.task.description,
        trajectory.status,
        trajectory.startedAt,
        trajectory.completedAt,
        JSON.stringify(trajectory.decisions),
        JSON.stringify(trajectory.retrospective),
        trajectory.searchText,
        trajectory.path,
        trajectory.updatedMs,
        trajectory.timestampMs,
      ]);
      insert.run([
        'trajectory',
        trajectory.id,
        trajectory.projectId,
        trajectory.searchText,
        trajectory.timestampMs,
        null,
      ]);
    }
    db.exec('COMMIT');
  } finally {
    insert.free();
    insertTrajectory.free();
  }
  const scannedPaths = `${LOCAL_SOURCE_PATHS.claude}, ${LOCAL_SOURCE_PATHS.codex}, ${LOCAL_SOURCE_PATHS.cursorRoot}, ${trajectoryRootDescription()}`;
  return new AiHist(db, { kind: 'jsonl', path: scannedPaths }, { projectScope: opts.projectScope });
}

async function pathExists(p: string): Promise<boolean> {
  try {
    await stat(p);
    return true;
  } catch {
    return false;
  }
}

async function readSqliteSnapshot(dbPath: string): Promise<Buffer> {
  const hasWal = await pathExists(`${dbPath}-wal`);
  const hasShm = await pathExists(`${dbPath}-shm`);
  if (!hasWal && !hasShm) {
    return readFile(dbPath);
  }

  const dir = await mkdtemp(join(tmpdir(), 'ai-hist-sqlite-snapshot-'));
  const snapshot = join(dir, 'snapshot.db');
  try {
    await execFileAsync('sqlite3', [dbPath, `.backup '${snapshot.replace(/'/g, "''")}'`], {
      timeout: 30_000,
      maxBuffer: 1024 * 1024,
    });
    return await readFile(snapshot);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

type ScannedOpenCodeRow = {
  sessionId: string | null;
  project: string | null;
  prompt: string;
  timestampMs: number;
};

async function scanOpenCode(SQL: SqlJsStatic): Promise<ScannedOpenCodeRow[]> {
  const dbPath = defaultOpenCodeDbPath();
  if (!(await pathExists(dbPath))) return [];
  try {
    const fileBuffer = await readSqliteSnapshot(dbPath);
    const db = new SQL.Database(fileBuffer);
    try {
      const rows = runQuery<{
        session_id: string;
        project: string | null;
        data: string;
        timestamp_ms: number;
      }>(
        db,
        `SELECT s.id AS session_id, s.directory AS project, p.data,
                COALESCE(p.time_created, m.time_created, s.time_created) AS timestamp_ms
         FROM part p
         JOIN message m ON m.id = p.message_id
         JOIN session s ON s.id = p.session_id
         WHERE json_extract(m.data, '$.role') = 'user'
           AND json_extract(p.data, '$.type') = 'text'
         ORDER BY p.time_created ASC`,
        [],
      );
      const scanned: ScannedOpenCodeRow[] = [];
      for (const row of rows) {
        const data = parseJson<{ type?: string; text?: unknown }>(row.data, {});
        const prompt = typeof data.text === 'string' ? data.text.trim() : '';
        if (!prompt) continue;
        scanned.push({
          sessionId: row.session_id,
          project: row.project,
          prompt,
          timestampMs: row.timestamp_ms ?? 0,
        });
      }
      return scanned;
    } finally {
      db.close();
    }
  } catch {
    return [];
  }
}

interface RawHistoryRow {
  id: number;
  source: string;
  session_id: string | null;
  project: string | null;
  prompt: string;
  timestamp_ms: number;
  git_branch: string | null;
}

interface RawSessionRow {
  session_id: string;
  source: string;
  cwd: string | null;
  git_branch: string | null;
  first_activity_ms: number | null;
  last_activity_ms: number | null;
  last_assistant_text: string | null;
  raw_path: string | null;
}

interface RawTrajectoryRow {
  id: string;
  version: number | null;
  persona_id: string | null;
  project_id: string | null;
  task_title: string | null;
  task_description: string | null;
  status: string | null;
  started_at: string | null;
  completed_at: string | null;
  decisions_json: string;
  retrospective_json: string;
  search_text: string;
  path: string | null;
  updated_ms: number;
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
    gitBranch: row.git_branch ?? null,
  };
}

function parseJson<T>(raw: string, fallback: T): T {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function rowToTrajectory(row: RawTrajectoryRow): TrajectoryEntry {
  return {
    id: row.id,
    version: row.version,
    personaId: row.persona_id,
    projectId: row.project_id,
    task: {
      title: row.task_title,
      description: row.task_description,
    },
    status: row.status,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    decisions: parseJson<TrajectoryDecision[]>(row.decisions_json, []),
    retrospective: parseJson<TrajectoryRetrospective>(row.retrospective_json, {
      summary: null,
      approach: null,
      learnings: [],
      confidence: null,
    }),
    searchText: row.search_text,
    path: row.path,
    updatedMs: row.updated_ms,
    timestampMs: row.timestamp_ms,
  };
}

function normalizeProjectScope(project: string | undefined): string | undefined {
  const trimmed = project?.trim();
  if (!trimmed) return undefined;
  return trimmed.replace(/[\\/]+$/, '') || trimmed;
}

function escapeLike(value: string): string {
  return value.replace(/\|/g, '||').replace(/%/g, '|%').replace(/_/g, '|_');
}

function scopedPathClause(column: string, project: string): { sql: string; params: unknown[] } {
  const normalized = normalizeProjectScope(project) ?? project;
  const escaped = escapeLike(normalized);
  const slashChildPattern = normalized === '/' ? '/%' : `${escaped}/%`;
  const backslashChildPattern = normalized === '\\' ? '\\%' : `${escaped}\\%`;
  return {
    sql: `(${column} = ? OR ${column} LIKE ? ESCAPE '|' OR ${column} LIKE ? ESCAPE '|')`,
    params: [normalized, slashChildPattern, backslashChildPattern],
  };
}

function scopedTrajectoryClause(project: string): { sql: string; params: unknown[] } {
  const normalized = normalizeProjectScope(project) ?? project;
  const pathScope = scopedPathClause('path', normalized);
  return {
    sql: `(project_id = ? OR ${pathScope.sql})`,
    params: [normalized, ...pathScope.params],
  };
}

function appendProjectFilter(
  clauses: string[],
  params: unknown[],
  project: string | undefined,
  projectScope: string | undefined,
): void {
  if (projectScope) {
    const scope = scopedPathClause('project', projectScope);
    clauses.push(scope.sql);
    params.push(...scope.params);
  }
  if (project) {
    clauses.push('project = ?');
    params.push(project);
  }
}

function normalizeTagName(tag: string): string {
  return tag.trim().toLowerCase().replace(/\s+/g, ' ');
}

function appendTagFilter(clauses: string[], params: unknown[], tag: string | undefined, alias = 'history'): void {
  const normalized = tag ? normalizeTagName(tag) : '';
  if (!normalized) return;
  clauses.push(
    `EXISTS (
      SELECT 1 FROM session_tags st
      JOIN tags t ON t.id = st.tag_id
      WHERE st.source = ${alias}.source
        AND st.session_id = ${alias}.session_id
        AND t.name = ?
    )`,
  );
  params.push(normalized);
}

function buildFilters(opts: ListOptions, projectScope?: string): { sql: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];
  if (opts.source) {
    clauses.push('source = ?');
    params.push(opts.source);
  }
  appendProjectFilter(clauses, params, opts.project, projectScope);
  appendTagFilter(clauses, params, opts.tag, 'history');
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
  private readonly _projectScope: string | undefined;
  private closed = false;

  /** @internal — use `openAiHist(...)` to construct. */
  constructor(db: Database, source: OpenSourceInfo, opts: Pick<OpenOptions, 'projectScope'> = {}) {
    this.db = db;
    this._source = source;
    this._projectScope = normalizeProjectScope(opts.projectScope);
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

  /** Server/client-wide project scope applied to every read, if configured. */
  get projectScope(): string | undefined {
    return this._projectScope;
  }

  close(): void {
    if (this.closed) return;
    this.db.close();
    this.closed = true;
  }

  private persistIfWritable(): void {
    if (this._source.kind !== 'sqlite') return;
    writeFileSync(this._source.path, Buffer.from(this.db.export()));
  }

  private ensureTag(name: string, color?: string | null): number {
    const normalized = normalizeTagName(name);
    if (!normalized) throw new Error('tag name cannot be empty');
    const displayName = name.trim();
    const now = Date.now();
    this.db.run(
      `INSERT INTO tags (name, display_name, color, created_ms, updated_ms)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(name) DO UPDATE SET
         display_name = excluded.display_name,
         color = COALESCE(excluded.color, tags.color),
         updated_ms = excluded.updated_ms`,
      [normalized, displayName, color ?? null, now, now],
    );
    return runQuery<{ id: number }>(this.db, 'SELECT id FROM tags WHERE name = ?', [normalized])[0].id;
  }

  private matchingSessions(sessionId: string, source?: Source): TaggedSession[] {
    const clauses = ['session_id = ?'];
    const params: unknown[] = [sessionId];
    if (source) {
      clauses.push('source = ?');
      params.push(source);
    }
    appendProjectFilter(clauses, params, undefined, this._projectScope);
    return runQuery<{
      source: string;
      session_id: string;
      project: string | null;
      entry_count: number;
      last_activity_ms: number | null;
    }>(
      this.db,
      `SELECT source, session_id, MIN(project) AS project, COUNT(*) AS entry_count,
              MAX(timestamp_ms) AS last_activity_ms
       FROM history
       WHERE ${clauses.join(' AND ')}
       GROUP BY source, session_id
       ORDER BY source`,
      params,
    ).map((row) => ({
      source: row.source as Source,
      sessionId: row.session_id,
      project: row.project,
      entryCount: row.entry_count,
      lastActivityMs: row.last_activity_ms,
    }));
  }

  tagSession(sessionId: string, tagName: string, opts: { source?: Source; color?: string | null } = {}): TaggedSession[] {
    const sessions = this.matchingSessions(sessionId, opts.source);
    if (sessions.length === 0) return [];
    const tagId = this.ensureTag(tagName, opts.color);
    const now = Date.now();
    const insert = this.db.prepare(
      'INSERT OR IGNORE INTO session_tags (source, session_id, tag_id, created_ms) VALUES (?, ?, ?, ?)',
    );
    try {
      for (const session of sessions) {
        insert.run([session.source, session.sessionId, tagId, now]);
      }
    } finally {
      insert.free();
    }
    this.persistIfWritable();
    return sessions;
  }

  untagSession(sessionId: string, tagName: string, opts: { source?: Source } = {}): number {
    const normalized = normalizeTagName(tagName);
    const sessions = this.matchingSessions(sessionId, opts.source);
    let removed = 0;
    for (const session of sessions) {
      this.db.run(
        `DELETE FROM session_tags
         WHERE source = ? AND session_id = ?
           AND tag_id IN (SELECT id FROM tags WHERE name = ?)`,
        [session.source, session.sessionId, normalized],
      );
      removed += this.db.getRowsModified();
    }
    this.persistIfWritable();
    return removed;
  }

  listTags(opts: { tag?: string; includeSessions?: boolean } = {}): Array<Tag & { sessions?: TaggedSession[] }> {
    const clauses: string[] = [];
    const params: unknown[] = [];
    if (opts.tag) {
      clauses.push('t.name = ?');
      params.push(normalizeTagName(opts.tag));
    }
    const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
    let scopedSessionSql = 'SELECT st.tag_id, st.id, st.created_ms FROM session_tags st';
    const scopedSessionParams: unknown[] = [];
    if (this._projectScope) {
      const scope = scopedPathClause('h.project', this._projectScope);
      scopedSessionSql = `SELECT st.tag_id, st.id, st.created_ms
        FROM session_tags st
        WHERE EXISTS (
          SELECT 1 FROM history h
          WHERE h.source = st.source
            AND h.session_id = st.session_id
            AND ${scope.sql}
        )`;
      scopedSessionParams.push(...scope.params);
    }
    return runQuery<{
      name: string;
      display_name: string;
      color: string | null;
      session_count: number;
      first_tagged_ms: number | null;
      last_tagged_ms: number | null;
    }>(
      this.db,
      `SELECT t.name, t.display_name, t.color, COUNT(st.id) AS session_count,
              MIN(st.created_ms) AS first_tagged_ms, MAX(st.created_ms) AS last_tagged_ms
       FROM tags t
       LEFT JOIN (${scopedSessionSql}) st ON st.tag_id = t.id
       ${where}
       GROUP BY t.id, t.name, t.display_name, t.color
       ORDER BY t.name`,
      [...scopedSessionParams, ...params],
    ).map((row) => {
      const tag: Tag & { sessions?: TaggedSession[] } = {
        name: row.name,
        displayName: row.display_name,
        color: row.color,
        sessionCount: row.session_count,
        firstTaggedMs: row.first_tagged_ms,
        lastTaggedMs: row.last_tagged_ms,
      };
      if (opts.includeSessions) {
        tag.sessions = this.sessionsByTag(row.name);
      }
      return tag;
    });
  }

  sessionsByTag(tagName: string): TaggedSession[] {
    const clauses = ['t.name = ?'];
    const params: unknown[] = [normalizeTagName(tagName)];
    if (this._projectScope) {
      const scope = scopedPathClause('h.project', this._projectScope);
      clauses.push(scope.sql);
      params.push(...scope.params);
    }
    return runQuery<{
      source: string;
      session_id: string;
      project: string | null;
      entry_count: number;
      last_activity_ms: number | null;
    }>(
      this.db,
      `SELECT st.source, st.session_id, MIN(h.project) AS project, COUNT(h.id) AS entry_count,
              MAX(h.timestamp_ms) AS last_activity_ms
       FROM session_tags st
       JOIN tags t ON t.id = st.tag_id
       JOIN history h ON h.source = st.source AND h.session_id = st.session_id
       WHERE ${clauses.join(' AND ')}
       GROUP BY st.source, st.session_id
       ORDER BY MAX(h.timestamp_ms) DESC`,
      params,
    ).map((row) => ({
      source: row.source as Source,
      sessionId: row.session_id,
      project: row.project,
      entryCount: row.entry_count,
      lastActivityMs: row.last_activity_ms,
    }));
  }

  searchByTag(tagName: string, opts: Omit<SearchOptions, 'tag'> = {}): HistoryEntry[] {
    return this.recent({ ...opts, tag: tagName });
  }

  /** Most recent prompts, newest first. */
  recent(opts: ListOptions = {}): HistoryEntry[] {
    const limit = opts.limit ?? 50;
    const { sql, params } = buildFilters(opts, this._projectScope);
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms, git_branch
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
   *
   * Implementation note: this used to use a correlated scalar subquery
   * to pick `first_prompt`, which ran in O(sessions × rows) — ~19s on a
   * 35K-row DB. Switched to `ROW_NUMBER() OVER (PARTITION BY session_id
   * ORDER BY timestamp_ms)` so first-prompt picking is a single pass
   * over the table (~300ms on the same DB). Plus the index ensure step
   * in `openAiHist` keeps it fast even when the DB was written by the
   * older Python CLI that didn't create `idx_history_session`.
   */
  listSessions(opts: ListOptions = {}): SessionSummary[] {
    const limit = opts.limit ?? 50;
    const { sql, params } = buildFilters(opts, this._projectScope);
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
      ),
      ranked AS (
        SELECT
          session_id,
          source,
          project,
          prompt,
          timestamp_ms,
          ROW_NUMBER() OVER (
            PARTITION BY session_id, source, project
            ORDER BY timestamp_ms ASC, id ASC
          ) AS rn_first,
          COUNT(*) OVER (PARTITION BY session_id, source, project) AS prompt_count,
          MIN(timestamp_ms) OVER (PARTITION BY session_id, source, project) AS first_activity_ms,
          MAX(timestamp_ms) OVER (PARTITION BY session_id, source, project) AS last_activity_ms
        FROM filtered
      )
      SELECT
        session_id,
        source,
        project,
        prompt AS first_prompt,
        first_activity_ms,
        last_activity_ms,
        prompt_count
      FROM ranked
      WHERE rn_first = 1
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
  getSession(sessionId: string, opts: Pick<ListOptions, 'source' | 'tag'> = {}): HistoryEntry[] {
    const clauses = ['session_id = ?'];
    const params: unknown[] = [sessionId];
    if (opts.source) {
      clauses.push('source = ?');
      params.push(opts.source);
    }
    appendProjectFilter(clauses, params, undefined, this._projectScope);
    appendTagFilter(clauses, params, opts.tag, 'history');
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms, git_branch
       FROM history
       WHERE ${clauses.join(' AND ')}
       ORDER BY timestamp_ms ASC`,
      params,
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
    appendProjectFilter(clauses, params, opts.project, this._projectScope);
    appendTagFilter(clauses, params, opts.tag, 'history');
    if (typeof opts.beforeMs === 'number') {
      clauses.push('timestamp_ms < ?');
      params.push(opts.beforeMs);
    }
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms, git_branch
       FROM history
       WHERE ${clauses.join(' AND ')}
       ORDER BY timestamp_ms DESC
       LIMIT ?`,
      [...params, limit],
    ).map(rowToEntry);
  }

  /** Search compacted per-run trajectory WHY: decisions and retrospectives. */
  searchTrajectories(query: string, opts: TrajectorySearchOptions = {}): TrajectoryEntry[] {
    const trimmed = query.trim();
    if (!trimmed) return [];
    const limit = opts.limit ?? 20;
    const escaped = trimmed.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
    const pattern = `%${escaped}%`;
    const clauses: string[] = [
      `(LOWER(search_text) LIKE LOWER(?) ESCAPE '\\'
          OR LOWER(COALESCE(task_title, '')) LIKE LOWER(?) ESCAPE '\\'
          OR LOWER(COALESCE(task_description, '')) LIKE LOWER(?) ESCAPE '\\'
          OR LOWER(COALESCE(persona_id, '')) LIKE LOWER(?) ESCAPE '\\'
          OR LOWER(COALESCE(project_id, '')) LIKE LOWER(?) ESCAPE '\\')`,
    ];
    const params: unknown[] = [pattern, pattern, pattern, pattern, pattern];
    for (const project of [this._projectScope, opts.project]) {
      if (!project) continue;
      const scope = scopedTrajectoryClause(project);
      clauses.push(scope.sql);
      params.push(...scope.params);
    }
    return runQuery<RawTrajectoryRow>(
      this.db,
      `SELECT id, version, persona_id, project_id, task_title, task_description, status,
              started_at, completed_at, decisions_json, retrospective_json, search_text,
              path, updated_ms, timestamp_ms
       FROM trajectories
       WHERE ${clauses.join(' AND ')}
       ORDER BY timestamp_ms DESC
       LIMIT ?`,
      [...params, limit],
    ).map(rowToTrajectory);
  }

  /** Best-matching per-run trajectory for a task query, or `null` if none match. */
  whyForTask(query: string): TrajectoryEntry | null {
    return this.searchTrajectories(query, { limit: 1 })[0] ?? null;
  }

  /** Single entry by id, or `null` if not found. */
  getEntry(id: number): HistoryEntry | null {
    const clauses = ['id = ?'];
    const params: unknown[] = [id];
    appendProjectFilter(clauses, params, undefined, this._projectScope);
    const rows = runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms, git_branch
       FROM history WHERE ${clauses.join(' AND ')}`,
      params,
    );
    return rows.length > 0 ? rowToEntry(rows[0]) : null;
  }

  /**
   * All entries whose timestamp falls within [timestampMs - windowMs,
   * timestampMs + windowMs], ordered oldest first. Used by get_context.
   */
  getInTimeWindow(timestampMs: number, windowMs: number): HistoryEntry[] {
    const clauses = ['timestamp_ms BETWEEN ? AND ?'];
    const params: unknown[] = [timestampMs - windowMs, timestampMs + windowMs];
    appendProjectFilter(clauses, params, undefined, this._projectScope);
    return runQuery<RawHistoryRow>(
      this.db,
      `SELECT id, source, session_id, project, prompt, timestamp_ms, git_branch
       FROM history
       WHERE ${clauses.join(' AND ')}
       ORDER BY timestamp_ms ASC`,
      params,
    ).map(rowToEntry);
  }

  /**
   * Find sessions matching the given repo/branch/source and return ranked
   * handoff candidates with a warm-start command for the target CLI.
   *
   * Queries the `sessions` table (populated by `ai-hist sync`) for objective
   * metadata, then joins the last N user prompts from `history` to generate
   * a brief on demand — no pre-computed summaries.
   */
  getHandoff(opts: GetHandoffOptions = {}): HandoffCandidate[] {
    const limit = opts.limit ?? 3;
    const clauses: string[] = [];
    const params: unknown[] = [];

    if (this._projectScope) {
      const escaped = this._projectScope.replace(/\|/g, '||').replace(/%/g, '|%').replace(/_/g, '|_');
      clauses.push("cwd LIKE ? ESCAPE '|'");
      params.push(`${escaped}%`);
    }
    if (opts.source) {
      clauses.push('source = ?');
      params.push(opts.source);
    }
    if (opts.repo) {
      const escaped = opts.repo.replace(/\|/g, '||').replace(/%/g, '|%').replace(/_/g, '|_');
      clauses.push("cwd LIKE ? ESCAPE '|'");
      params.push(`%${escaped}%`);
    }
    if (opts.branch) {
      const escaped = opts.branch.replace(/\|/g, '||').replace(/%/g, '|%').replace(/_/g, '|_');
      clauses.push("git_branch LIKE ? ESCAPE '|'");
      params.push(`%${escaped}%`);
    }

    const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
    // Over-fetch sessions because many (old or sub-agent) sessions have no
    // matching `history` prompts and get skipped below. Fetching exactly
    // `limit` could starve the result to empty even when good candidates exist
    // just past the cutoff. We stop scanning once we've collected `limit`.
    const fetchCount = Math.min(Math.max(limit * 5, limit), 100);
    const sessionRows = runQuery<RawSessionRow>(
      this.db,
      `SELECT session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms,
              last_assistant_text, raw_path
       FROM sessions
       ${where}
       ORDER BY last_activity_ms DESC
       LIMIT ?`,
      [...params, fetchCount],
    );

    const candidates: HandoffCandidate[] = [];
    for (const session of sessionRows) {
      if (candidates.length >= limit) break;
      // Filter by both session_id AND source to prevent cross-source prompt mixing
      // when two CLIs happen to use the same session ID (rare but possible).
      const prompts = runQuery<RawHistoryRow>(
        this.db,
        `SELECT id, source, session_id, project, prompt, timestamp_ms, git_branch
         FROM history
         WHERE session_id = ? AND source = ?
         ORDER BY timestamp_ms ASC`,
        [session.session_id, session.source],
      ).map(rowToEntry);
      if (prompts.length === 0) continue;

      const filesTouched = extractFilePaths(prompts.map((p) => p.prompt).join('\n'));
      // The first prompt is often injected boilerplate (system-reminder /
      // command wrappers), especially for relay-driven sessions. Prefer the
      // first prompt that looks like a real user instruction for the goal.
      const goalPrompt = prompts.find((p) => !isBoilerplatePrompt(p.prompt)) ?? prompts[0];
      const goal = goalPrompt.prompt.slice(0, 300).replace(/\n/g, ' ');
      const lastState = prompts[prompts.length - 1].prompt.slice(0, 300).replace(/\n/g, ' ');

      const resume = resumeCommand({
        source: session.source as Source,
        sessionId: session.session_id,
        project: session.cwd,
      });

      const targetSource = session.source === 'claude' ? 'codex' : 'claude';
      const warmStart = buildWarmStartCommand(
        targetSource,
        goal,
        filesTouched,
        lastState,
        session.last_assistant_text ?? null,
        session.cwd,
      );

      let confidence = Math.min(0.6, 0.2 + prompts.length * 0.02);
      if (opts.branch && session.git_branch?.includes(opts.branch)) confidence += 0.25;
      if (opts.repo && session.cwd?.includes(opts.repo)) confidence += 0.15;
      confidence = Math.min(1.0, confidence);

      candidates.push({
        sessionId: session.session_id,
        source: session.source as Source,
        cwd: session.cwd,
        gitBranch: session.git_branch,
        firstActivityMs: session.first_activity_ms,
        lastActivityMs: session.last_activity_ms,
        promptCount: prompts.length,
        goal,
        lastState,
        lastAssistantText: session.last_assistant_text ?? null,
        filesTouched,
        resumeCommand: resume,
        warmStartCommand: warmStart,
        confidence,
      });
    }

    return candidates.sort((a, b) => b.confidence - a.confidence);
  }

  /** Counts + date range, mirroring `ai-hist stats`. */
  stats(): Stats {
    const scopeClauses: string[] = [];
    const scopeParams: unknown[] = [];
    appendProjectFilter(scopeClauses, scopeParams, undefined, this._projectScope);
    const where = scopeClauses.length > 0 ? ` WHERE ${scopeClauses.join(' AND ')}` : '';
    const andScope = scopeClauses.length > 0 ? ` AND ${scopeClauses.join(' AND ')}` : '';
    const total =
      runQuery<{ c: number }>(this.db, `SELECT COUNT(*) AS c FROM history${where}`, scopeParams)[0]?.c ?? 0;
    const bySourceRows = runQuery<{ source: string; c: number }>(
      this.db,
      `SELECT source, COUNT(*) AS c FROM history${where} GROUP BY source`,
      scopeParams,
    );
    const bySource: Partial<Record<Source, number>> = {};
    for (const row of bySourceRows) {
      bySource[row.source as Source] = row.c;
    }
    const byProject = runQuery<{ project: string; c: number }>(
      this.db,
      `SELECT project, COUNT(*) AS c FROM history
       WHERE project IS NOT NULL AND project != ''${andScope}
       GROUP BY project ORDER BY c DESC LIMIT 10`,
      scopeParams,
    ).map((row) => ({ project: row.project, count: row.c }));
    const range = runQuery<{ mn: number | null; mx: number | null }>(
      this.db,
      `SELECT MIN(timestamp_ms) AS mn, MAX(timestamp_ms) AS mx FROM history${where}`,
      scopeParams,
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
 * Heuristic: does a prompt look like injected boilerplate rather than a real
 * user instruction? Relay/agent sessions often open with a system-reminder or
 * command wrapper that makes a poor "goal" summary.
 */
function isBoilerplatePrompt(prompt: string): boolean {
  const t = prompt.trimStart();
  return (
    t.startsWith('<system-reminder') ||
    t.startsWith('<command-') ||
    t.startsWith('Caveat:') ||
    t.startsWith('[Request interrupted')
  );
}

/**
 * Extract file paths with recognizable extensions from a block of text.
 * Heuristic — used to populate filesTouched in HandoffCandidate.
 */
function extractFilePaths(text: string): string[] {
  const exts = 'ts|tsx|js|jsx|mjs|cjs|py|go|rs|rb|java|cs|cpp|cc|c|h|json|yaml|yml|toml|md|sh|sql|css|scss|html|svelte|vue';
  const regex = new RegExp(
    `(?:^|[\\s,\`'"(])([~./][\\w./-]*\\.(?:${exts})|-?[\\w/-]+\\.(?:${exts}))\\b`,
    'gm',
  );
  const seen = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = regex.exec(text)) !== null) {
    const p = m[1].trim();
    if (p.length > 2 && p.length < 200) seen.add(p);
  }
  return Array.from(seen).slice(0, 20);
}

/**
 * Build a warm-start command for the target CLI, injecting context from a
 * prior session so the new agent can pick up mid-task.
 */
function buildWarmStartCommand(
  targetSource: string,
  goal: string,
  files: string[],
  lastState: string,
  lastAssistant: string | null,
  cwd: string | null,
): string {
  const filesLine = files.length > 0 ? ` Files touched: ${files.slice(0, 10).join(', ')}.` : '';
  const assistantLine = lastAssistant
    ? ` Last assistant state: ${lastAssistant.replace(/\n/g, ' ').slice(0, 200)}`
    : '';
  const context =
    `Picking up from previous session. Goal: ${goal}.${filesLine} Last user prompt: ${lastState}.${assistantLine}`;
  const cdPart = cwd ? `cd ${shellQuote(cwd)} && ` : '';
  return `${cdPart}${targetSource} ${shellQuote(context)}`;
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
