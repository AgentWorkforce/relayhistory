/**
 * In-process cloud push: read new local history/trajectory rows from the
 * ai-hist SQLite DB and POST them to relayhistory-cloud `/v1/ingest`.
 *
 * This is a wire-compatible TypeScript port of the Rust `ai-hist push`
 * (`crates/ai-hist/src/cloud.rs` + `crates/ai-hist-core/src/{outbox,convergence}.rs`).
 * The envelope shapes, `eventId` formats, `promptHash`, `batchId`, and cursor
 * semantics MUST match the Rust client exactly so the server's
 * `(orgId, machineId, batchId)` batch dedup and per-event upsert keys line up.
 *
 * Only one pusher should run per machine. In the Reflex flow that is this
 * in-process pusher (the Rust CLI `push` service is not scheduled), so this
 * module owns the cursor.
 */
import { createHash } from 'node:crypto';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { homedir, hostname as osHostname } from 'node:os';
import { join, dirname } from 'node:path';

import initSqlJs, { type Database, type SqlJsStatic } from 'sql.js';

import { loadStoredRelayhistoryAuth, type RelayhistoryAuth } from './cloud-client.js';

/** Cap on rows scanned per source per batch (mirrors Rust `limit`). */
const DEFAULT_LIMIT = 500;
/** Cap on records emitted across the whole batch (mirrors Rust `MAX_RECORDS`). */
const MAX_RECORDS = 900;

export interface SyncCursor {
  /** Highest `history.id` synced. */
  history_id: number;
  /** Highest `trajectories.rowid` synced. */
  trajectory_rowid: number;
}

export interface MachineIdentity {
  id: string;
  hostname?: string;
  label?: string;
  os?: string;
  cliVersion?: string;
}

/** A `/v1/ingest` record envelope. Optional fields are omitted from JSON when
 * undefined; `confidence` is the sole exception — it is always present and is
 * `null` when unknown (matching the Rust serializer). */
export interface ConvergenceEnvelope {
  v: number;
  kind: string;
  source: string;
  lens?: string;
  sessionId: string;
  eventId: string;
  ts: string;
  type: string;
  content: string;
  significance?: string;
  confidence: number | null;
  tags?: string[];
  actorName?: string;
  projectId?: string;
  taskTitle?: string;
  taskDescription?: string;
  taskStatus?: string;
  taskRef?: { system?: string; id?: string };
  record?: Record<string, unknown>;
}

export interface PushReport {
  sent: number;
  accepted: number;
  cursor: SyncCursor;
  batchId: string | null;
}

// ----- config dir / state files (kept alongside the TS auth.json) -----

function configDir(): string {
  return process.env.AI_HIST_CONFIG_DIR ?? join(homedir(), '.config', 'ai-hist');
}
function cursorPath(): string {
  return join(configDir(), 'cursor.json');
}
function machineIdPath(): string {
  return join(configDir(), 'machine-id');
}

function defaultDbPath(): string {
  const fromEnv = process.env.AI_HIST_DB;
  if (fromEnv && fromEnv.trim().length > 0) return fromEnv;
  return join(homedir(), '.local', 'share', 'ai-hist', 'ai-history.db');
}

async function writePrivate(path: string, body: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, body, { mode: 0o600 });
}

// ----- hashing (must match ai_hist_core::prompt_hash) -----

/** SHA-256 → lowercase hex → first 16 chars. Load-bearing: used for machine
 * id, batch id, and prompt event ids. */
export function promptHash(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex').slice(0, 16);
}

/** Deterministic, retry-safe batch id (mirrors Rust `batch_id`). */
export function batchId(machineId: string, from: SyncCursor, to: SyncCursor, count: number): string {
  const material = `${machineId}:${from.history_id}:${from.trajectory_rowid}:${to.history_id}:${to.trajectory_rowid}:${count}`;
  return `b_${promptHash(material)}`;
}

// ----- machine id + cursor persistence -----

export async function loadCursor(): Promise<SyncCursor> {
  try {
    const body = await readFile(cursorPath(), 'utf-8');
    const parsed = JSON.parse(body) as Partial<SyncCursor>;
    return {
      history_id: typeof parsed.history_id === 'number' ? parsed.history_id : 0,
      trajectory_rowid: typeof parsed.trajectory_rowid === 'number' ? parsed.trajectory_rowid : 0,
    };
  } catch {
    return { history_id: 0, trajectory_rowid: 0 };
  }
}

export async function saveCursor(cursor: SyncCursor): Promise<void> {
  await writePrivate(cursorPath(), JSON.stringify(cursor, null, 2));
}

function hostname(): string {
  const fromEnv = process.env.HOSTNAME;
  if (fromEnv && fromEnv.length > 0) return fromEnv;
  const fromOs = osHostname();
  return fromOs && fromOs.length > 0 ? fromOs : 'unknown-host';
}

/** Stable per-machine id, generated once and persisted (mirrors Rust `machine_id`). */
export async function machineId(): Promise<string> {
  try {
    const existing = (await readFile(machineIdPath(), 'utf-8')).trim();
    if (existing.length > 0) return existing;
  } catch {
    /* not generated yet */
  }
  // process.hrtime.bigint() is a monotonic nanosecond counter — a unique enough
  // salt so two machines with the same hostname don't collide.
  const nanos = process.hrtime.bigint().toString();
  const id = `m_${promptHash(`${hostname()}:${nanos}`)}`;
  await writePrivate(machineIdPath(), id);
  return id;
}

function osName(): string {
  switch (process.platform) {
    case 'darwin':
      return 'macos';
    case 'win32':
      return 'windows';
    default:
      return process.platform;
  }
}

export async function buildMachineIdentity(cliVersion?: string): Promise<MachineIdentity> {
  const envHost = process.env.HOSTNAME;
  return {
    id: await machineId(),
    hostname: envHost && envHost.length > 0 ? envHost : undefined,
    os: osName(),
    cliVersion,
  };
}

// ----- text normalization (must match convergence::normalize_home_path) -----

const HOME_PREFIXES = ['/users/', '/home/', '\\users\\'];

/** Replace the username segment in home-rooted paths with `~`, preserving the
 * original prefix casing. Mirrors Rust `normalize_home_path`. */
export function normalizeHomePath(input: string): string {
  let out = '';
  let i = 0;
  const lower = input.toLowerCase();
  while (i < input.length) {
    let matched = false;
    for (const prefix of HOME_PREFIXES) {
      if (lower.startsWith(prefix, i)) {
        // Find the end of the username segment (next '/' or '\\', or end).
        const segStart = i + prefix.length;
        let segEnd = segStart;
        while (segEnd < input.length && input[segEnd] !== '/' && input[segEnd] !== '\\') {
          segEnd += 1;
        }
        if (segEnd > segStart) {
          // Keep the original-cased prefix, replace the username with '~'.
          out += input.slice(i, i + prefix.length) + '~';
          i = segEnd;
          matched = true;
          break;
        }
      }
    }
    if (!matched) {
      out += input[i];
      i += 1;
    }
  }
  return out;
}

function epochMsToIso(ms: number): string {
  return new Date(ms).toISOString();
}

// ----- small JSON helpers -----

function trimmedOrUndef(v: unknown): string | undefined {
  if (typeof v !== 'string') return undefined;
  const t = v.trim();
  return t.length > 0 ? t : undefined;
}

function asNumber(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

function asObject(v: unknown): Record<string, unknown> | null {
  return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function parseJson(text: unknown): unknown {
  if (typeof text !== 'string' || text.trim().length === 0) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

// ----- history mapper (convergence::map_history_entry) -----

interface HistoryRow {
  id: number;
  source: string;
  session_id: string | null;
  prompt: string;
  prompt_hash: string | null;
  timestamp_ms: number;
}

function mapHistoryEntry(row: HistoryRow): ConvergenceEnvelope {
  const hash = row.prompt_hash && row.prompt_hash.length > 0 ? row.prompt_hash : promptHash(row.prompt);
  return {
    v: 1,
    kind: 'prompt',
    source: row.source,
    lens: 'history',
    sessionId: row.session_id ?? 'unsessioned',
    eventId: `prompt:${row.timestamp_ms}:${hash}`,
    ts: epochMsToIso(row.timestamp_ms),
    type: 'prompt',
    content: normalizeHomePath(row.prompt.trim()),
    confidence: null,
  };
}

// ----- trajectory mappers (convergence::map_trajectory / map_compacted_trajectory) -----

interface TrajectoryRow {
  id: string;
  persona_id: string | null;
  project_id: string | null;
  task_title: string | null;
  task_description: string | null;
  status: string | null;
  decisions_json: string;
  retrospective_json: string;
  timestamp_ms: number;
}

interface TrajContext {
  sessionId: string;
  tsIso: string;
  actorName?: string;
  projectId?: string;
  taskTitle?: string;
  taskDescription?: string;
  taskStatus?: string;
}

function alternativesText(alts: unknown): string[] {
  return asArray(alts)
    .map((a) => {
      if (typeof a === 'string') return a.trim();
      const o = asObject(a);
      if (!o) return '';
      const option = trimmedOrUndef(o.option);
      const reason = trimmedOrUndef(o.reason);
      if (option && reason) return `${option} (${reason})`;
      return option ?? reason ?? '';
    })
    .filter((s) => s.length > 0);
}

function decisionContent(d: Record<string, unknown>): string {
  const parts: string[] = [];
  const question = trimmedOrUndef(d.question);
  const chosen = trimmedOrUndef(d.chosen);
  const reasoning = trimmedOrUndef(d.reasoning);
  const alts = alternativesText(d.alternatives);
  if (question) parts.push(`Question: ${question}`);
  if (chosen) parts.push(`Chose: ${chosen}`);
  if (reasoning) parts.push(`Because: ${reasoning}`);
  if (alts.length > 0) parts.push(`Alternatives: ${alts.join('; ')}`);
  return normalizeHomePath(parts.join('\n').trim());
}

function decisionRecord(d: Record<string, unknown>): Record<string, unknown> | undefined {
  const chosen = trimmedOrUndef(d.chosen);
  const alts = alternativesText(d.alternatives).map(normalizeHomePath);
  const rec: Record<string, unknown> = {};
  if (chosen) rec.chosen = normalizeHomePath(chosen);
  if (alts.length > 0) rec.alternatives = alts;
  return Object.keys(rec).length > 0 ? rec : undefined;
}

/** Strings trimmed non-empty; objects → first of text/summary/description/value. */
function stringArray(v: unknown): string[] {
  return asArray(v)
    .map((item) => {
      if (typeof item === 'string') return item.trim();
      const o = asObject(item);
      if (!o) return '';
      return (
        trimmedOrUndef(o.text) ??
        trimmedOrUndef(o.summary) ??
        trimmedOrUndef(o.description) ??
        trimmedOrUndef(o.value) ??
        ''
      );
    })
    .filter((s) => s.length > 0);
}

function labeledFields(o: Record<string, unknown>, fields: [string, string][]): string {
  const parts: string[] = [];
  for (const [label, key] of fields) {
    const val = trimmedOrUndef(o[key]);
    if (val) parts.push(`${label}: ${val}`);
  }
  return normalizeHomePath(parts.join('\n').trim());
}

function baseEnvelope(
  ctx: TrajContext,
  source: string,
  lens: string,
  kind: string,
  eventId: string,
  content: string,
  confidence: number | null,
  extra?: { tags?: string[]; record?: Record<string, unknown> },
): ConvergenceEnvelope {
  return {
    v: 1,
    kind,
    source,
    lens,
    sessionId: ctx.sessionId,
    eventId,
    ts: ctx.tsIso,
    type: kind,
    content,
    confidence,
    tags: extra?.tags && extra.tags.length > 0 ? extra.tags : undefined,
    actorName: ctx.actorName,
    projectId: ctx.projectId,
    taskTitle: ctx.taskTitle,
    taskDescription: ctx.taskDescription,
    taskStatus: ctx.taskStatus,
    record: extra?.record,
  };
}

function isCompactedRollup(retro: unknown): retro is Record<string, unknown> {
  const o = asObject(retro);
  return !!o && o.type === 'compacted' && Array.isArray(o.sourceTrajectories);
}

function mapCompacted(
  trajId: string,
  ctx: TrajContext,
  compacted: Record<string, unknown>,
): ConvergenceEnvelope[] {
  const out: ConvergenceEnvelope[] = [];
  const source = trimmedOrUndef(compacted.source) ?? 'trajectories';
  const lens = trimmedOrUndef(compacted.lens) ?? source;
  const tagList = asArray(compacted.tags)
    .map((t) => (typeof t === 'string' ? t.trim() : ''))
    .filter((s) => s.length > 0);
  const tags = tagList.length > 0 ? tagList : ['compacted'];
  const isLearn = tags.includes('learn');

  const emit = (kind: string, eventId: string, content: string, record?: Record<string, unknown>) => {
    const c = content.trim();
    if (c.length === 0) return;
    out.push(baseEnvelope(ctx, source, lens, kind, eventId, c, null, { tags, record }));
  };

  // decisions[]
  asArray(compacted.decisions).forEach((d, i) => {
    const o = asObject(d);
    if (!o) return;
    const parts: string[] = [];
    const question = trimmedOrUndef(o.question);
    const chosen = trimmedOrUndef(o.chosen);
    const reasoning = trimmedOrUndef(o.reasoning);
    const impact = trimmedOrUndef(o.impact);
    if (question) parts.push(`Question: ${question}`);
    if (chosen) parts.push(`Chose: ${chosen}`);
    if (reasoning) parts.push(`Because: ${reasoning}`);
    if (impact) parts.push(`Impact: ${impact}`);
    const rec: Record<string, unknown> = {};
    if (chosen) rec.chosen = normalizeHomePath(chosen);
    if (impact) rec.impact = normalizeHomePath(impact);
    emit(
      'decision',
      `decision:${trajId}:${i}`,
      normalizeHomePath(parts.join('\n')),
      Object.keys(rec).length > 0 ? rec : undefined,
    );
  });

  // lessons[]
  asArray(compacted.lessons).forEach((l, i) => {
    const o = asObject(l);
    if (!o) return;
    emit(
      'reflection',
      `reflection:${trajId}:lesson:${i}`,
      labeledFields(o, [
        ['Context', 'context'],
        ['Lesson', 'lesson'],
        ['Recommendation', 'recommendation'],
      ]),
    );
  });

  // keyFindings[]
  stringArray(compacted.keyFindings).forEach((f, i) => {
    emit('finding', `finding:${trajId}:keyfinding:${i}`, normalizeHomePath(f));
  });

  // keyLearnings[]
  stringArray(compacted.keyLearnings).forEach((f, i) => {
    emit('finding', `finding:${trajId}:learning:${i}`, normalizeHomePath(f));
  });

  // conventions[]
  asArray(compacted.conventions).forEach((c, i) => {
    const o = asObject(c);
    if (!o) return;
    emit(
      'reflection',
      `reflection:${trajId}:convention:${i}`,
      labeledFields(o, [
        ['Pattern', 'pattern'],
        ['Rationale', 'rationale'],
        ['Scope', 'scope'],
      ]),
    );
  });

  // narrative (only when not a "learn" rollup)
  if (!isLearn) {
    const narrative = trimmedOrUndef(compacted.narrative);
    if (narrative) emit('reflection', `reflection:${trajId}:summary`, normalizeHomePath(narrative));
  }

  // openQuestions[]
  stringArray(compacted.openQuestions).forEach((q, i) => {
    emit('finding', `finding:${trajId}:openquestion:${i}`, normalizeHomePath(q));
  });

  return out;
}

function mapTrajectory(row: TrajectoryRow): ConvergenceEnvelope[] {
  const trajId = row.id.trim();
  if (trajId.length === 0) return [];

  const ctx: TrajContext = {
    sessionId: trajId,
    tsIso: epochMsToIso(row.timestamp_ms),
    actorName: trimmedOrUndef(row.persona_id),
    projectId: trimmedOrUndef(row.project_id),
    taskTitle: trimmedOrUndef(row.task_title),
    taskDescription: trimmedOrUndef(row.task_description),
    taskStatus: trimmedOrUndef(row.status),
  };

  const retro = parseJson(row.retrospective_json);
  if (isCompactedRollup(retro)) {
    return mapCompacted(trajId, ctx, retro);
  }

  const out: ConvergenceEnvelope[] = [];
  const source = 'trajectories';
  const lens = 'trajectories';

  // Decisions first.
  asArray(parseJson(row.decisions_json)).forEach((d, i) => {
    const o = asObject(d);
    if (!o) return;
    const content = decisionContent(o);
    if (content.length === 0) return;
    out.push(
      baseEnvelope(ctx, source, lens, 'decision', `decision:${trajId}:${i}`, content, asNumber(o.confidence), {
        record: decisionRecord(o),
      }),
    );
  });

  const retroObj = asObject(retro);
  if (retroObj) {
    const retroConf = asNumber(retroObj.confidence);
    const pushRetro = (kind: string, eventId: string, raw: unknown, confidence: number | null) => {
      const content = normalizeHomePath((trimmedOrUndef(raw) ?? '').trim());
      if (content.length === 0) return;
      out.push(baseEnvelope(ctx, source, lens, kind, eventId, content, confidence));
    };
    pushRetro('reflection', `reflection:${trajId}:summary`, retroObj.summary, retroConf);
    pushRetro('reflection', `reflection:${trajId}:approach`, retroObj.approach, retroConf);
    stringArray(retroObj.learnings).forEach((v, i) =>
      out.push(baseEnvelope(ctx, source, lens, 'finding', `finding:${trajId}:learning:${i}`, normalizeHomePath(v), null)),
    );
    stringArray(retroObj.suggestions).forEach((v, i) =>
      out.push(
        baseEnvelope(ctx, source, lens, 'reflection', `reflection:${trajId}:suggestion:${i}`, normalizeHomePath(v), null),
      ),
    );
    stringArray(retroObj.challenges).forEach((v, i) =>
      out.push(baseEnvelope(ctx, source, lens, 'finding', `finding:${trajId}:challenge:${i}`, normalizeHomePath(v), null)),
    );
  }

  return out;
}

// ----- outbox batch builder (outbox::build_outbox_batch) -----

export interface OutboxBatch {
  records: ConvergenceEnvelope[];
  cursor: SyncCursor;
}

function queryAll(db: Database, sql: string, params: Record<string, number>): Record<string, unknown>[] {
  const stmt = db.prepare(sql);
  try {
    stmt.bind(params);
    const rows: Record<string, unknown>[] = [];
    while (stmt.step()) rows.push(stmt.getAsObject());
    return rows;
  } finally {
    stmt.free();
  }
}

export function buildOutboxBatch(
  db: Database,
  cursor: SyncCursor,
  limit: number,
  incognito: Set<string>,
): OutboxBatch {
  const effLimit = Math.max(limit, 1);
  const next: SyncCursor = { ...cursor };
  const records: ConvergenceEnvelope[] = [];

  // History.
  const historyRows = queryAll(
    db,
    'SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms FROM history WHERE id > $id ORDER BY id ASC LIMIT $limit',
    { $id: cursor.history_id, $limit: effLimit },
  );
  for (const raw of historyRows) {
    if (records.length >= MAX_RECORDS) break;
    const row: HistoryRow = {
      id: Number(raw.id),
      source: String(raw.source),
      session_id: raw.session_id == null ? null : String(raw.session_id),
      prompt: String(raw.prompt ?? ''),
      prompt_hash: raw.prompt_hash == null ? null : String(raw.prompt_hash),
      timestamp_ms: Number(raw.timestamp_ms),
    };
    next.history_id = Math.max(next.history_id, row.id);
    if (row.session_id && incognito.has(row.session_id)) continue;
    records.push(mapHistoryEntry(row));
  }

  // Trajectories (rowid watermark; TEXT PK means rowid is separate).
  let trajRows: Record<string, unknown>[] = [];
  try {
    trajRows = queryAll(
      db,
      'SELECT rowid, id, persona_id, project_id, task_title, task_description, status, decisions_json, retrospective_json, timestamp_ms FROM trajectories WHERE rowid > $id ORDER BY rowid ASC LIMIT $limit',
      { $id: cursor.trajectory_rowid, $limit: effLimit },
    );
  } catch {
    trajRows = [];
  }
  for (const raw of trajRows) {
    const rowid = Number(raw.rowid);
    const id = String(raw.id ?? '');
    if (incognito.has(id)) {
      next.trajectory_rowid = Math.max(next.trajectory_rowid, rowid);
      continue;
    }
    const mapped = mapTrajectory({
      id,
      persona_id: raw.persona_id == null ? null : String(raw.persona_id),
      project_id: raw.project_id == null ? null : String(raw.project_id),
      task_title: raw.task_title == null ? null : String(raw.task_title),
      task_description: raw.task_description == null ? null : String(raw.task_description),
      status: raw.status == null ? null : String(raw.status),
      decisions_json: String(raw.decisions_json ?? ''),
      retrospective_json: String(raw.retrospective_json ?? ''),
      timestamp_ms: Number(raw.timestamp_ms),
    });
    if (records.length > 0 && records.length + mapped.length > MAX_RECORDS) break;
    next.trajectory_rowid = Math.max(next.trajectory_rowid, rowid);
    records.push(...mapped);
  }

  return { records, cursor: next };
}

// ----- HTTP (POST /v1/ingest) -----

interface IngestRequest {
  machine: MachineIdentity;
  batchId: string;
  cursors?: { history_id: number; trajectory_rowid: number };
  records: ConvergenceEnvelope[];
}

interface IngestResponse {
  accepted: number;
}

async function ingest(auth: RelayhistoryAuth, req: IngestRequest): Promise<IngestResponse> {
  const url = `${auth.baseUrl.replace(/\/$/, '')}/v1/ingest`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${auth.accessToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(req),
    signal: AbortSignal.timeout(30_000),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error(`ingest failed: HTTP ${resp.status}: ${body.slice(0, 300)}`);
  }
  const payload = (await resp.json()) as Partial<IngestResponse>;
  return { accepted: typeof payload.accepted === 'number' ? payload.accepted : 0 };
}

// ----- sql.js DB open -----

let _sqlPromise: Promise<SqlJsStatic> | null = null;
function getSqlJs(): Promise<SqlJsStatic> {
  if (!_sqlPromise) _sqlPromise = initSqlJs();
  return _sqlPromise;
}

export interface PushOptions {
  /** Override the SQLite path (default `$AI_HIST_DB` or the standard location). */
  dbPath?: string;
  /** Pre-loaded auth; falls back to `loadStoredRelayhistoryAuth()`. */
  auth?: RelayhistoryAuth | null;
  /** Rows scanned per source (default 500). */
  limit?: number;
  /** Session/trajectory ids to exclude (incognito). */
  incognito?: Iterable<string>;
  /** Reported as `machine.cliVersion`. */
  cliVersion?: string;
}

/**
 * Build the next outbox batch from the local DB and push it to
 * relayhistory-cloud. No-op (no HTTP) when there is nothing new. On success,
 * persists the advanced cursor. Returns `{ ok: false }`-style errors as thrown
 * exceptions; callers in a background loop should catch.
 */
export async function pushToCloud(opts: PushOptions = {}): Promise<PushReport> {
  const auth = opts.auth ?? (await loadStoredRelayhistoryAuth());
  if (!auth) {
    throw new Error('not authenticated — no relayhistory auth found (run reflex on / ai-hist login)');
  }

  const dbPath = opts.dbPath ?? defaultDbPath();
  let fileBuffer: Buffer;
  try {
    fileBuffer = await readFile(dbPath);
  } catch {
    // No local DB yet → nothing to push.
    const cursor = await loadCursor();
    return { sent: 0, accepted: 0, cursor, batchId: null };
  }

  const SQL = await getSqlJs();
  const db = new SQL.Database(fileBuffer);
  try {
    const cursor = await loadCursor();
    const incognito = new Set<string>(opts.incognito ?? []);
    const batch = buildOutboxBatch(db, cursor, opts.limit ?? DEFAULT_LIMIT, incognito);
    if (batch.records.length === 0) {
      return { sent: 0, accepted: 0, cursor, batchId: null };
    }

    const machine = await buildMachineIdentity(opts.cliVersion);
    const bid = batchId(machine.id, cursor, batch.cursor, batch.records.length);
    const resp = await ingest(auth, {
      machine,
      batchId: bid,
      cursors: { history_id: batch.cursor.history_id, trajectory_rowid: batch.cursor.trajectory_rowid },
      records: batch.records,
    });
    // Advance the cursor only after the server accepts the batch.
    await saveCursor(batch.cursor);
    return { sent: batch.records.length, accepted: resp.accepted, cursor: batch.cursor, batchId: bid };
  } finally {
    db.close();
  }
}
