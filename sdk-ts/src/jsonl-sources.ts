/**
 * Direct JSONL parsers for Claude / Codex / Cursor history files.
 *
 * Used as a fallback when the SQLite database the Python `ai-hist sync`
 * tool maintains isn't present — lets `npm install ai-hist` work
 * standalone for users who have any of these CLIs locally.
 *
 * Ports the parser logic from the canonical Python `ai-hist` script
 * (see https://github.com/AgentWorkforce/ai-hist/blob/main/ai-hist).
 * Format drift in those upstream CLIs is the main risk; the Python
 * source is the canonical reference.
 */

import { readFile, readdir, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, dirname, join } from 'node:path';

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

export type Source = 'claude' | 'codex' | 'cursor' | 'grok';

export interface RawRow {
  source: Source;
  sessionId: string | null;
  project: string | null;
  prompt: string;
  timestampMs: number;
  gitBranch: string | null;
}

const CLAUDE_HISTORY = join(homedir(), '.claude', 'history.jsonl');
const CODEX_HISTORY = join(homedir(), '.codex', 'history.jsonl');
const CURSOR_ROOT = join(homedir(), '.cursor', 'projects');
const GROK_SESSIONS_ROOT = join(homedir(), '.grok', 'sessions');

async function safeStat(path: string): Promise<{ size: number; mtimeMs: number } | null> {
  try {
    const s = await stat(path);
    return { size: s.size, mtimeMs: s.mtimeMs };
  } catch {
    return null;
  }
}

async function readLines(path: string): Promise<string[]> {
  try {
    const content = await readFile(path, 'utf8');
    return content.split('\n');
  } catch {
    return [];
  }
}

function readJsonRecord(line: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(line);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function parseClaudeLine(line: string): RawRow | null {
  if (!line.trim()) return null;
  const obj = readJsonRecord(line);
  if (!obj) return null;
  const display = typeof obj.display === 'string' ? obj.display.trim() : '';
  if (!display) return null;
  return {
    source: 'claude',
    sessionId: asString(obj.sessionId),
    project: asString(obj.project),
    prompt: display,
    timestampMs: asNumber(obj.timestamp) ?? 0,
    gitBranch: asString(obj.gitBranch),
  };
}

function parseCodexLine(line: string): RawRow | null {
  if (!line.trim()) return null;
  const obj = readJsonRecord(line);
  if (!obj) return null;
  const text = typeof obj.text === 'string' ? obj.text.trim() : '';
  if (!text) return null;
  const ts = asNumber(obj.ts) ?? 0;
  return {
    source: 'codex',
    // Python uses obj.session_id (snake_case).
    sessionId: asString(obj.session_id ?? obj.sessionId),
    project: null,
    prompt: text,
    // Python stores Codex's seconds-since-epoch as ms.
    timestampMs: Math.trunc(ts * 1000),
    gitBranch: null,
  };
}

/**
 * Cursor lines carry `{role, message: {content: [{type, text}, ...]}}` and
 * NO per-line timestamp. The Python tool falls back to the file mtime;
 * we do the same. User prompts are wrapped in `<user_query>...</user_query>`.
 */
function parseCursorLine(line: string): string | null {
  if (!line.trim()) return null;
  const obj = readJsonRecord(line);
  if (!obj) return null;
  if (obj.role !== 'user') return null;
  const message = obj.message;
  if (!message || typeof message !== 'object') return null;
  const content = (message as Record<string, unknown>).content;
  let text = '';
  if (typeof content === 'string') {
    text = content;
  } else if (Array.isArray(content)) {
    for (const c of content) {
      if (c && typeof c === 'object' && (c as { type?: unknown }).type === 'text') {
        const t = (c as { text?: unknown }).text;
        if (typeof t === 'string') {
          text = t;
          break;
        }
      }
    }
  }
  text = text.trim();
  if (!text) return null;
  if (text.startsWith('<user_query>') && text.endsWith('</user_query>')) {
    text = text.slice('<user_query>'.length, -'</user_query>'.length).trim();
  }
  return text || null;
}

// `~/.cursor/projects/<encoded-path>/...` — encoded path is the absolute
// project path with `/` replaced by `-`. Python's `_decode_cursor_project`
// just rejoins on `-` → `/`. We do the same; it's lossy for any real `-` in
// a path segment, but matches the Python tool's behavior for parity.
function decodeCursorProject(encoded: string): string {
  return `/${encoded.replace(/-/g, '/')}`;
}

async function readDirSafe(path: string): Promise<string[]> {
  try {
    return await readdir(path);
  } catch {
    return [];
  }
}

async function collectMatchingFiles(root: string, filename: string, out: string[] = []): Promise<string[]> {
  const rootStat = await safeStat(root);
  if (!rootStat) return out;
  for (const name of await readDirSafe(root)) {
    const full = join(root, name);
    const child = await safeStat(full);
    if (!child) continue;
    if (name === filename) {
      out.push(full);
    } else {
      await collectMatchingFiles(full, filename, out);
    }
  }
  return out;
}

async function scanClaude(): Promise<RawRow[]> {
  const lines = await readLines(CLAUDE_HISTORY);
  const rows: RawRow[] = [];
  for (const line of lines) {
    const row = parseClaudeLine(line);
    if (row) rows.push(row);
  }
  return rows;
}

async function scanCodex(): Promise<RawRow[]> {
  const lines = await readLines(CODEX_HISTORY);
  const rows: RawRow[] = [];
  for (const line of lines) {
    const row = parseCodexLine(line);
    if (row) rows.push(row);
  }
  return rows;
}

async function scanCursor(): Promise<RawRow[]> {
  const root = await safeStat(CURSOR_ROOT);
  if (!root) return [];
  const rows: RawRow[] = [];
  const projectDirs = await readDirSafe(CURSOR_ROOT);
  for (const projectDirName of projectDirs) {
    const projectDir = join(CURSOR_ROOT, projectDirName);
    if (!(await safeStat(projectDir))) continue;
    const tsRoot = join(projectDir, 'agent-transcripts');
    if (!(await safeStat(tsRoot))) continue;
    const projectPath = decodeCursorProject(projectDirName);
    const sessionDirs = await readDirSafe(tsRoot);
    for (const sessionDirName of sessionDirs) {
      const sessionDir = join(tsRoot, sessionDirName);
      if (!(await safeStat(sessionDir))) continue;
      const sessionId = sessionDirName;
      const jsonl = join(sessionDir, `${sessionId}.jsonl`);
      const fileStat = await safeStat(jsonl);
      if (!fileStat) continue;
      const tsMs = Math.trunc(fileStat.mtimeMs);
      for (const line of await readLines(jsonl)) {
        const text = parseCursorLine(line);
        if (!text) continue;
        rows.push({
          source: 'cursor',
          sessionId,
          project: projectPath,
          prompt: text,
          timestampMs: tsMs,
          gitBranch: null,
        });
      }
    }
    // Yield between project dirs so very large cursor histories don't
    // hold the event loop for seconds at a time.
    await yieldToEventLoop();
  }
  return rows;
}

function parseIsoMs(value: unknown, fallbackMs: number): number {
  if (typeof value !== 'string' || value.length === 0) return fallbackMs;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : fallbackMs;
}

function grokText(obj: Record<string, unknown>, role: 'user' | 'assistant'): string | null {
  if (obj.type !== role) return null;
  if (role === 'user' && obj.synthetic_reason) return null;
  const content = obj.content;
  const parts: string[] = [];
  if (typeof content === 'string') {
    parts.push(content);
  } else if (Array.isArray(content)) {
    for (const item of content) {
      if (!item || typeof item !== 'object') continue;
      const record = item as Record<string, unknown>;
      if (record.type === 'text' && typeof record.text === 'string') parts.push(record.text);
    }
  } else if (content && typeof content === 'object') {
    const record = content as Record<string, unknown>;
    if (typeof record.text === 'string') parts.push(record.text);
  }
  const text = parts.map((part) => part.trim()).filter(Boolean).join('\n');
  return text || null;
}

function grokProjectFromPath(chatPath: string): string | null {
  const encodedProject = basename(dirname(dirname(chatPath)));
  try {
    const decoded = decodeURIComponent(encodedProject);
    return decoded.length > 0 ? decoded : null;
  } catch {
    return null;
  }
}

async function readGrokSummary(chatPath: string): Promise<Record<string, unknown> | null> {
  try {
    const parsed = JSON.parse(await readFile(join(dirname(chatPath), 'summary.json'), 'utf8'));
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function nestedString(obj: Record<string, unknown> | null, path: string[]): string | null {
  let current: unknown = obj;
  for (const part of path) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) return null;
    current = (current as Record<string, unknown>)[part];
  }
  return asString(current);
}

async function scanGrok(): Promise<RawRow[]> {
  const root = await safeStat(GROK_SESSIONS_ROOT);
  if (!root) return [];
  const rows: RawRow[] = [];
  for (const chatPath of await collectMatchingFiles(GROK_SESSIONS_ROOT, 'chat_history.jsonl')) {
    const summary = await readGrokSummary(chatPath);
    const fileStat = await safeStat(chatPath);
    const fallbackMs = Math.trunc(fileStat?.mtimeMs ?? 0);
    const sessionId = nestedString(summary, ['info', 'id']) ?? basename(dirname(chatPath));
    const project = nestedString(summary, ['info', 'cwd']) ?? asString(summary?.git_root_dir) ?? grokProjectFromPath(chatPath);
    const gitBranch = asString(summary?.head_branch);
    const baseMs = parseIsoMs(summary?.created_at, fallbackMs);
    let idx = 0;
    for (const line of await readLines(chatPath)) {
      const obj = readJsonRecord(line);
      if (!obj) continue;
      const prompt = grokText(obj, 'user');
      if (!prompt) continue;
      rows.push({
        source: 'grok',
        sessionId,
        project,
        prompt,
        timestampMs: baseMs + idx,
        gitBranch,
      });
      idx += 1;
    }
    await yieldToEventLoop();
  }
  return rows;
}

/**
 * Scan all available local source files. Async + yields between sources
 * so a host event loop (e.g. Electron's main process) stays responsive.
 * Silently skips sources whose paths don't exist — that's the common
 * case for users who only have one CLI.
 */
export async function scanLocalSources(): Promise<RawRow[]> {
  const claudeRows = await scanClaude();
  await yieldToEventLoop();
  const codexRows = await scanCodex();
  await yieldToEventLoop();
  const cursorRows = await scanCursor();
  await yieldToEventLoop();
  const grokRows = await scanGrok();
  return [...claudeRows, ...codexRows, ...cursorRows, ...grokRows];
}

export const LOCAL_SOURCE_PATHS = {
  claude: CLAUDE_HISTORY,
  codex: CODEX_HISTORY,
  cursorRoot: CURSOR_ROOT,
  grokSessionsRoot: GROK_SESSIONS_ROOT,
};
