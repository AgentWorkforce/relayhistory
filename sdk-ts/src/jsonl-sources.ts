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
import { join } from 'node:path';

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

export type Source = 'claude' | 'codex' | 'cursor';

export interface RawRow {
  source: Source;
  sessionId: string | null;
  project: string | null;
  prompt: string;
  timestampMs: number;
}

const CLAUDE_HISTORY = join(homedir(), '.claude', 'history.jsonl');
const CODEX_HISTORY = join(homedir(), '.codex', 'history.jsonl');
const CURSOR_ROOT = join(homedir(), '.cursor', 'projects');

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
        });
      }
    }
    // Yield between project dirs so very large cursor histories don't
    // hold the event loop for seconds at a time.
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
  return [...claudeRows, ...codexRows, ...cursorRows];
}

export const LOCAL_SOURCE_PATHS = {
  claude: CLAUDE_HISTORY,
  codex: CODEX_HISTORY,
  cursorRoot: CURSOR_ROOT,
};
