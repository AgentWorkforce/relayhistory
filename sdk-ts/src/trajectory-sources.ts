import { readFile, readdir, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { basename, dirname, join } from 'node:path';

export interface TrajectoryDecision {
  question: string;
  chosen: string;
  reasoning: string;
  alternatives: string[];
}

export interface TrajectoryRetrospective {
  summary: string | null;
  approach: string | null;
  learnings: string[];
  confidence: unknown;
}

export interface RawTrajectory {
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
  path: string;
  updatedMs: number;
  timestampMs: number;
}

export const DEFAULT_TRAJECTORY_SEARCH_ROOT = join(homedir(), 'Projects');

function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

async function safeStat(path: string): Promise<{ isDirectory: boolean; isFile: boolean; size: number; mtimeMs: number } | null> {
  try {
    const s = await stat(path);
    return { isDirectory: s.isDirectory(), isFile: s.isFile(), size: s.size, mtimeMs: s.mtimeMs };
  } catch {
    return null;
  }
}

async function readDirSafe(path: string): Promise<string[]> {
  try {
    return await readdir(path);
  } catch {
    return [];
  }
}

async function walkDirs(root: string, visit: (path: string) => Promise<void>): Promise<void> {
  const rootStat = await safeStat(root);
  if (!rootStat?.isDirectory) return;
  await visit(root);
  for (const name of await readDirSafe(root)) {
    await walkDirs(join(root, name), visit);
  }
}

function configuredTrajectoryRoots(): string[] {
  const env = process.env.TRAJECTORY_ROOT?.trim();
  if (!env) return [];
  return env
    .split(process.platform === 'win32' ? ';' : ':')
    .map((part) => part.trim())
    .filter(Boolean);
}

export function trajectoryRootDescription(): string {
  const roots = configuredTrajectoryRoots();
  if (roots.length > 0) return roots.join(', ');
  return join(DEFAULT_TRAJECTORY_SEARCH_ROOT, '**', '.trajectories');
}

async function defaultTrajectoryRoots(): Promise<string[]> {
  const projects = await safeStat(DEFAULT_TRAJECTORY_SEARCH_ROOT);
  if (!projects?.isDirectory) return [];
  const roots: string[] = [];
  await walkDirs(DEFAULT_TRAJECTORY_SEARCH_ROOT, async (path) => {
    if (basename(path) === '.trajectories') roots.push(path);
  });
  return roots.sort();
}

async function trajectoryRoots(): Promise<string[]> {
  const configured = configuredTrajectoryRoots();
  if (configured.length > 0) return configured;
  return defaultTrajectoryRoots();
}

async function compactedJsonFiles(root: string): Promise<string[]> {
  const rootStat = await safeStat(root);
  if (!rootStat) return [];
  if (rootStat.isFile && root.endsWith('.json')) return [root];

  const searchRoot = basename(root) === 'compacted' ? dirname(root) : root;
  const files: string[] = [];
  await walkDirs(searchRoot, async (path) => {
    if (basename(path) !== 'compacted') return;
    for (const name of await readDirSafe(path)) {
      if (!name.endsWith('.json') || name === 'index.json' || name === '.sync-state.json') continue;
      const full = join(path, name);
      if ((await safeStat(full))?.isFile) files.push(full);
    }
  });
  return files;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : [];
}

function parseTimestampMs(value: string | null, fallbackMs: number): number {
  if (!value) return fallbackMs;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : fallbackMs;
}

function normalizeDecision(value: unknown): TrajectoryDecision | null {
  if (!isRecord(value)) return null;
  return {
    question: asString(value.question) ?? '',
    chosen: asString(value.chosen) ?? '',
    reasoning: asString(value.reasoning) ?? '',
    alternatives: asStringArray(value.alternatives),
  };
}

function normalizeRetrospective(value: unknown): TrajectoryRetrospective {
  const obj = isRecord(value) ? value : {};
  return {
    summary: asString(obj.summary),
    approach: asString(obj.approach),
    learnings: asStringArray(obj.learnings),
    confidence: obj.confidence ?? null,
  };
}

function buildSearchText(t: Omit<RawTrajectory, 'searchText'>): string {
  const parts = [
    t.id,
    t.personaId,
    t.projectId,
    t.status,
    t.task.title,
    t.task.description,
    ...t.decisions.flatMap((d) => [d.question, d.chosen, d.reasoning, ...d.alternatives]),
    t.retrospective.summary,
    t.retrospective.approach,
    ...t.retrospective.learnings,
    t.retrospective.confidence == null ? null : String(t.retrospective.confidence),
  ];
  return parts.filter((part): part is string => typeof part === 'string' && part.length > 0).join('\n');
}

export async function readTrajectoryFile(path: string): Promise<RawTrajectory | null> {
  const fileStat = await safeStat(path);
  if (!fileStat?.isFile) return null;

  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(path, 'utf8'));
  } catch {
    return null;
  }
  if (!isRecord(raw)) return null;
  const id = asString(raw.id);
  if (!id) return null;
  if (
    raw.type === 'compacted' &&
    Array.isArray(raw.sourceTrajectories) &&
    !('task' in raw || 'retrospective' in raw || 'personaId' in raw || 'projectId' in raw)
  ) {
    return null;
  }

  const task = isRecord(raw.task) ? raw.task : {};
  const completedAt = asString(raw.completedAt);
  const startedAt = asString(raw.startedAt);
  const timestampMs = parseTimestampMs(completedAt ?? startedAt ?? asString(raw.compactedAt), Math.trunc(fileStat.mtimeMs));
  const trajectory: Omit<RawTrajectory, 'searchText'> = {
    id,
    version: typeof raw.version === 'number' && Number.isFinite(raw.version) ? raw.version : null,
    personaId: asString(raw.personaId),
    projectId: asString(raw.projectId),
    task: {
      title: asString(task.title),
      description: asString(task.description),
    },
    status: asString(raw.status),
    startedAt,
    completedAt,
    decisions: Array.isArray(raw.decisions)
      ? raw.decisions.map(normalizeDecision).filter((d): d is TrajectoryDecision => d !== null)
      : [],
    retrospective: normalizeRetrospective(raw.retrospective),
    path,
    updatedMs: Math.trunc(fileStat.mtimeMs),
    timestampMs,
  };
  return { ...trajectory, searchText: buildSearchText(trajectory) };
}

export async function scanLocalTrajectories(): Promise<RawTrajectory[]> {
  const roots = await trajectoryRoots();
  const seen = new Set<string>();
  const trajectories: RawTrajectory[] = [];
  for (const root of roots) {
    for (const file of await compactedJsonFiles(root)) {
      if (seen.has(file)) continue;
      seen.add(file);
      const trajectory = await readTrajectoryFile(file);
      if (trajectory) trajectories.push(trajectory);
    }
    await yieldToEventLoop();
  }
  return trajectories;
}
