import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, dirname } from 'node:path';

// Re-export the in-process cloud push so `ai-hist/cloud` exposes auth + push.
export {
  pushToCloud,
  buildOutboxBatch,
  promptHash,
  batchId,
  machineId,
  buildMachineIdentity,
  loadCursor,
  saveCursor,
  normalizeHomePath,
  type SyncCursor,
  type MachineIdentity,
  type ConvergenceEnvelope,
  type PushReport,
  type PushOptions,
  type OutboxBatch,
} from './cloud-push.js';

export interface RelayhistoryAuth {
  baseUrl: string;
  accessToken: string;
  refreshToken?: string;
}

export type LoginCloudResult =
  | { ok: true; auth: RelayhistoryAuth }
  | { ok: false; error: string };

function authPath(): string {
  const configDir = process.env.AI_HIST_CONFIG_DIR ?? join(homedir(), '.config', 'ai-hist');
  return join(configDir, 'auth.json');
}

async function saveRelayhistoryAuth(auth: RelayhistoryAuth): Promise<void> {
  const p = authPath();
  await mkdir(dirname(p), { recursive: true });
  await writeFile(p, JSON.stringify(auth, null, 2), { mode: 0o600 });
}

export async function loadStoredRelayhistoryAuth(): Promise<RelayhistoryAuth | null> {
  try {
    const body = await readFile(authPath(), 'utf-8');
    const parsed = JSON.parse(body) as Partial<RelayhistoryAuth>;
    if (typeof parsed.accessToken !== 'string' || typeof parsed.baseUrl !== 'string') return null;
    return parsed as RelayhistoryAuth;
  } catch {
    return null;
  }
}

export async function loginCloud(
  relayAccessToken: string,
  opts: { baseUrl?: string; label?: string } = {}
): Promise<LoginCloudResult> {
  const baseUrl = opts.baseUrl ?? process.env.AI_HIST_BASE_URL ?? 'https://history.agentrelay.com';
  const url = `${baseUrl.replace(/\/$/, '')}/v1/cli/login`;

  let resp: Response;
  try {
    resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ agentRelayToken: relayAccessToken, label: opts.label }),
      signal: AbortSignal.timeout(15_000),
    });
  } catch (err) {
    return { ok: false, error: `Network error: ${err instanceof Error ? err.message : String(err)}` };
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    return { ok: false, error: `Login failed (HTTP ${resp.status}): ${text.slice(0, 200)}` };
  }

  let payload: Record<string, unknown>;
  try {
    payload = (await resp.json()) as Record<string, unknown>;
  } catch {
    return { ok: false, error: 'Login response was not valid JSON' };
  }

  const accessToken = payload.accessToken;
  const refreshToken = payload.refreshToken;
  if (typeof accessToken !== 'string') {
    return { ok: false, error: 'Login response missing accessToken' };
  }

  const auth: RelayhistoryAuth = {
    baseUrl,
    accessToken,
    ...(typeof refreshToken === 'string' ? { refreshToken } : {}),
  };

  try {
    await saveRelayhistoryAuth(auth);
  } catch (err) {
    return {
      ok: false,
      error: `Failed to save auth: ${err instanceof Error ? err.message : String(err)}`,
    };
  }

  return { ok: true, auth };
}
