import { readFile } from 'node:fs/promises';
import { request } from 'node:http';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';

const NOT_RUNNING = "Agent Relay desktop isn't running on this machine; open it, or use the Agent Relay MCP";
const DESKTOP_TIMEOUT = "Agent Relay desktop didn't answer in time";
const DEFAULT_TIMEOUT_MS = 15_000;
const MAX_RESPONSE_BYTES = 1024 * 1024;

export type RelayAgentWhere = 'this_computer' | 'cloud' | 'other_desktop';

export interface RelayAgent {
  name: string;
  address: string | null;
  kind: 'agent' | 'human' | 'service';
  where: RelayAgentWhere;
  status: 'active' | 'idle';
  last_seen_ms: number | null;
  description: string;
  is_self: boolean;
}

export interface RelayAgentRoster {
  agents: RelayAgent[];
  fetched_at_ms: number;
}

export interface RelayAgentsUnavailable {
  available: false;
  message: typeof NOT_RUNNING;
}

export interface ListRelayAgentsOptions {
  query?: string;
  where?: RelayAgentWhere;
  includeIdle?: boolean;
}

export interface RelaySocketRuntime {
  env?: Record<string, string | undefined>;
  home?: string;
  platform?: NodeJS.Platform;
  temporaryDirectory?: string;
  uid?: number;
  timeoutMs?: number;
}

export class RelayAgentsError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = 'RelayAgentsError';
  }
}

class SocketUnavailable extends Error {}
class SocketPermissionDenied extends Error {}

/** List live Relay participants through the local desktop app, never the cloud. */
export async function listRelayAgents(
  options: ListRelayAgentsOptions = {},
  runtime: RelaySocketRuntime = {},
): Promise<RelayAgentRoster | RelayAgentsUnavailable> {
  const query = new URLSearchParams();
  if (options.query !== undefined && options.query !== '') query.set('q', options.query);
  if (options.where !== undefined) query.set('where', options.where);
  if (options.includeIdle === true) query.set('include_idle', '1');
  const target = `/agents${query.size > 0 ? `?${query.toString()}` : ''}`;
  const paths = await relaySocketCandidates(runtime);
  let permissionError: SocketPermissionDenied | undefined;
  for (const path of paths) {
    try {
      return parseRoster(await requestRoster(path, target, runtime.timeoutMs ?? DEFAULT_TIMEOUT_MS));
    } catch (error) {
      if (error instanceof SocketUnavailable) continue;
      if (error instanceof SocketPermissionDenied) {
        permissionError = error;
        continue;
      }
      throw error;
    }
  }
  if (permissionError) {
    throw new RelayAgentsError('socket_access_denied', permissionError.message);
  }
  return { available: false, message: NOT_RUNNING };
}

/** Resolve desktop sockets in the public env, pointer-file, platform-default order. */
export async function relaySocketCandidates(runtime: RelaySocketRuntime = {}): Promise<string[]> {
  const env = runtime.env ?? process.env;
  const home = runtime.home ?? homedir();
  const platform = runtime.platform ?? process.platform;
  const temporary = runtime.temporaryDirectory ?? tmpdir();
  const uid = runtime.uid ?? process.getuid?.() ?? 0;
  const result: string[] = [];
  const add = (value: string | undefined) => {
    const path = value?.trim();
    if (path && !result.includes(path)) result.push(path);
  };

  add(env.AGENT_RELAY_SOCKET);
  try {
    add(await readFile(join(home, '.agentworkforce', 'desktop', 'relay-socket'), 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      // An unreadable/stale pointer must not prevent trying the platform path.
    }
  }

  if (platform === 'darwin') {
    add(join(home, 'Library', 'Application Support', 'com.agentrelay.desktop', 'run', 'relay.sock'));
    add(join(home, 'Library', 'Application Support', 'com.agentrelay.desktop.dev', 'run', 'relay.sock'));
    add(join(temporary, `agent-relay-${uid}`, 'relay.sock'));
    add(join(temporary, `agent-relay-dev-${uid}`, 'relay.sock'));
    add(join('/tmp', `agent-relay-${uid}`, 'relay.sock'));
    add(join('/tmp', `agent-relay-dev-${uid}`, 'relay.sock'));
  } else if (platform === 'linux') {
    const runtimeDirectory = env.XDG_RUNTIME_DIR?.trim();
    if (runtimeDirectory) add(join(runtimeDirectory, 'agent-relay', 'relay.sock'));
    else add(join(env.XDG_DATA_HOME?.trim() || join(home, '.local', 'share'), 'com.agentrelay.desktop', 'run', 'relay.sock'));
  }
  return result;
}

interface RosterResponse {
  status: number;
  body: Buffer;
}

/** Send one bounded HTTP request over a local Unix-domain socket. */
function requestRoster(path: string, target: string, timeoutMs: number): Promise<RosterResponse> {
  return new Promise((resolve, reject) => {
    let settled = false;
    let deadline: ReturnType<typeof setTimeout>;
    const finish = (action: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      action();
    };
    const clientRequest = request({
      socketPath: path,
      path: target,
      method: 'GET',
      headers: { Connection: 'close' },
    }, (response) => {
      const chunks: Buffer[] = [];
      let size = 0;
      response.on('data', (chunk: Buffer) => {
        size += chunk.length;
        if (size > MAX_RESPONSE_BYTES) {
          clientRequest.destroy();
          finish(() => reject(new RelayAgentsError('response_too_large', 'Agent Relay desktop returned too much roster data.')));
        } else {
          chunks.push(chunk);
        }
      });
      response.once('end', () => finish(() => resolve({
        status: response.statusCode ?? 0,
        body: Buffer.concat(chunks),
      })));
      response.once('error', (error) => finish(() => reject(
        new RelayAgentsError('invalid_response', `Agent Relay desktop response failed: ${error.message}`),
      )));
    });
    deadline = setTimeout(() => {
      finish(() => reject(new RelayAgentsError('timeout', DESKTOP_TIMEOUT)));
      clientRequest.destroy();
    }, timeoutMs);
    clientRequest.once('error', (error: NodeJS.ErrnoException) => {
      const message = `Cannot access Agent Relay desktop socket at ${path}: ${error.message}`;
      if (error.code === 'EACCES' || error.code === 'EPERM') {
        finish(() => reject(new SocketPermissionDenied(message)));
      } else if (error.code === 'ENOENT' || error.code === 'ENOTDIR'
        || error.code === 'ECONNREFUSED' || error.code === 'EINVAL') {
        finish(() => reject(new SocketUnavailable(error.message)));
      } else {
        finish(() => reject(new RelayAgentsError('socket_error', message)));
      }
    });
    clientRequest.end();
  });
}

/** Validate the desktop response envelope and reduce it to the documented roster. */
function parseRoster(response: RosterResponse): RelayAgentRoster {
  const status = response.status;
  let envelope: unknown;
  try { envelope = JSON.parse(response.body.toString('utf8')); }
  catch { throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned unreadable roster JSON.'); }
  const object = record(envelope);
  if (status < 200 || status >= 300 || object.ok !== true) {
    const detail = record(object.error);
    throw new RelayAgentsError(
      typeof detail.code === 'string' ? detail.code : `http_${status || 'error'}`,
      typeof detail.message === 'string' ? detail.message : `Agent Relay desktop returned HTTP ${status || 'error'}.`,
    );
  }
  const data = record(object.data);
  if (!Array.isArray(data.agents) || !Number.isSafeInteger(data.fetched_at_ms)) {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid roster.');
  }
  return {
    agents: data.agents.map(parseAgent),
    fetched_at_ms: data.fetched_at_ms as number,
  };
}

/** Validate and normalize one public roster entry. */
function parseAgent(value: unknown): RelayAgent {
  const agent = record(value);
  const kinds = ['agent', 'human', 'service'] as const;
  const locations = ['this_computer', 'cloud', 'other_desktop'] as const;
  const statuses = ['active', 'idle'] as const;
  if (typeof agent.name !== 'string' || agent.name.length === 0
    || !(agent.address === null || typeof agent.address === 'string')
    || !kinds.includes(agent.kind as typeof kinds[number])
    || !locations.includes(agent.where as typeof locations[number])
    || !statuses.includes(agent.status as typeof statuses[number])
    || !(agent.last_seen_ms === null || Number.isSafeInteger(agent.last_seen_ms))
    || typeof agent.description !== 'string'
    || typeof agent.is_self !== 'boolean') {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid roster entry.');
  }
  return {
    name: agent.name,
    address: agent.address as string | null,
    kind: agent.kind as RelayAgent['kind'],
    where: agent.where as RelayAgentWhere,
    status: agent.status as RelayAgent['status'],
    last_seen_ms: agent.last_seen_ms as number | null,
    description: agent.description.split(/\s+/u).filter(Boolean).join(' '),
    is_self: agent.is_self,
  };
}

/** Require a non-array JSON object. */
function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid roster response.');
  }
  return value as Record<string, unknown>;
}
