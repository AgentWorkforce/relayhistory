import { readFile } from 'node:fs/promises';
import { createConnection } from 'node:net';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';

const NOT_RUNNING = "Agent Relay desktop isn't running on this machine; open it, or use the Agent Relay MCP";
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
  for (const path of paths) {
    try {
      return parseRoster(await requestRoster(path, target, runtime.timeoutMs ?? 1_500));
    } catch (error) {
      if (error instanceof SocketUnavailable) continue;
      throw error;
    }
  }
  return { available: false, message: NOT_RUNNING };
}

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

function requestRoster(path: string, target: string, timeoutMs: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(path);
    const chunks: Buffer[] = [];
    let size = 0;
    let settled = false;
    const finish = (action: () => void) => {
      if (settled) return;
      settled = true;
      action();
    };
    socket.setTimeout(timeoutMs, () => {
      socket.destroy();
      finish(() => reject(new SocketUnavailable('socket timed out')));
    });
    socket.once('connect', () => {
      socket.end(`GET ${target} HTTP/1.1\r\nHost: relay\r\nConnection: close\r\n\r\n`);
    });
    socket.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_RESPONSE_BYTES) {
        socket.destroy();
        finish(() => reject(new RelayAgentsError('response_too_large', 'Agent Relay desktop returned too much roster data.')));
      } else {
        chunks.push(chunk);
      }
    });
    socket.once('end', () => finish(() => resolve(Buffer.concat(chunks))));
    socket.once('error', (error) => finish(() => reject(new SocketUnavailable(error.message))));
  });
}

function parseRoster(response: Buffer): RelayAgentRoster {
  const separator = response.indexOf('\r\n\r\n');
  if (separator < 0) throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an unreadable HTTP response.');
  const head = response.subarray(0, separator).toString('utf8');
  const status = Number(/^HTTP\/1\.[01] (\d{3})\b/.exec(head)?.[1] ?? 0);
  let envelope: unknown;
  try { envelope = JSON.parse(response.subarray(separator + 4).toString('utf8')); }
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

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid roster response.');
  }
  return value as Record<string, unknown>;
}
