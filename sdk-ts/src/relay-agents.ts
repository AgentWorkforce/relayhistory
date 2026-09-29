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

export interface RelayRegistration {
  name: string;
  address: string;
  already_registered: boolean;
}

export interface RelayLeaveResult {
  registered: false;
  already_unregistered: boolean;
}

export interface RelayStatus {
  name: string | null;
  session_id: string;
  registered: boolean;
}

export interface JoinRelayOptions {
  name?: string;
  description?: string;
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
  const response = await relayRequest('GET', target, undefined, runtime);
  return isUnavailable(response) ? response : parseRoster(parseEnvelope(response));
}

/** Put the kernel-identified session hosting this process on Relay through the desktop app. */
export async function joinRelay(
  options: JoinRelayOptions = {},
  runtime: RelaySocketRuntime = {},
): Promise<RelayRegistration | RelayAgentsUnavailable> {
  const body = {
    ...(options.name !== undefined ? { name: options.name } : {}),
    ...(options.description !== undefined ? { description: options.description } : {}),
  };
  const response = await relayRequest('POST', '/register', body, runtime);
  return isUnavailable(response) ? response : parseMutation(
    response, 'POST', '/register', parseRegistration,
  );
}

/** Remove the kernel-identified session hosting this process from Relay through the desktop app. */
export async function leaveRelay(
  runtime: RelaySocketRuntime = {},
): Promise<RelayLeaveResult | RelayAgentsUnavailable> {
  const response = await relayRequest('DELETE', '/register', undefined, runtime);
  return isUnavailable(response) ? response : parseMutation(
    response, 'DELETE', '/register', parseLeave,
  );
}

/** Report the session hosting this process's local Relay registration state. */
export async function relayStatus(
  runtime: RelaySocketRuntime = {},
): Promise<RelayStatus | RelayAgentsUnavailable> {
  const response = await relayRequest('GET', '/whoami', undefined, runtime);
  return isUnavailable(response) ? response : parseStatus(parseEnvelope(response));
}

async function relayRequest(
  method: 'GET' | 'POST' | 'DELETE',
  target: string,
  body: unknown | undefined,
  runtime: RelaySocketRuntime,
): Promise<RelayResponse | RelayAgentsUnavailable> {
  const paths = await relaySocketCandidates(runtime);
  let permissionError: SocketPermissionDenied | undefined;
  let timeoutError: RelayAgentsError | undefined;
  for (const path of paths) {
    try {
      return await requestSocket(path, method, target, body, runtime.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    } catch (error) {
      if (error instanceof SocketUnavailable) continue;
      if (error instanceof SocketPermissionDenied) {
        permissionError = error;
        continue;
      }
      if (error instanceof RelayAgentsError && error.code === 'timeout') {
        timeoutError = error;
        continue;
      }
      throw error;
    }
  }
  if (timeoutError) throw timeoutError;
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
    add(join(env.XDG_DATA_HOME?.trim() || join(home, '.local', 'share'), 'com.agentrelay.desktop', 'run', 'relay.sock'));
  }
  return result;
}

interface RelayResponse {
  status: number;
  body: Buffer;
}

function indeterminate(method: 'POST' | 'DELETE', target: string): RelayAgentsError {
  return new RelayAgentsError(
    'indeterminate_result',
    `Agent Relay desktop may have completed ${method} ${target} before the connection ended; use relay_status to inspect the result.`,
  );
}

/** Send one bounded HTTP request over a local Unix-domain socket. */
function requestSocket(
  path: string, method: 'GET' | 'POST' | 'DELETE', target: string, body: unknown | undefined, timeoutMs: number,
): Promise<RelayResponse> {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
    let settled = false;
    let requestSent = false;
    let deadline: ReturnType<typeof setTimeout>;
    const unavailable = (message: string): Error => {
      if (method !== 'GET' && requestSent) {
        return indeterminate(method, target);
      }
      return new SocketUnavailable(message);
    };
    const responseFailure = (message: string): Error => method === 'GET'
      ? new RelayAgentsError('invalid_response', `Agent Relay desktop response failed: ${message}`)
      : unavailable(message);
    const finish = (action: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(deadline);
      action();
    };
    const clientRequest = request({
      socketPath: path,
      path: target,
      method,
      headers: {
        Connection: 'close',
        ...(payload ? { 'Content-Type': 'application/json', 'Content-Length': payload.length } : {}),
      },
    }, (response) => {
      requestSent = true;
      const chunks: Buffer[] = [];
      let size = 0;
      response.on('data', (chunk: Buffer) => {
        size += chunk.length;
        if (size > MAX_RESPONSE_BYTES) {
          clientRequest.destroy();
          finish(() => reject(method === 'GET'
            ? new RelayAgentsError('response_too_large', 'Agent Relay desktop returned too much data.')
            : indeterminate(method, target)));
        } else {
          chunks.push(chunk);
        }
      });
      response.once('aborted', () => finish(() => reject(
        responseFailure('the connection ended before the complete response arrived'),
      )));
      response.once('error', (error) => finish(() => reject(responseFailure(error.message))));
      response.once('end', () => finish(() => {
        if (!response.complete) {
          reject(responseFailure('the connection ended before the complete response arrived'));
        } else {
          resolve({ status: response.statusCode ?? 0, body: Buffer.concat(chunks) });
        }
      }));
    });
    clientRequest.once('socket', (socket) => {
      const markSent = () => { requestSent = true; };
      if (socket.connecting) socket.once('connect', markSent);
      else markSent();
    });
    deadline = setTimeout(() => {
      finish(() => reject(!requestSent
        ? new SocketUnavailable(DESKTOP_TIMEOUT)
        : method !== 'GET'
          ? indeterminate(method, target)
          : new RelayAgentsError('timeout', DESKTOP_TIMEOUT)));
      clientRequest.destroy();
    }, timeoutMs);
    clientRequest.once('error', (error: NodeJS.ErrnoException) => {
      if (method !== 'GET' && requestSent) {
        finish(() => reject(unavailable(error.message)));
        return;
      }
      const message = `Cannot access Agent Relay desktop socket at ${path}: ${error.message}`;
      if (error.code === 'EACCES' || error.code === 'EPERM') {
        finish(() => reject(new SocketPermissionDenied(message)));
      } else if (error.code === 'ENOENT' || error.code === 'ENOTDIR'
        || error.code === 'ECONNREFUSED' || error.code === 'EINVAL'
        || error.code === 'EAGAIN' || error.code === 'ETIMEDOUT') {
        finish(() => reject(new SocketUnavailable(error.message)));
      } else {
        finish(() => reject(new RelayAgentsError('socket_error', message)));
      }
    });
    clientRequest.end(payload);
  });
}

function parseEnvelope(response: RelayResponse): unknown {
  const status = response.status;
  let envelope: unknown;
  try { envelope = JSON.parse(response.body.toString('utf8')); } catch {
    if (status < 200 || status >= 300) throw httpError(status);
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned unreadable JSON.');
  }
  if (status < 200 || status >= 300) {
    const object = optionalRecord(envelope);
    const detail = optionalRecord(object?.error);
    throw new RelayAgentsError(
      typeof detail?.code === 'string' ? detail.code : `http_${status || 'error'}`,
      typeof detail?.message === 'string' ? detail.message : `Agent Relay desktop returned HTTP ${status || 'error'}.`,
    );
  }
  const object = record(envelope);
  if (object.ok !== true) {
    const detail = optionalRecord(object.error);
    throw new RelayAgentsError(
      typeof detail?.code === 'string' ? detail.code : 'invalid_response',
      typeof detail?.message === 'string' ? detail.message : 'Agent Relay desktop returned an invalid roster response.',
    );
  }
  return object.data;
}

function parseMutation<T>(
  response: RelayResponse,
  method: 'POST' | 'DELETE',
  target: string,
  parse: (value: unknown) => T,
): T {
  try {
    return parse(parseEnvelope(response));
  } catch (error) {
    if (response.status >= 200 && response.status < 300
      && error instanceof RelayAgentsError && error.code === 'invalid_response') {
      throw indeterminate(method, target);
    }
    throw error;
  }
}

function parseRoster(value: unknown): RelayAgentRoster {
  const data = record(value);
  if (!Array.isArray(data.agents) || !Number.isSafeInteger(data.fetched_at_ms)) {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid roster.');
  }
  return {
    agents: data.agents.map(parseAgent),
    fetched_at_ms: data.fetched_at_ms as number,
  };
}

function httpError(status: number): RelayAgentsError {
  return new RelayAgentsError(
    `http_${status || 'error'}`, `Agent Relay desktop returned HTTP ${status || 'error'}.`,
  );
}

function optionalRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

function parseRegistration(value: unknown): RelayRegistration {
  const data = record(value);
  if (typeof data.name !== 'string' || data.name.length === 0
    || typeof data.address !== 'string' || data.address.length === 0
    || typeof data.already_registered !== 'boolean') {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid registration.');
  }
  return { name: data.name, address: data.address, already_registered: data.already_registered };
}

function parseLeave(value: unknown): RelayLeaveResult {
  const data = record(value);
  if (data.registered !== false || typeof data.already_unregistered !== 'boolean') {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid leave result.');
  }
  return { registered: false, already_unregistered: data.already_unregistered };
}

function parseStatus(value: unknown): RelayStatus {
  const data = record(value);
  if (!(data.name === null || (typeof data.name === 'string' && data.name.length > 0))
    || typeof data.session_id !== 'string' || data.session_id.length === 0
    || typeof data.registered !== 'boolean') {
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid relay status.');
  }
  return { name: data.name as string | null, session_id: data.session_id, registered: data.registered };
}

function isUnavailable(value: unknown): value is RelayAgentsUnavailable {
  return typeof value === 'object' && value !== null && (value as { available?: unknown }).available === false;
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
    throw new RelayAgentsError('invalid_response', 'Agent Relay desktop returned an invalid response.');
  }
  return value as Record<string, unknown>;
}
