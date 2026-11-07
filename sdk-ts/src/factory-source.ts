/** Factory Sessions API evidence projection. Hosts own credentials, pagination,
 * scheduling and delivery; RelayHistory owns interpreting provider JSON. */
export type FactoryMessage = Record<string, unknown>;
export type FactoryEvidence = {
  v: 1;
  source: 'droid';
  sessionId: string;
  eventId: string;
  lens: 'history';
  kind: 'prompt' | 'assistant' | 'tool_call' | 'marker';
  ts: string;
  content: string;
  actorRole: string | null;
  toolCalls?: Array<{ id: string; tool: string; input: unknown }>;
  record: {
    messageId: string;
    messageVersion: null;
    updatedAt: number | null;
    blockIds: string[];
    raw: FactoryMessage;
    toolResults?: Array<{ id: string; status: string | null; output?: unknown; error?: unknown }>;
  };
};
const object = (value: unknown): FactoryMessage =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as FactoryMessage : {};
function identity(value: unknown, code: string): string {
  if (typeof value !== 'string' || !value.trim() || value.length > 256 || /[\x00-\x1f\x7f]/.test(value))
    throw new Error(code);
  return value;
}
/** IDs remain opaque. Links and path delimiters must not become API paths. */
export function factorySessionId(value: string): string {
  const id = identity(value, 'FACTORY_INVALID_SESSION_ID');
  if (!/^[A-Za-z0-9_-]{1,256}$/.test(id)) throw new Error('FACTORY_INVALID_SESSION_ID');
  return id;
}
function timestamp(value: unknown, code: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0 || !Number.isFinite(new Date(value).getTime()))
    throw new Error(code);
  return value;
}
function readable(value: unknown): string {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value.flatMap(part => {
    const block = object(part);
    return block.type === 'text' && typeof block.text === 'string' ? [block.text] : [];
  }).join('\n');
  return '';
}
/** Project a complete message set (all pages). Never invent IDs, timestamps or usage. */
export function factoryEvidence(session: string, messages: unknown[]): FactoryEvidence[] {
  const sessionId = factorySessionId(session);
  const seen = new Set<string>();
  return messages.flatMap((value): FactoryEvidence[] => {
    const raw = object(value);
    const messageId = identity(raw.id, 'FACTORY_MISSING_MESSAGE_ID');
    if (seen.has(messageId)) throw new Error('FACTORY_DUPLICATE_MESSAGE_ID');
    seen.add(messageId);
    const createdAt = timestamp(raw.createdAt, 'FACTORY_INVALID_MESSAGE_TIMESTAMP');
    const updatedAt = raw.updatedAt === undefined ? null : timestamp(raw.updatedAt, 'FACTORY_INVALID_MESSAGE_UPDATED_AT');
    if (!Array.isArray(raw.content)) throw new Error('FACTORY_INVALID_MESSAGE_CONTENT');
    const role = typeof raw.role === 'string' ? raw.role : null;
    const blocks = raw.content.map(object);
    const prose = readable(raw.content);
    const blockIds = blocks.flatMap(block => typeof block.id === 'string' ? [identity(block.id, 'FACTORY_INVALID_BLOCK_ID')] : []);
    if (new Set(blockIds).size !== blockIds.length) throw new Error('FACTORY_DUPLICATE_BLOCK_ID');
    const toolCalls = role === 'assistant' ? blocks.flatMap(block => {
      if (block.type !== 'tool_use') return [];
      return [{ id: identity(block.id, 'FACTORY_INVALID_TOOL_ID'), tool: identity(block.name, 'FACTORY_INVALID_TOOL_NAME'), input: block.input ?? null }];
    }) : [];
    const toolResults = role === 'user' || role === 'tool' ? blocks.flatMap(block => {
      if (block.type !== 'tool_result') return [];
      const id = identity(block.toolUseId, 'FACTORY_INVALID_TOOL_RESULT_ID');
      if (block.isError !== undefined && typeof block.isError !== 'boolean') throw new Error('FACTORY_INVALID_TOOL_ERROR');
      return [{ id, status: block.isError === true ? 'errored' : block.isError === false ? 'completed' : null,
        ...(Object.hasOwn(block, 'content') ? { output: block.content } : {}),
        ...(block.isError === true ? { error: block.content ?? null } : {}),
      }];
    }) : [];
    const resultText = toolResults.map(result => readable(result.output)).filter(text => text.trim()).join('\n');
    const onlyResults = !prose.trim() && toolResults.length > 0 && blocks.every(block => block.type === 'tool_result' || (block.type === 'text' && typeof block.text === 'string' && !block.text.trim()));
    const mixed = role === 'user' && !!prose.trim() && toolResults.length > 0;
    const row: FactoryEvidence = {
      v: 1, source: 'droid', sessionId,
      eventId: `factory:${encodeURIComponent(sessionId)}:${encodeURIComponent(messageId)}`,
      lens: 'history',
      kind: onlyResults ? 'tool_call' : role === 'user' && prose.trim() ? 'prompt' : role === 'assistant' && prose.trim() ? 'assistant' : toolCalls.length || role === 'tool' ? 'tool_call' : 'marker',
      ts: new Date(createdAt).toISOString(),
      content: mixed ? prose : [prose, resultText].filter(text => text.trim()).join('\n'),
      actorRole: onlyResults ? 'tool' : role,
      ...(toolCalls.length ? { toolCalls } : {}),
      record: { messageId, messageVersion: null, updatedAt, blockIds, raw, ...(toolResults.length ? { toolResults } : {}) },
    };
    if (!mixed) return [row];
    return [row, { ...row, eventId: `factory-tool-results:${encodeURIComponent(sessionId)}:${encodeURIComponent(messageId)}`, kind: 'tool_call', actorRole: 'tool', content: resultText }];
  });
}
/** Validate one documented GET /sessions/{id}/messages envelope. A host must
 * fetch every page before treating its snapshot as complete or retiring rows. */
export function factoryMessagePage(value: unknown): { messages: FactoryMessage[]; nextCursor: string | null; hasMore: boolean } {
  const root = object(value);
  const pagination = object(root.pagination);
  if (!Array.isArray(root.messages) || typeof pagination.hasMore !== 'boolean' ||
      !(pagination.nextCursor === null || typeof pagination.nextCursor === 'string') ||
      (pagination.hasMore && !(typeof pagination.nextCursor === 'string' && pagination.nextCursor.trim())) ||
      (!pagination.hasMore && pagination.nextCursor !== null)) throw new Error('FACTORY_INVALID_MESSAGE_PAGE');
  const messages = root.messages.map(value => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('FACTORY_INVALID_MESSAGE_PAGE');
    return value as FactoryMessage;
  });
  return { messages, nextCursor: pagination.nextCursor, hasMore: pagination.hasMore };
}

/** Origin-independent row identities used by ai-hist's change feed and exports.
 * Hosts supply origin/revision fences and submit these through canonical delivery. */
export type FactoryCanonicalRecord = {
  kind: 'session_event' | 'tool_call' | 'session_marker';
  source: 'droid';
  session_id: string;
  key: string[];
  record_id: string;
  payload: Record<string, unknown>;
};
export async function factoryCanonicalRecordId(key: readonly string[]): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(key)));
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
}
function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    // UTF-8 / Unicode scalar ordering matches serde_json's ordered Map.
    const compare = (a: string, b: string): number => {
      const left = Array.from(a, c => c.codePointAt(0)!);
      const right = Array.from(b, c => c.codePointAt(0)!);
      for (let i = 0; i < Math.min(left.length, right.length); i++) if (left[i] !== right[i]) return left[i] - right[i];
      return left.length - right.length;
    };
    return `{${Object.keys(value).sort(compare).map(key => `${JSON.stringify(key)}:${stableJson((value as FactoryMessage)[key])}`).join(',')}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error('FACTORY_NON_JSON_EVIDENCE');
  return encoded;
}
/** Per-block API projection with the SAME native message/ordinal keys as local
 * Droid v2 acquisition. API-specific raw fields stay in non-activity markers.
 * A complete snapshot must be acquired before calling this function. */
export async function factoryCanonicalEvidence(session: string, messages: unknown[]): Promise<FactoryCanonicalRecord[]> {
  factoryEvidence(session, messages); // Shared identity/schema validation.
  const sessionId = factorySessionId(session);
  const out: FactoryCanonicalRecord[] = [];
  const calls = new Map<string, FactoryCanonicalRecord>();
  const add = async (kind: FactoryCanonicalRecord['kind'], uid: string, payload: Record<string, unknown>) => {
    const key = [kind, 'droid', sessionId, uid];
    const row: FactoryCanonicalRecord = { kind, source: 'droid', session_id: sessionId, key, record_id: await factoryCanonicalRecordId(key), payload: { source: 'droid', session_id: sessionId, ...payload } };
    out.push(row);
    return row;
  };
  const results: Array<{ id: string; error: boolean }> = [];
  for (const value of messages) {
    const raw = object(value);
    const mid = String(raw.id);
    const ts = Number(raw.createdAt);
    const blocks = raw.content as unknown[];
    const marker = (uid: string, kind: string, payload: unknown) => add('session_marker', uid, { marker_uid: uid, message_id: mid, ts_ms: ts, kind, payload_json: stableJson(payload) });
    await marker(`api-message:${mid}`, 'factory_api_message', raw);
    if (!['user', 'assistant', 'tool'].includes(String(raw.role))) continue;
    const base = { message_id: mid, parent_id: typeof raw.parentId === 'string' ? raw.parentId : null, ts_ms: ts, model: typeof raw.modelId === 'string' ? raw.modelId : null, provider: typeof raw.apiProvider === 'string' ? raw.apiProvider : null, raw_kind: 'message' };
    if (!blocks.length && raw.role !== 'tool') await add('session_event', `${mid}:0`, { ...base, event_uid: `${mid}:0`, role: raw.role, kind: 'text', text: null });
    for (const [index, value] of blocks.entries()) {
      const block = object(value);
      const uid = `${mid}:${index}`;
      const kind = block.type;
      if (!['text', 'thinking', 'tool_use', 'tool_result'].includes(String(kind)) || (raw.role === 'tool' && kind !== 'tool_result')) {
        await marker(`block:${uid}`, 'opaque_block', { type: 'opaque_block', block, messageId: mid, timestamp: new Date(ts).toISOString() });
        continue;
      }
      const event: Record<string, unknown> = { ...base, event_uid: uid, role: kind === 'tool_result' ? 'tool_result' : raw.role, kind };
      if (kind === 'text' || kind === 'thinking') {
        const text = block[kind];
        if (typeof text !== 'string') throw new Error('FACTORY_INVALID_BLOCK_TEXT');
        event.text = text;
      } else if (kind === 'tool_use') {
        const id = identity(block.id, 'FACTORY_INVALID_TOOL_ID');
        if (calls.has(id)) throw new Error('FACTORY_DUPLICATE_TOOL_ID');
        const name = identity(block.name, 'FACTORY_INVALID_TOOL_NAME');
        event.tool_use_id = id;
        event.text = stableJson(block);
        calls.set(id, await add('tool_call', id, { message_id: mid, tool_use_id: id, name, target: typeof object(block.input).file_path === 'string' ? object(block.input).file_path : null, args_json: stableJson(block.input ?? null), is_error: null, ts_ms: ts }));
      } else {
        Object.assign(event, await canonicalToolResult(block));
        if (typeof block.isError === 'boolean') results.push({ id: String(event.tool_use_id), error: block.isError });
      }
      await add('session_event', uid, event);
    }
  }
  for (const result of results) {
    const call = calls.get(result.id);
    if (call) call.payload.is_error = result.error ? 1 : 0;
  }
  return out;
}
async function canonicalToolResult(block: FactoryMessage): Promise<Record<string, unknown>> {
  const id = identity(block.toolUseId, 'FACTORY_INVALID_TOOL_RESULT_ID');
  if (block.isError !== undefined && typeof block.isError !== 'boolean') throw new Error('FACTORY_INVALID_TOOL_ERROR');
  const facts: Record<string, unknown> = { tool_use_id: id, text: readable(block.content), event_source: 'tool_result' };
  if (typeof block.isError === 'boolean') {
    facts.result_status = block.isError ? 'errored' : 'completed';
    facts.error_signal = block.isError ? 'tool_result.is_error' : null;
  }
  if (block.content !== null && block.content !== undefined) {
    const measured = typeof block.content === 'string' ? block.content : stableJson(block.content);
    const bytes = new TextEncoder().encode(measured);
    facts.payload_bytes = bytes.length;
    facts.payload_hash = (await factoryRawHash(bytes)).slice(0, 16);
    const lower = measured.replace(/[A-Z]/g, c => c.toLowerCase());
    facts.payload_truncated = ['<system-truncated>', '[truncated]', 'output truncated', 'result truncated', 'response truncated', 'truncated to '].some(marker => lower.includes(marker)) ? 1 : 0;
  }
  return facts;
}
async function factoryRawHash(bytes: Uint8Array): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
}
