/** Amp evidence projection. Cloud owns credentials, scheduling and delivery;
 * this module owns interpreting provider JSON. Unknown fields stay in raw. */
export type AmpMessage = Record<string, unknown>;
export type AmpEvidence = {
  v: 1;
  source: 'amp';
  sessionId: string;
  eventId: string;
  lens: 'history';
  kind: 'prompt' | 'assistant' | 'tool_call' | 'marker';
  ts: string;
  content: string;
  actorRole: string | null;
  /** One envelope per native message; mixed/multiple calls retain their own native identities. */
  toolCalls?: Array<{ id: string; tool: string; input: unknown }>;
  record: {
    messageId: string;
    messageVersion: number | null;
    raw: AmpMessage;
    toolResults?: Array<{ id: string; status: string | null; output?: unknown; error?: unknown }>;
    timestampInferred?: true;
  };
};
const object = (v: unknown): AmpMessage =>
  v !== null && typeof v === 'object' && !Array.isArray(v)
    ? (v as AmpMessage)
    : {};
export function ampThreadId(value: string): string {
  const id = value
    .trim()
    .replace(/^https:\/\/ampcode\.com\/threads\//, '')
    .replace(/\/$/, '');
  if (!/^T-[A-Za-z0-9-]{6,100}$/.test(id))
    throw new Error('AMP_INVALID_THREAD_ID');
  return id;
}
/** Native export and External API share message identity; no position ids or current-time timestamps. */
export function ampEvidence(
  threadId: string,
  messages: unknown[],
  fallbackTime?: string,
): AmpEvidence[] {
  const sessionId = ampThreadId(threadId);
  const seen = new Set<string>();
  return messages.map((value) => {
    const raw = object(value);
    const native = Object.hasOwn(raw, 'protocolMessageID') || Object.hasOwn(raw, 'protocolMessageVersion');
    const id = native ? raw.protocolMessageID : raw.messageID;
    if (
      (typeof id !== 'string' && typeof id !== 'number') ||
      !String(id).trim() ||
      String(id).length > 256
    )
      throw new Error('AMP_MISSING_MESSAGE_ID');
    if (typeof id === 'number' && (!Number.isSafeInteger(id) || id < 0))
      throw new Error('AMP_INVALID_MESSAGE_ID');
    const messageId = String(id);
    if (seen.has(messageId)) throw new Error('AMP_DUPLICATE_MESSAGE_ID');
    seen.add(messageId);
    const version = native ? raw.protocolMessageVersion : raw.messageVersion;
    if (version === null)
      throw new Error('AMP_INVALID_MESSAGE_VERSION');
    if (
      version !== undefined &&
      (!Number.isSafeInteger(version) || Number(version) < 0)
    )
      throw new Error('AMP_INVALID_MESSAGE_VERSION');
    const role = typeof raw.role === 'string' ? raw.role : null;
    const prose =
      typeof raw.content === 'string'
        ? raw.content
        : Array.isArray(raw.content)
          ? raw.content
              .flatMap((block: unknown) => {
                const b = object(block);
                return b.type === 'text' && typeof b.text === 'string'
                  ? [b.text]
                  : [];
              })
              .join('\n')
          : '';
    const nativeDate = raw.createdAt ?? object(raw.meta).sentAt;
    const date = nativeDate ?? fallbackTime;
    const time =
      typeof date === 'number'
        ? date
        : typeof date === 'string'
          ? Date.parse(date)
          : NaN;
    if (!Number.isFinite(time) || !Number.isFinite(new Date(time).getTime()))
      throw new Error('AMP_MISSING_MESSAGE_TIMESTAMP');
    const toolCalls = role === 'assistant' && Array.isArray(raw.content)
      ? raw.content.flatMap((block) => {
          const tool = object(block);
          return tool.type === 'tool_use' && typeof tool.id === 'string' && tool.id && typeof tool.name === 'string' && tool.name
            ? [{ id: tool.id, tool: tool.name, input: tool.input ?? null }]
            : [];
        })
      : [];
    const toolResults = role === 'user' && Array.isArray(raw.content)
      ? raw.content.flatMap((block) => {
          const result = object(block);
          if (result.type !== 'tool_result' || typeof result.toolUseID !== 'string' || !result.toolUseID) return [];
          const run = object(result.run);
          return [{
            id: result.toolUseID,
            status: typeof run.status === 'string' ? run.status : null,
            ...(Object.hasOwn(run, 'result') ? { output: run.result } : {}),
            ...(Object.hasOwn(run, 'error') ? { error: run.error } : {}),
          }];
        })
      : [];
    const onlyToolResults = !prose.trim() && toolResults.length > 0 &&
      Array.isArray(raw.content) && raw.content.every((block) => {
        const part = object(block);
        return (part.type === 'tool_result' && typeof part.toolUseID === 'string' && !!part.toolUseID) ||
          (part.type === 'text' && typeof part.text === 'string' && !part.text.trim());
      });
    const content = onlyToolResults
      ? toolResults.flatMap((result) => [result.output, result.error]
          .map((value) => typeof value === 'string' ? value : value == null ? '' : JSON.stringify(value))
          .filter((text) => text.trim())).join('\n')
      : prose;
    return {
      v: 1,
      source: 'amp',
      sessionId,
      eventId: `amp:${sessionId}:${messageId}`,
      lens: 'history',
      kind: onlyToolResults
        ? 'tool_call'
        : role === 'user' && content.trim()
        ? 'prompt'
        : role === 'assistant' && content.trim()
          ? 'assistant'
          : toolCalls.length || role === 'tool'
            ? 'tool_call'
            : 'marker',
      ts: new Date(time).toISOString(),
      content,
      actorRole: onlyToolResults ? 'tool' : role,
      ...(toolCalls.length ? { toolCalls } : {}),
      record: {
        messageId,
        messageVersion: version === undefined ? null : Number(version),
        raw,
        ...(toolResults.length ? { toolResults } : {}),
        ...(nativeDate == null ? { timestampInferred: true as const } : {}),
      },
    };
  });
}
export function ampExport(threadId: string, value: unknown): AmpEvidence[] {
  const root = object(value);
  if (root.id !== ampThreadId(threadId) || !Array.isArray(root.messages))
    throw new Error('AMP_INVALID_EXPORT');
  const created =
    typeof root.createdAt === 'string'
      ? root.createdAt
      : typeof root.created === 'number' &&
          Number.isFinite(new Date(root.created).getTime())
        ? new Date(root.created).toISOString()
        : undefined;
  return ampEvidence(threadId, root.messages, created);
}

/** Provider-owned CLI acquisition and remote submission contract. Hosts supply an isolated process runtime. */
export type AmpOperation =
  /** Bounded recent-thread picker; a host may select any other owned thread by ID and verify via export. */
  | { kind: 'discover' }
  | { kind: 'export'; threadId: string }
  | { kind: 'inject'; threadId: string; text: string };
export const AMP_CLI_VERSION = '0.0.1793857728-g655a2c';
export const AMP_CLI_BRIDGE = `const {spawnSync}=require('node:child_process');
let op;try{op=JSON.parse(process.env.RELAY_AMP_OPERATION)}catch{}
if(op&&typeof op.threadId==='string'){op.threadId=op.threadId.trim();const prefix='https://ampcode.com/threads/';if(op.threadId.startsWith(prefix))op.threadId=op.threadId.slice(prefix.length);if(op.threadId.endsWith('/'))op.threadId=op.threadId.slice(0,-1)}
if(!op||!['discover','export','inject'].includes(op.kind)||(op.kind!=='discover'&&(typeof op.threadId!=='string'||!/^T-[A-Za-z0-9-]{6,100}$/.test(op.threadId)))||(op.kind==='inject'&&(typeof op.text!=='string'||!op.text||op.text.includes(String.fromCharCode(0))))){process.stdout.write(JSON.stringify({ok:false,outcome:'rejected',code:'amp_invalid_operation'}));process.exit(0)}
const args=['--log-file','/dev/null'];
if(op.kind==='discover')args.push('threads','search','author:me','--json','--limit','100');
else if(op.kind==='export')args.push('threads','export',op.threadId);
else if(op.kind==='inject')args.push('threads','continue',op.threadId,'-ox');
const r=spawnSync(process.env.RELAY_AMP_CLI_PATH||'/tmp/relay-amp/node_modules/.bin/amp',args,{encoding:'utf8',input:op.kind==='inject'?op.text:undefined,timeout:45000,maxBuffer:16777216,env:{...process.env,RELAY_AMP_OPERATION:''}});
if(r.status!==0){process.stdout.write(JSON.stringify({ok:false,outcome:op.kind==='inject'?'unknown':'rejected',code:'amp_command_failed'}));process.exit(0)}
if(op.kind==='inject')process.stdout.write(JSON.stringify({ok:true,value:{accepted:true}}));
else{try{process.stdout.write(JSON.stringify({ok:true,value:JSON.parse(r.stdout)}))}catch{process.stdout.write(JSON.stringify({ok:false,outcome:'rejected',code:'amp_invalid_json'}))}}`;
