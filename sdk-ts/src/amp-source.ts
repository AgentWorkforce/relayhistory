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
  record: {
    messageId: string;
    messageVersion: number | null;
    raw: AmpMessage;
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
    const id =
      raw.messageID ?? raw.protocolMessageID ?? raw.id ?? raw.messageId;
    if (
      (typeof id !== 'string' && typeof id !== 'number') ||
      !String(id) ||
      String(id).length > 256
    )
      throw new Error('AMP_MISSING_MESSAGE_ID');
    if (typeof id === 'number' && (!Number.isSafeInteger(id) || id < 0))
      throw new Error('AMP_INVALID_MESSAGE_ID');
    const messageId = String(id);
    if (seen.has(messageId)) throw new Error('AMP_DUPLICATE_MESSAGE_ID');
    seen.add(messageId);
    const version =
      raw.messageVersion ?? raw.protocolMessageVersion ?? raw.version;
    if (
      ['messageVersion', 'protocolMessageVersion', 'version'].some(
        (key) => raw[key] === null,
      )
    )
      throw new Error('AMP_INVALID_MESSAGE_VERSION');
    if (
      version !== undefined &&
      (!Number.isSafeInteger(version) || Number(version) < 0)
    )
      throw new Error('AMP_INVALID_MESSAGE_VERSION');
    const role = typeof raw.role === 'string' ? raw.role : null;
    const content =
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
    const hasTool =
      Array.isArray(raw.content) &&
      raw.content.some((block) => object(block).type === 'tool_use');
    return {
      v: 1,
      source: 'amp',
      sessionId,
      eventId: `amp:${sessionId}:${messageId}`,
      lens: 'history',
      kind: role === 'user' && content
        ? 'prompt'
        : role === 'assistant' && content
          ? 'assistant'
          : hasTool || role === 'tool'
            ? 'tool_call'
            : 'marker',
      ts: new Date(time).toISOString(),
      content,
      actorRole: role,
      record: {
        messageId,
        messageVersion: version === undefined ? null : Number(version),
        raw,
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
  | { kind: 'discover' }
  | { kind: 'export'; threadId: string }
  | { kind: 'inject'; threadId: string; text: string };
export const AMP_CLI_VERSION = '0.0.1793857728-g655a2c';
export const AMP_CLI_BRIDGE = `const {spawnSync}=require('node:child_process');
const op=JSON.parse(process.env.RELAY_AMP_OPERATION);
const args=['--log-file','/dev/null'];
if(op.kind==='discover')args.push('threads','search','author:me','--json','--limit','100');
else if(op.kind==='export')args.push('threads','export',op.threadId);
else args.push('threads','continue',op.threadId,'-ox',op.text);
const r=spawnSync('/tmp/relay-amp/node_modules/.bin/amp',args,{encoding:'utf8',timeout:45000,maxBuffer:16777216,env:{...process.env,RELAY_AMP_OPERATION:''}});
if(r.status!==0){process.stdout.write(JSON.stringify({ok:false,outcome:op.kind==='inject'?'unknown':'rejected',code:'amp_command_failed'}));process.exit(0)}
if(op.kind==='inject')process.stdout.write(JSON.stringify({ok:true,value:{accepted:true}}));
else{try{process.stdout.write(JSON.stringify({ok:true,value:JSON.parse(r.stdout)}))}catch{process.stdout.write(JSON.stringify({ok:false,outcome:'rejected',code:'amp_invalid_json'}))}}`;
