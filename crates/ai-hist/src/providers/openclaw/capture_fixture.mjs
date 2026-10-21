// Run only to reproduce the synthetic fixtures, never part of ordinary ingestion.
//   npm install --prefix ~/Projects/scratch-openclaw/writers/<version> --ignore-scripts openclaw@<version>
//   OPENCLAW_WRITER=~/Projects/scratch-openclaw/writers/<version> node crates/ai-hist/src/providers/openclaw/capture_fixture.mjs
// <version> is one of the released writers in WRITERS below. Every row or line
// is written by that released OpenClaw writer into an isolated
// OPENCLAW_STATE_DIR, with a synthetic clock and sequential UUIDs so reruns
// are byte-identical. No gateway, model turn or private history is used.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

// Released writer -> what it stores. `schema` writers keep transcripts in the
// agent database; the legacy writer keeps sessions.json plus JSONL files.
const WRITERS = {
  '2026.10.4': { schema: 24, agents: ['main', 'ops'], oracle: 'oracle.json' },
  '2026.9.6': { schema: 23, agents: ['main'], oracle: 'oracle-schema23.json' },
  '2026.9.5': { schema: 21, agents: ['main'], oracle: 'oracle-schema21.json' },
  '2026.8.1': { schema: 19, agents: ['main'], oracle: 'oracle-schema19.json' },
  '2026.7.35': { legacy: true, agents: ['main', 'ops'], oracle: 'oracle-legacy.json' },
};
const prefix = process.env.OPENCLAW_WRITER;
if (!prefix) throw new Error(`set OPENCLAW_WRITER to the npm prefix holding one of openclaw@${Object.keys(WRITERS).join(', ')}`);
const root = path.join(prefix, 'node_modules', 'openclaw');
const pkg = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
const writer = WRITERS[pkg.version];
if (!writer) throw new Error(`unsupported writer openclaw@${pkg.version}`);
const fixtures = path.resolve('crates/ai-hist/tests/fixtures/openclaw');
const here = path.dirname(new URL(import.meta.url).pathname);
const phase = process.argv[2];
const writing = phase === 'write' || phase === 'oracle';
const work = writing ? process.env.OPENCLAW_FIXTURE_WORK : fs.mkdtempSync(path.join(os.tmpdir(), 'openclaw-synthetic-'));
const state = path.join(work, 'state');
if (!writing) {
  // The writer runs in its own process so its workers close every database
  // before the rows are sanitized and dumped.
  fs.mkdirSync(path.join(work, 'home'), { recursive: true });
  const child = (name) => execFileSync(process.execPath, [new URL(import.meta.url).pathname, name], {
    stdio: 'inherit',
    env: { ...process.env, HOME: path.join(work, 'home'), OPENCLAW_STATE_DIR: state, OPENCLAW_FIXTURE_WORK: work },
  });
  child('write');
  if (writer.legacy) copyLegacyTree();
  else dumpTables();
  // Upstream's own reader re-reads the sanitized store as the oracle.
  child('oracle');
  fs.rmSync(work, { recursive: true });
  process.exit(0);
}
if (phase === 'write') {
  // Deterministic writer inputs, installed before any OpenClaw module loads:
  // a synthetic clock and sequential UUIDs replace wall time and randomness.
  const crypto = await import('node:crypto');
  const { syncBuiltinESMExports } = await import('node:module');
  let uuid = 0;
  crypto.default.randomUUID = () => `00000000-0000-4000-8000-${String(++uuid).padStart(12, '0')}`;
  syncBuiltinESMExports();
  const RealDate = Date;
  let tick = 1790000000000;
  const now = () => (tick += 1);
  globalThis.Date = class extends RealDate {
    constructor(...args) { super(...(args.length ? args : [now()])); }
    static now() { return now(); }
  };
}
const load = (name) => import(pathToFileURL(path.join(root, pkg.exports[`./plugin-sdk/${name}`].default)).href);
const store = await load('session-store-runtime');
const transcript = await load('session-transcript-runtime');
// The JSONL writer only accepts file-safe session IDs.
const MAIN_ID = writer.legacy ? 'fixture-main-0001' : 'fixture/main:α';
const SESSIONS = [
  ['main', 'agent:main:main', MAIN_ID],
  ['main', 'agent:main:telegram:direct:fixture-peer', 'fixture-telegram-0001'],
  ['ops', 'agent:ops:relay:dm:fixture-sender', 'fixture-relay-0001'],
  ...(writer.schema === 24
    ? [
      ['ops', 'agent:ops:cron:fixture-job', 'fixture-cron-run-1'],
      ['ops', 'agent:ops:cron:fixture-job', 'fixture-cron-run-2'],
    ]
    : []),
].filter(([agent]) => writer.agents.includes(agent));
if (phase === 'oracle') {
  const oracle = {
    package: `openclaw@${pkg.version}`,
    reader: 'plugin-sdk/session-transcript-runtime readSessionTranscriptEvents',
    sessions: [],
  };
  for (const [agentId, sessionKey, sessionId] of SESSIONS) {
    const events = await transcript.readSessionTranscriptEvents({ agentId, sessionKey, sessionId });
    oracle.sessions.push({
      agentId, sessionKey, sessionId,
      events: events.map((e) => ({ type: e.type, id: e.id, role: e.message?.role ?? null, toolCallId: e.message?.toolCallId ?? null })),
    });
  }
  fs.writeFileSync(path.join(here, writer.oracle), JSON.stringify(oracle, null, 2) + '\n');
  process.exit(0);
}

const usage = (input, output, cacheRead, cost) => ({
  input, output, cacheRead, cacheWrite: 0, totalTokens: input + output + cacheRead,
  cost: { input: cost / 2, output: cost / 2, cacheRead: 0, cacheWrite: 0, total: cost },
});
const assistant = (content, extra) => ({
  role: 'assistant', content, api: 'openai-responses', provider: 'openai', model: 'fixture-model',
  stopReason: 'stop', ...extra,
});
// The legacy index also carries the catalog fields the database later holds
// in its session windows and nodes.
const legacyEntry = (fields) => (writer.legacy ? fields : {});
async function session(agentId, sessionKey, sessionId, createdAt, messages, fields = {}) {
  await store.upsertSessionEntry({ agentId, sessionKey, entry: { sessionId, updatedAt: createdAt, ...legacyEntry(fields) } });
  for (const message of messages) {
    const result = await transcript.appendSessionTranscriptMessageByIdentity({
      agentId, sessionKey, sessionId, cwd: '/fixture/project', message,
    });
    if (!result?.appended) throw new Error(`append rejected for ${sessionKey}`);
  }
}

await session('main', 'agent:main:main', MAIN_ID, 1789999990000, [
  { role: 'user', content: [{ type: 'text', text: 'Find the OpenClaw fixture needle' }], timestamp: 1790000001000 },
  assistant([
    { type: 'thinking', thinking: 'Read the fixture notes first.' },
    { type: 'text', text: 'I will read the notes.' },
    { type: 'toolCall', id: 'call_fixture|read:1', name: 'read', arguments: { path: '/fixture/project/notes.txt' } },
  ], { stopReason: 'toolUse', usage: usage(100, 20, 5, 0.001), responseId: 'resp-fixture-1', timestamp: 1790000002000 }),
  { role: 'toolResult', toolCallId: 'call_fixture|read:1', toolName: 'read', content: [{ type: 'text', text: 'The OpenClaw fixture needle is here.' }], isError: false, timestamp: 1790000003000 },
  assistant([{ type: 'text', text: 'Found the OpenClaw fixture needle.' }], { usage: usage(130, 10, 100, 0.002), responseId: 'resp-fixture-2', timestamp: 1790000004000 }),
], { label: 'Fixture main session', chatType: 'direct', modelProvider: 'openai', model: 'fixture-model' });
const mirror = await transcript.appendAssistantMirrorMessageByIdentity({
  agentId: 'main', sessionKey: 'agent:main:main', sessionId: MAIN_ID, text: 'Found the OpenClaw fixture needle.',
  deliveryMirror: { kind: 'channel-final' }, idempotencyKey: 'fixture-delivery-1', updateMode: 'none',
});
if (!mirror.ok) throw new Error(`mirror append rejected: ${mirror.reason}`);

// The current writer's fixture also carries heartbeats and isolated cron
// runs; older writers' fixtures stay as published.
if (writer.schema === 24) {
  // A shared-mode heartbeat appends into the main session: the current
  // transcript marker, then the legacy default prompt (upstream matches
  // `prompt` or `prompt\n...`), each with a quiet acknowledgement.
  const HEARTBEAT_LEGACY = 'Read HEARTBEAT.md if it exists (workspace context). Follow it strictly. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply HEARTBEAT_OK.';
  for (const [prompt, ack, extra] of [
    ['[OpenClaw heartbeat poll]', 'NO_REPLY', { provenance: { kind: 'internal_system', sourceTool: 'heartbeat' } }],
    [`${HEARTBEAT_LEGACY}\nCurrent time: fixture`, 'HEARTBEAT_OK', {}],
  ]) {
    for (const message of [
      { role: 'user', content: prompt, timestamp: 1790000006000, ...extra },
      assistant([{ type: 'text', text: ack }], { usage: usage(20, 1, 0, 0.0001), timestamp: 1790000006500 }),
    ]) {
      const result = await transcript.appendSessionTranscriptMessageByIdentity({
        agentId: 'main', sessionKey: 'agent:main:main', sessionId: MAIN_ID, cwd: '/fixture/project', message,
      });
      if (!result?.appended) throw new Error('heartbeat append rejected');
    }
  }
}

await session('main', 'agent:main:telegram:direct:fixture-peer', 'fixture-telegram-0001', 1789999991000, [
  { role: 'user', content: 'Run the failing fixture command', timestamp: 1790000101000 },
  assistant([{ type: 'toolCall', id: 'call_fixture_exec', name: 'exec', arguments: { command: 'false' } }],
    { stopReason: 'toolUse', usage: usage(40, 8, 0, 0.0005), timestamp: 1790000102000 }),
  // Long enough that a database writer stores it zstd-compressed.
  { role: 'toolResult', toolCallId: 'call_fixture_exec', toolName: 'exec', content: [{ type: 'text', text: `exit status 1\n${'fixture stderr line\n'.repeat(120)}` }], isError: true, timestamp: 1790000103000 },
  assistant([{ type: 'text', text: 'The fixture command failed as expected.' }], { usage: usage(60, 9, 0, 0.0006), timestamp: 1790000104000 }),
], { chatType: 'direct', channel: 'telegram' });

if (writer.agents.includes('ops')) {
  await session('ops', 'agent:ops:relay:dm:fixture-sender', 'fixture-relay-0001', 1789999992000, [
    { role: 'user', content: [{ type: 'text', text: 'Relay question for the ops agent' }], timestamp: 1790000201000 },
    assistant([{ type: 'text', text: 'Relay answer from the ops agent.' }], { usage: usage(30, 6, 0, 0.0003), timestamp: 1790000202000 }),
  ]);
}
if (writer.schema === 24) {
  // An isolated cron job keeps one session key and opens a new window
  // (session ID) for every run; its prompt is not a user turn.
  await session('ops', 'agent:ops:cron:fixture-job', 'fixture-cron-run-1', 1789999993000, [
    assistant([{ type: 'text', text: 'Cron run one checked the fixture queue.' }], { usage: usage(50, 7, 0, 0.0004), timestamp: 1790000301000 }),
  ]);
  await session('ops', 'agent:ops:cron:fixture-job', 'fixture-cron-run-2', 1789999994000, [
    assistant([{ type: 'text', text: 'NO_REPLY' }], { usage: usage(45, 1, 0, 0.0003), timestamp: 1790000401000 }),
  ]);
}

process.exit(0);

function dumpTables() {
  const sql = (db, ...statements) => execFileSync('sqlite3', [db, ...statements], { encoding: 'utf8' });
  const database = (agent) => path.join(state, 'agents', agent, 'agent', 'openclaw-agent.sqlite');

  // Keep upstream DDL for the transcript tables a reader consumes, plus rows.
  // Derived FTS/vector tables, triggers and unrelated stores are omitted, as
  // are tables an older schema does not have yet.
  const tables = ['schema_meta', 'session_nodes', 'conversations', 'session_windows', 'transcript_events',
    'transcript_event_identities', 'session_transcript_active_events', 'transcript_rewrite_watermarks',
    'session_transcript_cold_archives'];
  fs.mkdirSync(fixtures, { recursive: true });
  for (const agent of writer.agents) {
    const db = database(agent);
    const version = sql(db, 'PRAGMA user_version').trim();
    if (Number(version) !== writer.schema) throw new Error(`expected agent schema ${writer.schema}, found ${version}`);
    const lines = ['BEGIN TRANSACTION;', `PRAGMA user_version=${version};`];
    for (const table of tables) {
      const ddl = sql(db, `SELECT sql || ';' FROM sqlite_schema WHERE type='table' AND name='${table}'`).trim();
      if (ddl) lines.push(ddl);
    }
    const dump = sql(db, '.dump').split('\n');
    for (const table of tables) {
      lines.push(...dump.filter((line) => line.startsWith(`INSERT INTO ${table} `) || line.startsWith(`INSERT INTO "${table}" `)));
    }
    lines.push('COMMIT;');
    fs.writeFileSync(path.join(fixtures, `${agent}-schema${version}.sql`), lines.join('\n') + '\n');
  }
}

// Copy each agent's sessions directory as a home tree. The index records the
// absolute transcript path the writer used; it is rewritten to a fixed
// synthetic state directory so the fixture never names the capture machine.
function copyLegacyTree() {
  const out = path.join(fixtures, 'legacy-jsonl', '.openclaw', 'agents');
  fs.rmSync(path.join(fixtures, 'legacy-jsonl'), { recursive: true, force: true });
  for (const agent of writer.agents) {
    const from = path.join(state, 'agents', agent, 'sessions');
    const to = path.join(out, agent, 'sessions');
    fs.mkdirSync(to, { recursive: true });
    for (const name of fs.readdirSync(from).sort()) {
      const text = fs.readFileSync(path.join(from, name), 'utf8');
      fs.writeFileSync(path.join(to, name), name === 'sessions.json' ? text.replaceAll(state, '/fixture/state') : text);
    }
  }
}
