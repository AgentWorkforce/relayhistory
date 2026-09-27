import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import {
  HistoryPluginRegistry,
  MAX_HANDOFF_INTENT_CHARS,
  RelayHistoryError,
  SessionNotFoundError,
  createHandoff,
  resumeHandoff,
  type HistorySource,
} from './index.js';

test('createHandoff keeps the pointer intent bounded', async () => {
  await assert.rejects(
    createHandoff('x'.repeat(MAX_HANDOFF_INTENT_CHARS + 1), { env: {} }),
    (error: unknown) => error instanceof RelayHistoryError && error.code === 'INVALID_ARGUMENT',
  );
});

test('createHandoff resolves the invoking harness session through the local catalog', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-create-handoff-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const home = join(root, 'home');
  const sessionId = 'codex-current-handoff';
  const directory = join(home, '.codex', 'sessions', '2026', '09', '27');
  await mkdir(directory, { recursive: true });
  await writeFile(join(directory, `rollout-${sessionId}.jsonl`), [
    JSON.stringify({
      timestamp: '2026-09-27T10:00:00.000Z',
      type: 'session_meta',
      payload: { id: sessionId, cwd: '/work/handoff' },
    }),
    JSON.stringify({
      timestamp: '2026-09-27T10:00:01.000Z',
      type: 'event_msg',
      payload: { type: 'user_message', message: 'implement live handoff' },
    }),
  ].join('\n') + '\n');
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  t.after(() => {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE;
    else process.env.USERPROFILE = saved.USERPROFILE;
  });
  const pointer = await createHandoff('continue the implementation', {
    dbPath: join(root, 'history.db'),
    env: {
      CODEX_THREAD_ID: sessionId,
      AGENT_RELAY_AGENT_NAME: 'sender-agent',
      AGENT_RELAY_USER_ID: 'user-sender',
    },
  });
  assert.deepEqual(pointer, {
    source: 'codex',
    session_id: sessionId,
    intent: 'continue the implementation',
    origin_agent: 'sender-agent',
    origin_user: 'user-sender',
  });
});

test('handoff tools are in the default MCP inventory', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-mcp-handoff-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL('./mcp-server.js', import.meta.url))],
    env: {
      ...Object.fromEntries(Object.entries(process.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      )),
      HOME: root,
      USERPROFILE: root,
      XDG_DATA_HOME: join(root, 'share'),
      AI_HIST_DB: join(root, 'history.db'),
    },
    stderr: 'pipe',
  });
  const client = new Client({ name: 'handoff-inventory-test', version: '1' });
  t.after(() => client.close());
  await client.connect(transport);
  const names = (await client.listTools()).tools.map((tool) => tool.name);
  assert.ok(names.includes('create_handoff'));
  assert.ok(names.includes('resume_handoff'));
});

function teammateSource(options: { missing?: boolean } = {}): HistorySource {
  const sessionId = 'teammate-session';
  return {
    id: 'cloud',
    instanceId: 'workspace-fixture',
    location: 'remote',
    supportedSources: ['claude'],
    discover: async () => ({
      observations: [{
        source: 'claude',
        session_id: sessionId,
        source_stamp: 'workspace-listing-1',
        first_prompt: 'ship handoff support',
      }],
    }),
    hydrate: async () => {
      if (options.missing)
        throw new SessionNotFoundError(
          'not visible in this workspace',
          'SESSION_NOT_FOUND',
        );
      return {
        source_stamp: 'workspace-snapshot-1',
        source_bytes: 512,
        covered_kinds: ['history', 'session_event', 'tool_call', 'file_edit'],
        records: [
          {
            kind: 'history',
            payload: {
              source: 'claude', session_id: sessionId, timestamp_ms: 10,
              prompt: 'ship handoff support', project: '/work/shared',
            },
          },
          {
            kind: 'session_event',
            payload: {
              source: 'claude', session_id: sessionId, event_uid: 'event-user',
              ts_ms: 10, role: 'user', kind: 'text', text: 'ship handoff support',
            },
          },
          {
            kind: 'session_event',
            payload: {
              source: 'claude', session_id: sessionId, event_uid: 'event-assistant',
              ts_ms: 20, role: 'assistant', kind: 'text', text: 'implemented the receiver',
            },
          },
          {
            kind: 'tool_call',
            payload: {
              source: 'claude', session_id: sessionId, tool_use_id: 'tool-1',
              name: 'Edit', args_json: '{"file_path":"handoff.ts"}', ts_ms: 15,
            },
          },
          {
            kind: 'file_edit',
            payload: {
              source: 'claude', session_id: sessionId, tool_use_id: 'tool-1',
              file_path: 'handoff.ts', tool_name: 'Edit',
              structured_patch_json: '"+resume"', ts_ms: 16,
            },
          },
        ],
      };
    },
  };
}

test('resumeHandoff acquires a teammate session and composes all continuation evidence', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-resume-handoff-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [teammateSource()] });
  const resumed = await resumeHandoff('claude', 'teammate-session', {
    dbPath: join(root, 'history.db'),
    plugins,
  });
  assert.equal(resumed.contract_version, 1);
  assert.equal(resumed.session_id, 'teammate-session');
  assert.deepEqual(resumed.prompts.map((entry) => entry.prompt), ['ship handoff support']);
  assert.deepEqual(resumed.events.map((event) => event.text), [
    'ship handoff support',
    'implemented the receiver',
  ]);
  assert.deepEqual(resumed.tool_calls.map((call) => call.name), ['Edit']);
  assert.deepEqual(resumed.file_edits.map((edit) => edit.filePath), ['handoff.ts']);
  assert.equal(resumed.next_cursor, null);
});

test('resumeHandoff explicitly rejects a session outside the authenticated workspace', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-cross-org-handoff-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [teammateSource({ missing: true })] });
  await assert.rejects(
    resumeHandoff('claude', 'teammate-session', {
      dbPath: join(root, 'history.db'),
      plugins,
    }),
    (error: unknown) => error instanceof RelayHistoryError
      && error.code === 'HANDOFF_WORKSPACE_MISMATCH'
      && error.message.includes('cross-organization'),
  );
});

test('resumeHandoff returns independent bounded cursors for every evidence class', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-page-handoff-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [teammateSource()] });
  const first = await resumeHandoff('claude', 'teammate-session', {
    dbPath: join(root, 'history.db'), plugins, limit: 1,
  });
  assert.equal(first.events.length, 1);
  assert.ok(first.next_cursor?.events);
  const second = await resumeHandoff('claude', 'teammate-session', {
    dbPath: join(root, 'history.db'), plugins, limit: 1,
    cursor: first.next_cursor ?? undefined,
  });
  assert.deepEqual(second.events.map((event) => event.text), ['implemented the receiver']);
  assert.deepEqual(second.prompts, []);
  assert.deepEqual(second.tool_calls, []);
  assert.deepEqual(second.file_edits, []);
  assert.equal(second.next_cursor, null);
});
