import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, writeFile, chmod, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  HistoryPluginRegistry,
  HistoryDeliveryError,
  RelayHistoryError,
  type HistoryExportRecord,
  type HistoryExportBatch,
  hydrateSession,
  getSessionEventsPage,
} from 'ai-hist';
import {
  createHistoryPlugin,
  projectDeliveredTranscript,
  relayHistoryDestination,
  relayHistoryInstance,
  getSessionThreadWithHistory,
  relayHistorySource,
} from './plugin.js';

/**
 * Relationship rows actually ingested for the fixture session.
 *
 * Read through the public SDK rather than by opening the database: the claim
 * is that declining related evidence stops it being *indexed*, and a count the
 * SDK cannot see would not prove that.
 */
async function relationshipRows(dbPath: string): Promise<number> {
  const { getSessionRelationships } = await import('ai-hist');
  const topology = await getSessionRelationships({
    source: 'claude', sessionId: 'session-fixture', dbPath,
  });
  return topology.asParent.length + topology.asChild.length;
}

const account =
  'relayhistory:' +
  createHash('sha256')
    .update(JSON.stringify(['org-fixture', 'workspace-fixture']))
    .digest('hex');
function record(
  kind: HistoryExportRecord['kind'],
  id: string,
  payload: unknown,
): HistoryExportRecord {
  return {
    schema_version: 1,
    origin_id: 'origin-fixture',
    record_id: id,
    revision_id: 'revision-' + id,
    revision: 1,
    kind,
    source: 'claude',
    session_id: 'session-fixture',
    operation: 'upsert',
    payload: {
      source: 'claude',
      session_id: 'session-fixture',
      ...(payload as Record<string, unknown>),
    },
  };
}
const records = [
  record('session_event', 'a', {
    event_uid: 'later',
    ts_ms: 20,
    role: 'assistant',
    text: 'answer',
  }),
  record('history', 'b', { timestamp_ms: 10, prompt: 'question' }),
  record('session_event', 'z', { event_uid: 'earlier', ts_ms: 10, role: 'user', text: 'question' }),
  record('tool_call', 'tool', { args_json: '{"path":"fixture.ts"}' }),
  record('file_edit', 'edit', { patches: '[{"patch":"+fixture"}]' }),
];
/** Answers the delivery operations the way the real Rust helper does: its
 * receiver applies the legacy-scheduler, account and instance guards before it
 * maps or sends anything, so these tests still assert user-visible behaviour.
 * Nothing past those guards is emulated. */
async function fixtureHelper(
  t: test.TestContext,
  state: 'clear' | 'active' | 'unknown' = 'clear',
  recordsPath?: string,
) {
  const dir = await mkdtemp(join(tmpdir(), 'rh-plugin-fixture-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const path = join(dir, 'helper');
  await writeFile(
    path,
    `#!${process.execPath}\nlet input='';process.stdin.on('data',chunk=>input+=chunk);process.stdin.on('end',()=>{const request=JSON.parse(input);let value;switch(request.operation){case 'deliveryMigrationStatus':value={state:${JSON.stringify(state)},jobs:[]};break;case 'deliveryRead':if(request.args.readOptions.expectedAccount!==${JSON.stringify(account)}){process.stdout.write(JSON.stringify({version:1,ok:false,error:{code:'DELIVERY_PERMISSION_DENIED'}}));return;}value={protocolVersion:1,listing:'live',records:${recordsPath ? `JSON.parse(require('node:fs').readFileSync(${JSON.stringify(recordsPath)},'utf8'))` : JSON.stringify(records)},nextCursor:null};break;case 'deliveryPrepare':case 'deliverySend':{const state=${JSON.stringify(state)};if(state==='active'||(state==='unknown'&&request.args.acknowledgeUninspectedSchedules!==true)){process.stdout.write(JSON.stringify({version:1,ok:false,error:{code:'DELIVERY_PERMISSION_DENIED'}}));return;}if(request.args.expectedAccount&&request.args.batch.account_id!==request.args.expectedAccount){process.stdout.write(JSON.stringify({version:1,ok:false,error:{code:'DELIVERY_PERMISSION_DENIED'}}));return;}if(request.args.instanceId&&request.args.batch.instance_id!==request.args.instanceId){process.stdout.write(JSON.stringify({version:1,ok:false,error:{code:'DELIVERY_MAPPING_VERSION_MISMATCH'}}));return;}process.stdout.write(JSON.stringify({version:1,ok:false,error:{code:'INVALID_ARGUMENT'}}));return;}case 'cloudResolveSession':value={auth:{baseUrl:'https://fixture.invalid',accessToken:'fixture-token',orgId:'org-fixture',workspaceId:'workspace-fixture'}};break;default:process.stdout.write(JSON.stringify({version:1,ok:false,error:{code:'INVALID_ARGUMENT'}}));return;}process.stdout.write(JSON.stringify({version:1,ok:true,value}));});\n`,
  );
  await chmod(path, 0o700);
  return path;
}
test('transcript prefers events, orders timestamps, and preserves evidence separately', () => {
  assert.deepEqual(
    projectDeliveredTranscript(records).map((row) => row.text),
    ['question', 'answer'],
  );
  assert.equal(records.length, 5);
  assert.equal(projectDeliveredTranscript([records[1]])[0].representation, 'history');
});
test('standard plugin thread sees delivery-only sessions and pins one account across legacy reads', async (t) => {
  const binaryPath = await fixtureHelper(t);
  const original = globalThis.fetch;
  let token: string | null = null;
  globalThis.fetch = (async (_url, init) => {
    token = new Headers(init?.headers).get('authorization');
    return new Response(
      JSON.stringify({ session: null, outcomes: [], links: [], nextCursor: null }),
      { headers: { 'content-type': 'application/json' } },
    );
  }) as typeof fetch;
  t.after(() => {
    globalThis.fetch = original;
  });
  const plugin = createHistoryPlugin({
    binaryPath,
    baseUrl: 'https://fixture.invalid',
    expectedAccount: account,
  });
  const result = (await plugin
    .tools!.find((tool) => tool.name === 'get_session_thread')!
    .run({ source: 'claude', session_id: 'session-fixture' })) as {
    transcript: Array<{ text: string }>;
    deliveredHistory: HistoryExportRecord[];
    legacyStatus: { available: boolean };
  };
  assert.deepEqual(
    result.transcript.map((row) => row.text),
    ['question', 'answer'],
  );
  assert.deepEqual(result.deliveredHistory, records);
  assert.equal(result.legacyStatus.available, true);
  assert.equal(token, 'Bearer fixture-token');
  let resolves = 0;
  const composed = await getSessionThreadWithHistory(
    { source: 'claude', sessionId: 'session-fixture' },
    {
      binaryPath,
      expectedAccount: account,
      resolveSession: async () => {
        resolves++;
        return {
          auth: {
            baseUrl: 'https://fixture.invalid',
            accessToken: 'pinned-token',
            orgId: 'org-fixture',
            workspaceId: 'workspace-fixture',
          },
        };
      },
    },
  );
  assert.equal(resolves, 1);
  assert.equal(token, 'Bearer pinned-token');
  assert.equal(composed.deliveredHistory.length, 5);
});
test('thread refuses changed account before reading or combining either representation', async (t) => {
  const binaryPath = await fixtureHelper(t);
  await assert.rejects(
    getSessionThreadWithHistory(
      { source: 'claude', sessionId: 'session-fixture' },
      {
        binaryPath,
        expectedAccount: account,
        resolveSession: async () => ({
          auth: {
            baseUrl: 'https://fixture.invalid',
            accessToken: 'other-token',
            orgId: 'other-org',
          },
        }),
      },
    ),
    HistoryDeliveryError,
  );
});
test('loading and registering optional plugin is inert with unavailable auth helper', () => {
  const registry = new HistoryPluginRegistry();
  registry.register(
    createHistoryPlugin({ binaryPath: '/fixture/never-execute', expectedAccount: account }),
  );
  assert.equal(registry.sourceConnectors(['cloud']).length, 1);
});
test('pending batches cannot follow changed endpoint even when account and user label match', async (t) => {
  const binaryPath = await fixtureHelper(t);
  const oldOptions = { binaryPath, baseUrl: 'https://stage-one.invalid', instanceId: 'fixture' };
  const batch = {
    instance_id: relayHistoryInstance(oldOptions),
    account_id: account,
  } as HistoryExportBatch;
  const next = relayHistoryDestination({ ...oldOptions, baseUrl: 'https://stage-two.invalid' });
  await assert.rejects(
    next.send(
      {
        mapping_version: 'relayhistory-delivery-v1',
        body: 'immutable',
        content_type: 'application/json',
        sha256: 'fixture',
      },
      { signal: new AbortController().signal, batch, idempotencyKey: 'fixture' },
    ),
    (error: unknown) =>
      error instanceof HistoryDeliveryError && error.failure === 'mapping_version_mismatch',
  );
  assert.equal(
    relayHistoryInstance(oldOptions),
    relayHistoryInstance({ ...oldOptions, baseUrl: 'https://stage-one.invalid/' }),
  );
});
test('active legacy schedule cannot be acknowledged away; unknown requires explicit acknowledgment', async (t) => {
  for (const state of ['active', 'unknown'] as const) {
    const binaryPath = await fixtureHelper(t, state);
    const options = { binaryPath, baseUrl: 'https://fixture.invalid' };
    const batch = { instance_id: relayHistoryInstance(options) } as HistoryExportBatch;
    await assert.rejects(
      relayHistoryDestination(options).prepare(batch, { signal: new AbortController().signal }),
      (error: unknown) =>
        error instanceof HistoryDeliveryError && error.failure === 'permission_denied',
    );
    if (state === 'active')
      await assert.rejects(
        relayHistoryDestination({
          ...options,
          acknowledgeUninspectedLegacySchedules: true,
        }).prepare(batch, { signal: new AbortController().signal }),
        (error: unknown) =>
          error instanceof HistoryDeliveryError && error.failure === 'permission_denied',
      );
    else
      await assert.rejects(
        relayHistoryDestination({
          ...options,
          acknowledgeUninspectedLegacySchedules: true,
        }).prepare(batch, { signal: new AbortController().signal }),
        (error: unknown) =>
          error instanceof RelayHistoryError &&
          !(error instanceof HistoryDeliveryError && error.failure === 'permission_denied'),
      );
  }
});

test('MCP startup and cached reads with a configured cloud plugin never invoke its auth helper', async (t) => {
  const { Client } = await import('@modelcontextprotocol/sdk/client/index.js');
  const { StdioClientTransport } = await import('@modelcontextprotocol/sdk/client/stdio.js');
  const { fileURLToPath } = await import('node:url');
  const { access } = await import('node:fs/promises');
  const dir = await mkdtemp(join(tmpdir(), 'rh-mcp-auth-trap-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const marker = join(dir, 'auth-was-read');
  const helper = join(dir, 'trap');
  await writeFile(
    helper,
    `#!${process.execPath}\nrequire('node:fs').writeFileSync(${JSON.stringify(marker)},'unexpected');process.exit(1);\n`,
  );
  await chmod(helper, 0o700);
  const config = join(dir, 'config.json');
  await writeFile(
    config,
    JSON.stringify({
      plugins: [
        {
          module: fileURLToPath(new URL('./index.js', import.meta.url)),
          options: { binaryPath: helper, expectedAccount: account },
        },
      ],
    }),
  );
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  Object.assign(env, {
    HOME: dir,
    USERPROFILE: dir,
    XDG_DATA_HOME: join(dir, 'share'),
    AI_HIST_DB: join(dir, 'history.db'),
    AI_HIST_PLUGIN_CONFIG: config,
  });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL('../../../../sdk-ts/dist/mcp-server.js', import.meta.url))],
    env,
    stderr: 'pipe',
  });
  const client = new Client({ name: 'auth-trap-fixture', version: '1' });
  t.after(() => client.close());
  await client.connect(transport);
  assert.ok((await client.listTools()).tools.some((tool) => tool.name === 'get_session_thread'));
  await client.callTool({ name: 'recent_history', arguments: { scope: 'remote' } });
  await assert.rejects(access(marker));
});

test('multi-origin delivery merges canonical identities without comparing origin revisions', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-origin-merge-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const recordsPath = join(dir, 'records.json');
  const binaryPath = await fixtureHelper(t, 'clear', recordsPath);
  const dbPath = join(dir, 'history.db');
  const a = {
    ...record('session_event', 'event-a', {
      event_uid: 'shared',
      ts_ms: 1,
      role: 'assistant',
      kind: 'text',
      text: 'origin-a',
    }),
    origin_id: 'a',
    revision: 2,
  };
  const b = {
    ...record('session_event', 'event-b', {
      event_uid: 'shared',
      ts_ms: 1,
      role: 'assistant',
      kind: 'text',
      text: 'origin-b',
    }),
    origin_id: 'b',
    revision: 900,
  };
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [relayHistorySource({ binaryPath, expectedAccount: account })] });
  const hydrate = () =>
    hydrateSession({
      source: 'claude',
      sessionId: 'session-fixture',
      scope: 'remote',
      plugins: registry,
      dbPath,
    });
  await writeFile(recordsPath, JSON.stringify([b, a]));
  await hydrate();
  assert.deepEqual(
    (await getSessionEventsPage('session-fixture', { source: 'claude', dbPath })).events.map(
      (row) => row.text,
    ),
    ['origin-a'],
  );
  b.payload = { ...(b.payload as object), text: 'origin-b-updated' };
  b.revision = 901;
  b.revision_id = 'revision-b-updated';
  await writeFile(recordsPath, JSON.stringify([a, b]));
  await hydrate();
  assert.equal(
    (await getSessionEventsPage('session-fixture', { source: 'claude', dbPath })).events[0].text,
    'origin-a',
  );
  await writeFile(
    recordsPath,
    JSON.stringify([{ ...a, operation: 'delete', payload: null, revision_id: 'a-deleted' }, b]),
  );
  await hydrate();
  assert.equal(
    (await getSessionEventsPage('session-fixture', { source: 'claude', dbPath })).events[0].text,
    'origin-b-updated',
  );
  await writeFile(
    recordsPath,
    JSON.stringify([
      { ...a, operation: 'delete', payload: null, revision_id: 'a-deleted' },
      { ...b, operation: 'delete', payload: null, revision_id: 'b-deleted' },
    ]),
  );
  await hydrate();
  assert.equal(
    (await getSessionEventsPage('session-fixture', { source: 'claude', dbPath })).events.length,
    0,
  );
  await writeFile(
    recordsPath,
    JSON.stringify([record('session', 'metadata', { first_prompt: 'metadata only' })]),
  );
  const result = await hydrate();
  assert.equal(result.status, 'capability_limited');
  assert.equal(result.capability, 'shallow_only');
});

// A sparse session reports the kinds its export carried and no more. Declaring
// the untouched kinds covered would be right for an unfiltered export -- and
// costs such a session priority in a merge -- but a delivery job exports only
// its configured selection, so this listing cannot tell "the session has none"
// from "no job exported it". Until the read protocol carries the contributing
// selections, understating is the side to be wrong on.
test('a sparse session reports the kinds its export carried, and no more', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-sparse-coverage-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const recordsPath = join(dir, 'records.json');
  const binaryPath = await fixtureHelper(t, 'clear', recordsPath);
  const dbPath = join(dir, 'history.db');
  // Prompts and one assistant turn: no tool calls, no file edits, no
  // delegation. Nothing here is missing -- the session simply has none.
  await writeFile(recordsPath, JSON.stringify([
    record('history', 'prompt', { timestamp_ms: 10, prompt: 'question' }),
    record('session_event', 'answer', {
      event_uid: 'answer', ts_ms: 20, role: 'assistant', kind: 'text', text: 'answer',
    }),
  ]));
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [relayHistorySource({ binaryPath, expectedAccount: account })] });
  const result = await hydrateSession({
    source: 'claude', sessionId: 'session-fixture', scope: 'remote', plugins: registry, dbPath,
  });

  assert.equal(result.capability, 'partial');
  assert.deepEqual(result.coverage, ['history', 'session_event']);
  assert.equal(result.evidence.toolCalls, 0);
  assert.equal(result.evidence.fileEdits, 0);
  assert.equal(result.evidence.prompts, 1);
});

// A delivery job exports only the kinds in its configured selection, so an
// absent kind means either "this session has none" or "no job ever exported
// it" -- and the listing cannot tell them apart. Treating one delivered row as
// proof that every kind was examined turns a filtered export into a `full`
// claim over evidence nobody looked at.
test('a filtered export does not claim coverage of the kinds it never carried', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-filtered-export-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const recordsPath = join(dir, 'records.json');
  const binaryPath = await fixtureHelper(t, 'clear', recordsPath);
  const dbPath = join(dir, 'history.db');
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [relayHistorySource({ binaryPath, expectedAccount: account })] });
  const hydrate = () => hydrateSession({
    source: 'claude', sessionId: 'session-fixture', scope: 'remote', plugins: registry, dbPath,
  });

  // A job selecting history, session_event and session: one delivered event,
  // and nothing about tool calls, file edits or delegation.
  await writeFile(recordsPath, JSON.stringify([
    record('history', 'prompt', { timestamp_ms: 10, prompt: 'question' }),
    record('session_event', 'answer', {
      event_uid: 'answer', ts_ms: 20, role: 'assistant', kind: 'text', text: 'answer',
    }),
    record('session', 'metadata', { first_prompt: 'question' }),
  ]));
  const filtered = await hydrate();
  for (const kind of ['tool_call', 'file_edit', 'relationship']) {
    assert.equal(
      filtered.coverage.includes(kind as never), false,
      `${kind} was never exported by this job, so it is not covered`,
    );
  }
  assert.equal(filtered.capability, 'partial');

  // Positive control: an export that did carry every kind still reports them,
  // so the assertions above are about the job's selection and not about the
  // plugin having stopped reporting coverage at all.
  await writeFile(recordsPath, JSON.stringify([
    record('history', 'prompt2', { timestamp_ms: 11, prompt: 'question' }),
    record('session_event', 'answer2', {
      event_uid: 'answer2', ts_ms: 21, role: 'assistant', kind: 'text', text: 'answer',
    }),
    record('tool_call', 'tool2', { tool_use_id: 'tool-2', name: 'Edit' }),
    record('file_edit', 'edit2', { tool_use_id: 'tool-2', file_path: '/work/a.rs', tool_name: 'Edit' }),
    {
      ...record('relationship', 'delegation2', {}),
      payload: {
        source: 'claude', parent_session_id: 'session-fixture', relationship_uid: 'delegation2',
        child_session_id: 'child2', relationship: 'delegated', identity_status: 'observed',
        evidence_kind: 'transcript', created_ms: 1, updated_ms: 1,
      },
    },
  ]));
  const complete = await hydrate();
  for (const kind of ['history', 'session_event', 'tool_call', 'file_edit', 'relationship']) {
    assert.ok(complete.coverage.includes(kind as never), `${kind} was exported and is covered`);
  }
  assert.equal(complete.capability, 'full');
});

// `includeRelated: false` is part of the request, not of the local path. A
// connector that kept reporting relationship coverage would reinstate, through
// the merge union, exactly the kind the local result dropped.
test('a remote snapshot honours includeRelated: false and the merge does not reinstate it', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-include-related-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const recordsPath = join(dir, 'records.json');
  const binaryPath = await fixtureHelper(t, 'clear', recordsPath);
  const dbPath = join(dir, 'history.db');
  await writeFile(recordsPath, JSON.stringify([
    record('history', 'prompt', { timestamp_ms: 10, prompt: 'question' }),
    // Built by hand: `session_relationships` is keyed by `parent_session_id`
    // and has no `session_id` column, which `record` would inject.
    {
      ...record('relationship', 'delegation', {}),
      payload: {
        source: 'claude',
        parent_session_id: 'session-fixture',
        relationship_uid: 'delegation',
        child_session_id: 'child-fixture',
        relationship: 'delegated',
        identity_status: 'observed',
        evidence_kind: 'transcript',
        created_ms: 1,
        updated_ms: 1,
      },
    },
  ]));
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [relayHistorySource({ binaryPath, expectedAccount: account })] });
  const hydrate = (scope: 'remote' | 'all', includeRelated: boolean) => hydrateSession({
    source: 'claude', sessionId: 'session-fixture', scope, plugins: registry, dbPath, includeRelated,
  });

  const declined = await hydrate('remote', false);
  assert.equal(declined.coverage.includes('relationship'), false);
  assert.equal(declined.capability, 'partial');
  assert.deepEqual(declined.relatedSessionIds, []);
  // The row was in the export and was not ingested: declining is a real
  // acquisition choice, not a relabelling of the same evidence.
  assert.equal(await relationshipRows(dbPath), 0);

  // scope 'all' merges the local and remote presences. The union must not put
  // back what both sides were asked to leave out.
  const merged = await hydrate('all', false);
  assert.equal(merged.coverage.includes('relationship'), false);
  assert.equal(merged.capability, 'partial');
  assert.equal(await relationshipRows(dbPath), 0);

  // Asking for it brings both the coverage and the rows back, so the assertions
  // above are about the option and not about an export that never had one.
  const requested = await hydrate('remote', true);
  assert.equal(requested.coverage.includes('relationship'), true);
  assert.equal(await relationshipRows(dbPath), 1);
  // Still `partial`: this export carried only history and the delegation edge,
  // so the other kinds are genuinely uncovered. What the option controls is
  // whether `relationship` is among them, which is what is asserted above.
  assert.equal(requested.capability, 'partial');
});

test('source discovery orders by evidence recency before applying its limit', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-discovery-recency-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const recordsPath = join(dir, 'records.json');
  const rows = [
    ['old', 1],
    ['new', 900],
    ['middle', 20],
  ].map(([session, ts]) => ({
    ...record('session_event', String(session), {
      event_uid: 'event-' + session,
      ts_ms: ts,
      role: 'user',
      kind: 'text',
      text: session,
      session_id: session,
    }),
    session_id: String(session),
  }));
  await writeFile(recordsPath, JSON.stringify(rows));
  const binaryPath = await fixtureHelper(t, 'clear', recordsPath);
  const source = relayHistorySource({ binaryPath, expectedAccount: account });
  const page = await source.discover({ limit: 1 });
  assert.equal(page.observations[0].session_id, 'new');
  assert.equal(page.observations[0].first_activity_ms, 900);
  assert.equal(page.observations[0].last_activity_ms, 900);
});


test('oversized persisted helper requests block as invalid payload instead of retrying forever',async t=>{
  const {helperRequest}=await import('./helper.js');
  const binaryPath=await fixtureHelper(t);const options={binaryPath,baseUrl:'https://fixture.invalid'};
  const prepared={mapping_version:'relayhistory-delivery-v1',content_type:'application/json',body:'x'.repeat(16*1_048_576),sha256:'fixture'};
  // The actual serializer bound fires before any helper executable is launched.
  await assert.rejects(helperRequest('deliverySend',{prepared},{binaryPath:'/never-launched-for-oversize'}),(error:unknown)=>error instanceof RelayHistoryError&&error.code==='INVALID_ARGUMENT');
  const batch={instance_id:relayHistoryInstance(options),account_id:account} as HistoryExportBatch;
  await assert.rejects(relayHistoryDestination(options).send(prepared,{signal:new AbortController().signal,batch,idempotencyKey:'fixture'}),(error:unknown)=>error instanceof HistoryDeliveryError&&error.failure==='invalid_payload');
});
