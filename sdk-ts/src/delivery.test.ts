import { nativeCall } from './native.js';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { access, cp, link, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { gunzipSync } from 'node:zlib';
import {
  beginHistoryExport, closeHistoryExport, controlHistoryDelivery, createHistoryDelivery,
  DEFAULT_DELIVERY_LIMITS, deliveryRequest, drainHistoryDelivery, exportHistory, historyDeliveryStatus,
  historyDeliveryRetention, HistoryDeliveryError, HistoryPluginRegistry, loadHistoryPlugins, readHistoryExportPage, runHistoryDelivery,
  type DeliveryJobConfig, type HistoryDestination, type HistoryExportBatch, type HistoryExportSelection,
} from './index.js';

const run = promisify(execFile);
const sdkRoot = fileURLToPath(new URL('../', import.meta.url));
const sdkModule = new URL('./index.js', import.meta.url).href;
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const selection: HistoryExportSelection = { all_sources: false, sources: ['claude'], sessions: [], kinds: ['history'], excluded_sessions: [] };

async function fixture(body: (dbPath: string, root: string) => Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), 'history-delivery-test-'));
  const dbPath = join(root, 'history.db');
  try {
    await writeFile(dbPath, gunzipSync(await readFile(new URL('../fixtures/offline-history.db.gz', import.meta.url))));
    await body(dbPath, root);
  } finally { await rm(root, { recursive: true, force: true }); }
}
function config(instance = 'one'): DeliveryJobConfig {
  return { destination_id: 'fixture', instance_id: instance, account_id: 'fixture-account', mapping_version: '1',
    selection, limits: { ...DEFAULT_DELIVERY_LIMITS } };
}
function ack(batch: Readonly<HistoryExportBatch>) {
  return { batch_id: batch.batch_id, accepted_revision_ids: batch.records.map((record) => record.revision_id),
    unsupported_revision_ids: [], acceptance_level: 'durable' as const };
}
function destination(overrides: Partial<HistoryDestination> = {}): HistoryDestination {
  return { id: 'fixture', mappingVersion: '1', idempotency: 'revision', orderedRevisions: true,
    supportedKinds: ['history'], supportsTombstones: true,
    prepare: async (batch) => ({ content_type: 'application/json', body: JSON.stringify(batch.records) }),
    send: async (_payload, { batch }) => ack(batch), ...overrides };
}
function registry(value: HistoryDestination, instanceId = 'one'): HistoryPluginRegistry {
  const result = new HistoryPluginRegistry();
  result.register({ destinations: [{ instanceId, destination: value }] });
  return result;
}

// Upload state-machine regressions now run in probe Rust delivery_worker and durable_delivery tests.
test('plugin registration is inert, per-client, and rejects collisions atomically', async () => {
  await fixture(async (dbPath) => {
    let calls = 0;
    const one = registry(destination({ prepare: async () => { calls++; throw new Error('should not execute'); } }));
    assert.equal(calls, 0);
    await assert.rejects(historyDeliveryStatus(undefined, { dbPath }), { code: 'HISTORY_DELIVERY_MOVED' });
    assert.equal(new HistoryPluginRegistry().destination('fixture', 'one'), undefined);
    assert.throws(() => one.register({ destinations: [{ instanceId: 'two', destination: destination() }], commands: [{ name: 'sync', run: async () => null }] }), /duplicate command/);
    assert.equal(one.destination('fixture', 'two'), undefined);
    assert.throws(() => one.register({ tools: [{ name: 'delivery_status', description: 'duplicate', run: async () => null }] }), /duplicate tool/);
    await assert.rejects(loadHistoryPlugins([{ module: 'missing-explicit-history-plugin' }]), /configured history plugin 1/);
    assert.ok(await loadHistoryPlugins([]));
  });
});

test('standalone export has replayable bounded cursors and creates no delivery job', async () => {
  await fixture(async (dbPath) => {
    const snapshot = await beginHistoryExport(selection, { dbPath, limits: { ...DEFAULT_DELIVERY_LIMITS, max_batch_records: 1 } });
    try {
      const first = await readHistoryExportPage(snapshot.cursor, { dbPath });
      assert.equal(first.records.length, 1);
      assert.deepEqual(await readHistoryExportPage(snapshot.cursor, { dbPath }), first);
      assert.ok(first.next_cursor);
    } finally { await closeHistoryExport(snapshot.snapshot_id, { dbPath }); }
    const rows = [];
    for await (const row of exportHistory(selection, { dbPath })) rows.push(row);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows.map((row) => row.session_id).sort(), ['both', 'local-only', 'remote-only']);
    assert.ok(rows.every((row) => row.origin_id && row.record_id && row.revision_id));
    await assert.rejects(historyDeliveryStatus(undefined, { dbPath }), { code: 'HISTORY_DELIVERY_MOVED' });
  });
});

test('NDJSON CLI emits only complete records and no remote acceptance claims', async () => {
  await fixture(async (dbPath, root) => {
    const selectionPath = join(root, 'selection.json');
    await writeFile(selectionPath, JSON.stringify(selection));
    const { stdout } = await run(process.execPath, [join(dirname(fileURLToPath(import.meta.url)), 'cli.js'), 'export', '--selection', selectionPath, '--db', dbPath, '--no-warning']);
    const records = stdout.trim().split('\n').map((line) => JSON.parse(line) as { revision_id: string });
    assert.equal(records.length, 3);
    assert.ok(records.every((record) => record.revision_id));
    await assert.rejects(historyDeliveryStatus(undefined, { dbPath }), { code: 'HISTORY_DELIVERY_MOVED' });
  });
});


test('NDJSON output cannot replace the active database through its path, symlink, or hard link', async () => {
  await fixture(async (dbPath, root) => {
    const selectionPath = join(root, 'selection.json');
    await writeFile(selectionPath, JSON.stringify(selection));
    const symbolic = join(root, 'history-symlink.db');
    const hard = join(root, 'history-hardlink.db');
    await symlink(dbPath, symbolic); await link(dbPath, hard);
    const before = await readFile(dbPath);
    for (const output of [dbPath, symbolic, hard]) {
      await assert.rejects(run(process.execPath, [join(dirname(fileURLToPath(import.meta.url)), 'cli.js'), 'export',
        '--selection', selectionPath, '--db', dbPath, '--out', output, '--no-warning']),
      (error: unknown) => typeof error === 'object' && error !== null && 'stderr' in error
        && String(error.stderr).includes('export output must not replace the active history database'));
      assert.deepEqual(await readFile(dbPath), before);
    }
  });
});


test('plugin CLI passes arguments after its explicit separator verbatim', async () => {
  await fixture(async (_dbPath, root) => {
    await writeFile(join(root, 'plugin.mjs'), `export function createHistoryPlugin() { return { commands: [{ name: 'echo-args', run: async args => args }] }; }`);
    const configPath = join(root, 'config.json');
    await writeFile(configPath, JSON.stringify({ plugins: [{ module: './plugin.mjs' }] }));
    const pluginArgs = ['--base-url', 'https://example.invalid', '--key=value', '', '-h', '--', '--config', 'plugin-value'];
    const result = await run(process.execPath, [join(sdkRoot, 'dist/cli.js'), '--no-warning', 'plugin', 'echo-args', '--config', configPath, '--', ...pluginArgs]);
    assert.deepEqual(JSON.parse(result.stdout), pluginArgs);
    await assert.rejects(run(process.execPath, [join(sdkRoot, 'dist/cli.js'), 'plugin', 'echo-args', '--unknown', 'value', '--config', configPath, '--', '-h']));
  });
});


test('export rejects aliases through symlinked parents before creating a new database', async () => {
  await fixture(async (_dbPath, root) => {
    const real = join(root, 'real'); const alias = join(root, 'alias');
    await mkdir(real); await symlink(real, alias, 'dir');
    const selectionPath = join(root, 'selection.json');
    await writeFile(selectionPath, JSON.stringify(selection));
    const {runHistoryExportCommand} = await import('./delivery-cli.js');
    await assert.rejects(runHistoryExportCommand({dbPath:join(real,'new.db'),outputPath:join(alias,'new.db'),selectionPath}), /active history database/);
    await assert.rejects(readFile(join(real,'new.db')), {code:'ENOENT'});
  });
});

test('arbitrary plugin MCP tools have conservative side-effect annotations', async () => {
  await fixture(async (dbPath, root) => {
    const plugin = join(root,'tool.mjs');
    await writeFile(plugin, `export function createHistoryPlugin() { return {tools:[{name:'write_fixture',description:'Arbitrary fixture action',run:async()=>({ok:true})}]}; }`);
    const configPath=join(root,'tools.json'); await writeFile(configPath,JSON.stringify({plugins:[{module:plugin}]}));
    const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string,string]=>entry[1]!==undefined));
    Object.assign(env,{HOME:root,USERPROFILE:root,XDG_DATA_HOME:join(root,'share'),AI_HIST_DB:dbPath,AI_HIST_PLUGIN_CONFIG:configPath});
    const transport = new StdioClientTransport({command:process.execPath,args:[join(sdkRoot,'dist/mcp-server.js')],env,stderr:'pipe'});
    const client = new Client({name:'plugin-annotations-test',version:'1'});
    try {
      await client.connect(transport);
      const tool=(await client.listTools()).tools.find(item=>item.name==='write_fixture');
      assert.deepEqual(tool?.annotations,{readOnlyHint:false,destructiveHint:true,idempotentHint:false,openWorldHint:true});
    } finally {await client.close();await transport.close();}
  });
});



test('SDK, CLI and MCP report migration without creating a store or invoking receivers', async t => {
  const root=await mkdtemp(join(tmpdir(),'upload-migration-'));
  t.after(()=>rm(root,{recursive:true,force:true}));
  const dbPath=join(root,'absent','history.db');
  let called=false;
  const receivers=registry(destination({prepare:async()=>{called=true;throw new Error('must not run');}}));
  for (const operation of [
    ()=>createHistoryDelivery(config(),{dbPath}),
    ()=>historyDeliveryStatus(undefined,{dbPath}),
    ()=>historyDeliveryRetention({dbPath}),
    ()=>controlHistoryDelivery('job','pause',{dbPath}),
    ()=>drainHistoryDelivery(receivers,{dbPath}),
    ()=>runHistoryDelivery(receivers,{dbPath}),
    ()=>deliveryRequest({operation:'create_job',config:config()},{dbPath}),
  ]) await assert.rejects(operation,{code:'HISTORY_DELIVERY_MOVED'});
  assert.equal(called,false);
  await assert.rejects(run(process.execPath,[join(sdkRoot,'dist/cli.js'),'delivery','status','--db',dbPath]),error=>String((error as {stderr?:string}).stderr).includes('agent-relay-probe'));
  const env=Object.fromEntries(Object.entries(process.env).filter((entry):entry is [string,string]=>entry[1]!==undefined));
  env.AI_HIST_DB=dbPath;delete env.AI_HIST_PLUGIN_CONFIG;
  const transport=new StdioClientTransport({command:process.execPath,args:[join(sdkRoot,'dist/mcp-server.js')],env,stderr:'pipe'});
  const client=new Client({name:'migration',version:'1'});
  try { await client.connect(transport);const result=await client.callTool({name:'delivery_status',arguments:{}});assert.equal(result.isError,true);assert.match(JSON.stringify(result),/agent-relay-probe/); }
  finally { await client.close(); }
  await assert.rejects(access(join(root,'absent')),{code:'ENOENT'});
});

test('native export accepts the full decoded selection budget and bounds the wire envelope separately', async () => {
  await fixture(async dbPath => {
    const large: HistoryExportSelection = { ...selection, sources: [], sessions: [{ source: 'claude', session_id: '' }] };
    large.sessions[0].session_id = 'x'.repeat(65_536 - Buffer.byteLength(JSON.stringify(large)));
    assert.equal(Buffer.byteLength(JSON.stringify(large)), 65_536);
    const snapshot = await beginHistoryExport(large, { dbPath });
    await closeHistoryExport(snapshot.snapshot_id, { dbPath });
    const request = JSON.stringify({ operation: 'create_export', selection: large,
      limits: DEFAULT_DELIVERY_LIMITS, ttl_ms: 60_000, now_ms: Date.now() });
    assert.ok(Buffer.byteLength(request) > 65_536);
    const escaped = request.replace(/x/g, '\\u0078');
    const wireSnapshot = JSON.parse(await nativeCall(native => native.historyExport(escaped, dbPath))) as { snapshot_id: string };
    await closeHistoryExport(wireSnapshot.snapshot_id, { dbPath });
    large.sessions[0].session_id += 'x';
    await assert.rejects(beginHistoryExport(large, { dbPath }), { code: 'HISTORY_EXPORT_FAILED' });
    await assert.rejects(nativeCall(native => native.historyExport(' '.repeat(6 * 65_536 + 4097), dbPath)),
      (error: unknown) => error instanceof Error && /bounded envelope limit/.test(error.message));
  });
});
