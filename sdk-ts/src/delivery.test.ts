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

test('plugin registration is inert, per-client, and rejects collisions atomically', async () => {
  await fixture(async (dbPath) => {
    let calls = 0;
    const one = registry(destination({ prepare: async () => { calls++; throw new Error('should not execute'); } }));
    assert.equal(calls, 0);
    assert.deepEqual(await historyDeliveryStatus(undefined, { dbPath }), []);
    assert.equal(new HistoryPluginRegistry().destination('fixture', 'one'), undefined);
    assert.throws(() => one.register({ destinations: [{ instanceId: 'two', destination: destination() }], commands: [{ name: 'sync', run: async () => null }] }), /duplicate command/);
    assert.equal(one.destination('fixture', 'two'), undefined);
    assert.throws(() => one.register({ tools: [{ name: 'delivery_status', description: 'duplicate', run: async () => null }] }), /duplicate tool/);
    await assert.rejects(loadHistoryPlugins([{ module: 'missing-explicit-history-plugin' }]), /configured history plugin 1/);
    assert.ok(await loadHistoryPlugins([]));
  });
});

test('lost acknowledgment retries persisted identical payload without remapping or duplicate effects', async () => {
  await fixture(async (dbPath) => {
    const job = await createHistoryDelivery(config(), { dbPath });
    const bodies: string[] = []; const batches: string[] = []; const receiver = new Map<string, unknown>();
    let preparations = 0;
    const first = registry(destination({
      prepare: async (batch) => { preparations++; return { content_type: 'application/json', body: JSON.stringify(batch.records) }; },
      send: async (payload, { batch }) => {
        bodies.push(payload.body); batches.push(batch.batch_id);
        for (const record of batch.records) receiver.set(record.revision_id, record);
        throw new HistoryDeliveryError('transient');
      },
    }));
    const failed = await drainHistoryDelivery(first, { dbPath, maxBatches: 1 });
    assert.equal(failed.statuses[0].failure, 'transient');
    assert.equal(failed.statuses[0].acknowledged_records, 0);
    assert.equal(failed.statuses[0].pending_records, 3);
    await controlHistoryDelivery(job.job_id, 'retry', { dbPath });
    // Recreate every host/plugin object, retaining only the database and fake receiver.
    const restarted = registry(destination({
      prepare: async () => { throw new Error('retry must use persisted mapping'); },
      send: async (payload, { batch }) => {
        bodies.push(payload.body); batches.push(batch.batch_id);
        for (const record of batch.records) receiver.set(record.revision_id, record);
        return ack(batch);
      },
    }));
    const delivered = await drainHistoryDelivery(restarted, { dbPath });
    assert.deepEqual(delivered.issues, []);
    assert.equal(delivered.statuses[0].acknowledged_records, 3);
    assert.equal(delivered.statuses[0].pending_records, 0);
    assert.equal(delivered.statuses[0].acceptance_level, 'durable');
    assert.equal(preparations, 1); assert.equal(receiver.size, 3);
    assert.equal(bodies[0], bodies[1]); assert.equal(batches[0], batches[1]);
    assert.ok(delivered.retention.usedBytes < delivered.retention.limitBytes);
  });
});

test('partial and invalid acknowledgments cannot skip holes; explicit retry unblocks permanent failure', async () => {
  await fixture(async (dbPath) => {
    const job = await createHistoryDelivery(config(), { dbPath });
    const partial = await drainHistoryDelivery(registry(destination({ send: async (_payload, { batch }) => ({ ...ack(batch), accepted_revision_ids: [batch.records[0].revision_id] }) })), { dbPath, maxBatches: 1 });
    assert.equal(partial.statuses[0].acknowledged_records, 0);
    assert.equal(partial.statuses[0].pending_records, 3);
    await controlHistoryDelivery(job.job_id, 'retry', { dbPath });
    const invalid = await drainHistoryDelivery(registry(destination({ send: async (_payload, { batch }) => ({ ...ack(batch), batch_id: 'wrong-batch' }) })), { dbPath });
    assert.equal(invalid.statuses[0].state, 'blocked');
    assert.equal(invalid.statuses[0].failure, 'invalid_payload');
    assert.equal(invalid.statuses[0].acknowledged_records, 0);
    await controlHistoryDelivery(job.job_id, 'retry', { dbPath });
    const success = await drainHistoryDelivery(registry(destination()), { dbPath });
    assert.equal(success.statuses[0].acknowledged_records, 3);
  });
});

test('one blocked destination cannot stop another and mapping upgrades preserve pending work', async () => {
  await fixture(async (dbPath) => {
    const first = await createHistoryDelivery(config('one'), { dbPath });
    await createHistoryDelivery(config('two'), { dbPath });
    const selected = registry(destination({ send: async () => { throw new HistoryDeliveryError('authentication_required'); } }));
    selected.register({ destinations: [{ instanceId: 'two', destination: destination() }] });
    const result = await drainHistoryDelivery(selected, { dbPath });
    assert.equal(result.statuses.find((job) => job.config.instance_id === 'one')?.failure, 'authentication_required');
    assert.equal(result.statuses.find((job) => job.config.instance_id === 'two')?.acknowledged_records, 3);
    await controlHistoryDelivery(first.job_id, 'retry', { dbPath });
    const upgraded = await drainHistoryDelivery(registry(destination({ mappingVersion: '2' })), { dbPath, jobIds: [first.job_id] });
    assert.equal(upgraded.statuses[0].failure, 'mapping_version_mismatch');
    assert.equal(upgraded.statuses[0].pending_records, 3);
  });
});

test('request deadline and worker shutdown leave unconfirmed work retryable', async () => {
  await fixture(async (dbPath) => {
    await createHistoryDelivery(config(), { dbPath });
    let aborted = false;
    // The deadline covers mapping and native persistence too. Hang in the first
    // plugin phase so this checks cancellation even on a slow debug-native host.
    const hanging = registry(destination({ prepare: async (_batch, { signal }) => {
      signal.addEventListener('abort', () => { aborted = true; }, { once: true });
      return new Promise(() => {});
    } }));
    const result = await drainHistoryDelivery(hanging, { dbPath, requestTimeoutMs: 100 });
    assert.equal(aborted, true);
    assert.equal(result.statuses[0].failure, 'transient');
    assert.equal(result.statuses[0].acknowledged_records, 0);
    const controller = new AbortController();
    let callbacks = 0;
    await runHistoryDelivery(hanging, { dbPath, signal: controller.signal, pollIntervalMs: 10,
      onProgress: () => { callbacks++; controller.abort(); } });
    assert.equal(callbacks, 1);
    assert.equal((await historyDeliveryStatus(undefined, { dbPath }))[0].pending_records, 3);
  });
});

test('lease renewal prevents a concurrent host from dispatching the same batch', async () => {
  await fixture(async (dbPath) => {
    await createHistoryDelivery(config(), { dbPath });
    let entered!: () => void;
    const sending = new Promise<void>((resolve) => { entered = resolve; });
    let finish!: () => void;
    const release = new Promise<void>((resolve) => { finish = resolve; });
    let sends = 0;
    const selected = registry(destination({ send: async (_payload, { batch }) => { sends++; entered(); await release; return ack(batch); } }));
    // Allow native I/O scheduler latency, then wait longer than the original
    // lease: only successful renewal can prevent the second dispatch.
    const first = drainHistoryDelivery(selected, { dbPath, leaseMs: 3_000, requestTimeoutMs: 15_000 });
    await sending;
    await pause(4_000);
    const second = await drainHistoryDelivery(selected, { dbPath, leaseMs: 3_000, requestTimeoutMs: 15_000 });
    assert.equal(second.attempts, 0);
    finish();
    assert.equal((await first).statuses[0].acknowledged_records, 3);
    assert.equal(sends, 1);
  });
});

test('eligibility changed during mapping is rechecked before transport', async () => {
  await fixture(async (dbPath) => {
    await createHistoryDelivery(config(), { dbPath });
    let sent = 0;
    const selected = registry(destination({
      prepare: async (batch) => {
        for (const id of ['local-only', 'remote-only', 'both']) await deliveryRequest({ operation: 'set_session_excluded', session: { source: 'claude', session_id: id }, excluded: true }, { dbPath });
        return { content_type: 'application/json', body: JSON.stringify(batch.records) };
      },
      send: async (_payload, { batch }) => { sent++; return ack(batch); },
    }));
    await drainHistoryDelivery(selected, { dbPath });
    assert.equal(sent, 0);
    const [job] = await historyDeliveryStatus(undefined, { dbPath });
    await controlHistoryDelivery(job.job_id, 'retry', { dbPath });
    const suppressed = await drainHistoryDelivery(selected, { dbPath });
    assert.equal(suppressed.statuses[0].suppressed_records, 3);
    assert.equal(suppressed.statuses[0].acknowledged_records, 0);
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
    assert.deepEqual(await historyDeliveryStatus(undefined, { dbPath }), []);
  });
});

test('installed fixture plugin survives process termination after acceptance and checks account identity', async () => {
  await fixture(async (dbPath, root) => {
    const modules = join(root, 'node_modules');
    await mkdir(modules);
    await symlink(sdkRoot, join(modules, 'ai-hist'), 'dir');
    await cp(new URL('../fixtures/destination-plugin', import.meta.url), join(modules, 'history-fixture-destination'), { recursive: true });
    const receiverPath = join(root, 'receiver.json');
    const job = await createHistoryDelivery(config(), { dbPath });
    const options = { instanceId: 'one', accountId: 'fixture-account', receiverPath };
    const script = `import { loadHistoryPlugins, drainHistoryDelivery } from ${JSON.stringify(sdkModule)};
      const registry = await loadHistoryPlugins([{ module: 'history-fixture-destination', options: ${JSON.stringify({ ...options, crashAfterAcceptance: true })} }], { baseDirectory: ${JSON.stringify(root)} });
      await drainHistoryDelivery(registry, { dbPath: ${JSON.stringify(dbPath)}, leaseMs: 2_000, requestTimeoutMs: 10000 });`;
    await assert.rejects(run(process.execPath, ['--input-type=module', '--eval', script]), { code: 73 });
    const accepted = JSON.parse(await readFile(receiverPath, 'utf8')) as { records: Record<string, { origin_id: string; record_id: string; revision_id: string; revision: number }>; latest: Record<string, { revision_id: string; revision: number }>; attempts: unknown[] };
    assert.equal(Object.keys(accepted.records).length, 3);
    assert.equal((await historyDeliveryStatus(job.job_id, { dbPath }))[0].acknowledged_records, 0);
    await pause(2_100);
    const restarted = await loadHistoryPlugins([{ module: 'history-fixture-destination', options }], { baseDirectory: root });
    const result = await drainHistoryDelivery(restarted, { dbPath });
    assert.equal(result.statuses[0].acknowledged_records, 3);
    const retried = JSON.parse(await readFile(receiverPath, 'utf8')) as typeof accepted;
    assert.equal(Object.keys(retried.records).length, 3);
    assert.deepEqual(retried.attempts[0], retried.attempts[1]);
    const wrong = await createHistoryDelivery({ ...config('wrong'), account_id: 'another-account' }, { dbPath });
    const mismatch = await loadHistoryPlugins([{ module: 'history-fixture-destination', options: { ...options, instanceId: 'wrong' } }], { baseDirectory: root });
    const blocked = await drainHistoryDelivery(mismatch, { dbPath, jobIds: [wrong.job_id] });
    assert.equal(blocked.statuses[0].failure, 'permission_denied');
    assert.equal((JSON.parse(await readFile(receiverPath, 'utf8')) as typeof accepted).attempts.length, 2);
    // A stale request may already be in flight when another host sends a newer
    // revision. The receiver deduplicates revisions AND guards materialized state.
    const old = Object.values(accepted.records)[0];
    const newer = { ...old, revision: old.revision + 1, revision_id: `${old.revision_id}-new` };
    const send = async (record: typeof old) => {
      const body = JSON.stringify([record]);
      const batch = { schema_version: 1, origin_id: record.origin_id, batch_id: `late-${record.revision_id}`,
        job_id: job.job_id, generation: 1, destination_id: 'fixture', instance_id: 'one', account_id: 'fixture-account', mapping_version: '1', records: [record] } as HistoryExportBatch;
      await restarted.destination('fixture', 'one')!.send({ mapping_version: '1', content_type: 'application/json', body,
        sha256: createHash('sha256').update(body).digest('hex') }, { batch, signal: new AbortController().signal, idempotencyKey: batch.batch_id });
    };
    await send(newer);
    await send(old);
    const afterLate = JSON.parse(await readFile(receiverPath, 'utf8')) as typeof accepted;
    assert.equal(afterLate.latest[JSON.stringify([old.origin_id, old.record_id])].revision_id, newer.revision_id);
    assert.equal(Object.keys(afterLate.records).length, 4);

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
    assert.deepEqual(await historyDeliveryStatus(undefined, { dbPath }), []);
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

test('native delivery envelope permits worst-case escaping within the decoded payload limit', async () => {
  await fixture(async (dbPath) => {
    const settings = config(); settings.limits.max_prepared_bytes = 8 * 1_048_576;
    await createHistoryDelivery(settings,{dbPath});
    const body = '\0'.repeat(7 * 1_048_576);
    let received = false;
    const result = await drainHistoryDelivery(registry(destination({prepare:async()=>({content_type:'application/octet-stream',body}),send:async(payload,{batch})=>{
      assert.equal(payload.body,body); received=true; return ack(batch);
    }})),{dbPath,requestTimeoutMs:30_000});
    assert.equal(received,true); assert.equal(result.statuses[0].acknowledged_records,3);
  });
});


test('arbitrary plugin MCP tools have conservative side-effect annotations', async () => {
  await fixture(async (dbPath, root) => {
    const plugin = join(root,'tool.mjs');
    await writeFile(plugin, `export function createHistoryPlugin() { return {tools:[{name:'write_fixture',description:'Arbitrary fixture action',run:async()=>({ok:true})}]}; }`);
    const configPath=join(root,'tools.json'); await writeFile(configPath,JSON.stringify({plugins:[{module:plugin}]}));
    const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string,string]=>entry[1]!==undefined));
    Object.assign(env,{HOME:root,USERPROFILE:root,AI_HIST_DB:dbPath,AI_HIST_PLUGIN_CONFIG:configPath});
    const transport = new StdioClientTransport({command:process.execPath,args:[join(sdkRoot,'dist/mcp-server.js')],env,stderr:'pipe'});
    const client = new Client({name:'plugin-annotations-test',version:'1'});
    try {
      await client.connect(transport);
      const tool=(await client.listTools()).tools.find(item=>item.name==='write_fixture');
      assert.deepEqual(tool?.annotations,{readOnlyHint:false,destructiveHint:true,idempotentHint:false,openWorldHint:true});
    } finally {await client.close();await transport.close();}
  });
});


test('removing a delivery exclusion exposes the required generation recovery code', async () => {
  await fixture(async (dbPath) => {
    const job = await createHistoryDelivery(config(), { dbPath });
    const request = { operation: 'set_session_excluded', session: { source: 'claude', session_id: 'local-only' } };
    await deliveryRequest({ ...request, excluded: true }, { dbPath });
    await assert.rejects(deliveryRequest({ ...request, excluded: false }, { dbPath }), { code: 'DELIVERY_GENERATION_REQUIRED' });
    await controlHistoryDelivery(job.job_id, 'cancel', { dbPath });
    await deliveryRequest({ ...request, excluded: false }, { dbPath });
  });
});


test('SDK, CLI and MCP delivery status leave a missing store and its parents absent', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'history-status-missing-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const parent = join(root, 'absent');
  const dbPath = join(parent, 'history.db');
  assert.deepEqual(await historyDeliveryStatus(undefined, { dbPath }), []);
  assert.deepEqual(await historyDeliveryRetention({ dbPath }), { usedBytes: 0, limitBytes: 256 * 1048576 });
  await assert.rejects(historyDeliveryStatus('unknown', { dbPath }), /unknown delivery job/);
  await assert.rejects(access(parent));
  const output = await run(process.execPath, [join(sdkRoot, 'dist/cli.js'), 'delivery', 'status', '--db', dbPath]);
  assert.deepEqual(JSON.parse(output.stdout).jobs, []);
  await assert.rejects(access(parent));
  const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string,string] => entry[1] !== undefined));
  env.AI_HIST_DB = dbPath;
  delete env.AI_HIST_PLUGIN_CONFIG;
  const transport = new StdioClientTransport({ command: process.execPath, args: [join(sdkRoot, 'dist/mcp-server.js')], env, stderr: 'pipe' });
  const client = new Client({ name: 'missing-delivery-status', version: '1' });
  try {
    await client.connect(transport);
    const result = await client.callTool({ name: 'delivery_status', arguments: {} });
    assert.notEqual(result.isError, true);
    await assert.rejects(access(parent));
  } finally { await client.close(); }
});
