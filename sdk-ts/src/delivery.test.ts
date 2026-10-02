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
  beginHistoryExport, closeHistoryExport,
  canonicalDeliveryJson, DEFAULT_DELIVERY_LIMITS, deliveryBatchDigest, deliveryDigest,
  deliveryRecordDigest, exportHistory,
  HistoryDeliveryError, HistoryPluginRegistry, loadHistoryPlugins, readHistoryExportPage,
  parseDeliveryConflict, recoverDeliveryConflict,
  type HistoryDestination, type HistoryExportBatch, type HistoryExportSelection,
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

function conflictBatch(): HistoryExportBatch {
  const records = ['before', 'poison', 'after'].map((text, index) => ({
    schema_version: 1, origin_id: 'origin', record_id: `record-${index}`,
    revision_id: `revision-${index}`, revision: index + 1, kind: 'history' as const,
    source: 'claude', session_id: index === 0 ? '' : 'session', operation: 'upsert' as const,
    payload: { text, nested: index },
  }));
  return { schema_version: 1, origin_id: 'origin', batch_id: 'batch', job_id: 'job', generation: 1,
    destination_id: 'fixture', instance_id: 'one', account_id: 'relayhistory:account', mapping_version: '1', records };
}

test('only an exact 409 delivery_conflict response enables recovery', () => {
  const batch = conflictBatch(); const record = batch.records[1];
  const conflict = { type: 'record_revision' as const, originId: batch.origin_id,
    recordId: record.record_id, submittedRevisionId: record.revision_id, submittedRevision: record.revision,
    submittedDigest: deliveryRecordDigest(record), currentRevisionId: 'durable-revision',
    currentRevision: record.revision, currentDigest: 'a'.repeat(64) };
  const body = JSON.stringify({ error: { code: 'delivery_conflict', message: 'conflict',
    conflict, conflicts: [conflict], conflictCount: 1 }, correlationId: 'request' });
  assert.deepEqual(parseDeliveryConflict(409, body)?.error.conflict, conflict);
  assert.equal(parseDeliveryConflict(400, body), undefined);
  assert.equal(parseDeliveryConflict(409, body.replace('delivery_conflict', 'cursor_not_found')), undefined);
  assert.equal(parseDeliveryConflict(409, '{'), undefined);
  assert.equal(parseDeliveryConflict(409, { error: { code: 'delivery_conflict', message: 'conflict',
    conflict: { ...conflict, currentRevision: conflict.currentRevision + 1 },
    conflicts: [conflict], conflictCount: 1 } }), undefined);
  assert.equal(parseDeliveryConflict(409, { error: { code: 'delivery_conflict', message: 'conflict',
    conflict, conflicts: [], conflictCount: 1 } }), undefined);
});

test('delivery digests match JSON wire semantics for sparse arrays', () => {
  const sparse = Array<number>(2);
  sparse[0] = 1;
  const batch = conflictBatch();
  const record = { ...batch.records[1], payload: { sparse } };
  const onWire = JSON.parse(JSON.stringify(record));
  assert.deepEqual(onWire.payload.sparse, [1, null]);
  assert.equal(deliveryRecordDigest(record), deliveryRecordDigest(onWire));
});

test('delivery digests omit object values JSON does not put on the wire', () => {
  const batch = conflictBatch();
  const record = { ...batch.records[1], payload: {
    kept: 'value', omittedUndefined: undefined,
    omittedFunction: () => 'value', omittedSymbol: Symbol('value'),
    array: [undefined, () => 'value', Symbol('value')],
  } };
  const onWire = JSON.parse(JSON.stringify(record));
  assert.deepEqual(onWire.payload, { kept: 'value', array: [null, null, null] });
  assert.equal(deliveryRecordDigest(record), deliveryRecordDigest(onWire));
});

test('delivery digest Unicode key ordering matches the protocol fixture', () => {
  const value = { '\uE000': 1, '\u{10000}': 2 };
  const canonical = canonicalDeliveryJson(value);
  assert.equal(canonical, '{"\u{10000}":2,"\uE000":1}');
  assert.equal(Buffer.from(canonical, 'utf8').toString('hex'),
    '7b22f0908080223a322c22ee8080223a317d');
  assert.equal(deliveryDigest(value),
    '9d4cdc71dda603c42f9b21d88d0c2ffc31a76cd1bd461d7359406cf169845f1e');
});

test('a proven record conflict quarantines only the poison revision and drains later records', () => {
  const batch = conflictBatch(); const record = batch.records[1];
  const conflict = {
    type: 'record_revision', originId: batch.origin_id, recordId: record.record_id,
    submittedRevisionId: record.revision_id, submittedRevision: record.revision,
    submittedDigest: deliveryRecordDigest(record), currentRevisionId: 'durable-revision',
    currentRevision: record.revision, currentDigest: 'b'.repeat(64),
  } as const;
  const response = parseDeliveryConflict(409, { error: { code: 'delivery_conflict', message: 'conflict',
    conflict, conflicts: [conflict], conflictCount: 1 } });
  assert.ok(response);
  const first = recoverDeliveryConflict(batch, response);
  const replay = recoverDeliveryConflict(batch, response);
  assert.deepEqual(first, replay, 'recovery must be deterministic across a lost response');
  assert.deepEqual(first.quarantinedRevisionIds, ['revision-1']);
  assert.deepEqual(first.retryBatch?.records.map((item) => item.revision_id), ['revision-0', 'revision-2']);
  assert.notEqual(first.retryBatch?.batch_id, batch.batch_id);
  assert.deepEqual(ack(first.retryBatch!).accepted_revision_ids, ['revision-0', 'revision-2'],
    'records behind the conflict remain deliverable');
  assert.deepEqual(first.retryBatch?.records.map((item) => item.revision), [1, 3],
    'recovery must not invent record revisions');
  const changed = { ...conflict, submittedDigest: 'c'.repeat(64) };
  const changedResponse = parseDeliveryConflict(409, { error: { code: 'delivery_conflict',
    message: 'conflict', conflict: changed, conflicts: [changed], conflictCount: 1 } });
  assert.ok(changedResponse);
  assert.throws(() => recoverDeliveryConflict(batch, changedResponse), /does not match submitted content/);
});

test('a batch-id conflict deterministically rekeys the whole immutable batch', () => {
  const batch = conflictBatch();
  const response = parseDeliveryConflict(409, { error: { code: 'delivery_conflict', message: 'conflict', conflict: {
    type: 'batch_id', originId: batch.origin_id, batchId: batch.batch_id,
    submittedDigest: deliveryBatchDigest(batch), currentDigest: 'd'.repeat(64),
  } } });
  assert.ok(response);
  const recovery = recoverDeliveryConflict(batch, response);
  assert.deepEqual(recovery.quarantinedRevisionIds, []);
  assert.deepEqual(recovery.retryBatch?.records, batch.records);
  assert.notEqual(recovery.retryBatch?.batch_id, batch.batch_id);
  assert.equal(recoverDeliveryConflict(batch, response).retryBatch?.batch_id, recovery.retryBatch?.batch_id);
});

test('a singleton record conflict completes as quarantine without another request', () => {
  const original = conflictBatch(); const batch = { ...original, records: [original.records[1]] };
  const record = batch.records[0];
  const conflict = {
    type: 'record_revision', originId: batch.origin_id, recordId: record.record_id,
    submittedRevisionId: record.revision_id, submittedRevision: record.revision,
    submittedDigest: deliveryRecordDigest(record), currentRevisionId: 'durable-revision',
    currentRevision: record.revision, currentDigest: 'e'.repeat(64),
  } as const;
  const response = parseDeliveryConflict(409, { error: { code: 'delivery_conflict', message: 'conflict',
    conflict, conflicts: [conflict], conflictCount: 1 } })!;
  assert.deepEqual(recoverDeliveryConflict(batch, response), {
    quarantinedRevisionIds: ['revision-1'], retryBatch: null,
  });
});

test('all reported record conflicts are quarantined in one deterministic recovery', () => {
  const batch = conflictBatch();
  const conflicts = batch.records.slice(1).map((record, index) => ({
    type: 'record_revision' as const, originId: batch.origin_id, recordId: record.record_id,
    submittedRevisionId: record.revision_id, submittedRevision: record.revision,
    submittedDigest: deliveryRecordDigest(record), currentRevisionId: `durable-${index}`,
    currentRevision: record.revision, currentDigest: String(index + 1).repeat(64),
  }));
  const response = parseDeliveryConflict(409, { error: { code: 'delivery_conflict',
    message: 'conflict', conflict: conflicts[0], conflicts, conflictCount: conflicts.length } });
  assert.ok(response);
  const recovery = recoverDeliveryConflict(batch, response);
  assert.deepEqual(recovery.quarantinedRevisionIds, ['revision-1', 'revision-2']);
  assert.deepEqual(recovery.retryBatch?.records.map((record) => record.revision_id), ['revision-0']);
  assert.deepEqual(recoverDeliveryConflict(batch, response), recovery);
});

test('receiver collation order is accepted without a conflicting UTF-16 sort check', () => {
  const original = conflictBatch();
  const records = original.records.slice(1).map((record, index) => ({
    ...record, record_id: index === 0 ? "\uE000" : "\u{10000}",
  }));
  const batch = { ...original, records };
  const conflicts = records.map((record, index) => ({
    type: 'record_revision' as const, originId: batch.origin_id, recordId: record.record_id,
    submittedRevisionId: record.revision_id, submittedRevision: record.revision,
    submittedDigest: deliveryRecordDigest(record), currentRevisionId: `current-${index}`,
    currentRevision: record.revision, currentDigest: String(index + 3).repeat(64),
  }));
  // PostgreSQL byte/collation order can put U+E000 before U+10000, while JS
  // UTF-16 comparison puts the astral character first. Ordering is the
  // receiver's concern; actionable identity validation must be collation-free.
  const response = parseDeliveryConflict(409, { error: { code: 'delivery_conflict',
    message: 'conflict', conflict: conflicts[0], conflicts, conflictCount: 2 } });
  assert.ok(response);
  assert.deepEqual(recoverDeliveryConflict(batch, response).quarantinedRevisionIds,
    records.map((record) => record.revision_id));
});

test('plugin registration is inert, per-client, and rejects collisions atomically', async () => {
  await fixture(async () => {
    let calls = 0;
    const one = registry(destination({ prepare: async () => { calls++; throw new Error('should not execute'); } }));
    assert.equal(calls, 0);
    assert.equal(new HistoryPluginRegistry().destination('fixture', 'one'), undefined);
    assert.throws(() => one.register({ destinations: [
      { instanceId: 'two', destination: destination() },
      { instanceId: 'one', destination: destination() },
    ] }), /duplicate destination/);
    assert.equal(one.destination('fixture', 'two'), undefined);
    await assert.rejects(loadHistoryPlugins([{ module: 'missing-explicit-history-plugin' }]), /configured history plugin 1/);
    assert.ok(await loadHistoryPlugins([]));
  });
});

test('standalone export has replayable bounded cursors', async () => {
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

test('an expired export snapshot is released when its cursor returns', async () => {
  await fixture(async dbPath => {
    const snapshot = await beginHistoryExport(selection, { dbPath });
    const page = (now_ms: number) => nativeCall(native => native.historyExport(
      JSON.stringify({ operation: 'export_page', cursor: snapshot.cursor, now_ms }), dbPath));
    await assert.rejects(page(snapshot.expires_at_ms), /export snapshot expired/);
    await assert.rejects(page(snapshot.expires_at_ms), /export cursor not found/);
  });
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
