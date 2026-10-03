import { nativeCall } from './native.js';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import { access, cp, link, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
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

test('delivery digests serialize sparse array slots exactly as wire nulls', () => {
  const sparse = Array<string>(2);
  sparse[0] = 'present';
  const wire = JSON.parse(JSON.stringify({ values: sparse }));
  const record = { ...conflictBatch().records[0], payload: { values: sparse } };
  const received = { ...record, payload: wire };
  assert.equal(deliveryRecordDigest(record), deliveryRecordDigest(received));

  const trailingHole = Array<string>(1);
  const trailing = { ...record, payload: { values: trailingHole } };
  const trailingWire = { ...trailing, payload: JSON.parse(JSON.stringify(trailing.payload)) };
  assert.equal(deliveryRecordDigest(trailing), deliveryRecordDigest(trailingWire),
    'a trailing hole must not collapse to an empty array');
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

test('a file: URI --db is percent-decoded as SQLite decodes it before the output guard', async () => {
  await fixture(async (dbPath, root) => {
    const selectionPath = join(root, 'selection.json');
    await writeFile(selectionPath, JSON.stringify(selection));
    // SQLite opens `%41` as `A` and keeps the malformed `%ZZ` literal.
    const literal = join(root, 'hist%ZZA.db');
    await cp(dbPath, literal);
    const uri = `file:${join(root, 'hist%ZZ%41.db')}`;
    const {runHistoryExportCommand} = await import('./delivery-cli.js');
    for (const outputPath of [literal, `${literal}-wal`, `${literal}-shm`]) {
      await assert.rejects(runHistoryExportCommand({ dbPath: uri, outputPath, selectionPath }), /active history database/, outputPath);
    }
    const outputPath = join(root, 'export.ndjson');
    await runHistoryExportCommand({ dbPath: uri, outputPath, selectionPath });
    assert.ok((await readFile(outputPath, 'utf8')).length > 0);
  });
});

test('export refuses the live WAL and SHM sidecars and leaves committed rows readable', async (t) => {
  // node:sqlite ships unflagged from Node 22.5; on older runtimes the
  // path-only guard is still covered by the alias tests above.
  const sqlite = await import('node:sqlite' as string).catch(() => null) as
    | { DatabaseSync: new (path: string, options?: { readOnly?: boolean }) => {
        exec(sql: string): void; prepare(sql: string): { get(): unknown }; close(): void } }
    | null;
  if (!sqlite) { t.skip('node:sqlite is unavailable on this runtime'); return; }
  await fixture(async (dbPath, root) => {
    const selectionPath = join(root, 'selection.json');
    await writeFile(selectionPath, JSON.stringify(selection));
    const alias = join(root, 'alias'); await symlink(root, alias, 'dir');
    // A live writer whose committed row exists only in the WAL.
    const writer = new sqlite.DatabaseSync(dbPath);
    try {
      writer.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;');
      writer.exec('CREATE TABLE export_guard_probe(value TEXT); INSERT INTO export_guard_probe VALUES (\'committed\');');
      const walHardLink = join(root, 'wal-hardlink');
      await link(`${dbPath}-wal`, walHardLink);
      const {runHistoryExportCommand} = await import('./delivery-cli.js');
      // The native store opens SQLite URI filenames, so a `file:` --db names
      // the same database and must protect the same sidecars.
      // pathToFileURL gives the platform's well-formed URI (`file:///C:/…` on
      // Windows); the localhost form inserts the authority into it.
      const url = pathToFileURL(dbPath).href;
      for (const db of [dbPath, `file:${dbPath}`, `${url}?mode=rwc`, `${url.replace('file://', 'file://localhost')}#x`]) {
        for (const outputPath of [`${dbPath}-wal`, `${dbPath}-shm`, `${dbPath}-journal`,
          join(alias, 'history.db-wal'), join(alias, 'history.db-shm'), walHardLink]) {
          await assert.rejects(runHistoryExportCommand({ dbPath: db, outputPath, selectionPath }), /active history database/, `${db} -> ${outputPath}`);
        }
      }
      await assert.rejects(runHistoryExportCommand({ dbPath: `file://elsewhere${dbPath}`, outputPath: join(root, 'x.ndjson'), selectionPath }),
        /unsupported SQLite URI authority/);
      const reader = new sqlite.DatabaseSync(dbPath, { readOnly: true });
      try {
        assert.deepEqual({ ...reader.prepare('SELECT value FROM export_guard_probe').get() as object }, { value: 'committed' });
      } finally { reader.close(); }
      // An ordinary existing output file is still replaced.
      const outputPath = join(root, 'export.ndjson');
      await writeFile(outputPath, 'stale');
      await runHistoryExportCommand({ dbPath, outputPath, selectionPath });
      const exported = await readFile(outputPath, 'utf8');
      assert.notEqual(exported, 'stale');
      // ...and a URI --db really does export the same database.
      await runHistoryExportCommand({ dbPath: `file:${dbPath}`, outputPath, selectionPath });
      assert.equal((await readFile(outputPath, 'utf8')).split('\n').length, exported.split('\n').length);
    } finally { writer.close(); }
  });
});

test('an abandoned export snapshot is released when its TTL elapses, with no further export call', async (t) => {
  // node:sqlite ships unflagged from Node 22.5; it is the independent writer
  // and checkpointer here.
  const sqlite = await import('node:sqlite' as string).catch(() => null) as
    | { DatabaseSync: new (path: string) => {
        exec(sql: string): void; prepare(sql: string): { get(): unknown }; close(): void } }
    | null;
  if (!sqlite) { t.skip('node:sqlite is unavailable on this runtime'); return; }
  await fixture(async dbPath => {
    const writer = new sqlite.DatabaseSync(dbPath);
    // The snapshot lives in another process: two SQLite copies in one
    // process do not see each other's POSIX locks, so an in-process holder
    // could never block this checkpoint in the first place.
    const holder = spawn(process.execPath, ['--input-type=module', '-e', `
      import { beginHistoryExport } from ${JSON.stringify(sdkModule)};
      // Never closed and never paged again: only its TTL can release it.
      await beginHistoryExport(${JSON.stringify(selection)}, { dbPath: ${JSON.stringify(dbPath)}, ttlMs: 1_500 });
      process.stdout.write('ready\\n');
      setTimeout(() => {}, 10_000);`], { stdio: ['ignore', 'pipe', 'pipe'] });
    // Listeners go on before anything else can run, and the wait is bounded,
    // so a holder that fails or never reports cannot hang the suite.
    let stderr = '';
    holder.stderr.on('data', (chunk: Buffer) => { stderr += String(chunk); });
    let timer: NodeJS.Timeout | undefined;
    const ready = new Promise<void>((resolve, reject) => {
      holder.stdout.on('data', (chunk: Buffer) => { if (String(chunk).includes('ready')) resolve(); });
      holder.on('error', reject);
      holder.on('exit', (code, signal) => reject(new Error(`snapshot holder exited early (${code ?? signal}): ${stderr}`)));
      timer = setTimeout(() => reject(new Error(`snapshot holder never became ready: ${stderr}`)), 20_000);
    });
    try {
      writer.exec('PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS ttl_probe(n INTEGER);');
      const checkpoint = (n: number) => {
        writer.exec(`INSERT INTO ttl_probe VALUES (${n})`);
        return writer.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get() as { busy: number };
      };
      try { await ready; } finally { clearTimeout(timer); }
      assert.equal(checkpoint(1).busy, 1, 'a live snapshot holds the checkpoint back');
      await pause(3_000);
      assert.equal(holder.exitCode, null, 'the holder process is still running');
      assert.equal(checkpoint(2).busy, 0, 'the expired snapshot still holds its read transaction');
    } finally { holder.kill(); writer.close(); }
  });
});

test('an export snapshot left open does not keep the process alive', async () => {
  await fixture(async dbPath => {
    const script = `import { beginHistoryExport } from ${JSON.stringify(sdkModule)};
      await beginHistoryExport(${JSON.stringify(selection)}, { dbPath: ${JSON.stringify(dbPath)}, ttlMs: 3600000 });`;
    // A process kept alive by the reaper would hit the timeout and reject.
    await run(process.execPath, ['--input-type=module', '-e', script], { timeout: 30_000 });
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
