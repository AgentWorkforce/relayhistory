import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { promisify } from 'node:util';
import {
  beginHistoryExport, closeHistoryExport, DEFAULT_EXPORT_LIMITS, exportLimitsFromPublished,
  migrateStore, readHistoryExportPage,
  type HistoryExportRecord, type HistoryExportSelection,
} from './index.js';

const run = promisify(execFile);
const selection: HistoryExportSelection = {
  all_sources: false, sources: ['claude'], sessions: [], kinds: ['history'], excluded_sessions: [],
};
const ROWS = 501;

test('a published delivery limit sizes the export page and an absent one does not', () => {
  assert.deepEqual(exportLimitsFromPublished(undefined), DEFAULT_EXPORT_LIMITS);
  assert.deepEqual(exportLimitsFromPublished(null), DEFAULT_EXPORT_LIMITS);
  assert.deepEqual(exportLimitsFromPublished({}), DEFAULT_EXPORT_LIMITS);
  assert.deepEqual(exportLimitsFromPublished({ limits: {} }), DEFAULT_EXPORT_LIMITS);
  for (const maxRecords of [0, -1, 1.5, '500', null, true]) {
    assert.deepEqual(exportLimitsFromPublished({ maxRecords }), DEFAULT_EXPORT_LIMITS, String(maxRecords));
  }

  const published = exportLimitsFromPublished({ maxRecords: 500, maxRequestBytes: 2_097_152, contentEncodings: ['zstd'] });
  assert.equal(published.max_batch_records, 500);
  assert.equal(published.max_batch_bytes, DEFAULT_EXPORT_LIMITS.max_batch_bytes);
  assert.equal(published.max_scan_records, 500);

  const receipt = exportLimitsFromPublished({
    receiptId: 'receipt', acceptanceLevel: 'durable',
    limits: { maxRecords: 500, maxRequestBytes: 2_097_152 },
  });
  assert.deepEqual(receipt, published);

  assert.equal(exportLimitsFromPublished({ maxRecords: 50 }).max_batch_records, 50);
  assert.equal(exportLimitsFromPublished({ maxRecords: 50 }).max_scan_records, DEFAULT_EXPORT_LIMITS.max_scan_records);
  assert.equal(exportLimitsFromPublished({ limits: { maxRecords: 50 } }).max_batch_records, 50);
  const capped = exportLimitsFromPublished({ maxRecords: 10_001 });
  assert.equal(capped.max_batch_records, 10_000);
  assert.equal(capped.max_scan_records, 10_000);
  assert.equal(capped.max_batch_bytes, DEFAULT_EXPORT_LIMITS.max_batch_bytes);
});

async function seed(count: number): Promise<{ dbPath: string; root: string }> {
  const root = await mkdtemp(join(tmpdir(), 'history-export-limits-'));
  const dbPath = join(root, 'ai-history.db');
  await migrateStore({ dbPath });
  // Node 20, which CI uses, has no `node:sqlite`. The schema is the one
  // `migrateStore` just wrote; this only inserts rows.
  await run('python3', ['-c', `
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.executemany(
    "INSERT INTO history (source, session_id, prompt, timestamp_ms) VALUES ('claude', 's', 'p', ?)",
    [(i,) for i in range(int(sys.argv[2]))],
)
conn.commit()
conn.close()
`, dbPath, String(count)]);
  return { dbPath, root };
}

async function exportRecords(dbPath: string, publishedLimits?: unknown): Promise<HistoryExportRecord[][]> {
  const snapshot = await beginHistoryExport(selection, publishedLimits === undefined ? { dbPath } : { dbPath, publishedLimits });
  const pages: HistoryExportRecord[][] = [];
  try {
    let cursor: string | null = snapshot.cursor;
    while (cursor) {
      const page = await readHistoryExportPage(cursor, { dbPath });
      pages.push(page.records);
      cursor = page.next_cursor;
    }
  } finally {
    await closeHistoryExport(snapshot.snapshot_id, { dbPath });
  }
  return pages;
}

test('a published maxRecords of 500 fills one page and an omitted limit stops at 100', async () => {
  const { dbPath, root } = await seed(ROWS);
  try {
    const omitted = await exportRecords(dbPath);
    const published = await exportRecords(dbPath, { maxRecords: 500 });
    const lowered = await exportRecords(dbPath, { limits: { maxRecords: 50, maxRequestBytes: 2_097_152 } });
    const explicit = await beginHistoryExport(selection, {
      dbPath,
      limits: { ...DEFAULT_EXPORT_LIMITS, max_batch_records: 1 },
      publishedLimits: { maxRecords: 500 },
    });
    let explicitLength = 0;
    try {
      explicitLength = (await readHistoryExportPage(explicit.cursor, { dbPath })).records.length;
    } finally {
      await closeHistoryExport(explicit.snapshot_id, { dbPath });
    }
    const ids = (pages: HistoryExportRecord[][]) => pages.flat().map((record) => record.record_id);

    assert.deepEqual(omitted.map((page) => page.length), [100, 100, 100, 100, 100, 1]);
    assert.deepEqual(published.map((page) => page.length), [500, 1]);
    assert.deepEqual(lowered.map((page) => page.length), Array.from({ length: 10 }, () => 50).concat(1));
    assert.equal(explicitLength, 1);
    assert.deepEqual(ids(published), ids(omitted));
    assert.deepEqual(ids(lowered), ids(omitted));
    assert.ok(published.length < omitted.length);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
