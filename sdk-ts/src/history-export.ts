import { nativeCall } from './native.js';
export interface HistoryExportStorageOptions { dbPath?: string }
async function exportRequest<T>(request: Record<string, unknown>, options: HistoryExportStorageOptions): Promise<T> {
  return nativeCall(async native => JSON.parse(await native.historyExport(JSON.stringify(request), options.dbPath)) as T);
}
import { type HistoryExportRecord, type HistoryExportSelection } from './delivery-contracts.js';

export interface HistoryExportLimits { max_batch_records: number; max_batch_bytes: number; max_scan_records: number }
export const DEFAULT_EXPORT_LIMITS: HistoryExportLimits = { max_batch_records: 100, max_batch_bytes: 1_048_576, max_scan_records: 400 };
/** Native `ExportLimits` rejects a record or scan cap outside this range. */
const NATIVE_EXPORT_CAP = 10_000;
export interface HistoryExportHandle { snapshot_id: string; cursor: string; expires_at_ms: number }
export interface HistoryExportPage {
  schema_version: number; origin_id: string; records: HistoryExportRecord[]; next_cursor: string | null;
}
export interface HistoryExportOptions extends HistoryExportStorageOptions {
  /** Resolved page limits. When set, `publishedLimits` is not read. */
  limits?: HistoryExportLimits;
  /** A delivery receipt, or the `GET /v1/delivery/limits` body. */
  publishedLimits?: unknown;
  ttlMs?: number; signal?: AbortSignal;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Positive integer `maxRecords` from a receipt or a limits body. */
function publishedMaxRecords(published: unknown): number | undefined {
  if (!isRecord(published)) return undefined;
  // A receipt nests the server object under `limits`. The GET body is that object.
  const limits = isRecord(published.limits) ? published.limits : published;
  const maxRecords = limits.maxRecords;
  if (!Number.isSafeInteger(maxRecords) || (maxRecords as number) < 1) return undefined;
  return Math.min(maxRecords as number, NATIVE_EXPORT_CAP);
}

/** Page limits for a published delivery receipt or `GET /v1/delivery/limits` body.
 *
 * A positive integer `maxRecords` is the page's record cap, clamped to the
 * native maximum. Anything else leaves the page at {@link DEFAULT_EXPORT_LIMITS}.
 * `maxRequestBytes` counts the decoded HTTP body, not this page's JSON, so
 * the byte cap stays 1 MiB.
 */
export function exportLimitsFromPublished(published?: unknown): HistoryExportLimits {
  const max_batch_records = publishedMaxRecords(published) ?? DEFAULT_EXPORT_LIMITS.max_batch_records;
  return {
    max_batch_records,
    max_batch_bytes: DEFAULT_EXPORT_LIMITS.max_batch_bytes,
    // A page stops at the first cap it hits. The scan cap rises only so the
    // page can hold the published record count, and never past the native maximum.
    max_scan_records: Math.min(NATIVE_EXPORT_CAP, Math.max(DEFAULT_EXPORT_LIMITS.max_scan_records, max_batch_records)),
  };
}

/** A consistent snapshot of the store as it stands now. */
export function beginHistoryExport(selection: HistoryExportSelection, options: HistoryExportOptions = {}): Promise<HistoryExportHandle> {
  const limits = options.limits ?? exportLimitsFromPublished(options.publishedLimits);
  return exportRequest({ operation: 'create_export', selection, limits,
    ttl_ms: options.ttlMs ?? 3_600_000, now_ms: Date.now() }, options);
}
export function readHistoryExportPage(cursor: string, options: HistoryExportStorageOptions = {}): Promise<HistoryExportPage> {
  return exportRequest({ operation: 'export_page', cursor, now_ms: Date.now() }, options);
}
export function closeHistoryExport(snapshotId: string, options: HistoryExportStorageOptions = {}): Promise<void> {
  return exportRequest({ operation: 'close_export', snapshot_id: snapshotId }, options);
}
/** Records carry the change feed's identity and revision for each row, its
 * provenance, and the row as stored. Closing/breaking the iterator releases its
 * snapshot. A snapshot is the store as it stood when it began, and lives in
 * this process until closed or expired; the cursor of the page just read
 * serves that page again. */
export async function* exportHistory(selection: HistoryExportSelection, options: HistoryExportOptions = {}): AsyncGenerator<HistoryExportRecord> {
  options.signal?.throwIfAborted();
  const snapshot = await beginHistoryExport(selection, options);
  try {
    let cursor: string | null = snapshot.cursor;
    while (cursor) {
      options.signal?.throwIfAborted();
      const page = await readHistoryExportPage(cursor, options);
      for (const record of page.records) {
        options.signal?.throwIfAborted();
        yield record;
      }
      cursor = page.next_cursor;
    }
  } finally {
    await closeHistoryExport(snapshot.snapshot_id, options);
  }
}
/** Each chunk is one complete NDJSON record. Success means export completed,
 * not that a downstream process or remote service durably accepted it. */
export async function* exportHistoryNdjson(selection: HistoryExportSelection, options: HistoryExportOptions = {}): AsyncGenerator<string> {
  for await (const record of exportHistory(selection, options)) yield `${JSON.stringify(record)}\n`;
}
