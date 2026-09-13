import { deliveryRequest, type HistoryDeliveryOptions } from './delivery.js';
import { DEFAULT_DELIVERY_LIMITS, type DeliveryLimits, type HistoryExportRecord, type HistoryExportSelection } from './delivery-contracts.js';

export interface HistoryExportHandle { snapshot_id: string; cursor: string; expires_at_ms: number }
export interface HistoryExportPage {
  schema_version: number; origin_id: string; records: HistoryExportRecord[]; next_cursor: string | null;
}
export interface HistoryExportOptions extends HistoryDeliveryOptions {
  limits?: DeliveryLimits; ttlMs?: number; signal?: AbortSignal;
}
/** A bounded historical snapshot, separate from delivery jobs and acknowledgments. */
export function beginHistoryExport(selection: HistoryExportSelection, options: HistoryExportOptions = {}): Promise<HistoryExportHandle> {
  return deliveryRequest({ operation: 'create_export', selection, limits: options.limits ?? DEFAULT_DELIVERY_LIMITS,
    ttl_ms: options.ttlMs ?? 3_600_000, now_ms: Date.now() }, options);
}
export function readHistoryExportPage(cursor: string, options: HistoryDeliveryOptions = {}): Promise<HistoryExportPage> {
  return deliveryRequest({ operation: 'export_page', cursor, now_ms: Date.now() }, options);
}
export function closeHistoryExport(snapshotId: string, options: HistoryDeliveryOptions = {}): Promise<void> {
  return deliveryRequest({ operation: 'close_export', snapshot_id: snapshotId }, options);
}
/** Records preserve canonical identity, revision, provenance, and raw evidence.
 * Closing/breaking the iterator releases its snapshot. Explicit handles support
 * durable cursor resume until expiry; a snapshot never follows later changes. */
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
