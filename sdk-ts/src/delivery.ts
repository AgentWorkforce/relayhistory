/** Destination-facing helpers. The destination contract lives in delivery-contracts. */
import { createHash } from 'node:crypto';
import { RelayHistoryError } from './sdk-common.js';
import type {
  DeliveryConflict, DeliveryConflictRecovery, DeliveryConflictResponse, DeliveryFailure,
  DeliveryRecordRevisionConflict,
  HistoryExportBatch, HistoryExportRecord,
} from './delivery-contracts.js';
export * from './delivery-contracts.js';
export * from './delivery-plugins.js';
/** Safe classification only: arbitrary plugin errors are never persisted/logged. */
export class HistoryDeliveryError extends RelayHistoryError {
  constructor(readonly failure: DeliveryFailure, readonly retryAfterMs?: number) {
    super(`history destination requires ${failure}`, 'HISTORY_DELIVERY_FAILED');
  }
}

/** Translate an HTTP Retry-After header into an absolute retry time. */
export function deliveryRetryAfter(value: string | null, now = Date.now()): number | undefined {
  if (value === null) return undefined;
  const seconds = /^\d+$/.test(value) ? Number(value) : NaN;
  const parsed = Number.isFinite(seconds) ? now + seconds * 1_000 : Date.parse(value);
  return Number.isSafeInteger(parsed) && parsed >= now ? parsed : undefined;
}

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function label(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 512;
}
function digestValue(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
}
function positiveRevision(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) > 0;
}
const MAX_DELIVERY_CONFLICTS = 100;

/** Canonical delivery JSON: object key order is ignored and array order is
 * significant, matching the durable receiver's digest contract. */
export function canonicalDeliveryJson(value: unknown): string {
  if (Array.isArray(value)) {
    const items = Array.from({ length: value.length }, (_, index) => {
      const item = value[index];
      return item === undefined || typeof item === 'function' || typeof item === 'symbol'
        ? 'null'
        : canonicalDeliveryJson(item);
    });
    return `[${items.join(',')}]`;
  }
  if (object(value)) {
    const entries = Object.keys(value).sort().flatMap((key) => {
      const item = value[key];
      return item === undefined || typeof item === 'function' || typeof item === 'symbol'
        ? []
        : [`${JSON.stringify(key)}:${canonicalDeliveryJson(item)}`];
    });
    return `{${entries.join(',')}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError('delivery values must be JSON serializable');
  return encoded;
}

/** Lowercase SHA-256 of canonical semantic JSON. */
export function deliveryDigest(value: unknown): string {
  return createHash('sha256').update(canonicalDeliveryJson(value), 'utf8').digest('hex');
}

/** The receiver normalizes the change feed's empty no-session spelling before
 * taking record and batch digests. Exported snapshots ordinarily use null, but
 * keeping this here makes recovery agree with every accepted v1 request. */
function normalizedRecord(record: Readonly<HistoryExportRecord>): HistoryExportRecord {
  return record.session_id === '' ? { ...record, session_id: null } : record;
}
function normalizedBatch(batch: Readonly<HistoryExportBatch>): HistoryExportBatch {
  if (!batch.records.some((record) => record.session_id === '')) return { ...batch, records: [...batch.records] };
  return { ...batch, records: batch.records.map(normalizedRecord) };
}

export function deliveryRecordDigest(record: Readonly<HistoryExportRecord>): string {
  return deliveryDigest(normalizedRecord(record));
}
export function deliveryBatchDigest(batch: Readonly<HistoryExportBatch>): string {
  return deliveryDigest({ protocolVersion: 1, batch: normalizedBatch(batch) });
}

function parseConflict(value: unknown): DeliveryConflict | undefined {
  if (!object(value) || !label(value.originId)
    || !digestValue(value.submittedDigest) || !digestValue(value.currentDigest)
    || value.submittedDigest === value.currentDigest) return undefined;
  if (value.type === 'batch_id') {
    if (!label(value.batchId)) return undefined;
    return { type: value.type, originId: value.originId, batchId: value.batchId,
      submittedDigest: value.submittedDigest, currentDigest: value.currentDigest };
  }
  if (value.type !== 'record_revision' || !label(value.recordId)
    || !label(value.submittedRevisionId) || !label(value.currentRevisionId)
    || !positiveRevision(value.submittedRevision) || !positiveRevision(value.currentRevision)
    || value.submittedRevision !== value.currentRevision) return undefined;
  return { type: value.type, originId: value.originId, recordId: value.recordId,
    submittedRevisionId: value.submittedRevisionId, submittedRevision: value.submittedRevision,
    submittedDigest: value.submittedDigest, currentRevisionId: value.currentRevisionId,
    currentRevision: value.currentRevision, currentDigest: value.currentDigest };
}

/** Parse only the protocol's exact actionable response. As with
 * `cursor_not_found`, an unrelated status/code, malformed JSON, or incomplete
 * detail is not recovery authority. */
export function parseDeliveryConflict(status: number, body: unknown): DeliveryConflictResponse | undefined {
  if (status !== 409) return undefined;
  let value = body;
  if (typeof body === 'string') {
    try { value = JSON.parse(body) as unknown; } catch { return undefined; }
  }
  if (!object(value) || !object(value.error) || value.error.code !== 'delivery_conflict'
    || typeof value.error.message !== 'string') return undefined;
  const conflict = parseConflict(value.error.conflict);
  if (!conflict) return undefined;
  if (value.correlationId !== undefined && typeof value.correlationId !== 'string') return undefined;
  let error: DeliveryConflictResponse['error'];
  if (conflict.type === 'batch_id') {
    if (Object.hasOwn(value.error, 'conflicts') || Object.hasOwn(value.error, 'conflictCount')) return undefined;
    error = { code: 'delivery_conflict', message: value.error.message, conflict };
  } else {
    if (!Array.isArray(value.error.conflicts) || value.error.conflicts.length < 1
      || value.error.conflicts.length > MAX_DELIVERY_CONFLICTS
      || !positiveRevision(value.error.conflictCount)
      || value.error.conflictCount < value.error.conflicts.length) return undefined;
    const conflicts: DeliveryRecordRevisionConflict[] = [];
    const revisionIds = new Set<string>();
    for (const candidate of value.error.conflicts) {
      const parsed = parseConflict(candidate);
      if (parsed?.type !== 'record_revision' || revisionIds.has(parsed.submittedRevisionId)) return undefined;
      revisionIds.add(parsed.submittedRevisionId);
      conflicts.push(parsed);
    }
    if (JSON.stringify(conflicts[0]) !== JSON.stringify(conflict)) return undefined;
    error = { code: 'delivery_conflict', message: value.error.message, conflict,
      conflicts, conflictCount: value.error.conflictCount };
  }
  return { error,
    ...(typeof value.correlationId === 'string' ? { correlationId: value.correlationId } : {}) };
}

function recoveryBatch(parent: Readonly<HistoryExportBatch>, records: HistoryExportRecord[]): HistoryExportBatch {
  const batch_id = `conflict-${deliveryDigest([
    'relayhistory-delivery-conflict-recovery-v1', parent.origin_id, parent.batch_id,
    deliveryBatchDigest(parent),
    records.map((record) => record.revision_id),
  ])}`;
  return { ...parent, batch_id, records };
}

/** Turn receiver-proven conflicts into deterministic queue progress. This
 * helper does not invent a higher record revision. A semantic re-derivation
 * must first be committed to the source store, whose change feed supplies the
 * higher revision; otherwise a retry could overwrite a durable equal-revision
 * value and destroy idempotency. */
export function recoverDeliveryConflict(
  batch: Readonly<HistoryExportBatch>, response: DeliveryConflictResponse,
): DeliveryConflictRecovery {
  const conflict = response.error.conflict;
  if (conflict.originId !== batch.origin_id) throw new TypeError('delivery conflict origin does not match submitted batch');
  if (conflict.type === 'batch_id') {
    if (conflict.batchId !== batch.batch_id || conflict.submittedDigest !== deliveryBatchDigest(batch)) {
      throw new TypeError('delivery batch conflict does not match submitted content');
    }
    return { quarantinedRevisionIds: [], retryBatch: recoveryBatch(batch, [...batch.records]) };
  }
  if (!('conflicts' in response.error)) {
    throw new TypeError('delivery record conflict is missing its conflict set');
  }
  const recordError = response.error;
  if (recordError.conflictCount < recordError.conflicts.length
    || recordError.conflictCount > batch.records.length) {
    throw new TypeError('delivery conflict count does not match submitted batch');
  }
  const quarantined = new Set<string>();
  for (const item of recordError.conflicts) {
    if (item.originId !== batch.origin_id) {
      throw new TypeError('delivery conflict origin does not match submitted batch');
    }
    const matching = batch.records.filter((record) => record.record_id === item.recordId
      && record.revision_id === item.submittedRevisionId && record.revision === item.submittedRevision);
    if (matching.length !== 1 || item.submittedDigest !== deliveryRecordDigest(matching[0])
      || quarantined.has(matching[0].revision_id)) {
      throw new TypeError('delivery record conflict does not match submitted content');
    }
    quarantined.add(matching[0].revision_id);
  }
  const records = batch.records.filter((record) => !quarantined.has(record.revision_id));
  return { quarantinedRevisionIds: [...quarantined],
    retryBatch: records.length === 0 ? null : recoveryBatch(batch, records) };
}
