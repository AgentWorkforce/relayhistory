import { sessionStoreCall, SESSION_STORE_OPS } from './native.js';
import { InvalidArgumentError, NativeContractMismatchError } from './sdk-common.js';

export interface RetentionMaintenanceOptions {
  dbPath?: string;
  /** Persist this policy only with apply. Zero keeps evidence forever. */
  days?: number;
  /** Default false: a policy preview, with no database creation or mutation. */
  apply?: boolean;
  /** Explicitly permit losing evidence before upload; requires apply. */
  allowUnuploaded?: boolean;
  /** One bounded maintenance batch; 1..1024, default 128. */
  maxRows?: number;
}
export interface RetentionBatchReport {
  rows: number;
  sessions: number;
  protectedSessions: number;
  awaitingUploadAck: boolean;
  busy: boolean;
  deferred: boolean;
  cycleComplete: boolean;
}
export interface RetentionMaintenanceResult {
  dryRun: boolean;
  /** This previews policy, never a deletion estimate. */
  preview: 'policy_only';
  storedDays: number;
  effectiveDays: number;
  maxRows: number;
  allowUnuploaded: boolean;
  generation: number;
  report: RetentionBatchReport | null;
}
export interface CompactDatabaseResult {
  dbBytesBefore: number;
  walBytesBefore: number;
  dbBytesAfter: number;
  walBytesAfter: number;
  reclaimableBefore: number;
  walTruncated: boolean;
  elapsedMs: number;
}
function integer(value: unknown, name: string, minimum: number, maximum: number): void {
  if (value !== undefined && (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum)) {
    throw new InvalidArgumentError(`${name} must be an integer in ${minimum}..${maximum}`, 'INVALID_ARGUMENT');
  }
}
function count(row: Record<string, unknown>, key: string): number {
  const value = row[key];
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new NativeContractMismatchError(`maintenance ${key} must be a nonnegative integer`, 'NATIVE_CONTRACT_MISMATCH');
  }
  return value;
}
function flag(row: Record<string, unknown>, key: string): boolean {
  const value = row[key];
  if (typeof value !== 'boolean') throw new NativeContractMismatchError(`maintenance ${key} must be boolean`, 'NATIVE_CONTRACT_MISMATCH');
  return value;
}
/** Preview policy by default, or explicitly save it and run one bounded batch. */
export async function maintainRetention(options: RetentionMaintenanceOptions = {}): Promise<RetentionMaintenanceResult> {
  if (!options || typeof options !== 'object' || Array.isArray(options)) throw new InvalidArgumentError('retention options must be an object', 'INVALID_ARGUMENT');
  integer(options.days, 'days', 0, 0xffffffff);
  integer(options.maxRows, 'maxRows', 1, 1024);
  for (const key of ['apply', 'allowUnuploaded'] as const) {
    if (options[key] !== undefined && typeof options[key] !== 'boolean') throw new InvalidArgumentError(`${key} must be boolean`, 'INVALID_ARGUMENT');
  }
  if (options.allowUnuploaded && !options.apply) throw new InvalidArgumentError('allowUnuploaded requires apply', 'INVALID_ARGUMENT');
  const result = await sessionStoreCall(SESSION_STORE_OPS.retention, options);
  if (result.preview !== 'policy_only') throw new NativeContractMismatchError('retention preview kind is invalid', 'NATIVE_CONTRACT_MISMATCH');
  const raw = result.report;
  if (raw !== null && (typeof raw !== 'object' || Array.isArray(raw))) throw new NativeContractMismatchError('retention report is invalid', 'NATIVE_CONTRACT_MISMATCH');
  const row = raw as Record<string, unknown> | null;
  return {
    dryRun: flag(result, 'dryRun'), preview: 'policy_only', storedDays: count(result, 'storedDays'), effectiveDays: count(result, 'effectiveDays'),
    maxRows: count(result, 'maxRows'), allowUnuploaded: flag(result, 'allowUnuploaded'), generation: count(result, 'generation'),
    report: row === null ? null : { rows: count(row, 'rows'), sessions: count(row, 'sessions'), protectedSessions: count(row, 'protectedSessions'),
      awaitingUploadAck: flag(row, 'awaitingUploadAck'), busy: flag(row, 'busy'), deferred: flag(row, 'deferred'), cycleComplete: flag(row, 'cycleComplete') },
  };
}
/** Reclaim SQLite free space without deleting rows. Requires an existing store. */
export async function compactDatabase(options: { dbPath?: string } = {}): Promise<CompactDatabaseResult> {
  const row = await sessionStoreCall(SESSION_STORE_OPS.compact, options);
  return { dbBytesBefore: count(row, 'dbBytesBefore'), walBytesBefore: count(row, 'walBytesBefore'), dbBytesAfter: count(row, 'dbBytesAfter'),
    walBytesAfter: count(row, 'walBytesAfter'), reclaimableBefore: count(row, 'reclaimableBefore'), walTruncated: flag(row, 'walTruncated'), elapsedMs: count(row, 'elapsedMs') };
}
