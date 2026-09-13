/** Portable export/delivery wire types. Field names match the versioned Rust contract. */
export type HistoryEvidenceKind = 'history' | 'session_event' | 'tool_call' | 'file_edit'
  | 'session' | 'presence' | 'relationship' | 'commit_link' | 'trajectory';
export interface HistorySessionIdentity { source: string; session_id: string }
export interface HistoryExportSelection {
  all_sources: boolean;
  sources: string[];
  sessions: HistorySessionIdentity[];
  kinds: HistoryEvidenceKind[];
  excluded_sessions: HistorySessionIdentity[];
}
export interface DeliveryLimits {
  max_batch_records: number; max_batch_bytes: number; max_scan_records: number; max_prepared_bytes: number;
}
export const DEFAULT_DELIVERY_LIMITS: Readonly<DeliveryLimits> = Object.freeze({
  max_batch_records: 100, max_batch_bytes: 1_048_576, max_scan_records: 400, max_prepared_bytes: 2_097_152,
});
export interface DeliveryJobConfig {
  destination_id: string; instance_id: string; account_id: string; mapping_version: string;
  selection: HistoryExportSelection; limits: DeliveryLimits;
}
export interface HistoryExportRecord {
  schema_version: number; origin_id: string; record_id: string; revision_id: string; revision: number;
  kind: HistoryEvidenceKind; source: string; session_id: string | null; operation: 'upsert' | 'delete';
  /** Original stored fields, timestamps, and raw evidence strings; null for tombstones. */
  payload: unknown;
}
export interface HistoryExportBatch {
  schema_version: number; origin_id: string; batch_id: string; job_id: string; generation: number;
  destination_id: string; instance_id: string; account_id: string; mapping_version: string;
  records: HistoryExportRecord[];
}
export interface DeliveryLease {
  job_id: string; batch_id: string; worker_id: string; fence: number; expires_at_ms: number;
}
export interface PreparedHistoryPayload {
  mapping_version: string; content_type: string; body: string; sha256: string;
}
export interface ClaimedHistoryBatch {
  lease: DeliveryLease; batch: HistoryExportBatch; prepared: PreparedHistoryPayload | null;
}
export interface DeliveryAcknowledgment {
  batch_id: string;
  accepted_revision_ids: string[];
  unsupported_revision_ids: string[];
  /** A receipt for future processing is not durable acceptance. */
  acceptance_level: 'durable' | 'indexed';
}
export type DeliveryFailure = 'transient' | 'rate_limited' | 'authentication_required'
  | 'permission_denied' | 'invalid_payload' | 'unsupported_evidence' | 'mapping_version_mismatch';
export interface DeliveryStatus {
  job_id: string; config: DeliveryJobConfig; generation: number; state: 'active' | 'paused' | 'blocked' | 'cancelled';
  bootstrap_complete: boolean; journal_cursor: number; acknowledged_cursor: number;
  pending_records: number; pending_bytes: number; oldest_pending_ms: number | null; unqueued_changes: number;
  next_attempt_ms: number; last_attempt_ms: number | null; last_acknowledged_ms: number | null;
  acceptance_level: 'durable' | 'indexed' | null; failure: DeliveryFailure | null;
  suppressed_records: number; acknowledged_records: number;
}
export interface DeliveryPrepareResult { batch_id: string | null; scanned_records: number; bootstrap_complete: boolean }

/** Destination implementations are trusted application code, not a sandbox. */
export interface HistoryDestination {
  id: string;
  mappingVersion: string;
  /** 'none' honestly allows duplicate remote effects after uncertain outcomes. */
  idempotency: 'revision' | 'none';
  /** Required: the receiver must reject stale revisions or apply them in order. */
  orderedRevisions: true;
  supportedKinds: readonly HistoryEvidenceKind[];
  supportsTombstones: boolean;
  /** Pure payload mapping. Never include credentials in the persisted body. */
  prepare(batch: Readonly<HistoryExportBatch>, context: { signal: AbortSignal }): Promise<{ content_type: string; body: string }>;
  /** Send precisely the stored body. Authentication headers may rotate. Verify
   * the remote account matches batch.account_id before writing. Do not launch
   * interactive login; report authentication_required when user action is needed. */
  send(payload: Readonly<PreparedHistoryPayload>, context: {
    signal: AbortSignal; batch: Readonly<HistoryExportBatch>; idempotencyKey: string;
  }): Promise<DeliveryAcknowledgment>;
}

export interface HistoryPlugin {
  destinations?: ReadonlyArray<{ instanceId: string; destination: HistoryDestination }>;
  /** Optional host integrations, registered only for explicitly loaded plugins. */
  commands?: ReadonlyArray<{ name: string; run(args: readonly string[]): Promise<unknown> }>;
  tools?: ReadonlyArray<{
    name: string; description: string;
    run(input: Record<string, unknown>): Promise<unknown>;
  }>;
}
