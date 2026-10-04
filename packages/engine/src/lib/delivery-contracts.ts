/** Protocol 1 matches the local history export contract, not legacy ingest. */
export const DELIVERY_KINDS = [
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "session",
  "presence",
  "relationship",
  "commit_link",
  "trajectory",
  "source_observation",
  "observation_evidence",
  "session_marker",
  /** Stable joins for source session -> issue -> Garden run -> coding session -> PR. */
  "session_lineage",
  /** Durable lifecycle evidence for an existing-session input turn. */
  "turn_receipt",
] as const;
export type DeliveryKind = (typeof DELIVERY_KINDS)[number];
export interface HistoryExportRecord {
  schema_version: 1;
  origin_id: string;
  record_id: string;
  revision_id: string;
  revision: number;
  kind: DeliveryKind;
  source: string;
  session_id: string | null;
  operation: "upsert" | "delete";
  payload: Record<string, unknown> | null;
}
export interface HistoryExportBatch {
  schema_version: 1;
  origin_id: string;
  batch_id: string;
  job_id: string;
  generation: number;
  destination_id: string;
  instance_id: string;
  account_id: string;
  mapping_version: string;
  records: HistoryExportRecord[];
}
/** The receipt persisted with a batch and replayed exactly on retry. */
export interface StoredDeliveryReceipt {
  protocolVersion: 1;
  /** Content-addressed server receipt. Stable across replay of the same batch. */
  receiptId: string;
  batchId: string;
  acceptedRevisionIds: string[];
  unsupportedRevisionIds: [];
  acceptanceLevel: "durable";
}
/**
 * The server's current batch limits. Advisory and response-time only: they are
 * not part of the content-addressed receipt and are never persisted with it, so
 * a replayed receipt reports the limits in force when it is replayed.
 */
export interface DeliveryLimits {
  maxRecords: number;
  maxRequestBytes: number;
}
export interface DeliveryReceipt extends StoredDeliveryReceipt {
  limits: DeliveryLimits;
}
export interface DeliveryRecordRevisionConflict {
  type: "record_revision";
  originId: string;
  recordId: string;
  submittedRevisionId: string;
  submittedRevision: number;
  submittedDigest: string;
  currentRevisionId: string;
  currentRevision: number;
  currentDigest: string;
}
export interface DeliveryBatchIdConflict {
  type: "batch_id";
  originId: string;
  batchId: string;
  submittedDigest: string;
  currentDigest: string;
}
export type DeliveryConflict =
  DeliveryRecordRevisionConflict | DeliveryBatchIdConflict;
export const MAX_DELIVERY_CONFLICTS = 100;
export const DELIVERY_MAPPING_VERSION = "relayhistory-delivery-v1";
export const MAX_DELIVERY_BYTES = 2_097_152;
export const MAX_DELIVERY_RECORDS = 500;
export const DELIVERY_LIMITS: Readonly<DeliveryLimits> = Object.freeze({
  maxRecords: MAX_DELIVERY_RECORDS,
  maxRequestBytes: MAX_DELIVERY_BYTES,
});
/** Encoded response cap, below the optional helper's 8 MiB reader. */
export const MAX_DELIVERY_PAGE_BYTES = 3_145_728;
export const MAX_DELIVERY_RECORD_BYTES = MAX_DELIVERY_PAGE_BYTES - 16_384;
export class DeliveryError extends Error {
  constructor(
    public readonly code: string,
    public readonly status: 400 | 403 | 409 | 413 | 422 | 503,
    message: string,
    public readonly conflict?: DeliveryConflict,
    public readonly conflicts?: DeliveryRecordRevisionConflict[],
    public readonly conflictCount?: number,
  ) {
    super(message);
  }
}
