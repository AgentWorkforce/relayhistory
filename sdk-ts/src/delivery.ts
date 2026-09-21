/** Compatibility types and explicit migration errors. Uploads run in the probe. */
import { RelayHistoryError } from './sdk-common.js';
import type { HistoryPluginRegistry } from './delivery-plugins.js';
import type { DeliveryFailure, DeliveryJobConfig, DeliveryStatus } from './delivery-contracts.js';
export * from './delivery-contracts.js';
export * from './delivery-plugins.js';
export interface HistoryDeliveryOptions { dbPath?: string }
function moved(): never {
  throw new RelayHistoryError('Upload jobs moved to agent-relay-probe / @relayhistory/capture. Manage existing jobs there; local history and export remain available in ai-hist.', 'HISTORY_DELIVERY_MOVED');
}
/** @deprecated Use the probe package. No native loading or database mutation occurs. */
export async function deliveryRequest<T>(_request: Record<string, unknown>, _options: HistoryDeliveryOptions = {}): Promise<T> { return moved(); }
export async function historyDeliveryRetention(_options: HistoryDeliveryOptions = {}): Promise<{ usedBytes: number; limitBytes: number }> { return moved(); }
export async function compactHistoryDelivery(_options: HistoryDeliveryOptions = {}): Promise<void> { return moved(); }
export async function setHistoryDeliveryRetention(_maxBytes: number, _options: HistoryDeliveryOptions = {}): Promise<void> { return moved(); }
export async function createHistoryDelivery(_config: DeliveryJobConfig, _options: HistoryDeliveryOptions = {}): Promise<DeliveryStatus> { return moved(); }
export async function historyDeliveryStatus(_jobId?: string, _options: HistoryDeliveryOptions = {}): Promise<DeliveryStatus[]> { return moved(); }
export async function controlHistoryDelivery(_jobId: string, _action: 'pause' | 'resume' | 'retry' | 'cancel', _options: HistoryDeliveryOptions = {}): Promise<DeliveryStatus> { return moved(); }
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

export interface DeliveryDrainOptions extends HistoryDeliveryOptions {
  jobIds?: readonly string[];
  signal?: AbortSignal;
  workerId?: string;
  maxBatches?: number;
  maxPrepareSteps?: number;
  requestTimeoutMs?: number;
  leaseMs?: number;
}
export interface DeliveryDrainResult {
  attempts: number;
  statuses: DeliveryStatus[];
  issues: Array<{ jobId: string; code: 'DESTINATION_NOT_REGISTERED' | 'DELIVERY_STATE_FAILED' | 'DELIVERY_RETENTION_LIMIT'; detail?: string }>;
  retention: { usedBytes: number; limitBytes: number };
}


export async function drainHistoryDelivery(_registry: HistoryPluginRegistry, _options: DeliveryDrainOptions = {}): Promise<DeliveryDrainResult> { return moved(); }
export async function runHistoryDelivery(_registry: HistoryPluginRegistry, _options: DeliveryDrainOptions & { pollIntervalMs?: number; onProgress?: (result: DeliveryDrainResult) => void | Promise<void> } = {}): Promise<void> { return moved(); }
