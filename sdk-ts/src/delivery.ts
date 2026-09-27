/** Destination-facing helpers. The destination contract lives in delivery-contracts. */
import { RelayHistoryError } from './sdk-common.js';
import type { DeliveryFailure } from './delivery-contracts.js';
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
