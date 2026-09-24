/** Probe-owned job control. Never calls the local native addon's upload stubs. */
import { defaultDbPath, type DeliveryJobConfig, type DeliveryStatus, type HistoryDeliveryOptions, type DeliveryDrainOptions, type DeliveryDrainResult } from 'ai-hist';
import { helperRequest, type HelperOptions } from './helper.js';
export interface ProbeDeliveryOptions extends HistoryDeliveryOptions, HelperOptions {}
export function deliveryRequest<T>(request: Record<string, unknown>, options: ProbeDeliveryOptions = {}): Promise<T> {
  return helperRequest('probeDelivery', { dbPath: options.dbPath ?? defaultDbPath(), deliveryRequest: request }, options);
}
export function createHistoryDelivery(config: DeliveryJobConfig, options: ProbeDeliveryOptions = {}): Promise<DeliveryStatus> {
  return deliveryRequest({ operation: 'create_job', config, now_ms: Date.now() }, options);
}
export async function historyDeliveryStatus(jobId?: string, options: ProbeDeliveryOptions = {}): Promise<DeliveryStatus[]> {
  const value = await deliveryRequest<DeliveryStatus | DeliveryStatus[]>(jobId ? { operation: 'status', job_id: jobId } : { operation: 'list_jobs' }, options);
  return jobId ? [value as DeliveryStatus] : value as DeliveryStatus[];
}
export function controlHistoryDelivery(jobId: string, action: 'pause' | 'resume' | 'retry' | 'cancel', options: ProbeDeliveryOptions = {}): Promise<DeliveryStatus> {
  return deliveryRequest({ operation: `${action}_job`, job_id: jobId }, options);
}

export interface ProbeDrainOptions extends ProbeDeliveryOptions, DeliveryDrainOptions {
  baseUrl?: string;
  instanceId: string;
  expectedAccount: string;
  acknowledgeUninspectedLegacySchedules?: boolean;
}
/** A bounded drain in the probe helper. Cancelling terminates that helper and
 * leaves its durable lease/prepared batch recoverable on the next attempt. */
export function drainProbeDelivery(options: ProbeDrainOptions): Promise<DeliveryDrainResult> {
  const { jobIds, workerId, maxBatches, maxPrepareSteps, requestTimeoutMs, leaseMs } = options;
  return helperRequest('probeDeliveryDrain', {
    dbPath: options.dbPath ?? defaultDbPath(), baseUrl: options.baseUrl,
    instanceId: options.instanceId, expectedAccount: options.expectedAccount,
    acknowledgeUninspectedSchedules: options.acknowledgeUninspectedLegacySchedules,
    drainOptions: { jobIds, workerId, maxBatches, maxPrepareSteps, requestTimeoutMs, leaseMs },
  }, options);
}
export async function historyDeliveryRetention(options: ProbeDeliveryOptions = {}): Promise<{ usedBytes: number; limitBytes: number }> {
  const [usedBytes, limitBytes] = await deliveryRequest<[number, number]>({ operation: 'retained_bytes' }, options);
  return { usedBytes, limitBytes };
}
export async function setHistoryDeliveryRetention(maxBytes: number, options: ProbeDeliveryOptions = {}): Promise<void> {
  await deliveryRequest({ operation: 'set_retention_limit', max_bytes: maxBytes }, options);
}
/** Largest number of journal rows one compaction transaction deletes or examines. */
export const MAX_COMPACTION_PAGE = 10_000;
export async function compactHistoryDelivery(options: ProbeDeliveryOptions = {}): Promise<void> {
  await deliveryRequest({ operation: 'expire_exports', now_ms: Date.now(), limit: 32 }, options);
  await deliveryRequest({ operation: 'compact_journal_pass', page_size: MAX_COMPACTION_PAGE }, options);
  await deliveryRequest({ operation: 'compact_receipts', limit: MAX_COMPACTION_PAGE }, options);
}
