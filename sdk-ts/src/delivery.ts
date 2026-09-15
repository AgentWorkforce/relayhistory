import { randomUUID } from 'node:crypto';
import { nativeCall } from './native.js';
import { InvalidArgumentError, RelayHistoryError } from './sdk-common.js';
import { HistoryPluginRegistry } from './delivery-plugins.js';
import type {
  DeliveryAcknowledgment, DeliveryFailure, DeliveryJobConfig, DeliveryStatus, HistoryDestination,
  HistoryExportBatch, PreparedHistoryPayload,
} from './delivery-contracts.js';
export * from './delivery-contracts.js';
export * from './delivery-plugins.js';

export interface HistoryDeliveryOptions { dbPath?: string }
/** Narrow typed JSON RPC; SQL, leases, capture and checkpoints remain in Rust. */
export async function deliveryRequest<T>(request: Record<string, unknown>, options: HistoryDeliveryOptions = {}): Promise<T> {
  return nativeCall(async (native) => JSON.parse(await native.historyDelivery(JSON.stringify(request), options.dbPath)) as T);
}
export async function historyDeliveryRetention(options: HistoryDeliveryOptions = {}): Promise<{ usedBytes: number; limitBytes: number }> {
  const [usedBytes, limitBytes] = await deliveryRequest<[number, number]>({ operation: 'retained_bytes' }, options);
  return { usedBytes, limitBytes };
}
export async function compactHistoryDelivery(options: HistoryDeliveryOptions = {}): Promise<void> {
  await deliveryRequest({ operation: 'expire_exports', now_ms: Date.now(), limit: 32 }, options);
  await deliveryRequest({ operation: 'compact_journal', limit: 1_000 }, options);
  await deliveryRequest({ operation: 'compact_receipts', limit: 1_000 }, options);
}
export function setHistoryDeliveryRetention(maxBytes: number, options: HistoryDeliveryOptions = {}): Promise<void> {
  return deliveryRequest({ operation: 'set_retention_limit', max_bytes: maxBytes }, options);
}
export function createHistoryDelivery(config: DeliveryJobConfig, options: HistoryDeliveryOptions = {}): Promise<DeliveryStatus> {
  return deliveryRequest({ operation: 'create_job', config, now_ms: Date.now() }, options);
}
export function historyDeliveryStatus(jobId?: string, options: HistoryDeliveryOptions = {}): Promise<DeliveryStatus[]> {
  return deliveryRequest(jobId ? { operation: 'status', job_id: jobId } : { operation: 'list_jobs' }, options)
    .then((value) => jobId ? [value as DeliveryStatus] : value as DeliveryStatus[]);
}
export function controlHistoryDelivery(jobId: string, action: 'pause' | 'resume' | 'retry' | 'cancel', options: HistoryDeliveryOptions = {}): Promise<DeliveryStatus> {
  if (!['pause', 'resume', 'retry', 'cancel'].includes(action)) throw new InvalidArgumentError('invalid delivery control', 'INVALID_ARGUMENT');
  return deliveryRequest({ operation: `${action}_job`, job_id: jobId }, options);
}

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

function integer(value: number, name: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new InvalidArgumentError(`${name} must be between ${minimum} and ${maximum}`, 'INVALID_ARGUMENT');
  return value;
}
function frozen<T>(value: T): T {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) frozen(child);
    Object.freeze(value);
  }
  return value;
}
/** One call into a registered JavaScript destination, as core's worker sees it. */
interface ReceiverCall { destinationId: string; instanceId: string }
interface PrepareCall extends ReceiverCall { batch: HistoryExportBatch }
interface SendCall extends ReceiverCall { payload: PreparedHistoryPayload; batch: HistoryExportBatch; idempotencyKey: string }

/** Safe classification only: arbitrary plugin errors never reach the worker. */
function classified(error: unknown): string {
  const failure = error instanceof HistoryDeliveryError ? error : new HistoryDeliveryError('transient');
  return JSON.stringify({ ok: false, failure: failure.failure, retryAfterMs: failure.retryAfterMs });
}

/** Adapt one destination method into the reply envelope core's worker expects.
 * The request deadline is armed here because only this side can abort the
 * AbortSignal a destination observes while it is still running. */
function receiver<Call extends ReceiverCall, Value>(
  registry: HistoryPluginRegistry, options: DeliveryDrainOptions, requestTimeoutMs: number,
  invoke: (destination: HistoryDestination, call: Call, signal: AbortSignal) => Promise<Value>,
): (argumentJson: string) => Promise<string> {
  return async (argumentJson) => {
    const abort = new AbortController();
    const stop = () => abort.abort();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const call = JSON.parse(argumentJson) as Call;
      const destination = registry.destination(call.destinationId, call.instanceId);
      if (!destination) throw new HistoryDeliveryError('transient');
      options.signal?.addEventListener('abort', stop, { once: true });
      if (options.signal?.aborted) abort.abort();
      timer = setTimeout(stop, requestTimeoutMs);
      const deadline = new Promise<never>((_resolve, reject) => {
        const fail = () => reject(new HistoryDeliveryError('transient'));
        abort.signal.addEventListener('abort', fail, { once: true });
        if (abort.signal.aborted) fail();
      });
      const value = await Promise.race([invoke(destination, call, abort.signal), deadline]);
      if (abort.signal.aborted) throw new HistoryDeliveryError('transient');
      return JSON.stringify({ ok: true, value });
    } catch (error) {
      return classified(error);
    } finally {
      clearTimeout(timer);
      options.signal?.removeEventListener('abort', stop);
    }
  };
}

/** One bounded drain. It never waits for a retry deadline or enables a job.
 *
 * The drain loop itself - round-robin scheduling, leases and their keepalive,
 * payload persistence, the eligibility recheck before transport, acknowledgment
 * checking and failure classification - lives once, in Rust. This host only
 * describes its registered destinations and answers the worker's calls. */
export async function drainHistoryDelivery(registry: HistoryPluginRegistry, options: DeliveryDrainOptions = {}): Promise<DeliveryDrainResult> {
  const maxBatches = integer(options.maxBatches ?? 100, 'maxBatches', 1, 10_000);
  const maxPrepareSteps = integer(options.maxPrepareSteps ?? 100, 'maxPrepareSteps', 1, 10_000);
  const leaseMs = integer(options.leaseMs ?? 30_000, 'leaseMs', 30, 86_400_000);
  const requestTimeoutMs = integer(options.requestTimeoutMs ?? 30_000, 'requestTimeoutMs', 1, 3_600_000);
  const request = JSON.stringify({
    jobIds: options.jobIds ? [...options.jobIds] : undefined,
    workerId: options.workerId ?? randomUUID(),
    maxBatches, maxPrepareSteps, leaseMs, requestTimeoutMs,
    destinations: registry.registeredDestinations().map(({ destinationId, instanceId, destination }) => ({
      destinationId, instanceId, mappingVersion: destination.mappingVersion,
      supportedKinds: [...destination.supportedKinds], supportsTombstones: destination.supportsTombstones,
    })),
  });
  const prepare = receiver(registry, options, requestTimeoutMs,
    (destination, call: PrepareCall, signal) => destination.prepare(frozen(call.batch), { signal }));
  const send = receiver(registry, options, requestTimeoutMs,
    (destination, call: SendCall, signal): Promise<DeliveryAcknowledgment> =>
      destination.send(frozen(call.payload), { batch: frozen(call.batch), signal, idempotencyKey: call.idempotencyKey }));
  const cancelled = async (): Promise<boolean> => options.signal?.aborted === true;
  return nativeCall(async (native) => JSON.parse(
    await native.historyDeliveryDrain(request, options.dbPath, prepare, send, cancelled)) as DeliveryDrainResult);
}

async function wait(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return;
  await new Promise<void>((resolve) => {
    const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', done); resolve(); };
    const timer = setTimeout(done, ms);
    signal?.addEventListener('abort', done, { once: true });
  });
}

/** Opt-in worker using the same durable drain path; usable under a supervisor. */
export async function runHistoryDelivery(registry: HistoryPluginRegistry, options: DeliveryDrainOptions & {
  pollIntervalMs?: number; onProgress?: (result: DeliveryDrainResult) => void | Promise<void>;
} = {}): Promise<void> {
  const poll = integer(options.pollIntervalMs ?? 1_000, 'pollIntervalMs', 10, 60_000);
  let previous = '';
  while (!options.signal?.aborted) {
    const result = await drainHistoryDelivery(registry, options);
    const signature = JSON.stringify({ statuses: result.statuses, issues: result.issues, retention: result.retention });
    if (signature !== previous) { await options.onProgress?.(result); previous = signature; }
    if (!options.signal?.aborted) await wait(poll, options.signal);
  }
}
