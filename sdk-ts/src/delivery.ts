import { randomUUID } from 'node:crypto';
import { nativeCall } from './native.js';
import { InvalidArgumentError, RelayHistoryError } from './sdk-common.js';
import { HistoryPluginRegistry } from './delivery-plugins.js';
import type {
  ClaimedHistoryBatch, DeliveryAcknowledgment, DeliveryFailure, DeliveryJobConfig, DeliveryLease,
  DeliveryPrepareResult, DeliveryStatus, HistoryDestination, PreparedHistoryPayload,
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
function acknowledgment(value: DeliveryAcknowledgment, claim: ClaimedHistoryBatch): void {
  const expected = new Set(claim.batch.records.map((record) => record.revision_id));
  if (!value || value.batch_id !== claim.batch.batch_id || !['durable', 'indexed'].includes(value.acceptance_level)
    || !Array.isArray(value.accepted_revision_ids) || !Array.isArray(value.unsupported_revision_ids)) {
    throw new HistoryDeliveryError('invalid_payload');
  }
  const ids = [...value.accepted_revision_ids, ...value.unsupported_revision_ids];
  if (new Set(ids).size !== ids.length || ids.some((id) => typeof id !== 'string' || !expected.has(id))) {
    throw new HistoryDeliveryError('invalid_payload');
  }
}

async function attempt(claim: ClaimedHistoryBatch, destination: HistoryDestination, config: DeliveryJobConfig, options: DeliveryDrainOptions): Promise<void> {
  const leaseMs = options.leaseMs!;
  const abort = new AbortController();
  const aborted = () => abort.abort();
  options.signal?.addEventListener('abort', aborted, { once: true });
  if (options.signal?.aborted) abort.abort();
  const timeout = setTimeout(aborted, options.requestTimeoutMs);
  let lease: DeliveryLease = claim.lease;
  let renewing: Promise<void> = Promise.resolve();
  let stopped = false;
  let renewalTimer: ReturnType<typeof setTimeout> | undefined;
  const renew = () => {
    renewalTimer = setTimeout(() => {
      renewing = deliveryRequest<DeliveryLease>({ operation: 'renew_lease', lease, lease_ms: leaseMs, now_ms: Date.now() }, options)
        .then((value) => { lease = value; if (!stopped) renew(); })
        .catch(() => abort.abort());
    }, Math.max(1, Math.floor(leaseMs / 3)));
  };
  const stopRenewal = async () => { stopped = true; clearTimeout(renewalTimer); await renewing; };
  renew();
  let rejectAbort: (() => void) | undefined;
  const interrupted = new Promise<never>((_resolve, reject) => {
    rejectAbort = () => reject(new HistoryDeliveryError('transient'));
    abort.signal.addEventListener('abort', rejectAbort, { once: true });
    if (abort.signal.aborted) rejectAbort();
  });
  try {
    const work = async (): Promise<DeliveryAcknowledgment> => {
      if (abort.signal.aborted) throw new HistoryDeliveryError('transient');
      if (destination.mappingVersion !== claim.batch.mapping_version) throw new HistoryDeliveryError('mapping_version_mismatch');
      if (claim.batch.records.some((record) => !destination.supportedKinds.includes(record.kind)
        || (record.operation === 'delete' && !destination.supportsTombstones))) throw new HistoryDeliveryError('unsupported_evidence');
      const batch = frozen(claim.batch);
      if (!claim.prepared) {
        const prepared = await destination.prepare(batch, { signal: abort.signal });
        if (abort.signal.aborted) throw new HistoryDeliveryError('transient');
        if (!prepared || typeof prepared.body !== 'string' || typeof prepared.content_type !== 'string'
          || Buffer.byteLength(prepared.body) > config.limits.max_prepared_bytes
          || !prepared.content_type || prepared.content_type.length > 200 || /[\r\n]/.test(prepared.content_type)) throw new HistoryDeliveryError('invalid_payload');
        await deliveryRequest<PreparedHistoryPayload>({ operation: 'store_prepared_payload', lease,
          mapping_version: destination.mappingVersion, content_type: prepared.content_type, body: prepared.body, now_ms: Date.now() }, options);
      }
      if (abort.signal.aborted) throw new HistoryDeliveryError('transient');
      // Eligibility can change while a plugin prepares its payload. Recheck it
      // in core immediately before transport and use the persisted bytes.
      const payload = await deliveryRequest<PreparedHistoryPayload>({ operation: 'validate_dispatch', lease, now_ms: Date.now() }, options);
      if (abort.signal.aborted) throw new HistoryDeliveryError('transient');
      const result = await destination.send(frozen(payload), { batch, signal: abort.signal, idempotencyKey: batch.batch_id });
      acknowledgment(result, claim);
      return result;
    };
    const ack = await Promise.race([work(), interrupted]);
    await stopRenewal();
    if (abort.signal.aborted) throw new HistoryDeliveryError('transient');
    await deliveryRequest({ operation: 'acknowledge', lease, acknowledgment: ack, now_ms: Date.now() }, options);
  } catch (error) {
    await stopRenewal();
    const failure = error instanceof HistoryDeliveryError ? error : new HistoryDeliveryError('transient');
    // The core fences this write. If another worker owns the job, even failure
    // recording must fail rather than modifying that worker's progress.
    await deliveryRequest({ operation: 'record_failure', lease, failure: failure.failure,
      retry_after_ms: failure.retryAfterMs, now_ms: Date.now() }, options);
  } finally {
    await stopRenewal();
    clearTimeout(timeout);
    options.signal?.removeEventListener('abort', aborted);
    if (rejectAbort) abort.signal.removeEventListener('abort', rejectAbort);
  }
}

/** One bounded drain. It never waits for a retry deadline or enables a job. */
export async function drainHistoryDelivery(registry: HistoryPluginRegistry, options: DeliveryDrainOptions = {}): Promise<DeliveryDrainResult> {
  const maxBatches = integer(options.maxBatches ?? 100, 'maxBatches', 1, 10_000);
  const maxPrepareSteps = integer(options.maxPrepareSteps ?? 100, 'maxPrepareSteps', 1, 10_000);
  const leaseMs = integer(options.leaseMs ?? 30_000, 'leaseMs', 30, 86_400_000);
  const requestTimeoutMs = integer(options.requestTimeoutMs ?? 30_000, 'requestTimeoutMs', 1, 3_600_000);
  const workerId = options.workerId ?? randomUUID();
  const selected = new Set(options.jobIds);
  let attempts = 0;
  let preparedSteps = 0;
  const issues: DeliveryDrainResult['issues'] = [];
  await compactHistoryDelivery(options);
  const listed = await historyDeliveryStatus(undefined, options);
  if ([...selected].some((id) => !listed.some((job) => job.job_id === id))) throw new InvalidArgumentError('unknown delivery job selection', 'INVALID_ARGUMENT');
  const jobs = listed.filter((job) => !options.jobIds || selected.has(job.job_id));
  // Round-robin jobs: one failed destination never consumes another's cursor.
  let progressed = true;
  while (progressed && attempts < maxBatches && preparedSteps < maxPrepareSteps && !options.signal?.aborted) {
    progressed = false;
    for (const entry of jobs) {
      if (attempts >= maxBatches || preparedSteps >= maxPrepareSteps || options.signal?.aborted) break;
      try {
        const [job] = await historyDeliveryStatus(entry.job_id, options);
        if (job.state !== 'active' || job.next_attempt_ms > Date.now()) continue;
        const destination = registry.destination(job.config.destination_id, job.config.instance_id);
        if (!destination) {
          if (!issues.some((issue) => issue.jobId === job.job_id)) issues.push({ jobId: job.job_id, code: 'DESTINATION_NOT_REGISTERED' });
          continue;
        }
        const prepared = await deliveryRequest<DeliveryPrepareResult>({ operation: 'prepare_batch', job_id: job.job_id, now_ms: Date.now() }, options);
        preparedSteps++;
        if (!prepared.batch_id) { progressed ||= !prepared.bootstrap_complete || prepared.scanned_records > 0; continue; }
        const claim = await deliveryRequest<ClaimedHistoryBatch | null>({ operation: 'claim_batch', job_id: job.job_id,
          worker_id: workerId, lease_ms: leaseMs, now_ms: Date.now() }, options);
        if (!claim) continue;
        attempts++;
        await attempt(claim, destination, job.config, { ...options, leaseMs, requestTimeoutMs });
        progressed = true;
      } catch (error) {
        const detail = error instanceof RelayHistoryError && ['HISTORY_DELIVERY_FAILED', 'DELIVERY_RETENTION_LIMIT'].includes(error.code)
          ? error.message : 'delivery state operation failed';
        if (!issues.some((issue) => issue.jobId === entry.job_id)) issues.push({ jobId: entry.job_id, code: error instanceof RelayHistoryError && error.code === 'DELIVERY_RETENTION_LIMIT' ? 'DELIVERY_RETENTION_LIMIT' : 'DELIVERY_STATE_FAILED', detail });
      }
    }
  }
  const statuses = (await historyDeliveryStatus(undefined, options)).filter((job) => !options.jobIds || selected.has(job.job_id));
  await compactHistoryDelivery(options);
  return { attempts, statuses, issues, retention: await historyDeliveryRetention(options) };
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
