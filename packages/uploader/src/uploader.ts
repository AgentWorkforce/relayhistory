/**
 * Drain the local change feed into a History service.
 *
 * The cursor is a named consumer inside the local store, one per endpoint, account
 * and selection. A page is read, its selected rows become bounded batches, each batch
 * is sent until the server returns its durable receipt, and only then is the page
 * committed. A crash, a lost response or a failed batch leaves the cursor where it was,
 * so the next run sends the same batches again and the server answers with the
 * receipts it already issued.
 */
import { createHash } from "node:crypto";
import {
  canonicalDeliveryJson,
  commitChanges,
  getChangesPage,
  recoverDeliveryConflict,
  type ChangeKind,
  type ChangesPage,
  type HistoryExportBatch,
  type HistoryExportRecord,
  type HistoryExportSelection,
  type Watermark,
} from "ai-hist";
import {
  HistoryClient,
  UploadError,
  type DeliveryReceipt,
  type ServerLimits,
} from "./client.js";
import { endpointBase, type UploaderConfig } from "./config.js";
import { deliveryRecord, selected } from "./records.js";
import type { Logger } from "./log.js";

export const MAPPING_VERSION = "relayhistory-delivery-v1";
export const DESTINATION_ID = "relayhistory";
/** Feed rows read per page. Batches are cut from a page; a page commits as a whole. */
export const SCAN_LIMIT = 1_000;
/** The shape of the SDK's conflict-retry batch id, the longest a batch is sent under. */
const RECOVERY_ID = `conflict-${"0".repeat(64)}`;

export interface Feed {
  getChangesPage: typeof getChangesPage;
  commitChanges: typeof commitChanges;
}

export interface RetryPolicy {
  /** Sends of one batch before the run gives up on a transient failure. */
  attempts: number;
  baseMs: number;
  maxMs: number;
}

export const DEFAULT_RETRY: RetryPolicy = {
  attempts: 6,
  baseMs: 500,
  maxMs: 30_000,
};

export interface UploadOptions {
  config: UploaderConfig;
  log: Logger;
  feed?: Feed;
  fetch?: typeof fetch;
  retry?: RetryPolicy;
  signal?: AbortSignal;
  /** Read and select without sending or committing. */
  dryRun?: boolean;
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  random?: () => number;
}

export interface UploadSummary {
  consumer: string;
  originId: string | null;
  /** Feed rows read. */
  scanned: number;
  /** Rows the selection admitted. */
  selected: number;
  /** Records the server durably accepted (including replays of earlier sends). */
  accepted: number;
  /** Records skipped because the server proved it holds different content at that revision. */
  quarantined: number;
  batches: number;
  /** Per-session counts of admitted rows, keyed `source/session_id`. */
  sessions: Record<string, number>;
  /**
   * The consumer cursor as stored after the run's last commit; null when nothing was
   * ever committed (no local feed). For a dry run, which never commits, the feed
   * position the run read up to.
   */
  cursor: Watermark | null;
}

const sha256 = (value: string) =>
  createHash("sha256").update(value, "utf8").digest("hex");
const encoder = new TextEncoder();

/**
 * The store cursor's name: a digest of the endpoint, the account and the selection.
 * Another endpoint or account never moves this cursor, and a changed selection starts
 * a new one from the beginning of the feed, so newly selected history is backfilled.
 * The account and endpoint are hashed in, never spelled out; the token is not part of
 * it at all, so rotating a token keeps the cursor.
 */
export function consumerName(
  config: Pick<UploaderConfig, "endpoint" | "accountId" | "selection">,
): string {
  const selection = config.selection;
  // Compare fields, not joined strings: identities may contain commas.
  // Each list is a set, as the export selection reads it: order and repeats name the
  // same selection, so neither may change the cursor.
  const sortedIds = (ids: HistoryExportSelection["sessions"]) =>
    [
      ...new Map(
        ids.map((id) => [JSON.stringify([id.source, id.session_id]), id]),
      ).values(),
    ]
      .map((id) => [id.source, id.session_id])
      .sort(([a, b], [c, d]) =>
        a < c ? -1 : a > c ? 1 : b < d ? -1 : b > d ? 1 : 0,
      );
  const sortedSet = (values: readonly string[]) => [...new Set(values)].sort();
  const identity = canonicalDeliveryJson([
    "relayhistory-upload-v1",
    endpointBase(config.endpoint),
    config.accountId,
    selection.all_sources,
    sortedSet(selection.sources),
    sortedIds(selection.sessions),
    sortedSet(selection.kinds),
    sortedIds(selection.excluded_sessions),
  ]);
  return `relayhistory-upload:${sha256(identity).slice(0, 32)}`;
}

function batchFor(
  config: UploaderConfig,
  consumer: string,
  originId: string,
  records: HistoryExportRecord[],
): HistoryExportBatch {
  return {
    schema_version: 1,
    origin_id: originId,
    // Deterministic: a resend after a lost response reuses the identity, so the
    // server replays its receipt instead of storing a second batch.
    batch_id: `upload-${sha256(
      canonicalDeliveryJson([
        "relayhistory-upload-batch-v1",
        consumer,
        originId,
        records.map((r) => r.revision_id),
      ]),
    )}`,
    job_id: consumer,
    generation: 1,
    destination_id: DESTINATION_ID,
    instance_id: config.instanceId,
    account_id: config.accountId,
    mapping_version: MAPPING_VERSION,
    records,
  };
}

/** Cut records into batches within the record and request-byte limits. */
export function cutBatches(
  config: UploaderConfig,
  consumer: string,
  originId: string,
  records: HistoryExportRecord[],
  limits: { maxRecords: number; maxBytes: number },
): HistoryExportBatch[] {
  // Sized for the longest identity the batch can be sent under: a conflict retry
  // renames it `conflict-<sha256>`, longer than `upload-<sha256>`, and must still fit.
  const envelope = encoder.encode(
    JSON.stringify({
      protocolVersion: 1,
      batch: {
        ...batchFor(config, consumer, originId, []),
        batch_id: RECOVERY_ID,
      },
    }),
  ).length;
  const batches: HistoryExportBatch[] = [];
  let current: HistoryExportRecord[] = [];
  let bytes = envelope;
  for (const record of records) {
    const size = encoder.encode(JSON.stringify(record)).length;
    if (envelope + size > limits.maxBytes)
      throw new UploadError(
        "invalid_payload",
        `record ${record.record_id} (${record.kind}) exceeds the ${limits.maxBytes}-byte request limit; exclude its session to continue`,
      );
    if (
      current.length &&
      (current.length >= limits.maxRecords ||
        bytes + size + 1 > limits.maxBytes)
    ) {
      batches.push(batchFor(config, consumer, originId, current));
      current = [];
      bytes = envelope;
    }
    // A comma separates each record after the first.
    bytes += current.length ? size + 1 : size;
    current.push(record);
  }
  if (current.length)
    batches.push(batchFor(config, consumer, originId, current));
  return batches;
}

function defaultSleep(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort);
      resolve();
    }, ms);
    const abort = () => {
      clearTimeout(timer);
      reject(signal!.reason);
    };
    signal?.addEventListener("abort", abort, { once: true });
  });
}

/**
 * A conflict recovery must account for every submitted record exactly once: either
 * quarantined (proven conflicting) or carried in the retry under a new batch identity.
 * Anything else would let the page commit with records that were never accepted. The
 * SDK guarantees this today; the uploader checks it rather than relying on that.
 */
export function checkRecovery(
  sending: HistoryExportBatch,
  recovery: {
    quarantinedRevisionIds: string[];
    retryBatch: HistoryExportBatch | null;
  },
): void {
  const sent = sending.records.map((record) => record.revision_id);
  const accounted = [
    ...recovery.quarantinedRevisionIds,
    ...(recovery.retryBatch?.records.map((record) => record.revision_id) ?? []),
  ];
  // A retry always gets a new identity: reusing the submitted batch id would be
  // answered with that batch's conflict again (or its receipt, for different content).
  const renamed =
    recovery.retryBatch === null ||
    recovery.retryBatch.batch_id !== sending.batch_id;
  const progress =
    recovery.quarantinedRevisionIds.length > 0 || recovery.retryBatch !== null;
  if (
    !renamed ||
    !progress ||
    accounted.length !== sent.length ||
    new Set(accounted).size !== sent.length ||
    !accounted.every((id) => sent.includes(id))
  )
    throw new UploadError(
      "delivery_conflict",
      "conflict recovery does not account for every submitted record",
    );
}

export async function upload(options: UploadOptions): Promise<UploadSummary> {
  const { config, log } = options;
  const feed = options.feed ?? { getChangesPage, commitChanges };
  const retry = options.retry ?? DEFAULT_RETRY;
  const sleep = options.sleep ?? defaultSleep;
  const random = options.random ?? Math.random;
  const kinds = config.selection.kinds as ChangeKind[];
  const consumer = consumerName(config);
  const client = new HistoryClient({
    endpoint: config.endpoint,
    token: config.token,
    ...(options.fetch ? { fetch: options.fetch } : {}),
  });
  const summary: UploadSummary = {
    consumer,
    originId: null,
    scanned: 0,
    selected: 0,
    accepted: 0,
    quarantined: 0,
    batches: 0,
    sessions: {},
    cursor: null,
  };
  const dbPath = config.dbPath;
  const storage = dbPath ? { dbPath } : {};

  let server: ServerLimits | undefined;
  const limits = async () => {
    server ??= await withRetry(() => client.limits(options.signal));
    return {
      maxRecords: Math.min(config.limits.maxRecords, server.maxRecords),
      maxBytes: Math.min(config.limits.maxBytes, server.maxRequestBytes),
    };
  };

  async function withRetry<T>(send: () => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await send();
      } catch (error) {
        if (
          !(error instanceof UploadError) ||
          !error.retryable ||
          attempt >= retry.attempts
        )
          throw error;
        const backoff = Math.min(
          retry.maxMs,
          retry.baseMs * 2 ** (attempt - 1),
        );
        const wait = Math.max(
          Math.round(backoff * random()),
          (error.retryAt ?? 0) - Date.now(),
        );
        log.warn("retrying", { failure: error.failure, attempt, waitMs: wait });
        await sleep(wait, options.signal);
      }
    }
  }

  /** Send one batch until it is durably accepted, recovering proven conflicts. */
  async function deliver(batch: HistoryExportBatch) {
    let pending: HistoryExportBatch | null = batch;
    // A record conflict always quarantines at least one record, so those rounds end
    // with the batch. A batch-id conflict renames the batch without shrinking it; a
    // second one means the server holds different content under the recovery id too.
    let batchIdRecoveries = 0;
    while (pending) {
      const sending: HistoryExportBatch = pending;
      const outcome = await withRetry(() =>
        client.deliver(sending, options.signal),
      );
      summary.batches += 1;
      if (outcome.type === "receipt") {
        summary.accepted += outcome.receipt.acceptedRevisionIds.length;
        logReceipt(outcome.receipt, sending);
        return;
      }
      // The SDK verifies the conflict names exactly what was sent before any record
      // is skipped; it never invents a newer revision.
      let recovery;
      try {
        recovery = recoverDeliveryConflict(sending, outcome.response);
      } catch {
        throw new UploadError(
          "delivery_conflict",
          "server conflict does not match the submitted batch",
        );
      }
      checkRecovery(sending, recovery);
      if (
        outcome.response.error.conflict.type === "batch_id" &&
        ++batchIdRecoveries > 1
      )
        throw new UploadError(
          "delivery_conflict",
          "server refused the recovery batch identity as well",
        );
      summary.quarantined += recovery.quarantinedRevisionIds.length;
      log.warn("conflict", {
        batchId: sending.batch_id,
        type: outcome.response.error.conflict.type,
        quarantined: recovery.quarantinedRevisionIds.length,
        recordIds: sending.records
          .filter((record) =>
            recovery.quarantinedRevisionIds.includes(record.revision_id),
          )
          .map((record) => record.record_id),
      });
      pending = recovery.retryBatch;
    }
  }

  function logReceipt(receipt: DeliveryReceipt, batch: HistoryExportBatch) {
    log.info("batch accepted", {
      batchId: batch.batch_id,
      // Server-chosen text: logged only in the engine's own content-addressed form.
      ...(/^rhr_[0-9a-f]{64}$/.test(receipt.receiptId)
        ? { receiptId: receipt.receiptId }
        : {}),
      records: receipt.acceptedRevisionIds.length,
    });
  }

  let position: Watermark | undefined;
  for (;;) {
    options.signal?.throwIfAborted();
    const page: ChangesPage = await feed.getChangesPage({
      ...storage,
      kinds,
      limit: SCAN_LIMIT,
      // A dry run never commits, so it walks forward from the cursor by position.
      ...(options.dryRun && position ? { from: position } : { consumer }),
    });
    const originId = page.head.epoch;
    if (page.position.epoch !== originId)
      throw new UploadError(
        "invalid_payload",
        "local store returned a position from another database",
      );
    // Pin the origin for the run, empty pages included: a store replaced mid-run is a
    // new origin whose cursor starts over, never a continuation of this one.
    if (summary.originId && summary.originId !== originId)
      throw new UploadError(
        "invalid_payload",
        "local store was replaced during the upload; run again",
      );
    if (page.changes.length === 0) {
      // An exhausted drain positions itself at the head even when no change of the
      // selected kinds lies below it, so `position` is how far the feed was read, not
      // the stored cursor. Nothing up to it needs delivering, which is the same reason
      // a page of only unselected rows is committed; committing it makes the reported
      // cursor the stored one. A store with no feed yet (revision 0) has nothing to commit.
      if (options.dryRun) summary.cursor = position ?? page.position;
      else if (page.position.revision > 0)
        summary.cursor = (
          await feed.commitChanges(consumer, page.position, {
            ...storage,
            kinds,
          })
        ).cursor;
      break;
    }
    summary.originId = originId;
    summary.scanned += page.changes.length;

    const records: HistoryExportRecord[] = [];
    for (const change of page.changes) {
      if (!selected(change, config.selection)) continue;
      records.push(deliveryRecord(change, originId));
      const key = `${change.sourceName}/${change.sessionId}`;
      summary.sessions[key] = (summary.sessions[key] ?? 0) + 1;
    }
    summary.selected += records.length;

    if (options.dryRun) {
      position = page.position;
      summary.cursor = page.position;
    } else {
      if (records.length) {
        for (const batch of cutBatches(
          config,
          consumer,
          originId,
          records,
          await limits(),
        ))
          await deliver(batch);
      }
      // Every selected record on the page is durable (or proven conflicting): only now
      // does the cursor move past the page.
      const committed = await feed.commitChanges(consumer, page.position, {
        ...storage,
        kinds,
      });
      summary.cursor = committed.cursor;
    }
    if (page.done) break;
  }
  return summary;
}
