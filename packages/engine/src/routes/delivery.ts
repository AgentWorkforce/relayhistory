import { Hono } from "hono";
import {
  hostContext,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import {
  acceptDelivery,
  listDelivery,
  parseDeliveryRequest,
  requireDeliveryAccount,
} from "../lib/delivery.js";
import {
  DELIVERY_LIMITS,
  DeliveryError,
  MAX_DELIVERY_BYTES,
  type HistoryExportBatch,
} from "../lib/delivery-contracts.js";
/** `read.bytes` counts what was received so far, including when parsing fails. */
async function boundedJson(
  request: Request,
  read: { bytes: number },
): Promise<unknown> {
  const reader = request.body?.getReader();
  if (!reader)
    throw new DeliveryError(
      "invalid_delivery",
      400,
      "Delivery request body is required",
    );
  const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
  let size = 0;
  let text = "";
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      read.bytes = size;
      if (size > MAX_DELIVERY_BYTES) {
        await reader.cancel();
        throw new DeliveryError(
          "delivery_too_large",
          413,
          `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes`,
        );
      }
      text += decoder.decode(chunk.value, { stream: true });
    }
    text += decoder.decode();
    return JSON.parse(text);
  } catch (error) {
    if (error instanceof DeliveryError) throw error;
    throw new DeliveryError(
      "invalid_delivery",
      400,
      "Delivery request must be valid UTF-8 JSON",
    );
  } finally {
    reader.releaseLock();
  }
}
function failure(c: HistoryContext, error: unknown) {
  const safe =
    error instanceof DeliveryError
      ? error
      : new DeliveryError(
          "delivery_unavailable",
          503,
          "Durable delivery is temporarily unavailable",
        );
  // Never log query parameters or user history through the global error handler.
  return c.json(
    {
      error: {
        code: safe.code,
        message: safe.message,
        ...(safe.conflict ? { conflict: safe.conflict } : {}),
        ...(safe.conflicts ? { conflicts: safe.conflicts } : {}),
        ...(safe.conflictCount ? { conflictCount: safe.conflictCount } : {}),
      },
      correlationId: c.get("correlationId") ?? "",
    },
    safe.status,
  );
}
const LOGGED_CONFLICTS = 5;
function logConflict(
  c: HistoryContext,
  batch: HistoryExportBatch | undefined,
  error: unknown,
) {
  if (
    !(error instanceof DeliveryError) ||
    error.code !== "delivery_conflict" ||
    !batch
  )
    return;
  const auth = getAuth(c);
  const conflicts = error.conflicts?.slice(0, LOGGED_CONFLICTS).map((item) => ({
    recordId: item.recordId,
    submittedRevisionId: item.submittedRevisionId,
    submittedRevision: item.submittedRevision,
    currentRevisionId: item.currentRevisionId,
    currentRevision: item.currentRevision,
  }));
  console.warn(
    JSON.stringify({
      event: "delivery.conflict",
      correlationId: c.get("correlationId") ?? "",
      orgId: auth.orgId,
      workspaceId: auth.workspaceId ?? "",
      originId: batch.origin_id,
      batchId: batch.batch_id,
      type: error.conflict?.type ?? "unknown",
      ...(error.conflictCount ? { conflictCount: error.conflictCount } : {}),
      ...(conflicts ? { conflicts } : {}),
    }),
  );
}
/**
 * The shape of one delivery batch for telemetry: record and kind counts, distinct
 * sessions and the largest session's share. Session ids and payloads are not part of
 * it. Records with no session (`session_id` null, the normalized form of `''`) count in
 * `records` and `kinds` but are not a session.
 */
export function deliveryBatchShape(batch: HistoryExportBatch | undefined) {
  if (!batch)
    return { records: 0, kinds: {}, sessions: 0, max_session_records: 0 };
  const kinds: Record<string, number> = {};
  const perSession = new Map<string, number>();
  for (const record of batch.records) {
    kinds[record.kind] = (kinds[record.kind] ?? 0) + 1;
    const session = record.session_id;
    if (session === null || session === "") continue;
    perSession.set(session, (perSession.get(session) ?? 0) + 1);
  }
  return {
    records: batch.records.length,
    kinds,
    sessions: perSession.size,
    max_session_records: Math.max(0, ...perSession.values()),
  };
}

export function createDeliveryRoutes<E extends HistoryEnv>(
  deps: HistoryEngineDeps<E>,
): Hono<HistoryEnv> {
  const database = (c: HistoryContext) => deps.database(hostContext<E>(c));
  const deliveryRoutes = new Hono<HistoryEnv>();

  deliveryRoutes.post(
    "/delivery/batches",
    requireScope("rth:sync"),
    async (c) => {
      const started = Date.now();
      const observe = deps.observeDeliveryBatch?.(hostContext<E>(c));
      let batch: HistoryExportBatch | undefined;
      const read = { bytes: 0 };
      let outcome = "accepted";
      let conflictCount: number | undefined;
      try {
        const db = database(c);
        if (!db)
          throw new DeliveryError(
            "not_configured",
            503,
            "DATABASE_URL is required",
          );
        batch = parseDeliveryRequest(await boundedJson(c.req.raw, read));
        return c.json(await acceptDelivery(db, getAuth(c), batch));
      } catch (error) {
        outcome =
          error instanceof DeliveryError ? error.code : "delivery_unavailable";
        if (error instanceof DeliveryError) conflictCount = error.conflictCount;
        logConflict(c, batch, error);
        return failure(c, error);
      } finally {
        observe?.({
          auth: getAuth(c),
          batch,
          bytes: read.bytes,
          outcome,
          ...(conflictCount ? { conflictCount } : {}),
          durationMs: Date.now() - started,
        });
      }
    },
  );

  // Lets a client size its first batch before sending it. The same object is
  // merged into every success receipt; neither is persisted protocol state.
  deliveryRoutes.get("/delivery/limits", requireScope("rth:sync"), (c) =>
    c.json({ ...DELIVERY_LIMITS }),
  );

  deliveryRoutes.get(
    "/delivery/records",
    requireScope("rth:read"),
    async (c) => {
      try {
        const db = database(c);
        if (!db)
          throw new DeliveryError(
            "not_configured",
            503,
            "DATABASE_URL is required",
          );
        if (
          [...new URL(c.req.url).searchParams.keys()].some(
            (key) => new URL(c.req.url).searchParams.getAll(key).length > 1,
          )
        )
          throw new DeliveryError(
            "invalid_delivery",
            400,
            "Repeated delivery filters are unsupported",
          );
        await requireDeliveryAccount(
          getAuth(c),
          c.req.header("X-RelayHistory-Expected-Account"),
        );
        return c.json(await listDelivery(db, getAuth(c), c.req.query()));
      } catch (error) {
        return failure(c, error);
      }
    },
  );

  return deliveryRoutes;
}
