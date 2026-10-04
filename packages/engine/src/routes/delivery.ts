import { Hono } from "hono";
import {
  hostContext,
  type DeliveryBatchObservation,
  type HistoryContext,
  type HistoryEngineDeps,
  type HistoryEnv,
} from "../env.js";
import { containRejection } from "../lib/host-hooks.js";
import { getAuth, requireScope } from "../middleware/auth.js";
import {
  acceptDelivery,
  listDelivery,
  parseDeliveryRequest,
  requireDeliveryAccount,
} from "../lib/delivery.js";
import {
  readBoundedJson,
  type BoundedJsonFailure,
} from "../lib/bounded-json.js";
import {
  DELIVERY_LIMITS,
  DeliveryError,
  MAX_DELIVERY_BYTES,
  type HistoryExportBatch,
} from "../lib/delivery-contracts.js";
const DELIVERY_BODY_ERRORS: Record<BoundedJsonFailure, () => DeliveryError> = {
  missing: () =>
    new DeliveryError(
      "invalid_delivery",
      400,
      "Delivery request body is required",
    ),
  malformed: () =>
    new DeliveryError(
      "invalid_delivery",
      400,
      "Delivery request must be valid UTF-8 JSON",
    ),
  too_large: () =>
    new DeliveryError(
      "delivery_too_large",
      413,
      `Delivery request exceeds ${MAX_DELIVERY_BYTES} bytes`,
    ),
};
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
  // Host telemetry never changes a delivery result: a failing observer is dropped.
  const observer = (c: HistoryContext) => {
    const unavailable = () =>
      console.warn("[delivery] batch observer unavailable");
    const failed = () => console.warn("[delivery] batch observer failed");
    let observe: ((observation: DeliveryBatchObservation) => void) | undefined;
    try {
      const made: unknown = deps.observeDeliveryBatch?.(hostContext<E>(c));
      if (typeof made === "function")
        observe = made as (observation: DeliveryBatchObservation) => void;
      // An async factory is not a supported shape; contain its rejection and skip it.
      else containRejection(made, unavailable);
    } catch {
      unavailable();
    }
    return (observation: DeliveryBatchObservation) => {
      try {
        containRejection(observe?.(observation), failed);
      } catch {
        failed();
      }
    };
  };

  deliveryRoutes.post(
    "/delivery/batches",
    requireScope("rth:sync"),
    async (c) => {
      const started = Date.now();
      const observe = observer(c);
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
        batch = parseDeliveryRequest(
          await readBoundedJson(c.req.raw, {
            maxBytes: MAX_DELIVERY_BYTES,
            error: (failure) => DELIVERY_BODY_ERRORS[failure](),
            read,
          }),
        );
        return c.json(await acceptDelivery(db, getAuth(c), batch));
      } catch (error) {
        outcome =
          error instanceof DeliveryError ? error.code : "delivery_unavailable";
        if (error instanceof DeliveryError) conflictCount = error.conflictCount;
        logConflict(c, batch, error);
        return failure(c, error);
      } finally {
        observe({
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
        const keys = [...new URL(c.req.url).searchParams.keys()];
        if (new Set(keys).size !== keys.length)
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
