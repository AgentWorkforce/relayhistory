/**
 * The History service's delivery endpoints. Every response is classified; a success
 * is accepted only when it is the durable receipt for exactly the batch sent.
 * Response bodies never reach logs or error messages beyond their error code.
 */
import { endpointBase } from "./config.js";
import {
  deliveryRetryAfter,
  parseDeliveryConflict,
  type DeliveryConflictResponse,
  type DeliveryFailure,
  type HistoryExportBatch,
} from "ai-hist";

export type UploadFailure = DeliveryFailure | "invalid_response";

export class UploadError extends Error {
  override name = "UploadError";
  constructor(
    readonly failure: UploadFailure,
    message: string,
    /** Absolute time before which a retry should not be sent. */
    readonly retryAt?: number,
  ) {
    super(message);
  }
  get retryable() {
    return this.failure === "transient" || this.failure === "rate_limited";
  }
}

export interface ServerLimits {
  maxRecords: number;
  maxRequestBytes: number;
}

export interface DeliveryReceipt {
  protocolVersion: 1;
  receiptId: string;
  batchId: string;
  acceptedRevisionIds: string[];
  unsupportedRevisionIds: string[];
  acceptanceLevel: "durable";
}

export type DeliveryOutcome =
  | { type: "receipt"; receipt: DeliveryReceipt }
  | { type: "conflict"; response: DeliveryConflictResponse };

export interface HistoryClientOptions {
  endpoint: URL;
  token: string;
  fetch?: typeof fetch;
  /** Per-request timeout. */
  timeoutMs?: number;
}

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorCode(body: unknown): string | undefined {
  return object(body) &&
    object(body.error) &&
    typeof body.error.code === "string"
    ? body.error.code
    : undefined;
}

/** Map a non-success status to a failure. The code is a server-defined identifier. */
function classify(
  status: number,
  body: unknown,
  retryAfter: string | null,
): UploadError {
  const code = errorCode(body);
  const label = code ? `${status} ${code}` : String(status);
  if (status === 401)
    return new UploadError(
      "authentication_required",
      `token rejected (${label})`,
    );
  if (status === 403)
    return new UploadError(
      "permission_denied",
      code === "delivery_account_mismatch"
        ? "token file accountId does not match the token's tenant"
        : `token lacks permission (${label})`,
    );
  if (status === 429)
    return new UploadError(
      "rate_limited",
      `rate limited (${label})`,
      deliveryRetryAfter(retryAfter),
    );
  if (status === 422 && code === "unsupported_mapping")
    return new UploadError(
      "mapping_version_mismatch",
      `server refused the mapping (${label})`,
    );
  if (status === 422 && code === "unsupported_evidence")
    return new UploadError(
      "unsupported_evidence",
      `server refused an evidence kind (${label})`,
    );
  if (status >= 400 && status < 500)
    return new UploadError(
      "invalid_payload",
      `server refused the batch (${label})`,
    );
  return new UploadError(
    "transient",
    `server unavailable (${label})`,
    deliveryRetryAfter(retryAfter),
  );
}

/** A durable receipt for precisely `batch`, or an error. */
export function validateReceipt(
  batch: HistoryExportBatch,
  value: unknown,
): DeliveryReceipt {
  const sent = batch.records.map((record) => record.revision_id);
  if (
    !object(value) ||
    value.protocolVersion !== 1 ||
    typeof value.receiptId !== "string" ||
    !value.receiptId ||
    value.batchId !== batch.batch_id ||
    value.acceptanceLevel !== "durable" ||
    !Array.isArray(value.unsupportedRevisionIds) ||
    value.unsupportedRevisionIds.length !== 0 ||
    !Array.isArray(value.acceptedRevisionIds) ||
    value.acceptedRevisionIds.length !== sent.length ||
    new Set(value.acceptedRevisionIds).size !== sent.length ||
    !value.acceptedRevisionIds.every((id) => sent.includes(id as string))
  )
    throw new UploadError(
      "invalid_response",
      "server response is not the durable receipt for this batch",
    );
  return value as unknown as DeliveryReceipt;
}

export class HistoryClient {
  readonly #endpoint: URL;
  readonly #token: string;
  readonly #fetch: typeof fetch;
  readonly #timeoutMs: number;

  constructor(options: HistoryClientOptions) {
    this.#endpoint = options.endpoint;
    this.#token = options.token;
    this.#fetch = options.fetch ?? fetch;
    this.#timeoutMs = options.timeoutMs ?? 60_000;
  }

  async #request(
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ) {
    const url = `${endpointBase(this.#endpoint)}${path}`;
    const timeout = AbortSignal.timeout(this.#timeoutMs);
    let response: Response;
    try {
      response = await this.#fetch(url, {
        method,
        headers: {
          authorization: `Bearer ${this.#token}`,
          accept: "application/json",
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        redirect: "error",
        signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
      });
    } catch (error) {
      if (signal?.aborted) throw error;
      // Network errors can echo the URL; the class is enough.
      throw new UploadError("transient", "server unreachable");
    }
    let text: string;
    try {
      text = await response.text();
    } catch (error) {
      if (signal?.aborted) throw error;
      // The server may already have committed this batch; a resend gets its receipt.
      throw new UploadError("transient", "server response was interrupted");
    }
    let parsed: unknown;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      // A truncated success is indistinguishable from a lost one: resend.
      if (response.status === 200)
        throw new UploadError("transient", "server response was incomplete");
      parsed = undefined;
    }
    return {
      status: response.status,
      body: parsed,
      retryAfter: response.headers.get("retry-after"),
    };
  }

  async limits(signal?: AbortSignal): Promise<ServerLimits> {
    const response = await this.#request(
      "GET",
      "/v1/delivery/limits",
      undefined,
      signal,
    );
    if (response.status !== 200)
      throw classify(response.status, response.body, response.retryAfter);
    const body = response.body;
    if (
      !object(body) ||
      !Number.isSafeInteger(body.maxRecords) ||
      (body.maxRecords as number) < 1 ||
      !Number.isSafeInteger(body.maxRequestBytes) ||
      (body.maxRequestBytes as number) < 1024
    )
      throw new UploadError(
        "invalid_response",
        "server delivery limits are malformed",
      );
    return {
      maxRecords: body.maxRecords as number,
      maxRequestBytes: body.maxRequestBytes as number,
    };
  }

  async deliver(
    batch: HistoryExportBatch,
    signal?: AbortSignal,
  ): Promise<DeliveryOutcome> {
    const response = await this.#request(
      "POST",
      "/v1/delivery/batches",
      { protocolVersion: 1, batch },
      signal,
    );
    if (response.status === 200)
      return {
        type: "receipt",
        receipt: validateReceipt(batch, response.body),
      };
    if (response.status === 409) {
      const conflict = parseDeliveryConflict(409, response.body);
      if (conflict) return { type: "conflict", response: conflict };
      throw new UploadError(
        "delivery_conflict",
        "server reported a conflict without recoverable detail",
      );
    }
    throw classify(response.status, response.body, response.retryAfter);
  }
}
