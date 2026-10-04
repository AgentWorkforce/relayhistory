// A conflict retry carries the SDK's `conflict-…` batch id, which is longer than the
// uploader's `upload-…` id. A batch cut to the request limit must still fit when retried.
import { deliveryBatchDigest, recoverDeliveryConflict } from "ai-hist";
import { describe, expect, it } from "vitest";
import { deliveryRecord } from "../src/records.js";
import { cutBatches } from "../src/uploader.js";
import { change, config } from "./support.js";

const size = (batch: object) =>
  new TextEncoder().encode(JSON.stringify({ protocolVersion: 1, batch }))
    .length;

describe("conflict retries stay within the request limit", () => {
  it("a batch-id recovery of a batch cut to the limit still fits", () => {
    const records = [1, 2, 3, 4].map((r) =>
      deliveryRecord(change(r), "00000000000000aa"),
    );
    const whole = cutBatches(config(), "c", "00000000000000aa", records, {
      maxRecords: 10,
      maxBytes: 1_048_576,
    })[0];
    // A limit that exactly fits all four records in one uploader batch.
    const maxBytes = size(whole);
    for (const batch of cutBatches(config(), "c", "00000000000000aa", records, {
      maxRecords: 10,
      maxBytes,
    })) {
      expect(size(batch)).toBeLessThanOrEqual(maxBytes);
      const recovery = recoverDeliveryConflict(batch, {
        error: {
          code: "delivery_conflict",
          message: "x",
          conflict: {
            type: "batch_id",
            originId: batch.origin_id,
            batchId: batch.batch_id,
            submittedDigest: deliveryBatchDigest(batch),
            currentDigest: "e".repeat(64),
          },
        },
      });
      expect(recovery.retryBatch!.batch_id).toMatch(/^conflict-/);
      expect(size(recovery.retryBatch!)).toBeLessThanOrEqual(maxBytes);
    }
  });
});
