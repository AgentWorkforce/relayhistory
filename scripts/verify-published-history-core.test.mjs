import assert from "node:assert/strict";
import test from "node:test";
import {
  assertPublishedContract,
  peerMinimum,
} from "./verify-published-history-core.mjs";
function compatible() {
  return {
    sdk: {
      HistoryPluginRegistry: class {
        sourceConnectors() {
          return [];
        }
      },
      ...Object.fromEntries(
        [
          "createHistoryDelivery",
          "historyDeliveryStatus",
          "drainHistoryDelivery",
          "controlHistoryDelivery",
          "discoverSourcePlugins",
          "hydrateSourcePlugin",
          "getSourceObservation",
        ].map((name) => [name, () => {}]),
      ),
    },
    native: {
      nativeContractVersion: () => 14,
      historyDelivery() {},
      applySourceEvidence() {},
      getSourceObservation() {},
    },
  };
}
test("peer gate checks the oldest admitted version and rejects ranges without an explicit floor", () => {
  assert.equal(peerMinimum("^0.16.0"), "0.16.0");
  assert.equal(peerMinimum("0.17.2"), "0.17.2");
  for (const range of [
    "*",
    "latest",
    ">=0.16.0",
    "^0.16.0 || ^0.17.0",
    "file:../sdk-ts",
  ])
    assert.throws(() => peerMinimum(range));
});
test("published version satisfying semver still fails without native14 and source/delivery APIs", () => {
  const { sdk, native } = compatible();
  assertPublishedContract(sdk, native, "0.16.0", "0.16.0");
  assert.throws(
    () =>
      assertPublishedContract(
        sdk,
        { ...native, nativeContractVersion: () => 12 },
        "0.16.0",
        "0.16.0",
      ),
    /contract 14/,
  );
  assert.throws(
    () =>
      assertPublishedContract(
        { ...sdk, hydrateSourcePlugin: undefined },
        native,
        "0.16.0",
        "0.16.0",
      ),
    /hydrateSourcePlugin/,
  );
  assert.throws(
    () =>
      assertPublishedContract(
        sdk,
        { ...native, applySourceEvidence: undefined },
        "0.16.0",
        "0.16.0",
      ),
    /applySourceEvidence/,
  );
  assert.throws(
    () => assertPublishedContract(sdk, native, "0.16.2", "0.16.0"),
    /peer minimum/,
  );
});
