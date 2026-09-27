import assert from "node:assert/strict";
import test from "node:test";
import { nativeContractVersion } from "./history-package-contract.mjs";
import {
  assertPublishedContract,
  peerMinimum,
} from "./verify-published-history-core.mjs";

/** The contract this checkout requires; the gate reads the same source. */
const required = nativeContractVersion();

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
          "beginHistoryExport",
          "readHistoryExportPage",
          "exportHistory",
          "closeHistoryExport",
          "discoverSourcePlugins",
          "hydrateSourcePlugin",
          "getSourceObservation",
        ].map((name) => [name, () => {}]),
      ),
    },
    native: {
      nativeContractVersion: () => required,
      historyExport() {},
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
test("published version satisfying semver still fails without the required native contract and source/delivery APIs", () => {
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
    new RegExp(`contract ${required}`),
  );
  assert.throws(
    () =>
      assertPublishedContract(
        sdk,
        { ...native, historyExport: undefined },
        "0.16.0",
        "0.16.0",
      ),
    /historyExport/,
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
