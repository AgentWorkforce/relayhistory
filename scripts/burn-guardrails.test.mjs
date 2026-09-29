import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

import {
  DEFAULT_CUTOVER_TAG,
  READER_FILES,
  parityReadiness,
  reappearedReaders,
  tripwireVerdict,
} from "./burn-guardrails.mjs";

function sdkPackage({ features = {}, dependencies = [], targets = [] } = {}) {
  return {
    packages: [
      { name: "relayburn-cli", features: {}, dependencies: [], targets: [] },
      { name: "relayburn-sdk", features, dependencies, targets },
    ],
  };
}

test("parity is not ready against burn as it stands before #557", () => {
  const verdict = parityReadiness(
    sdkPackage({
      features: { "test-utils": [] },
      dependencies: [{ name: "serde" }],
      targets: [
        { name: "relayburn_sdk", kind: ["lib"] },
        { name: "integration", kind: ["test"] },
      ],
    }),
  );
  assert.equal(verdict.ready, false);
  assert.equal(verdict.missing.length, 3);
  assert.match(verdict.missing.join(" "), /relayhistory-source/);
  assert.match(verdict.missing.join(" "), /ai-hist/);
  assert.match(verdict.missing.join(" "), /relayhistory_parity/);
});

test("parity is ready once the feature, dependency and test target all exist", () => {
  const verdict = parityReadiness(
    sdkPackage({
      features: { "relayhistory-source": ["dep:ai-hist"] },
      dependencies: [{ name: "ai-hist", optional: true }],
      targets: [{ name: "relayhistory_parity", kind: ["test"] }],
    }),
  );
  assert.deepEqual(verdict, { ready: true, missing: [] });
});

test("a same-named non-test target does not count as the parity suite", () => {
  const verdict = parityReadiness(
    sdkPackage({
      features: { "relayhistory-source": ["dep:ai-hist"] },
      dependencies: [{ name: "ai-hist" }],
      targets: [{ name: "relayhistory_parity", kind: ["bench"] }],
    }),
  );
  assert.equal(verdict.ready, false);
});

test("a workspace without relayburn-sdk is not ready", () => {
  assert.deepEqual(parityReadiness({ packages: [] }), {
    ready: false,
    missing: ["package `relayburn-sdk`"],
  });
});

test("reappearedReaders names exactly the reader files that exist", () => {
  const present = new Set([path.join("burn", READER_FILES[1])]);
  assert.deepEqual(
    reappearedReaders("burn", (p) => present.has(p)),
    [READER_FILES[1]],
  );
  assert.deepEqual(reappearedReaders("burn", () => false), []);
});

test("the tripwire is inactive until the cutover tag exists, readers or not", () => {
  const verdict = tripwireVerdict({ tag: DEFAULT_CUTOVER_TAG, tagExists: false, present: READER_FILES });
  assert.equal(verdict.status, "inactive");
  assert.match(verdict.message, /relayburn-sdk-v5\.0\.0/);
});

test("after the cutover tag, any reader file fails and none passes", () => {
  const fail = tripwireVerdict({ tag: "t", tagExists: true, present: [READER_FILES[2]] });
  assert.equal(fail.status, "fail");
  assert.match(fail.message, /reader\/opencode\.rs/);
  assert.equal(tripwireVerdict({ tag: "t", tagExists: true, present: [] }).status, "pass");
});

test("workflows run the guardrails the way the script expects", async () => {
  const ci = await readFile(new URL("../.github/workflows/ci.yml", import.meta.url), "utf8");
  assert.match(ci, /node scripts\/burn-guardrails\.mjs parity-probe --burn-dir burn/);
  assert.match(ci, /if: steps\.probe\.outputs\.ready == 'true'/);
  assert.match(
    ci,
    /--features relayhistory-source --test relayhistory_parity/,
  );

  const tripwire = await readFile(
    new URL("../.github/workflows/burn-reader-tripwire.yml", import.meta.url),
    "utf8",
  );
  assert.match(tripwire, /schedule:/);
  assert.doesNotMatch(tripwire, /pull_request/);
  assert.match(tripwire, /node scripts\/burn-guardrails\.mjs tripwire --burn-dir burn/);
  assert.ok(tripwire.includes(DEFAULT_CUTOVER_TAG), "workflow default tag matches the script's");
});
