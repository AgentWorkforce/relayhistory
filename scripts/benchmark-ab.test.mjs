import assert from "node:assert/strict";
import test from "node:test";
import { AB_BOUNDS, cachegrindInstructions, compare, metricOf, renderComparison } from "./benchmark-ab.mjs";

const sample = (side, phase, values) => ({ side, phase, round: 0, dbBytes: 1000, elapsedMs: 10, ...values });

test("cachegrind instructions come from the summary line's Ir column", () => {
  const text = "desc: --cache-sim=no\ncmd: x\nevents: Ir\nfl=a\n1 2\nsummary: 123456789\n";
  assert.equal(cachegrindInstructions(text), 123456789);
  assert.equal(cachegrindInstructions("events: Ir Dr\nsummary: 10 20\n"), 10);
  assert.equal(cachegrindInstructions("no summary"), null);
});

test("a metric is used only when every sample has it", () => {
  assert.equal(metricOf([sample("base", "x", { instructions: 1 }), sample("head", "x", { instructions: 2 })]), "instructions");
  // A base harness that predates `cpuMs` falls both sides back to wall time.
  assert.equal(metricOf([sample("base", "x", { cpuMs: null }), sample("head", "x", { cpuMs: 3 })]), "elapsedMs");
});

test("a steady-state phase past its bound fails; a cold phase is only reported", () => {
  const verdict = compare([
    sample("base", "unchanged_sync", { instructions: 1000 }),
    sample("head", "unchanged_sync", { instructions: 1000 * AB_BOUNDS.unchanged_sync + 5 }),
    sample("base", "cold_sync", { instructions: 1000 }),
    sample("head", "cold_sync", { instructions: 2000 }),
  ]);
  assert.equal(verdict.ok, false);
  assert.deepEqual(verdict.rows.map((row) => [row.phase, row.ok]), [["cold_sync", true], ["unchanged_sync", false]]);
  assert.match(renderComparison(verdict), /FAILED/);

  const within = compare([
    sample("base", "unchanged_sync", { instructions: 1000 }),
    sample("head", "unchanged_sync", { instructions: 1015 }),
  ]);
  assert.equal(within.ok, true);
});
