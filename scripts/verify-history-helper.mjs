/** An unsupported request verifies the executable/protocol without auth or network. */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function verifyHelperResult(result) {
  if (result.error) throw result.error;
  assert.equal(result.status, 0, "Helper must exit successfully");
  assert.equal(result.signal, null, "Helper must not exit from a signal");
  assert.equal(
    result.stderr,
    "",
    "Helper protocol verification must not write diagnostics",
  );
  const response = JSON.parse(result.stdout);
  assert.equal(response.version, 1);
  assert.equal(response.ok, false);
  assert.match(response.error.code, /^[A-Z_]+$/);
}

if (
  process.argv[1] &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  verifyHelperResult(
    spawnSync(process.argv[2], [], {
      input: JSON.stringify({
        version: 1,
        operation: "fixtureUnsupported",
        args: {},
      }),
      encoding: "utf8",
      timeout: 10000,
      maxBuffer: 65536,
    }),
  );
  console.log("Optional history helper protocol verified");
}
