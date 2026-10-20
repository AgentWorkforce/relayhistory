import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const root = path.dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(fs.readFileSync(path.join(root, "manifest.json"), "utf8"));
const read = (file) => JSON.parse(fs.readFileSync(file, "utf8"));
const rows = (file) => fs.readFileSync(file, "utf8").trim().split("\n").map(JSON.parse);

// The native reader strips the writer's request envelope and clock metadata.
// Compare only text messages: the reader hides result steps and renumbers them.
function userText(content) {
  const prefix = "<USER_REQUEST>\n";
  const suffix = "\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\n";
  const closing = content.lastIndexOf(suffix);
  assert.ok(content.startsWith(prefix) && closing >= prefix.length);
  return content.slice(prefix.length, closing);
}

for (const item of manifest) {
  test(`${item.name}: native transcript agrees with official restart reader`, () => {
    const directory = path.join(root, "cases", item.name, item.root);
    const transcript = rows(path.join(directory, "transcript_full.jsonl"));
    const oracle = read(path.join(root, "oracle", item.name, "official-reader.json"));
    const actual = transcript.flatMap((row) => {
      assert.ok(Number.isFinite(Date.parse(row.created_at)));
      if (row.type === "USER_INPUT" && row.source === "USER_EXPLICIT")
        return [{ role: "user", content: userText(row.content) }];
      if (row.type === "PLANNER_RESPONSE" && row.source === "MODEL" && row.content)
        return [{ role: "assistant", content: row.content }];
      return [];
    });
    const expected = oracle
      .filter((step) => step.type === "TEXT_RESPONSE" && step.content)
      .map((step) => ({ role: step.source === "USER" ? "user" : "assistant", content: step.content }));
    assert.deepEqual(actual, expected);
    for (const [name, hashes] of Object.entries(item.records)) {
      const bytes = fs.readFileSync(path.join(directory, name));
      assert.equal(createHash("sha256").update(bytes).digest("hex"), hashes.sanitized_sha256);
      const fixture = path.resolve(root, "../../../../crates/ai-hist/tests/fixtures/antigravity", item.name, item.root, name);
      assert.deepEqual(fs.readFileSync(fixture), bytes);
    }
  });
}

test("resumed native reader retains the earlier messages after process restart", () => {
  const before = read(path.join(root, "oracle/resumed/reader-after-restart.json"));
  const after = read(path.join(root, "oracle/resumed/official-reader.json"));
  assert.equal(before.length, 2);
  assert.equal(after.length, 4);
  assert.deepEqual(after.slice(0, 2), before);
});

test("full transcript retains text that the compact writer truncates", () => {
  const item = manifest.find((entry) => entry.name === "long");
  const directory = path.join(root, "cases", item.name, item.root);
  const full = rows(path.join(directory, "transcript_full.jsonl"))[1];
  const compact = rows(path.join(directory, "transcript.jsonl"))[1];
  assert.ok(full.content.length > compact.content.length);
  assert.deepEqual(compact.truncated_fields, ["content"]);
  assert.equal(full.truncated_fields, undefined);
  assert.ok(full.content.endsWith(" final sentinel."));
});

test("tool call IDs exist in the reader but are unavailable in transcript evidence", () => {
  const item = manifest.find((entry) => entry.name === "tool");
  const transcript = rows(path.join(root, "cases", item.name, item.root, "transcript_full.jsonl"));
  const oracle = read(path.join(root, "oracle/tool/official-reader.json"));
  assert.deepEqual(transcript[1].tool_calls, [{ name: "fixture_echo", args: { value: "blue" } }]);
  assert.equal(oracle[1].tool_calls[0].id, "fixture-call-001");
  assert.equal(transcript[1].tool_calls[0].id, undefined);
  assert.equal(transcript[2].type, "AGENCY_TOOL_CALL");
  assert.match(transcript[2].content, /fixture tool result: blue/);
});


test("Rust reference oracle matches each official reader capture", () => {
  const combined = read(path.resolve(root, "../../../../crates/ai-hist/tests/fixtures/antigravity/official-reader.json"));
  for (const item of manifest) {
    assert.deepEqual(combined[item.name], read(path.join(root, "oracle", item.name, "official-reader.json")));
  }
});
