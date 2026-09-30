import assert from "node:assert/strict";
import test from "node:test";
import {
  diffListings,
  forbiddenItems,
  normalizeListing,
} from "./check-public-api.mjs";

test("a listing is compared line by line, ignoring trailing whitespace and blank lines", () => {
  const expected = normalizeListing(
    "pub mod ai_hist\npub struct ai_hist::SessionStore  \n\n",
  );
  const actual = normalizeListing(
    "pub mod ai_hist\r\npub struct ai_hist::SessionStore\r\npub fn ai_hist::SessionStore::sessions(&self)\r\n",
  );
  assert.deepEqual(diffListings(expected, actual), {
    removed: [],
    added: ["pub fn ai_hist::SessionStore::sessions(&self)"],
  });
  assert.deepEqual(diffListings(actual, expected), {
    removed: ["pub fn ai_hist::SessionStore::sessions(&self)"],
    added: [],
  });
  assert.deepEqual(diffListings(expected, expected), { removed: [], added: [] });
});

test("a signature naming a rusqlite type is reported, a mention in a path segment is not", () => {
  const lines = [
    "pub fn ai_hist::EvidenceRecord::write(&self, &rusqlite::Connection) -> anyhow::Result<()>",
    "pub fn ai_hist::SessionStore::open(ai_hist::StoreOptions) -> core::result::Result<Self, ai_hist::Error>",
    "pub struct ai_hist::rusqlite_free_type",
  ];
  assert.deepEqual(forbiddenItems(lines), [lines[0]]);
  assert.deepEqual(forbiddenItems(lines, ["anyhow"]), [lines[0]]);
  assert.deepEqual(forbiddenItems(lines.slice(1)), []);
});
