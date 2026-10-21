import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import assert from 'node:assert/strict';

const fixture = new URL('../../../../crates/ai-hist/tests/fixtures/vtcode/basic/.vtcode/sessions/vt-synthetic-session/events.jsonl', import.meta.url);
const oracle = new URL('./native-reader.json', import.meta.url);

test('released native reader projection matches canonical fixture byte offsets and events', () => {
  const bytes = readFileSync(fixture);
  const native = JSON.parse(readFileSync(oracle, 'utf8'));
  assert.equal(native.session_id, 'vt-synthetic-session');
  assert.equal(native.event_count, 12);
  assert.equal(native.turn_count, 2);
  let offset = 0;
  const actual = [];
  for (const line of bytes.toString('utf8').match(/[^\n]*\n/g) ?? []) {
    const row = JSON.parse(line);
    assert.equal(row.schema_version, '0.18.0');
    actual.push({ offset, event: row.event });
    offset += Buffer.byteLength(line);
  }
  assert.deepEqual(actual, native.events);
  assert.equal(offset, bytes.length, 'native writer emitted complete newline-delimited records');
});
