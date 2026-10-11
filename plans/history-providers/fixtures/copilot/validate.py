"""Offline research-fixture checks, not a production Copilot parser."""
import copy
import datetime
import json
import pathlib
import unittest
import uuid

ROOT = pathlib.Path(__file__).parent

def journal(text):
    if not text.endswith('\n'):
        raise ValueError('uncommitted final JSONL record')
    rows = [json.loads(line) for line in text.splitlines()]
    header = rows[0]
    if header['type'] != 'session.start' or header['data']['version'] != 1:
        raise ValueError('unsupported research fixture schema')
    seen = set()
    previous = None
    for row in rows:
        uuid.UUID(row['id'])
        datetime.datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
        if row['id'] in seen or row['parentId'] != previous:
            raise ValueError('invalid native event chain')
        seen.add(row['id'])
        previous = row['id']
    return rows

def tools(rows):
    requests = {t['toolCallId']: t for row in rows if row['type'] == 'assistant.message' for t in row['data'].get('toolRequests', [])}
    starts = {row['data']['toolCallId']: row['data'] for row in rows if row['type'] == 'tool.execution_start'}
    ends = {row['data']['toolCallId']: row['data'] for row in rows if row['type'] == 'tool.execution_complete'}
    if requests.keys() != starts.keys() or starts.keys() != ends.keys():
        raise ValueError('broken native tool join')
    for key, request in requests.items():
        if request['name'] != starts[key]['toolName'] or request['arguments'] != starts[key]['arguments']:
            raise ValueError('conflicting tool facts')
    return requests, ends

class ResearchFixture(unittest.TestCase):
    def setUp(self):
        self.rows = journal((ROOT / 'events.jsonl').read_text())

    def test_actual_writer_and_independent_reader_agree_after_restart(self):
        reader = json.loads((ROOT / 'official-reader.json').read_text())
        self.assertEqual(reader[:len(self.rows)], self.rows)
        self.assertEqual(reader[-1]['type'], 'session.resume')
        self.assertEqual(reader[-1]['data']['eventCount'], len(self.rows))
        self.assertNotIn('session.resume', [r['type'] for r in self.rows])

    def test_semantic_input_is_content_oracle_not_timestamp_or_id_oracle(self):
        semantic = [json.loads(line) for line in (ROOT / 'semantic-input.jsonl').read_text().splitlines()]
        expected = [(r['role'], '\n'.join(b['text'] for b in r['content'])) for r in semantic if r['type'] == 'message']
        actual = [(r['type'].split('.')[0], r['data']['content']) for r in self.rows if r['type'] in ['user.message', 'assistant.message']]
        self.assertEqual(actual, expected)
        self.assertEqual(self.rows[0]['data']['startTime'], '2026-01-01T00:00:00.000Z')
        self.assertNotEqual(self.rows[1]['timestamp'], semantic[1]['timestamp'])
        self.assertNotEqual(self.rows[1]['id'], semantic[1]['id'])

    def test_tool_identity_is_shared_across_request_start_result(self):
        requests, ends = tools(self.rows)
        self.assertEqual(len(requests), 1)
        result = next(iter(ends.values()))
        self.assertTrue(result['success'])
        self.assertEqual(result['result']['content'], 'Hello synthetic fixture')

    def test_negative_broken_tool_join_is_detected(self):
        rows = copy.deepcopy(self.rows)
        next(r for r in rows if r['type'] == 'tool.execution_complete')['data']['toolCallId'] = 'wrong'
        with self.assertRaisesRegex(ValueError, 'broken native tool join'):
            tools(rows)

    def test_schema_and_partial_tail_are_not_silently_accepted(self):
        text = (ROOT / 'events.jsonl').read_text()
        with self.assertRaisesRegex(ValueError, 'final JSONL'):
            journal(text + '{')
        with self.assertRaises(json.JSONDecodeError):
            journal(text + '{broken}\n')
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            journal(text.replace('"version":1', '"version":999', 1))

    def test_rich_coverage_remains_unproven(self):
        self.assertTrue(all(not r.get('ephemeral') for r in self.rows))
        self.assertFalse(any(r['type'] == 'assistant.usage' or r['type'].startswith('subagent.') for r in self.rows))
        provenance = json.loads((ROOT / 'provenance.json').read_text())
        for kind in ['usage', 'delegation', 'edits']:
            self.assertEqual(provenance[kind], 'unproven')
        self.assertEqual(provenance['modelCalls'], 0)

if __name__ == '__main__':
    unittest.main()
