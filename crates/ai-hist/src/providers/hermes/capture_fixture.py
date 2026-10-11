# Run with pinned upstream checkout on PYTHONPATH and isolated HERMES_HOME.
import json, os, sqlite3
from pathlib import Path
from unittest.mock import patch
from hermes_state import SessionDB
root = Path(os.environ['HERMES_HOME'])
root.mkdir(parents=True, exist_ok=True)
output = Path(__file__).resolve().parents[3] / 'tests/fixtures/hermes'
with patch('time.time', return_value=1780000000.0):
    db = SessionDB(root / 'state.db')
    db.create_session('chat/not-a-uuid:α', 'cli', model='fixture-model', cwd='/fixture/project', profile_name='default')
    db.append_message('chat/not-a-uuid:α', 'user', 'Find the Hermes fixture needle', timestamp=1780000001, message_uid='msg-user')
    db.append_message('chat/not-a-uuid:α', 'assistant', 'I will inspect the file.', timestamp=1780000002, message_uid='msg-call', tool_calls=[{'id':'call/native:1','type':'function','function':{'name':'read_file','arguments':'{"path":"/fixture/project/notes.txt"}'}}])
    db.append_message('chat/not-a-uuid:α', 'tool', 'The Hermes fixture needle is here.', timestamp=1780000003, message_uid='msg-result', tool_call_id='call/native:1', tool_name='read_file')
    db.append_message('chat/not-a-uuid:α', 'assistant', 'Found the Hermes fixture needle.', reasoning_content='Inspect the recorded text.', finish_reason='stop', timestamp=1780000004, message_uid='msg-answer')
    db.end_session('chat/not-a-uuid:α', 'done')
    db.reopen_session('chat/not-a-uuid:α')
    db.append_message('chat/not-a-uuid:α', 'user', 'Continue after restart.', timestamp=1780000005, message_uid='msg-resume')
    db.append_message('chat/not-a-uuid:α', 'assistant', 'The historical answer is still available.', timestamp=1780000006, message_uid='msg-resumed-answer')
    db.update_token_counts('chat/not-a-uuid:α', input_tokens=1500, output_tokens=200, cache_read_tokens=300, api_call_count=2, model='fixture-model')
    db.archive_and_compact('chat/not-a-uuid:α', [{'role':'user','content':'Archived conversation summary.','_compressed_summary':True,'timestamp':1780000006.5,'message_uid':'msg-inplace-summary'}])
    db.end_session('chat/not-a-uuid:α', 'compression')
    db.create_session('compressed-child', 'cli', parent_session_id='chat/not-a-uuid:α', model='fixture-model')
    db.append_message('compressed-child', 'user', 'Compressed prior context.', _compressed_summary=True, timestamp=1780000007, message_uid='msg-summary')
    db.append_message('compressed-child', 'user', 'A fresh human question.', timestamp=1780000008, message_uid='msg-child-user')
    db.append_message('compressed-child', 'assistant', 'A fresh answer.', timestamp=1780000009, message_uid='msg-child-answer')
    oracle = [db.export_session(sid, include_inactive=True) for sid in ['chat/not-a-uuid:α','compressed-child']]
    (output/'official-export.json').write_text(json.dumps(oracle, indent=2, ensure_ascii=False)+'\n')
    db.close()
conn=sqlite3.connect(root/'state.db')
# Preserve actual upstream table DDL/rows, omit derived indexes, triggers and other stores.
tables=['schema_version','system_prompts','sessions','messages','session_model_usage']
lines=['BEGIN TRANSACTION;']
for table in tables:
    ddl=conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0]
    lines.append(ddl+';')
for table in tables:
    lines.extend(line for line in conn.iterdump() if line.startswith(f'INSERT INTO "{table}"'))
lines.append('COMMIT;')
(output/'schema31.sql').write_text('\n'.join(lines)+'\n')
