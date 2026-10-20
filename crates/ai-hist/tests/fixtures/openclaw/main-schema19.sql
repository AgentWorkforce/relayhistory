BEGIN TRANSACTION;
PRAGMA user_version=19;
CREATE TABLE schema_meta (
  meta_key TEXT NOT NULL PRIMARY KEY,
  role TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  agent_id TEXT,
  app_version TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE session_nodes (
  session_key TEXT NOT NULL PRIMARY KEY,
  current_session_id TEXT NOT NULL,
  entry_json TEXT NOT NULL,
  entry_valid INTEGER NOT NULL DEFAULT 0 CHECK (entry_valid IN (-1, 0, 1)),
  updated_at INTEGER NOT NULL,
  status TEXT CHECK (status IS NULL OR status IN ('running', 'done', 'failed', 'killed', 'timeout')),
  created_at INTEGER,
  created_via TEXT CHECK (created_via IS NULL OR created_via IN ('operator', 'spawn', 'channel', 'cron', 'talk', 'run', 'plugin', 'internal')),
  created_actor_type TEXT CHECK (created_actor_type IS NULL OR created_actor_type IN ('human', 'agent', 'system')),
  created_actor_id TEXT,
  owner_actor_type TEXT,
  owner_actor_id TEXT,
  owner_assigned_by_type TEXT,
  owner_assigned_by_id TEXT,
  owner_assigned_at INTEGER,
  project_id TEXT,
  parent_session_key TEXT,
  spawned_by TEXT,
  fork_source_session_key TEXT,
  fork_source_session_id TEXT,
  fork_source_entry_id TEXT,
  label TEXT,
  display_name TEXT,
  category TEXT,
  icon TEXT,
  pinned_at INTEGER,
  archived_at INTEGER,
  last_read_at INTEGER,
  last_interaction_at INTEGER,
  last_activity_at INTEGER
) STRICT;
CREATE TABLE conversations (
  conversation_id TEXT NOT NULL PRIMARY KEY,
  channel TEXT NOT NULL,
  account_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('direct', 'group', 'channel')),
  peer_id TEXT NOT NULL,
  delivery_target TEXT NOT NULL,
  parent_conversation_id TEXT,
  thread_id TEXT,
  native_channel_id TEXT,
  native_direct_user_id TEXT,
  label TEXT,
  metadata_json TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE session_windows (
  session_id TEXT NOT NULL PRIMARY KEY,
  session_key TEXT NOT NULL,
  previous_session_id TEXT,
  reason TEXT CHECK (reason IS NULL OR reason IN ('initial', 'reset', 'rollover', 'fork', 'rewind', 'switch', 'recovery', 'compaction')),
  session_scope TEXT NOT NULL DEFAULT 'conversation' CHECK (session_scope IN ('conversation', 'shared-main', 'group', 'channel')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  transcript_updated_at INTEGER DEFAULT NULL,
  transcript_observed_at INTEGER DEFAULT NULL,
  session_entry_provenance INTEGER NOT NULL DEFAULT 0 CHECK (session_entry_provenance IN (0, 1)),
  acp_owned INTEGER NOT NULL DEFAULT 0 CHECK (acp_owned IN (0, 1)),
  plugin_owner_id TEXT,
  hook_external_content_source TEXT CHECK (hook_external_content_source IS NULL OR hook_external_content_source IN ('gmail', 'webhook')),
  started_at INTEGER,
  ended_at INTEGER,
  status TEXT CHECK (status IS NULL OR status IN ('running', 'done', 'failed', 'killed', 'timeout')),
  chat_type TEXT CHECK (chat_type IS NULL OR chat_type IN ('direct', 'group', 'channel')),
  channel TEXT,
  account_id TEXT,
  primary_conversation_id TEXT,
  model_provider TEXT,
  model TEXT,
  agent_harness_id TEXT,
  parent_session_key TEXT,
  spawned_by TEXT,
  display_name TEXT,
  FOREIGN KEY (session_key) REFERENCES session_nodes(session_key) ON DELETE CASCADE,
  FOREIGN KEY (primary_conversation_id) REFERENCES conversations(conversation_id) ON DELETE SET NULL
) STRICT;
CREATE TABLE transcript_events (
  session_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  event_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (session_id, seq),
  FOREIGN KEY (session_id) REFERENCES "session_windows"(session_id) ON DELETE CASCADE
) STRICT;
CREATE TABLE transcript_event_identities (
  session_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  event_type TEXT,
  parent_id TEXT,
  message_idempotency_key TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (session_id, event_id),
  FOREIGN KEY (session_id, seq) REFERENCES transcript_events(session_id, seq) ON DELETE CASCADE
) STRICT;
CREATE TABLE session_transcript_active_events (
  session_id TEXT NOT NULL,
  active_position INTEGER NOT NULL CHECK (active_position >= 0),
  event_seq INTEGER NOT NULL,
  message_position INTEGER CHECK (message_position IS NULL OR message_position >= 0),
  context_eligible INTEGER,
  PRIMARY KEY (session_id, active_position),
  FOREIGN KEY (session_id, event_seq) REFERENCES transcript_events(session_id, seq) ON DELETE CASCADE
) STRICT;
CREATE TABLE transcript_rewrite_watermarks (
  session_id TEXT NOT NULL PRIMARY KEY,
  generation TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (session_id) REFERENCES "session_windows"(session_id) ON DELETE CASCADE
) STRICT;
INSERT INTO schema_meta VALUES('primary','agent',19,'main','2026.8.1',1790000000039,1790000000039);
INSERT INTO session_nodes VALUES('agent:main:main','fixture/main:α','{"sessionId":"fixture/main:α","updatedAt":1789999990000,"delivery":{"kind":"none"}}',1,1789999990000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_nodes VALUES('agent:main:telegram:direct:fixture-peer','fixture-telegram-0001','{"sessionId":"fixture-telegram-0001","updatedAt":1789999991000,"delivery":{"kind":"none"}}',1,1789999991000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_windows VALUES('fixture/main:α','agent:main:main',NULL,NULL,'conversation',1789999990000,1790000000123,1790000000126,1789999990000,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_windows VALUES('fixture-telegram-0001','agent:main:telegram:direct:fixture-peer',NULL,NULL,'conversation',1789999991000,1790000000185,1790000000188,1789999991000,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',0,'{"type":"session","version":3,"id":"fixture/main:α","timestamp":"2026-09-21T14:13:20.072Z","cwd":"/fixture/project"}',1790000000072);
INSERT INTO transcript_events VALUES('fixture/main:α',1,'{"type":"message","id":"00000000-0000-4000-8000-000000000003","parentId":null,"timestamp":"2026-09-21T14:13:20.071Z","message":{"role":"user","content":[{"type":"text","text":"Find the OpenClaw fixture needle"}],"timestamp":1790000001000}}',1790000000071);
INSERT INTO transcript_events VALUES('fixture/main:α',2,'{"type":"message","id":"00000000-0000-4000-8000-000000000006","parentId":"00000000-0000-4000-8000-000000000003","timestamp":"2026-09-21T14:13:20.086Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Read the fixture notes first."},{"type":"text","text":"I will read the notes."},{"type":"toolCall","id":"call_fixture|read:1","name":"read","arguments":{"path":"/fixture/project/notes.txt"}}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"toolUse","usage":{"input":100,"output":20,"cacheRead":5,"cacheWrite":0,"totalTokens":125,"cost":{"input":0.0005,"output":0.0005,"cacheRead":0,"cacheWrite":0,"total":0.001}},"responseId":"resp-fixture-1","timestamp":1790000002000}}',1790000000086);
INSERT INTO transcript_events VALUES('fixture/main:α',3,'{"type":"message","id":"00000000-0000-4000-8000-000000000008","parentId":"00000000-0000-4000-8000-000000000006","timestamp":"2026-09-21T14:13:20.098Z","message":{"role":"toolResult","toolCallId":"call_fixture|read:1","toolName":"read","content":[{"type":"text","text":"The OpenClaw fixture needle is here."}],"isError":false,"timestamp":1790000003000}}',1790000000098);
INSERT INTO transcript_events VALUES('fixture/main:α',4,'{"type":"message","id":"00000000-0000-4000-8000-000000000010","parentId":"00000000-0000-4000-8000-000000000008","timestamp":"2026-09-21T14:13:20.110Z","message":{"role":"assistant","content":[{"type":"text","text":"Found the OpenClaw fixture needle."}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":130,"output":10,"cacheRead":100,"cacheWrite":0,"totalTokens":240,"cost":{"input":0.001,"output":0.001,"cacheRead":0,"cacheWrite":0,"total":0.002}},"responseId":"resp-fixture-2","timestamp":1790000004000}}',1790000000110);
INSERT INTO transcript_events VALUES('fixture/main:α',5,'{"type":"message","id":"00000000-0000-4000-8000-000000000012","parentId":"00000000-0000-4000-8000-000000000010","timestamp":"2026-09-21T14:13:20.123Z","message":{"role":"assistant","content":[{"type":"text","text":"Found the OpenClaw fixture needle."}],"api":"openai-responses","provider":"openclaw","model":"delivery-mirror","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1790000000118,"idempotencyKey":"fixture-delivery-1","openclawDeliveryMirror":{"kind":"channel-final"}}}',1790000000123);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',0,'{"type":"session","version":3,"id":"fixture-telegram-0001","timestamp":"2026-09-21T14:13:20.147Z","cwd":"/fixture/project"}',1790000000147);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',1,'{"type":"message","id":"00000000-0000-4000-8000-000000000014","parentId":null,"timestamp":"2026-09-21T14:13:20.146Z","message":{"role":"user","content":"Run the failing fixture command","timestamp":1790000101000}}',1790000000146);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',2,'{"type":"message","id":"00000000-0000-4000-8000-000000000017","parentId":"00000000-0000-4000-8000-000000000014","timestamp":"2026-09-21T14:13:20.161Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"call_fixture_exec","name":"exec","arguments":{"command":"false"}}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"toolUse","usage":{"input":40,"output":8,"cacheRead":0,"cacheWrite":0,"totalTokens":48,"cost":{"input":0.00025,"output":0.00025,"cacheRead":0,"cacheWrite":0,"total":0.0005}},"timestamp":1790000102000}}',1790000000161);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',3,'{"type":"message","id":"00000000-0000-4000-8000-000000000019","parentId":"00000000-0000-4000-8000-000000000017","timestamp":"2026-09-21T14:13:20.173Z","message":{"role":"toolResult","toolCallId":"call_fixture_exec","toolName":"exec","content":[{"type":"text","text":"exit status 1\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\nfixture stderr line\n"}],"isError":true,"timestamp":1790000103000}}',1790000000173);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',4,'{"type":"message","id":"00000000-0000-4000-8000-000000000021","parentId":"00000000-0000-4000-8000-000000000019","timestamp":"2026-09-21T14:13:20.185Z","message":{"role":"assistant","content":[{"type":"text","text":"The fixture command failed as expected."}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":60,"output":9,"cacheRead":0,"cacheWrite":0,"totalTokens":69,"cost":{"input":0.0003,"output":0.0003,"cacheRead":0,"cacheWrite":0,"total":0.0006}},"timestamp":1790000104000}}',1790000000185);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','fixture/main:α',0,'session',NULL,NULL,1790000000072);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000003',1,'message',NULL,NULL,1790000000071);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000006',2,'message','00000000-0000-4000-8000-000000000003',NULL,1790000000086);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000008',3,'message','00000000-0000-4000-8000-000000000006',NULL,1790000000098);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000010',4,'message','00000000-0000-4000-8000-000000000008',NULL,1790000000110);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000012',5,'message','00000000-0000-4000-8000-000000000010','fixture-delivery-1',1790000000123);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','fixture-telegram-0001',0,'session',NULL,NULL,1790000000147);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000014',1,'message',NULL,NULL,1790000000146);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000017',2,'message','00000000-0000-4000-8000-000000000014',NULL,1790000000161);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000019',3,'message','00000000-0000-4000-8000-000000000017',NULL,1790000000173);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000021',4,'message','00000000-0000-4000-8000-000000000019',NULL,1790000000185);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',0,1,0,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',1,2,1,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',2,3,2,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',3,4,3,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',4,5,4,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',0,1,0,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',1,2,1,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',2,3,2,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',3,4,3,1);
INSERT INTO transcript_rewrite_watermarks VALUES('fixture/main:α','00000000000040008000000000000004',1790000000073);
INSERT INTO transcript_rewrite_watermarks VALUES('fixture-telegram-0001','00000000000040008000000000000015',1790000000148);
COMMIT;
