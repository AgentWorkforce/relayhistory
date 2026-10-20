BEGIN TRANSACTION;
PRAGMA user_version=24;
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
  snapshot_revision INTEGER NOT NULL DEFAULT 0,
  legacy_acp_migration_json TEXT,
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
  event_json TEXT,
  created_at INTEGER NOT NULL,
  event_zstd BLOB,
  event_utf8_bytes INTEGER CHECK (event_utf8_bytes IS NULL OR event_utf8_bytes >= 0),
  navigation_json TEXT,
  PRIMARY KEY (session_id, seq),
  FOREIGN KEY (session_id) REFERENCES "session_windows"(session_id) ON DELETE CASCADE,
  CHECK (
    (event_json IS NOT NULL AND event_zstd IS NULL)
    OR (
      event_json IS NULL AND event_zstd IS NOT NULL
      AND event_utf8_bytes IS NOT NULL
      AND event_utf8_bytes BETWEEN 1 AND 4194304
      AND length(event_zstd) BETWEEN 1 AND 4194304
      AND navigation_json IS NOT NULL
    )
  ),
  CHECK (
    navigation_json IS NULL OR CASE WHEN json_valid(navigation_json) THEN coalesce(
      octet_length(navigation_json) <= 16384
      AND json_type(navigation_json, '$.version') = 'integer'
      AND json_extract(navigation_json, '$.version') = 1
      AND json_type(navigation_json, '$.report') = 'object'
      AND json_extract(navigation_json, '$.report.kind') IN ('canonical', 'leaf', 'link', 'ignored')
      AND json_type(navigation_json, '$.navigation') = 'object'
      AND json_type(navigation_json, '$.reset') = 'object'
      AND json_type(navigation_json, '$.model') = 'object'
      AND json_type(navigation_json, '$.modelBytes') = 'integer'
      AND json_extract(navigation_json, '$.modelBytes') BETWEEN 0 AND 4194304
      AND json_type(navigation_json, '$.modelWithoutCheckpointBytes') = 'integer'
      AND json_extract(navigation_json, '$.modelWithoutCheckpointBytes') BETWEEN 0 AND 4194304
      AND json_type(navigation_json, '$.withoutCustomDataBytes') = 'integer'
      AND json_extract(navigation_json, '$.withoutCustomDataBytes') BETWEEN 0 AND 4194304,
      0) ELSE 0 END
  )
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
CREATE TABLE session_transcript_cold_archives (
  session_id TEXT NOT NULL PRIMARY KEY,
  generation TEXT NOT NULL,
  archive_name TEXT NOT NULL UNIQUE,
  archive_sha256 TEXT NOT NULL CHECK (length(archive_sha256) = 64),
  event_count INTEGER NOT NULL CHECK (event_count >= 1),
  raw_bytes INTEGER NOT NULL CHECK (raw_bytes >= 0),
  archive_bytes INTEGER NOT NULL CHECK (archive_bytes >= 0),
  last_seq INTEGER NOT NULL,
  archived_at INTEGER NOT NULL,
  storage TEXT NOT NULL CHECK (storage IN ('file', 'sqlite')),
  archive_blob BLOB,
  FOREIGN KEY (session_id) REFERENCES "session_windows"(session_id) ON DELETE CASCADE,
  CHECK (length(archive_name) > 0 AND archive_name NOT IN ('.', '..') AND archive_name NOT LIKE '%/%' AND archive_name NOT LIKE '%\%'),
  CHECK ((storage = 'file' AND archive_blob IS NULL) OR (storage = 'sqlite' AND archive_blob IS NOT NULL))
) STRICT;
INSERT INTO schema_meta VALUES('primary','agent',24,'main','2026.10.4',1790000000091,1790000000091);
INSERT INTO session_nodes VALUES('agent:main:main','fixture/main:α','{"sessionId":"fixture/main:α","updatedAt":1789999990000,"delivery":{"kind":"none"}}',0,NULL,1,1789999990000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_nodes VALUES('agent:main:telegram:direct:fixture-peer','fixture-telegram-0001','{"sessionId":"fixture-telegram-0001","updatedAt":1789999991000,"delivery":{"kind":"none"}}',0,NULL,1,1789999991000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_windows VALUES('fixture/main:α','agent:main:main',NULL,NULL,'conversation',1789999990000,1790000000252,1790000000255,1789999990000,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_windows VALUES('fixture-telegram-0001','agent:main:telegram:direct:fixture-peer',NULL,NULL,'conversation',1789999991000,1790000000314,1790000000317,1789999991000,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',0,'{"type":"session","version":4,"id":"fixture/main:α","timestamp":"2026-09-21T14:13:20.139Z","cwd":"/fixture/project"}',1790000000139,NULL,117,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',1,'{"type":"message","id":"00000000-0000-4000-8000-000000000008","parentId":null,"timestamp":"2026-09-21T14:13:20.138Z","message":{"role":"user","content":[{"type":"text","text":"Find the OpenClaw fixture needle"}],"timestamp":1790000001000}}',1790000000138,NULL,239,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',2,'{"type":"message","id":"00000000-0000-4000-8000-000000000011","parentId":"00000000-0000-4000-8000-000000000008","timestamp":"2026-09-21T14:13:20.155Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Read the fixture notes first."},{"type":"text","text":"I will read the notes."},{"type":"toolCall","id":"call_fixture|read:1","name":"read","arguments":{"path":"/fixture/project/notes.txt"}}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"toolUse","usage":{"input":100,"output":20,"cacheRead":5,"cacheWrite":0,"totalTokens":125,"cost":{"input":0.0005,"output":0.0005,"cacheRead":0,"cacheWrite":0,"total":0.001}},"responseId":"resp-fixture-1","timestamp":1790000002000}}',1790000000155,NULL,728,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',3,'{"type":"message","id":"00000000-0000-4000-8000-000000000013","parentId":"00000000-0000-4000-8000-000000000011","timestamp":"2026-09-21T14:13:20.167Z","message":{"role":"toolResult","toolCallId":"call_fixture|read:1","toolName":"read","content":[{"type":"text","text":"The OpenClaw fixture needle is here."}],"isError":false,"timestamp":1790000003000}}',1790000000167,NULL,352,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',4,'{"type":"message","id":"00000000-0000-4000-8000-000000000015","parentId":"00000000-0000-4000-8000-000000000013","timestamp":"2026-09-21T14:13:20.179Z","message":{"role":"assistant","content":[{"type":"text","text":"Found the OpenClaw fixture needle."}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":130,"output":10,"cacheRead":100,"cacheWrite":0,"totalTokens":240,"cost":{"input":0.001,"output":0.001,"cacheRead":0,"cacheWrite":0,"total":0.002}},"responseId":"resp-fixture-2","timestamp":1790000004000}}',1790000000179,NULL,563,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',5,'{"type":"message","id":"00000000-0000-4000-8000-000000000017","parentId":"00000000-0000-4000-8000-000000000015","timestamp":"2026-09-21T14:13:20.204Z","message":{"role":"assistant","content":[{"type":"text","text":"Found the OpenClaw fixture needle."}],"api":"openai-responses","provider":"openclaw","model":"delivery-mirror","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1790000000186,"idempotencyKey":"fixture-delivery-1","openclawDeliveryMirror":{"kind":"channel-final","sourceAssistantMessageId":"00000000-0000-4000-8000-000000000015"}}}',1790000000204,NULL,672,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',6,'{"type":"message","id":"00000000-0000-4000-8000-000000000019","parentId":"00000000-0000-4000-8000-000000000017","timestamp":"2026-09-21T14:13:20.216Z","message":{"role":"user","content":"[OpenClaw heartbeat poll]","timestamp":1790000006000,"provenance":{"kind":"internal_system","sourceTool":"heartbeat"}}}',1790000000216,NULL,306,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',7,'{"type":"message","id":"00000000-0000-4000-8000-000000000021","parentId":"00000000-0000-4000-8000-000000000019","timestamp":"2026-09-21T14:13:20.228Z","message":{"role":"assistant","content":[{"type":"text","text":"NO_REPLY"}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":20,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":21,"cost":{"input":0.00005,"output":0.00005,"cacheRead":0,"cacheWrite":0,"total":0.0001}},"timestamp":1790000006500}}',1790000000228,NULL,507,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',8,'{"type":"message","id":"00000000-0000-4000-8000-000000000023","parentId":"00000000-0000-4000-8000-000000000021","timestamp":"2026-09-21T14:13:20.240Z","message":{"role":"user","content":"Read HEARTBEAT.md if it exists (workspace context). Follow it strictly. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply HEARTBEAT_OK.\nCurrent time: fixture","timestamp":1790000006000}}',1790000000240,NULL,409,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',9,'{"type":"message","id":"00000000-0000-4000-8000-000000000025","parentId":"00000000-0000-4000-8000-000000000023","timestamp":"2026-09-21T14:13:20.252Z","message":{"role":"assistant","content":[{"type":"text","text":"HEARTBEAT_OK"}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":20,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":21,"cost":{"input":0.00005,"output":0.00005,"cacheRead":0,"cacheWrite":0,"total":0.0001}},"timestamp":1790000006500}}',1790000000252,NULL,511,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',0,'{"type":"session","version":4,"id":"fixture-telegram-0001","timestamp":"2026-09-21T14:13:20.274Z","cwd":"/fixture/project"}',1790000000274,NULL,123,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',1,'{"type":"message","id":"00000000-0000-4000-8000-000000000027","parentId":null,"timestamp":"2026-09-21T14:13:20.273Z","message":{"role":"user","content":"Run the failing fixture command","timestamp":1790000101000}}',1790000000273,NULL,213,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',2,'{"type":"message","id":"00000000-0000-4000-8000-000000000030","parentId":"00000000-0000-4000-8000-000000000027","timestamp":"2026-09-21T14:13:20.290Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"call_fixture_exec","name":"exec","arguments":{"command":"false"}}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"toolUse","usage":{"input":40,"output":8,"cacheRead":0,"cacheWrite":0,"totalTokens":48,"cost":{"input":0.00025,"output":0.00025,"cacheRead":0,"cacheWrite":0,"total":0.0005}},"timestamp":1790000102000}}',1790000000290,NULL,567,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',3,NULL,1790000000302,x'28b52ffd64200a950600724d2921406bab03388b9a50db73a287ca4f386bb22c67c1ee94ae0d23d9d8ee0c45517437cff31fb9fa7f4580d2066e1a2e1012c466c4f0585f730784cad878bc0575c466d9c663db3aeae82aa4f7e9be25715a156690a8a30b85cd3aad61e11c4b9c6303c8eb9aa8076e59d669750b74239113d611c457222a130a41ada47802585f4e330adba84a5b23bf609f42c618792102e97f22fd7fbad38c6664756031aae2b4320feb460f0030058f201c1c4f53d8fda532a0b8e36e0aaa994da2b9fad00818d124794551552180974c42cb001ec47cdddc',2848,'{"version":1,"report":{"kind":"canonical","hasParentId":true,"entry":{"id":"00000000-0000-4000-8000-000000000032","parentId":"00000000-0000-4000-8000-000000000030","timestamp":"2026-09-21T14:13:20.302Z","type":"message"}},"navigation":{"type":"message","id":"00000000-0000-4000-8000-000000000032","parentId":"00000000-0000-4000-8000-000000000030","timestamp":"2026-09-21T14:13:20.302Z","message":{"role":"toolResult"}},"reset":{"type":"message","id":"00000000-0000-4000-8000-000000000032","parentId":"00000000-0000-4000-8000-000000000030","timestamp":"2026-09-21T14:13:20.302Z","message":{"role":"toolResult"}},"model":{"type":"message","id":"00000000-0000-4000-8000-000000000032","parentId":"00000000-0000-4000-8000-000000000030","timestamp":"2026-09-21T14:13:20.302Z","message":{"role":"toolResult","toolCallId":"call_fixture_exec","toolName":"exec","isError":true,"timestamp":1790000103000,"content":[],"command":"","output":"","providerReplay":{"type":null},"details":{"openclawSyntheticMissingToolResult":false}}},"modelBytes":2848,"modelWithoutCheckpointBytes":2848,"withoutCustomDataBytes":2848}');
INSERT INTO transcript_events VALUES('fixture-telegram-0001',4,'{"type":"message","id":"00000000-0000-4000-8000-000000000034","parentId":"00000000-0000-4000-8000-000000000032","timestamp":"2026-09-21T14:13:20.314Z","message":{"role":"assistant","content":[{"type":"text","text":"The fixture command failed as expected."}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":60,"output":9,"cacheRead":0,"cacheWrite":0,"totalTokens":69,"cost":{"input":0.0003,"output":0.0003,"cacheRead":0,"cacheWrite":0,"total":0.0006}},"timestamp":1790000104000}}',1790000000314,NULL,536,NULL);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','fixture/main:α',0,'session',NULL,NULL,1790000000139);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000008',1,'message',NULL,NULL,1790000000138);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000011',2,'message','00000000-0000-4000-8000-000000000008',NULL,1790000000155);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000013',3,'message','00000000-0000-4000-8000-000000000011',NULL,1790000000167);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000015',4,'message','00000000-0000-4000-8000-000000000013',NULL,1790000000179);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000017',5,'message','00000000-0000-4000-8000-000000000015','fixture-delivery-1',1790000000204);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000019',6,'message','00000000-0000-4000-8000-000000000017',NULL,1790000000216);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000021',7,'message','00000000-0000-4000-8000-000000000019',NULL,1790000000228);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000023',8,'message','00000000-0000-4000-8000-000000000021',NULL,1790000000240);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000025',9,'message','00000000-0000-4000-8000-000000000023',NULL,1790000000252);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','fixture-telegram-0001',0,'session',NULL,NULL,1790000000274);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000027',1,'message',NULL,NULL,1790000000273);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000030',2,'message','00000000-0000-4000-8000-000000000027',NULL,1790000000290);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000032',3,'message','00000000-0000-4000-8000-000000000030',NULL,1790000000302);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000034',4,'message','00000000-0000-4000-8000-000000000032',NULL,1790000000314);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',0,1,0,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',1,2,1,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',2,3,2,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',3,4,3,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',4,5,4,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',5,6,5,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',6,7,6,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',7,8,7,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',8,9,8,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',0,1,0,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',1,2,1,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',2,3,2,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',3,4,3,1);
INSERT INTO transcript_rewrite_watermarks VALUES('fixture/main:α','00000000000040008000000000000009',1790000000140);
INSERT INTO transcript_rewrite_watermarks VALUES('fixture-telegram-0001','00000000000040008000000000000028',1790000000275);
COMMIT;
