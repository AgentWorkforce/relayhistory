BEGIN TRANSACTION;
PRAGMA user_version=23;
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
INSERT INTO schema_meta VALUES('primary','agent',23,'main','2026.9.6',1790000000064,1790000000064);
INSERT INTO session_nodes VALUES('agent:main:main','fixture/main:α','{"sessionId":"fixture/main:α","updatedAt":1789999990000,"delivery":{"kind":"none"}}',NULL,1,1789999990000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_nodes VALUES('agent:main:telegram:direct:fixture-peer','fixture-telegram-0001','{"sessionId":"fixture-telegram-0001","updatedAt":1789999991000,"delivery":{"kind":"none"}}',NULL,1,1789999991000,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_windows VALUES('fixture/main:α','agent:main:main',NULL,NULL,'conversation',1789999990000,1790000000177,1790000000180,1789999990000,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO session_windows VALUES('fixture-telegram-0001','agent:main:telegram:direct:fixture-peer',NULL,NULL,'conversation',1789999991000,1790000000239,1790000000242,1789999991000,1,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',0,'{"type":"session","version":4,"id":"fixture/main:α","timestamp":"2026-09-21T14:13:20.112Z","cwd":"/fixture/project"}',1790000000112,NULL,117,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',1,'{"type":"message","id":"00000000-0000-4000-8000-000000000004","parentId":null,"timestamp":"2026-09-21T14:13:20.111Z","message":{"role":"user","content":[{"type":"text","text":"Find the OpenClaw fixture needle"}],"timestamp":1790000001000}}',1790000000111,NULL,239,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',2,'{"type":"message","id":"00000000-0000-4000-8000-000000000007","parentId":"00000000-0000-4000-8000-000000000004","timestamp":"2026-09-21T14:13:20.128Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"Read the fixture notes first."},{"type":"text","text":"I will read the notes."},{"type":"toolCall","id":"call_fixture|read:1","name":"read","arguments":{"path":"/fixture/project/notes.txt"}}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"toolUse","usage":{"input":100,"output":20,"cacheRead":5,"cacheWrite":0,"totalTokens":125,"cost":{"input":0.0005,"output":0.0005,"cacheRead":0,"cacheWrite":0,"total":0.001}},"responseId":"resp-fixture-1","timestamp":1790000002000}}',1790000000128,NULL,728,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',3,'{"type":"message","id":"00000000-0000-4000-8000-000000000009","parentId":"00000000-0000-4000-8000-000000000007","timestamp":"2026-09-21T14:13:20.140Z","message":{"role":"toolResult","toolCallId":"call_fixture|read:1","toolName":"read","content":[{"type":"text","text":"The OpenClaw fixture needle is here."}],"isError":false,"timestamp":1790000003000}}',1790000000140,NULL,352,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',4,'{"type":"message","id":"00000000-0000-4000-8000-000000000011","parentId":"00000000-0000-4000-8000-000000000009","timestamp":"2026-09-21T14:13:20.152Z","message":{"role":"assistant","content":[{"type":"text","text":"Found the OpenClaw fixture needle."}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":130,"output":10,"cacheRead":100,"cacheWrite":0,"totalTokens":240,"cost":{"input":0.001,"output":0.001,"cacheRead":0,"cacheWrite":0,"total":0.002}},"responseId":"resp-fixture-2","timestamp":1790000004000}}',1790000000152,NULL,563,NULL);
INSERT INTO transcript_events VALUES('fixture/main:α',5,'{"type":"message","id":"00000000-0000-4000-8000-000000000013","parentId":"00000000-0000-4000-8000-000000000011","timestamp":"2026-09-21T14:13:20.177Z","message":{"role":"assistant","content":[{"type":"text","text":"Found the OpenClaw fixture needle."}],"api":"openai-responses","provider":"openclaw","model":"delivery-mirror","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1790000000159,"idempotencyKey":"fixture-delivery-1","openclawDeliveryMirror":{"kind":"channel-final","sourceAssistantMessageId":"00000000-0000-4000-8000-000000000011"}}}',1790000000177,NULL,672,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',0,'{"type":"session","version":4,"id":"fixture-telegram-0001","timestamp":"2026-09-21T14:13:20.199Z","cwd":"/fixture/project"}',1790000000199,NULL,123,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',1,'{"type":"message","id":"00000000-0000-4000-8000-000000000015","parentId":null,"timestamp":"2026-09-21T14:13:20.198Z","message":{"role":"user","content":"Run the failing fixture command","timestamp":1790000101000}}',1790000000198,NULL,213,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',2,'{"type":"message","id":"00000000-0000-4000-8000-000000000018","parentId":"00000000-0000-4000-8000-000000000015","timestamp":"2026-09-21T14:13:20.215Z","message":{"role":"assistant","content":[{"type":"toolCall","id":"call_fixture_exec","name":"exec","arguments":{"command":"false"}}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"toolUse","usage":{"input":40,"output":8,"cacheRead":0,"cacheWrite":0,"totalTokens":48,"cost":{"input":0.00025,"output":0.00025,"cacheRead":0,"cacheWrite":0,"total":0.0005}},"timestamp":1790000102000}}',1790000000215,NULL,567,NULL);
INSERT INTO transcript_events VALUES('fixture-telegram-0001',3,NULL,1790000000227,x'28b52ffd64200a9d0600828d2921406bab03388b9a50db73a287ca4f386bb22c67c1ee24b22e402660bb331445d1ddcff31f71f5ff0c85d2066e1a2e100ac466a4f0585f730884ead888bc0485c466d94664db3aeae822a4f7e9be25715a154690a8a30d85cd3a6da1e11c4b9c6301c8eb9aa8056e59d669750bf423910f146b1dbe12a132a10cd44a8c2780f5e534a3b08daab435f2420dec53c818237f2dd2ff44faff74a71dcdc8cac06254c569651ad68f0f0030058f201c1c4f53d8fda532a0b8e36e0aaa994da2b9fad00818d126794559d52330964c42cb001e94b689dd',2848,'{"version":1,"report":{"kind":"canonical","hasParentId":true,"entry":{"id":"00000000-0000-4000-8000-000000000020","parentId":"00000000-0000-4000-8000-000000000018","timestamp":"2026-09-21T14:13:20.227Z","type":"message"}},"navigation":{"type":"message","id":"00000000-0000-4000-8000-000000000020","parentId":"00000000-0000-4000-8000-000000000018","timestamp":"2026-09-21T14:13:20.227Z","message":{"role":"toolResult"}},"reset":{"type":"message","id":"00000000-0000-4000-8000-000000000020","parentId":"00000000-0000-4000-8000-000000000018","timestamp":"2026-09-21T14:13:20.227Z","message":{"role":"toolResult"}},"model":{"type":"message","id":"00000000-0000-4000-8000-000000000020","parentId":"00000000-0000-4000-8000-000000000018","timestamp":"2026-09-21T14:13:20.227Z","message":{"role":"toolResult","toolCallId":"call_fixture_exec","toolName":"exec","isError":true,"timestamp":1790000103000,"content":[],"command":"","output":"","providerReplay":{"type":null},"details":{"openclawSyntheticMissingToolResult":false}}},"modelBytes":2848,"modelWithoutCheckpointBytes":2848,"withoutCustomDataBytes":2848}');
INSERT INTO transcript_events VALUES('fixture-telegram-0001',4,'{"type":"message","id":"00000000-0000-4000-8000-000000000022","parentId":"00000000-0000-4000-8000-000000000020","timestamp":"2026-09-21T14:13:20.239Z","message":{"role":"assistant","content":[{"type":"text","text":"The fixture command failed as expected."}],"api":"openai-responses","provider":"openai","model":"fixture-model","stopReason":"stop","usage":{"input":60,"output":9,"cacheRead":0,"cacheWrite":0,"totalTokens":69,"cost":{"input":0.0003,"output":0.0003,"cacheRead":0,"cacheWrite":0,"total":0.0006}},"timestamp":1790000104000}}',1790000000239,NULL,536,NULL);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','fixture/main:α',0,'session',NULL,NULL,1790000000112);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000004',1,'message',NULL,NULL,1790000000111);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000007',2,'message','00000000-0000-4000-8000-000000000004',NULL,1790000000128);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000009',3,'message','00000000-0000-4000-8000-000000000007',NULL,1790000000140);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000011',4,'message','00000000-0000-4000-8000-000000000009',NULL,1790000000152);
INSERT INTO transcript_event_identities VALUES('fixture/main:α','00000000-0000-4000-8000-000000000013',5,'message','00000000-0000-4000-8000-000000000011','fixture-delivery-1',1790000000177);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','fixture-telegram-0001',0,'session',NULL,NULL,1790000000199);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000015',1,'message',NULL,NULL,1790000000198);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000018',2,'message','00000000-0000-4000-8000-000000000015',NULL,1790000000215);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000020',3,'message','00000000-0000-4000-8000-000000000018',NULL,1790000000227);
INSERT INTO transcript_event_identities VALUES('fixture-telegram-0001','00000000-0000-4000-8000-000000000022',4,'message','00000000-0000-4000-8000-000000000020',NULL,1790000000239);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',0,1,0,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',1,2,1,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',2,3,2,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',3,4,3,1);
INSERT INTO session_transcript_active_events VALUES('fixture/main:α',4,5,4,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',0,1,0,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',1,2,1,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',2,3,2,1);
INSERT INTO session_transcript_active_events VALUES('fixture-telegram-0001',3,4,3,1);
INSERT INTO transcript_rewrite_watermarks VALUES('fixture/main:α','00000000000040008000000000000005',1790000000113);
INSERT INTO transcript_rewrite_watermarks VALUES('fixture-telegram-0001','00000000000040008000000000000016',1790000000200);
COMMIT;
