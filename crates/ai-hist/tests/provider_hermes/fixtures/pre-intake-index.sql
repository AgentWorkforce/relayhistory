PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE canonical_evidence_protection (
 source TEXT NOT NULL, session_id TEXT NOT NULL, record_identity TEXT NOT NULL,
 PRIMARY KEY(source,session_id,record_identity)
);
CREATE TABLE change_feed_epochs (
             epoch INTEGER PRIMARY KEY,
             successor INTEGER NOT NULL,
             through_revision INTEGER NOT NULL,
             retired_kinds TEXT NOT NULL
         );
CREATE TABLE change_feed_store (
             singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
             epoch INTEGER NOT NULL,
             export_schema_digest TEXT
         );
INSERT INTO "change_feed_store" VALUES(1,4063090072101822563,'{"commit_link":"17c0123d3672e439eb7be95f8195f01fb6138b626d4e6e0821662bb7899c7f82","file_edit":"1c0842d91b10d75045aedffa202f879f8c6d1d933123e37fc808b5d398af40bf","history":"e61e46ace8b76f6da357e98f0eac5dd86007e5ea057d4970eb79afcac733c83f","observation_evidence":"a10014b766030574e50dcd525f5d08fb0f44933fdae9e6dba0e308b476ada96c","presence":"4ab915d073a43493bc134d3f4bd7e1ddd3ab8a4be81b6ddc4749dc6ade7f0e31","relationship":"c18eb25115b3bb3d9a23925e3ae7f47f0819d44e709707e11119bcfc2fe8c067","session":"a8dfea380d2a3f809ceca09ee3660762a4dbffac7d433e090cfb30588057e5c5","session_event":"11058d3eaee90fcaee76ed7786799e4adc26cd754d5246c6c5430ca40c8a6988","session_marker":"5e8c92cd16746762b17db8b39b1f26117c26c53fd32fad4ed6dcb4178b28b6a5","source_observation":"97c24ee40b5858f7d07dfb5a0ae0eb2269b9e5df7bc4f4bc9a163c8314ac42c0","tool_call":"e662e0ef3d073845b19e7e261f5afc9976959e359ed29654e20242a9cd19e6d5"}');
CREATE TABLE consumer_cursors (
             name TEXT PRIMARY KEY,
             revision INTEGER NOT NULL,
             updated_ms INTEGER NOT NULL,
             kinds TEXT NOT NULL DEFAULT '*'
         );
CREATE TABLE discovery_skips (
    source TEXT NOT NULL,
    locator TEXT NOT NULL,
    stamp TEXT NOT NULL,
    reason TEXT,
    updated_ms INTEGER,
    PRIMARY KEY (source, locator)
);
CREATE TABLE evidence_tombstones (
             kind TEXT NOT NULL,
             source TEXT NOT NULL,
             session_id TEXT NOT NULL,
             record_key TEXT NOT NULL,
             revision INTEGER NOT NULL,
             PRIMARY KEY (kind, source, session_id, record_key)
         );
CREATE TABLE file_edits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id TEXT,
    tool_use_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    lines_added INTEGER,
    lines_removed INTEGER,
    structured_patch_json TEXT,
    user_modified INTEGER,
    ts_ms INTEGER,
    git_branch TEXT,
    cwd TEXT,
    -- Which evidence backs this row: 'local' (a local parser read it from
    -- the provider's own files), 'remote' (a remote observation supplied
    -- it) or 'both'. See `EVIDENCE_LOCATION_TABLES`.
    location TEXT NOT NULL DEFAULT 'local' CHECK(location IN ('local', 'remote', 'both')), revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, session_id, tool_use_id)
);
CREATE TABLE grok_session_turns (
    session_id TEXT PRIMARY KEY,
    turns INTEGER NOT NULL
);
CREATE TABLE grok_unified_usage (
    row_key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    ts_ms INTEGER,
    pid INTEGER,
    model TEXT,
    event_id TEXT,
    usage_json TEXT NOT NULL,
    locator TEXT NOT NULL,
    line_offset INTEGER NOT NULL
);
CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    prompt_hash TEXT,
    timestamp_ms INTEGER NOT NULL, git_branch TEXT, revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, timestamp_ms, prompt)
);
INSERT INTO "history" VALUES(1,'hermes','default:636861742f6e6f742d612d757569643aceb1','/fixture/project','Find the Hermes fixture needle',NULL,1780000001000,NULL,13);
INSERT INTO "history" VALUES(2,'hermes','default:636861742f6e6f742d612d757569643aceb1','/fixture/project','Continue after restart.',NULL,1780000005000,NULL,25);
PRAGMA writable_schema=ON;
INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql)VALUES('table','history_fts','history_fts',0,'CREATE VIRTUAL TABLE history_fts USING fts5(
    prompt, project, content=''history'', content_rowid=''id''
)');
CREATE TABLE 'history_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO "history_fts_config" VALUES('version',4);
CREATE TABLE 'history_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
INSERT INTO "history_fts_data" VALUES(1,X'020804');
INSERT INTO "history_fts_data" VALUES(10,X'000000000102020002010101020101');
INSERT INTO "history_fts_data" VALUES(137438953473,X'00000046053066696E640102020305787475726501080501010201066865726D657301020401066E6565646C65010206010770726F6A6563740106010103010374686501020304090D0B0B0E');
INSERT INTO "history_fts_data" VALUES(274877906945,X'00000043063061667465720202030108636F6E74696E75650202020107666978747572650206010102010770726F6A6563740206010103010772657374617274020204040A0D0E0E');
CREATE TABLE 'history_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
INSERT INTO "history_fts_docsize" VALUES(1,X'0502');
INSERT INTO "history_fts_docsize" VALUES(2,X'0302');
CREATE TABLE 'history_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
INSERT INTO "history_fts_idx" VALUES(1,X'',2);
INSERT INTO "history_fts_idx" VALUES(2,X'',2);
CREATE TABLE observation_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL);
INSERT INTO "observation_clock" VALUES(1,36);
CREATE TABLE observation_discovery_skips (
 source TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,locator TEXT NOT NULL,stamp TEXT NOT NULL,updated_ms INTEGER NOT NULL,
 PRIMARY KEY(source,location,connector_id,connector_instance,locator)
);
CREATE TABLE observation_evidence (
 source TEXT NOT NULL,session_id TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,
 evidence_uid TEXT NOT NULL, payload_json TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(source,session_id,location,connector_id,connector_instance,evidence_uid),
 FOREIGN KEY(source,session_id,location,connector_id,connector_instance) REFERENCES session_observations(source,session_id,location,connector_id,connector_instance) ON DELETE CASCADE
);
CREATE TABLE observation_hydration_checkpoints (
 source TEXT NOT NULL,session_id TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,
 source_stamp TEXT,parser_version INTEGER NOT NULL,last_event_at_ms INTEGER,source_bytes INTEGER NOT NULL,records_parsed INTEGER NOT NULL,include_related INTEGER NOT NULL,updated_ms INTEGER NOT NULL,
 committed_offset INTEGER NOT NULL DEFAULT 0,prefix_hash TEXT,dev_ino TEXT,parser_state_json TEXT,
 PRIMARY KEY(source,session_id,location,connector_id,connector_instance),
 FOREIGN KEY(source,session_id,location,connector_id,connector_instance) REFERENCES session_observations(source,session_id,location,connector_id,connector_instance) ON DELETE CASCADE
);
INSERT INTO "observation_hydration_checkpoints" VALUES('hermes','default:636861742f6e6f742d612d757569643aceb1','local','hermes','default','hermes_schema31_v4:sha256:d5298a1b51aed36e34d9d6a942775e5cc8e99f3c3eba1c5295687d7d27ee9f48',20,1780000006500,0,7,1,1792605811455,0,NULL,NULL,'{"v":5}');
CREATE TABLE observation_versions(source TEXT NOT NULL,session_id TEXT NOT NULL,location TEXT NOT NULL,connector_id TEXT NOT NULL,connector_instance TEXT NOT NULL,version INTEGER NOT NULL,PRIMARY KEY(source,session_id,location,connector_id,connector_instance));
INSERT INTO "observation_versions" VALUES('hermes','default:636f6d707265737365642d6368696c64','local','hermes','default',5);
INSERT INTO "observation_versions" VALUES('hermes','default:636861742f6e6f742d612d757569643aceb1','local','hermes','default',36);
CREATE TABLE project_identity_refreshed (epoch INTEGER PRIMARY KEY, revision INTEGER NOT NULL);
INSERT INTO "project_identity_refreshed" VALUES(4063090072101822563,10);
CREATE TABLE schema_migrations (name TEXT PRIMARY KEY);
INSERT INTO "schema_migrations" VALUES('export_capture_retired_v1');
INSERT INTO "schema_migrations" VALUES('session_delete_continuity_reopen_v1');
INSERT INTO "schema_migrations" VALUES('session_delete_edge_seeks_v1');
INSERT INTO "schema_migrations" VALUES('session_events_fts_update_of_v1');
INSERT INTO "schema_migrations" VALUES('history_fts_update_of_v1');
INSERT INTO "schema_migrations" VALUES('fts_update_changed_only_v1');
INSERT INTO "schema_migrations" VALUES('session_events_fts_write_unit_v1');
INSERT INTO "schema_migrations" VALUES('session_relationships_v2');
INSERT INTO "schema_migrations" VALUES('session_events_tool_result_fidelity_v1');
INSERT INTO "schema_migrations" VALUES('evidence_location_v1');
INSERT INTO "schema_migrations" VALUES('session_markers_combined_v1');
INSERT INTO "schema_migrations" VALUES('session_events_raw_facts_v1');
INSERT INTO "schema_migrations" VALUES('session_project_key_v1');
INSERT INTO "schema_migrations" VALUES('session_presences_local_backfill_v1');
INSERT INTO "schema_migrations" VALUES('session_markers_v1');
INSERT INTO "schema_migrations" VALUES('session_markers_v2');
INSERT INTO "schema_migrations" VALUES('claude_request_evidence_v2');
INSERT INTO "schema_migrations" VALUES('claude_standalone_records_v1');
INSERT INTO "schema_migrations" VALUES('connector_observations_v1');
INSERT INTO "schema_migrations" VALUES('change_feed_v2');
CREATE TABLE session_commit_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    repo TEXT NOT NULL,
    branch TEXT,
    commit_sha TEXT NOT NULL,
    note_ref TEXT,
    match_method TEXT NOT NULL,
    confidence REAL NOT NULL,
    files_json TEXT,
    numstat_json TEXT,
    evidence_json TEXT,
    created_at_ms INTEGER NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, session_id, commit_sha, match_method)
);
CREATE TABLE session_continuity_evidence (
    source TEXT NOT NULL,
    locator TEXT NOT NULL,
    session_id TEXT NOT NULL,
    file_session_id TEXT,
    first_parent_uuid TEXT,
    first_ts_ms INTEGER,
    in_log_session_ids_json TEXT NOT NULL DEFAULT '[]',
    has_resume_marker INTEGER NOT NULL DEFAULT 0,
    resume_target TEXT,
    explicit_targets_json TEXT NOT NULL DEFAULT '{}',
    source_version TEXT,
    origin_session_id TEXT,
    -- Why this evidence has not produced its edges yet; NULL once resolved.
    pending_reason TEXT,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (source, locator)
);
CREATE TABLE session_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project TEXT,
    project_key TEXT,
    project_key_method TEXT,
    cwd TEXT,
    git_branch TEXT,
    message_id TEXT,
    parent_id TEXT,
    ts_ms INTEGER NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'tool_result')),
    kind TEXT NOT NULL CHECK(kind IN ('text', 'thinking', 'tool_use', 'tool_result')),
    text TEXT,
    model TEXT,
    token_json TEXT,
    -- The upstream inference provider, when the harness records it as its own
    -- field. OpenCode writes `providerID`; Claude and Codex do not name one,
    -- so theirs stays null rather than being guessed from the model string.
    provider TEXT,
    event_uid TEXT NOT NULL,
    tool_use_id TEXT,
    payload_bytes INTEGER,
    payload_truncated INTEGER,
    payload_hash TEXT,
    call_index INTEGER,
    event_index INTEGER,
    result_status TEXT,
    event_source TEXT,
    error_signal TEXT,
    subagent_session_id TEXT,
    agent_id TEXT,
    request_id TEXT,
    provider_message_id TEXT,
    -- Why the turn ended, as the harness itself reported it.
    stop_reason TEXT,
    agent_version TEXT,
    is_sidechain INTEGER,
    is_meta INTEGER,
    turn_id TEXT,
    request_span TEXT,
    raw_facts_version INTEGER,
    raw_kind TEXT,
    -- Why a user-role row is not a human prompt: a slash-command triad row,
    -- a task notification, hook output, a system reminder, Codex context.
    -- Null for a genuine prompt and for every model-output row. See
    -- `ingest::control`.
    control_kind TEXT,
    -- Claude: the usage blob exactly as this one record carried it, kept
    -- only where `token_json` holds a different blob -- the one a streamed
    -- request's copies settle into -- and JSON `null` for a copy that carried
    -- none. Null wherever `token_json` already is the record's own.
    record_token_json TEXT,
    -- Which evidence backs this row: 'local' (a local parser read it from
    -- the provider's own files), 'remote' (a remote observation supplied
    -- it) or 'both'. See `EVIDENCE_LOCATION_TABLES`.
    location TEXT NOT NULL DEFAULT 'local' CHECK(location IN ('local', 'remote', 'both')), revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, session_id, event_uid)
);
INSERT INTO "session_events" VALUES(1,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'1',NULL,1780000001000,'user','text','Find the Hermes fixture needle',NULL,NULL,NULL,'1:text',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',14);
INSERT INTO "session_events" VALUES(2,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'2',NULL,1780000002000,'assistant','text','I will inspect the file.',NULL,NULL,NULL,'2:text',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',16);
INSERT INTO "session_events" VALUES(3,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path',NULL,NULL,'2',NULL,1780000002000,'assistant','tool_use','{"function":{"arguments":"{\"path\":\"/fixture/project/notes.txt\"}","name":"read_file"},"id":"call/native:1","type":"function"}',NULL,NULL,NULL,'2:tool:0','call/native:1',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'tool_calls',NULL,NULL,'local',18);
INSERT INTO "session_events" VALUES(4,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'3',NULL,1780000003000,'tool_result','tool_result','The Hermes fixture needle is here.',NULL,NULL,NULL,'3:tool_result','call/native:1',34,0,'7f822c27df4d4837',0,0,'unknown','function_call_output',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',20);
INSERT INTO "session_events" VALUES(5,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'4',NULL,1780000004000,'assistant','text','Found the Hermes fixture needle.',NULL,NULL,NULL,'4:text',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'stop',NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',22);
INSERT INTO "session_events" VALUES(6,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'4',NULL,1780000004000,'assistant','thinking','Inspect the recorded text.',NULL,NULL,NULL,'4:thinking',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'stop',NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',23);
INSERT INTO "session_events" VALUES(7,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'5',NULL,1780000005000,'user','text','Continue after restart.',NULL,NULL,NULL,'5:text',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',26);
INSERT INTO "session_events" VALUES(8,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'6',NULL,1780000006000,'assistant','text','The historical answer is still available.',NULL,NULL,NULL,'6:text',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'messages',NULL,NULL,'local',28);
INSERT INTO "session_events" VALUES(9,'hermes','default:636861742f6e6f742d612d757569643aceb1',NULL,'/fixture/project','path','/fixture/project',NULL,'7',NULL,1780000006500,'user','text','Archived conversation summary.',NULL,NULL,NULL,'7:text',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5,'messages','synthetic',NULL,'local',30);
INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql)VALUES('table','session_events_fts','session_events_fts',0,'CREATE VIRTUAL TABLE session_events_fts USING fts5(
    text, role, project, content=''session_events'', content_rowid=''id''
)');
CREATE TABLE 'session_events_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO "session_events_fts_config" VALUES('version',4);
CREATE TABLE 'session_events_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
INSERT INTO "session_events_fts_data" VALUES(1,X'09350A00');
INSERT INTO "session_events_fts_data" VALUES(10,X'000000000109090009010101020101030101040101050101060101070101080101090101');
INSERT INTO "session_events_fts_data" VALUES(137438953473,X'00000040053066696E640102020305787475726501020501066865726D657301020401066E6565646C650102060103746865010203010475736572010601010204090A0B0B08');
INSERT INTO "session_events_fts_data" VALUES(274877906945,X'0000003F0A30617373697374616E740206010102010466696C6502020601016902020202066E73706563740202040103746865020205010477696C6C020203041009060B08');
INSERT INTO "session_events_fts_data" VALUES(412316860417,X'0000009F02303103020F0109617267756D656E747303020302087373697374616E740306010102010463616C6C03020D010466696C6503020B030578747572650302050207756E6374696F6E030402110102696403020C01046E616D6503020903047469766503020E02046F7465730302070104706174680302040206726F6A65637403020601047265616403020A0103747874030208020379706503021004060E0F09090A0D07090909090B0908');
INSERT INTO "session_events_fts_data" VALUES(549755813889,X'0000005208306669787475726504020401046865726504020704036D65730402030102697304020601066E6565646C650402050106726573756C740406010103010374686504020202036F6F6C0406010102040C0908070B0D08');
INSERT INTO "session_events_fts_data" VALUES(687194767361,X'000000470A30617373697374616E74050601010201076669787475726505020502046F756E6405020201066865726D657305020401066E6565646C65050206010374686505020304100C090B0B');
INSERT INTO "session_events_fts_data" VALUES(824633720833,X'0000003D0A30617373697374616E7406060101020107696E737065637406020201087265636F726465640602040104746578740602050202686506020304100C0D09');
INSERT INTO "session_events_fts_data" VALUES(962072674305,X'00000032063061667465720702030108636F6E74696E75650702020107726573746172740702040104757365720706010102040A0D0C');
INSERT INTO "session_events_fts_data" VALUES(1099511627777,X'000000530730616E7377657208020402087373697374616E74080601010202087661696C61626C65080207010A686973746F726963616C0802030102697308020501057374696C6C0802060103746865080202040B0F0D0F070A');
INSERT INTO "session_events_fts_data" VALUES(1236950581249,X'0000003909306172636869766564090202010C636F6E766572736174696F6E090203010773756D6D6172790902040104757365720906010102040D110C');
CREATE TABLE session_events_fts_defer (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
);
CREATE TABLE 'session_events_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
INSERT INTO "session_events_fts_docsize" VALUES(1,X'050100');
INSERT INTO "session_events_fts_docsize" VALUES(2,X'050100');
INSERT INTO "session_events_fts_docsize" VALUES(3,X'100100');
INSERT INTO "session_events_fts_docsize" VALUES(4,X'060200');
INSERT INTO "session_events_fts_docsize" VALUES(5,X'050100');
INSERT INTO "session_events_fts_docsize" VALUES(6,X'040100');
INSERT INTO "session_events_fts_docsize" VALUES(7,X'030100');
INSERT INTO "session_events_fts_docsize" VALUES(8,X'060100');
INSERT INTO "session_events_fts_docsize" VALUES(9,X'030100');
CREATE TABLE 'session_events_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
INSERT INTO "session_events_fts_idx" VALUES(1,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(2,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(3,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(4,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(5,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(6,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(7,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(8,X'',2);
INSERT INTO "session_events_fts_idx" VALUES(9,X'',2);
CREATE TABLE session_hydration_checkpoints (
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    location TEXT NOT NULL CHECK(location IN ('local', 'remote')),
    source_stamp TEXT,
    parser_version INTEGER NOT NULL,
    last_event_at_ms INTEGER,
    source_bytes INTEGER NOT NULL DEFAULT 0,
    records_parsed INTEGER NOT NULL DEFAULT 0,
    include_related INTEGER NOT NULL DEFAULT 1,
    last_tool_result_index INTEGER,
    updated_ms INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL DEFAULT 0,
    prefix_hash TEXT,
    dev_ino TEXT,
    parser_state_json TEXT,
    source_diagnostics_json TEXT,
    PRIMARY KEY (source, session_id, location)
);
INSERT INTO "session_hydration_checkpoints" VALUES('hermes','default:636861742f6e6f742d612d757569643aceb1','local','hermes_schema31_v4:sha256:d5298a1b51aed36e34d9d6a942775e5cc8e99f3c3eba1c5295687d7d27ee9f48',20,1780000006500,0,7,1,0,1792605811454,0,NULL,NULL,'{"v":5}','[]');
CREATE TABLE session_identity_correlations (
    source TEXT NOT NULL,
    local_session_id TEXT NOT NULL,
    remote_session_id TEXT NOT NULL,
    relationship TEXT NOT NULL,
    evidence_kind TEXT NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (source, local_session_id, relationship)
);
CREATE TABLE session_markers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    marker_uid TEXT NOT NULL,
    ts_ms INTEGER,
    message_id TEXT,
    parent_id TEXT,
    turn_id TEXT,
    kind TEXT NOT NULL,
    subkind TEXT,
    text TEXT,
    payload_json TEXT,
    -- Which evidence backs this row: 'local' (a local parser read it from
    -- the provider's own files), 'remote' (a remote observation supplied
    -- it) or 'both'. See `EVIDENCE_LOCATION_TABLES`.
    location TEXT NOT NULL DEFAULT 'local' CHECK(location IN ('local', 'remote', 'both')), revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, session_id, marker_uid)
);
INSERT INTO "session_markers" VALUES(1,'hermes','default:636861742f6e6f742d612d757569643aceb1','session',1780000000000,NULL,NULL,NULL,'session_metadata','hermes',NULL,'{"end_reason":"compression","model":"fixture-model","native_session_id":"chat/not-a-uuid:α","parent_session_id":null,"profile":"default","schema_version":31,"title":null}','local',11);
INSERT INTO "session_markers" VALUES(2,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:1',1780000001000,'1',NULL,NULL,'message_metadata','hermes',NULL,'{"active":false,"compacted":true,"compressed_summary":false,"display_kind":null,"observed":false,"tool_name":null}','local',12);
INSERT INTO "session_markers" VALUES(3,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:2',1780000002000,'2',NULL,NULL,'message_metadata','hermes',NULL,'{"active":false,"compacted":true,"compressed_summary":false,"display_kind":null,"observed":false,"tool_name":null}','local',15);
INSERT INTO "session_markers" VALUES(4,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:3',1780000003000,'3',NULL,NULL,'message_metadata','hermes',NULL,'{"active":false,"compacted":true,"compressed_summary":false,"display_kind":null,"observed":false,"tool_name":"read_file"}','local',19);
INSERT INTO "session_markers" VALUES(5,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:4',1780000004000,'4',NULL,NULL,'message_metadata','hermes',NULL,'{"active":false,"compacted":true,"compressed_summary":false,"display_kind":null,"observed":false,"tool_name":null}','local',21);
INSERT INTO "session_markers" VALUES(6,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:5',1780000005000,'5',NULL,NULL,'message_metadata','hermes',NULL,'{"active":false,"compacted":true,"compressed_summary":false,"display_kind":null,"observed":false,"tool_name":null}','local',24);
INSERT INTO "session_markers" VALUES(7,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:6',1780000006000,'6',NULL,NULL,'message_metadata','hermes',NULL,'{"active":false,"compacted":true,"compressed_summary":false,"display_kind":null,"observed":false,"tool_name":null}','local',27);
INSERT INTO "session_markers" VALUES(8,'hermes','default:636861742f6e6f742d612d757569643aceb1','message:7',1780000006500,'7',NULL,NULL,'message_metadata','hermes',NULL,'{"active":true,"compacted":false,"compressed_summary":true,"display_kind":null,"observed":false,"tool_name":null}','local',29);
CREATE TABLE session_observations (
 source TEXT NOT NULL, session_id TEXT NOT NULL,
 location TEXT NOT NULL CHECK(location IN ('local','remote')),
 connector_id TEXT NOT NULL CHECK(length(trim(connector_id)) > 0),
 connector_instance TEXT NOT NULL CHECK(length(trim(connector_instance)) > 0),
 raw_locator TEXT, source_stamp TEXT, discovery_state TEXT NOT NULL DEFAULT 'shallow' CHECK(discovery_state IN ('shallow','full')),
 access_state TEXT NOT NULL DEFAULT 'available' CHECK(access_state IN ('available','unavailable','withdrawn')),
 updated_ms INTEGER NOT NULL,
 first_prompt TEXT, last_assistant_text TEXT, revision INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(source,session_id,location,connector_id,connector_instance)
);
INSERT INTO "session_observations" VALUES('hermes','default:636f6d707265737365642d6368696c64','local','hermes','default','/synthetic/hermes-pre-intake-home/.hermes/state.db','v15:1792605811385487126:49152','shallow','available',1792605811435,'A fresh human question.',NULL,4);
INSERT INTO "session_observations" VALUES('hermes','default:636861742f6e6f742d612d757569643aceb1','local','hermes','default','/synthetic/hermes-pre-intake-home/.hermes/state.db','v15:1792605811385487126:49152','full','available',1792605811454,'Find the Hermes fixture needle',NULL,34);
CREATE TABLE session_presences (
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    location TEXT NOT NULL CHECK(location IN ('local', 'remote')),
    raw_locator TEXT,
    source_stamp TEXT,
    discovery_state TEXT, revision INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, session_id, location)
);
INSERT INTO "session_presences" VALUES('hermes','default:636f6d707265737365642d6368696c64','local','/synthetic/hermes-pre-intake-home/.hermes/state.db','v15:1792605811385487126:49152','shallow',2);
INSERT INTO "session_presences" VALUES('hermes','default:636861742f6e6f742d612d757569643aceb1','local','/synthetic/hermes-pre-intake-home/.hermes/state.db','v15:1792605811385487126:49152','full',32);
CREATE TABLE session_relationships (
    source TEXT NOT NULL,
    parent_session_id TEXT NOT NULL,
    relationship_uid TEXT NOT NULL,
    child_session_id TEXT,
    relationship TEXT NOT NULL,
    identity_status TEXT NOT NULL CHECK(identity_status IN ('observed','unlinked')),
    child_agent_type TEXT,
    child_agent_name TEXT,
    child_model TEXT,
    spawn_depth INTEGER,
    evidence_kind TEXT NOT NULL,
    evidence_locator TEXT,
    evidence_ref TEXT,
    child_has_events INTEGER NOT NULL DEFAULT 0,
    spawned_at_ms INTEGER,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    origin_session_id TEXT, revision INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, parent_session_id, relationship_uid)
);
CREATE TABLE session_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    created_ms INTEGER NOT NULL,
    UNIQUE(source, session_id, tag_id)
);
CREATE TABLE sessions (
    session_id TEXT NOT NULL,
    source TEXT NOT NULL,
    cwd TEXT,
    git_branch TEXT,
    first_activity_ms INTEGER,
    last_activity_ms INTEGER,
    last_assistant_text TEXT,
    raw_path TEXT,
    parser_version INTEGER NOT NULL DEFAULT 1,
    first_prompt TEXT,
    models_json TEXT,
    originator TEXT,
    agent_version TEXT,
    repo_url TEXT,
    initial_commit TEXT,
    workspace_roots_json TEXT,
    source_stamp TEXT,
    discovery_state TEXT,
    project_key TEXT,
    project_key_method TEXT, title TEXT, session_kind TEXT, revision INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, source)
);
INSERT INTO "sessions" VALUES('default:636f6d707265737365642d6368696c64','hermes','/fixture/project',NULL,1780000000000,1780000009000,NULL,'/synthetic/hermes-pre-intake-home/.hermes/state.db',1,'A fresh human question.','["fixture-model"]',NULL,NULL,NULL,NULL,NULL,'v15:1792605811385487126:49152','shallow','/fixture/project','path',NULL,NULL,3);
INSERT INTO "sessions" VALUES('default:636861742f6e6f742d612d757569643aceb1','hermes','/fixture/project',NULL,1780000000000,1780000006500,'The historical answer is still available.','/synthetic/hermes-pre-intake-home/.hermes/state.db',20,'Find the Hermes fixture needle','["fixture-model"]',NULL,NULL,NULL,NULL,NULL,'hermes_schema31_v4:sha256:d5298a1b51aed36e34d9d6a942775e5cc8e99f3c3eba1c5295687d7d27ee9f48','full','/fixture/project','path',NULL,NULL,33);
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    color TEXT,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE TABLE tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id TEXT,
    tool_use_id TEXT NOT NULL,
    name TEXT NOT NULL,
    target TEXT,
    args_json TEXT,
    is_error INTEGER,
    ts_ms INTEGER,
    -- Which evidence backs this row: 'local' (a local parser read it from
    -- the provider's own files), 'remote' (a remote observation supplied
    -- it) or 'both'. See `EVIDENCE_LOCATION_TABLES`.
    location TEXT NOT NULL DEFAULT 'local' CHECK(location IN ('local', 'remote', 'both')), revision INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, session_id, tool_use_id)
);
INSERT INTO "tool_calls" VALUES(1,'hermes','default:636861742f6e6f742d612d757569643aceb1','2','call/native:1','read_file',NULL,'{"path":"/fixture/project/notes.txt"}',NULL,1780000002000,'local',17);
CREATE TABLE transcript_cursors (
    source TEXT NOT NULL,
    locator TEXT NOT NULL,
    committed_offset INTEGER NOT NULL DEFAULT 0,
    prefix_hash TEXT,
    dev_ino TEXT,
    parser_state_json TEXT,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (source, locator)
);
CREATE TRIGGER history_ai AFTER INSERT ON history BEGIN
    INSERT INTO history_fts(rowid, prompt, project)
    VALUES (new.id, new.prompt, new.project);
END;
CREATE TRIGGER history_au AFTER UPDATE OF id, prompt, project ON history
    WHEN old.id IS NOT new.id OR old.prompt IS NOT new.prompt OR old.project IS NOT new.project
BEGIN
    INSERT INTO history_fts(history_fts, rowid, prompt, project)
    VALUES('delete', old.id, old.prompt, old.project);
    INSERT INTO history_fts(rowid, prompt, project)
    VALUES (new.id, new.prompt, new.project);
END;
CREATE TRIGGER history_ad AFTER DELETE ON history BEGIN
    INSERT INTO history_fts(history_fts, rowid, prompt, project)
    VALUES('delete', old.id, old.prompt, old.project);
END;
CREATE TRIGGER session_events_ai AFTER INSERT ON session_events
WHEN NOT EXISTS (SELECT 1 FROM session_events_fts_defer)
BEGIN
    INSERT INTO session_events_fts(rowid, text, role, project)
    VALUES (new.id, new.text, new.role, new.project);
END;
CREATE TRIGGER session_events_au AFTER UPDATE OF text, role, project ON session_events
    WHEN old.text IS NOT new.text OR old.role IS NOT new.role OR old.project IS NOT new.project
BEGIN
    -- A row inserted while the deferral above is set is not indexed yet.
    -- Deleting an index entry that is not there corrupts FTS5. The new
    -- values are written either way, one entry. The unit's catch-up skips a
    -- row `session_events_fts_docsize` already holds, so this does not
    -- index it again.
    INSERT INTO session_events_fts(session_events_fts, rowid, text, role, project)
    SELECT 'delete', old.id, old.text, old.role, old.project
    WHERE EXISTS (SELECT 1 FROM session_events_fts_docsize WHERE id = old.id);
    INSERT INTO session_events_fts(rowid, text, role, project)
    VALUES (new.id, new.text, new.role, new.project);
END;
CREATE TRIGGER session_events_ad AFTER DELETE ON session_events BEGIN
    INSERT INTO session_events_fts(session_events_fts, rowid, text, role, project)
    SELECT 'delete', old.id, old.text, old.role, old.project
    WHERE EXISTS (SELECT 1 FROM session_events_fts_docsize WHERE id = old.id);
END;
CREATE INDEX idx_grok_unified_usage_session
    ON grok_unified_usage(session_id);
CREATE TRIGGER delete_session_presences
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_presences
    WHERE source = OLD.source AND session_id = OLD.session_id;
END;
CREATE TRIGGER delete_session_hydration_state
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_hydration_checkpoints
    WHERE source = OLD.source AND session_id = OLD.session_id;
    -- Before the DELETE below, not after: those rows are the only record of
    -- which transcripts depend on this session, and reconciliation finds a
    -- dependent by reading the edge that points at it. Deleting the edges
    -- first would leave every dependent resolved, with nothing left to
    -- rediscover it by, so rehydrating this session would never rebuild the
    -- continuation it used to carry.
    --
    -- Each end of an edge is its own seek, the parent and the child index.
    -- An OR across the two, or an `evidence_locator IS NOT NULL` term (a
    -- range on the locator index; IN never matches a NULL anyway), leaves
    -- the access path to statistics, and without them SQLite read every
    -- edge of the source for each deleted session.
    UPDATE session_continuity_evidence
    SET pending_reason = 'unreconciled'
    WHERE source = OLD.source
      AND locator IN (
        SELECT evidence_locator FROM session_relationships
        WHERE source = OLD.source AND parent_session_id = OLD.session_id
          AND relationship IN ('continuation', 'fork', 'resume')
        UNION ALL
        SELECT evidence_locator FROM session_relationships
        WHERE source = OLD.source AND child_session_id = OLD.session_id
          AND relationship IN ('continuation', 'fork', 'resume')
      );
    DELETE FROM session_relationships
    WHERE source = OLD.source AND parent_session_id = OLD.session_id;
    DELETE FROM session_relationships
    WHERE source = OLD.source AND child_session_id = OLD.session_id;
END;
CREATE TRIGGER delete_session_identity_correlations
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_identity_correlations
    WHERE source = OLD.source AND local_session_id = OLD.session_id;
END;
CREATE TRIGGER delete_session_markers
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_markers
    WHERE source = OLD.source AND session_id = OLD.session_id;
END;
CREATE TRIGGER delete_session_continuity_evidence
AFTER DELETE ON sessions
BEGIN
    DELETE FROM session_continuity_evidence
    WHERE source = OLD.source AND session_id = OLD.session_id;
END;
CREATE INDEX idx_history_hash ON history(prompt_hash);
CREATE INDEX idx_history_timestamp ON history(timestamp_ms DESC);
CREATE INDEX idx_history_session ON history(source, session_id);
CREATE INDEX idx_tags_name ON tags(name);
CREATE INDEX idx_session_tags_session ON session_tags(source, session_id);
CREATE INDEX idx_session_tags_tag ON session_tags(tag_id);
CREATE INDEX idx_sessions_recency ON sessions(last_activity_ms DESC, source, session_id);
CREATE INDEX idx_sessions_source_recency ON sessions(source, last_activity_ms DESC, session_id);
CREATE INDEX idx_sessions_identity ON sessions(source, session_id);
CREATE INDEX idx_sessions_raw_path ON sessions(source, raw_path);
CREATE INDEX idx_sessions_project_key ON sessions(source, project_key, last_activity_ms DESC);
CREATE INDEX idx_session_presences_location ON session_presences(location, source, session_id);
CREATE INDEX idx_session_presences_locator ON session_presences(location, source, raw_locator);
CREATE INDEX idx_session_relationships_parent ON session_relationships(source, parent_session_id);
CREATE INDEX idx_session_relationships_child ON session_relationships(source, child_session_id);
CREATE INDEX idx_session_relationships_locator ON session_relationships(source, evidence_locator);
CREATE INDEX idx_session_continuity_parent_uuid ON session_continuity_evidence(source, first_parent_uuid);
CREATE INDEX idx_session_continuity_pending ON session_continuity_evidence(source, session_id) WHERE pending_reason IS NOT NULL;
CREATE INDEX idx_session_events_session ON session_events(source, session_id);
CREATE INDEX idx_session_events_project ON session_events(source, session_id, project_key, project_key_method);
CREATE INDEX idx_session_events_message ON session_events(source, message_id);
CREATE INDEX idx_session_events_session_message ON session_events(source, session_id, message_id);
CREATE INDEX idx_session_events_claude_uid_unmatched ON session_events(source, event_uid) WHERE source = 'claude' AND substr(event_uid, -2) = ':0' AND (message_id IS NULL OR event_uid <> message_id || ':0');
CREATE INDEX idx_session_events_source_page ON session_events(source, session_id, ts_ms, id);
CREATE INDEX idx_session_events_page ON session_events(session_id, ts_ms, id);
CREATE INDEX idx_session_events_ts ON session_events(ts_ms DESC);
CREATE INDEX idx_session_events_role ON session_events(role);
CREATE INDEX idx_tool_calls_page_v2 ON tool_calls(source, session_id, (ts_ms IS NULL), ts_ms, id);
CREATE INDEX idx_file_edits_page_v2 ON file_edits(source, session_id, (ts_ms IS NULL), ts_ms, id);
CREATE INDEX idx_session_markers_page ON session_markers(source, session_id, (ts_ms IS NULL), ts_ms, id);
CREATE INDEX idx_tool_calls_message ON tool_calls(source, session_id, message_id);
CREATE INDEX idx_file_edits_message ON file_edits(source, session_id, message_id);
CREATE INDEX idx_file_edits_path ON file_edits(file_path);
CREATE INDEX idx_session_markers_session ON session_markers(source, session_id, ts_ms);
CREATE INDEX idx_session_commit_links_session ON session_commit_links(source, session_id);
CREATE INDEX idx_session_commit_links_commit ON session_commit_links(commit_sha);
CREATE INDEX idx_session_commit_links_repo ON session_commit_links(repo, branch);
CREATE INDEX idx_session_events_provider_message ON session_events(source, session_id, provider_message_id) WHERE provider_message_id IS NOT NULL;
CREATE VIEW session_requests AS
SELECT
    e.source AS source,
    e.session_id AS session_id,
    CASE
        WHEN NULLIF(e.request_id, '') IS NOT NULL THEN 'request-id:' || e.request_id
        WHEN e.source = 'codex' AND NULLIF(e.request_span, '') IS NOT NULL THEN 'request-span:' || e.request_span
        WHEN NULLIF(e.provider_message_id, '') IS NOT NULL THEN 'provider-message-id:' || e.provider_message_id
        WHEN NULLIF(e.request_span, '') IS NOT NULL THEN 'request-span:' || e.request_span
        ELSE 'record-id:' || e.message_id
    END AS request_key,
    CASE
        WHEN NULLIF(e.request_id, '') IS NOT NULL THEN 'request-id'
        WHEN e.source = 'codex' AND NULLIF(e.request_span, '') IS NOT NULL THEN 'request-span'
        WHEN NULLIF(e.provider_message_id, '') IS NOT NULL THEN 'provider-message-id'
        WHEN NULLIF(e.request_span, '') IS NOT NULL THEN 'request-span'
        ELSE 'record-id'
    END AS request_key_source,
    json_group_array(DISTINCT e.message_id) AS message_ids,
    MIN(e.provider) AS provider,
    COUNT(DISTINCT e.provider) AS provider_variants,
    MIN(e.id) AS id,
    MIN(e.ts_ms) AS first_ts_ms,
    MAX(e.ts_ms) AS last_ts_ms,
    MIN(e.model) AS model,
    COUNT(DISTINCT e.model) AS model_variants,
    MIN(e.token_json) AS token_json,
    COUNT(DISTINCT e.token_json) AS usage_variants,
    MAX(e.kind = 'thinking') AS has_thinking,
    COUNT(*) AS event_count
FROM session_events e INDEXED BY idx_session_events_source_page
WHERE e.role = 'assistant'
  AND e.message_id IS NOT NULL
  AND e.message_id <> ''
GROUP BY e.source, e.session_id, CASE
        WHEN NULLIF(e.request_id, '') IS NOT NULL THEN 'request-id:' || e.request_id
        WHEN e.source = 'codex' AND NULLIF(e.request_span, '') IS NOT NULL THEN 'request-span:' || e.request_span
        WHEN NULLIF(e.provider_message_id, '') IS NOT NULL THEN 'provider-message-id:' || e.provider_message_id
        WHEN NULLIF(e.request_span, '') IS NOT NULL THEN 'request-span:' || e.request_span
        ELSE 'record-id:' || e.message_id
    END;
CREATE INDEX idx_observation_locator ON session_observations(source,location,connector_id,connector_instance,raw_locator);
CREATE TRIGGER delete_observation_version AFTER DELETE ON session_observations BEGIN
 DELETE FROM observation_versions WHERE source=OLD.source AND session_id=OLD.session_id AND location=OLD.location AND connector_id=OLD.connector_id AND connector_instance=OLD.connector_instance;
END;
CREATE TRIGGER delete_canonical_evidence_protection AFTER DELETE ON sessions BEGIN
 DELETE FROM canonical_evidence_protection WHERE source=OLD.source AND session_id=OLD.session_id;
END;
CREATE TRIGGER delete_session_observations AFTER DELETE ON sessions BEGIN
 DELETE FROM observation_hydration_checkpoints WHERE source=OLD.source AND session_id=OLD.session_id;
 DELETE FROM observation_evidence WHERE source=OLD.source AND session_id=OLD.session_id;
 DELETE FROM session_observations WHERE source=OLD.source AND session_id=OLD.session_id;
END;
CREATE INDEX idx_evidence_tombstones_kind_revision ON evidence_tombstones(kind, revision);
CREATE INDEX idx_sessions_revision ON sessions(revision);
CREATE TRIGGER change_feed_sessions_insert AFTER INSERT ON sessions BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'session' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.session_id;
         END;
CREATE TRIGGER change_feed_sessions_update AFTER UPDATE ON sessions
         WHEN NEW.revision = OLD.revision AND (NEW.session_id IS NOT OLD.session_id OR NEW.source IS NOT OLD.source OR NEW.cwd IS NOT OLD.cwd OR NEW.git_branch IS NOT OLD.git_branch OR NEW.first_activity_ms IS NOT OLD.first_activity_ms OR NEW.last_activity_ms IS NOT OLD.last_activity_ms OR NEW.last_assistant_text IS NOT OLD.last_assistant_text OR NEW.raw_path IS NOT OLD.raw_path OR NEW.parser_version IS NOT OLD.parser_version OR NEW.first_prompt IS NOT OLD.first_prompt OR NEW.models_json IS NOT OLD.models_json OR NEW.originator IS NOT OLD.originator OR NEW.agent_version IS NOT OLD.agent_version OR NEW.repo_url IS NOT OLD.repo_url OR NEW.initial_commit IS NOT OLD.initial_commit OR NEW.workspace_roots_json IS NOT OLD.workspace_roots_json OR NEW.source_stamp IS NOT OLD.source_stamp OR NEW.discovery_state IS NOT OLD.discovery_state OR NEW.project_key IS NOT OLD.project_key OR NEW.project_key_method IS NOT OLD.project_key_method OR NEW.title IS NOT OLD.title OR NEW.session_kind IS NOT OLD.session_kind) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.session_id IS NOT NEW.session_id);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'session', OLD.source, OLD.session_id, OLD.session_id, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.session_id IS NOT NEW.session_id;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'session' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.session_id;
         END;
CREATE TRIGGER change_feed_sessions_delete AFTER DELETE ON sessions BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('session', OLD.source, OLD.session_id, OLD.session_id, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_session_events_revision ON session_events(revision);
CREATE TRIGGER change_feed_session_events_insert AFTER INSERT ON session_events BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_events SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'session_event' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.event_uid;
         END;
CREATE TRIGGER change_feed_session_events_update AFTER UPDATE ON session_events
         WHEN NEW.revision = OLD.revision AND (NEW.id IS NOT OLD.id OR NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.project IS NOT OLD.project OR NEW.project_key IS NOT OLD.project_key OR NEW.project_key_method IS NOT OLD.project_key_method OR NEW.cwd IS NOT OLD.cwd OR NEW.git_branch IS NOT OLD.git_branch OR NEW.message_id IS NOT OLD.message_id OR NEW.parent_id IS NOT OLD.parent_id OR NEW.ts_ms IS NOT OLD.ts_ms OR NEW.role IS NOT OLD.role OR NEW.kind IS NOT OLD.kind OR NEW.text IS NOT OLD.text OR NEW.model IS NOT OLD.model OR NEW.token_json IS NOT OLD.token_json OR NEW.provider IS NOT OLD.provider OR NEW.event_uid IS NOT OLD.event_uid OR NEW.tool_use_id IS NOT OLD.tool_use_id OR NEW.payload_bytes IS NOT OLD.payload_bytes OR NEW.payload_truncated IS NOT OLD.payload_truncated OR NEW.payload_hash IS NOT OLD.payload_hash OR NEW.call_index IS NOT OLD.call_index OR NEW.event_index IS NOT OLD.event_index OR NEW.result_status IS NOT OLD.result_status OR NEW.event_source IS NOT OLD.event_source OR NEW.error_signal IS NOT OLD.error_signal OR NEW.subagent_session_id IS NOT OLD.subagent_session_id OR NEW.agent_id IS NOT OLD.agent_id OR NEW.request_id IS NOT OLD.request_id OR NEW.provider_message_id IS NOT OLD.provider_message_id OR NEW.stop_reason IS NOT OLD.stop_reason OR NEW.agent_version IS NOT OLD.agent_version OR NEW.is_sidechain IS NOT OLD.is_sidechain OR NEW.is_meta IS NOT OLD.is_meta OR NEW.turn_id IS NOT OLD.turn_id OR NEW.request_span IS NOT OLD.request_span OR NEW.raw_kind IS NOT OLD.raw_kind OR NEW.control_kind IS NOT OLD.control_kind OR NEW.record_token_json IS NOT OLD.record_token_json OR NEW.location IS NOT OLD.location) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.event_uid IS NOT NEW.event_uid);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'session_event', OLD.source, OLD.session_id, OLD.event_uid, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.event_uid IS NOT NEW.event_uid;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_events SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'session_event' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.event_uid;
         END;
CREATE TRIGGER change_feed_session_events_delete AFTER DELETE ON session_events BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('session_event', OLD.source, OLD.session_id, OLD.event_uid, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_tool_calls_revision ON tool_calls(revision);
CREATE TRIGGER change_feed_tool_calls_insert AFTER INSERT ON tool_calls BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE tool_calls SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'tool_call' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.tool_use_id;
         END;
CREATE TRIGGER change_feed_tool_calls_update AFTER UPDATE ON tool_calls
         WHEN NEW.revision = OLD.revision AND (NEW.id IS NOT OLD.id OR NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.message_id IS NOT OLD.message_id OR NEW.tool_use_id IS NOT OLD.tool_use_id OR NEW.name IS NOT OLD.name OR NEW.target IS NOT OLD.target OR NEW.args_json IS NOT OLD.args_json OR NEW.is_error IS NOT OLD.is_error OR NEW.ts_ms IS NOT OLD.ts_ms OR NEW.location IS NOT OLD.location) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.tool_use_id IS NOT NEW.tool_use_id);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'tool_call', OLD.source, OLD.session_id, OLD.tool_use_id, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.tool_use_id IS NOT NEW.tool_use_id;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE tool_calls SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'tool_call' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.tool_use_id;
         END;
CREATE TRIGGER change_feed_tool_calls_delete AFTER DELETE ON tool_calls BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('tool_call', OLD.source, OLD.session_id, OLD.tool_use_id, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_file_edits_revision ON file_edits(revision);
CREATE TRIGGER change_feed_file_edits_insert AFTER INSERT ON file_edits BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE file_edits SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'file_edit' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.tool_use_id;
         END;
CREATE TRIGGER change_feed_file_edits_update AFTER UPDATE ON file_edits
         WHEN NEW.revision = OLD.revision AND (NEW.id IS NOT OLD.id OR NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.message_id IS NOT OLD.message_id OR NEW.tool_use_id IS NOT OLD.tool_use_id OR NEW.file_path IS NOT OLD.file_path OR NEW.tool_name IS NOT OLD.tool_name OR NEW.lines_added IS NOT OLD.lines_added OR NEW.lines_removed IS NOT OLD.lines_removed OR NEW.structured_patch_json IS NOT OLD.structured_patch_json OR NEW.user_modified IS NOT OLD.user_modified OR NEW.ts_ms IS NOT OLD.ts_ms OR NEW.git_branch IS NOT OLD.git_branch OR NEW.cwd IS NOT OLD.cwd OR NEW.location IS NOT OLD.location) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.tool_use_id IS NOT NEW.tool_use_id);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'file_edit', OLD.source, OLD.session_id, OLD.tool_use_id, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.tool_use_id IS NOT NEW.tool_use_id;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE file_edits SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'file_edit' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.tool_use_id;
         END;
CREATE TRIGGER change_feed_file_edits_delete AFTER DELETE ON file_edits BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('file_edit', OLD.source, OLD.session_id, OLD.tool_use_id, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_session_markers_revision ON session_markers(revision);
CREATE TRIGGER change_feed_session_markers_insert AFTER INSERT ON session_markers BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_markers SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'session_marker' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.marker_uid;
         END;
CREATE TRIGGER change_feed_session_markers_update AFTER UPDATE ON session_markers
         WHEN NEW.revision = OLD.revision AND (NEW.id IS NOT OLD.id OR NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.marker_uid IS NOT OLD.marker_uid OR NEW.ts_ms IS NOT OLD.ts_ms OR NEW.message_id IS NOT OLD.message_id OR NEW.parent_id IS NOT OLD.parent_id OR NEW.turn_id IS NOT OLD.turn_id OR NEW.kind IS NOT OLD.kind OR NEW.subkind IS NOT OLD.subkind OR NEW.text IS NOT OLD.text OR NEW.payload_json IS NOT OLD.payload_json OR NEW.location IS NOT OLD.location) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.marker_uid IS NOT NEW.marker_uid);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'session_marker', OLD.source, OLD.session_id, OLD.marker_uid, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.marker_uid IS NOT NEW.marker_uid;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_markers SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'session_marker' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.marker_uid;
         END;
CREATE TRIGGER change_feed_session_markers_delete AFTER DELETE ON session_markers BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('session_marker', OLD.source, OLD.session_id, OLD.marker_uid, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_session_relationships_revision ON session_relationships(revision);
CREATE TRIGGER change_feed_session_relationships_insert AFTER INSERT ON session_relationships BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_relationships SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'relationship' AND source = NEW.source AND session_id = NEW.parent_session_id AND record_key = NEW.relationship_uid;
         END;
CREATE TRIGGER change_feed_session_relationships_update AFTER UPDATE ON session_relationships
         WHEN NEW.revision = OLD.revision AND (NEW.source IS NOT OLD.source OR NEW.parent_session_id IS NOT OLD.parent_session_id OR NEW.relationship_uid IS NOT OLD.relationship_uid OR NEW.child_session_id IS NOT OLD.child_session_id OR NEW.relationship IS NOT OLD.relationship OR NEW.identity_status IS NOT OLD.identity_status OR NEW.child_agent_type IS NOT OLD.child_agent_type OR NEW.child_agent_name IS NOT OLD.child_agent_name OR NEW.child_model IS NOT OLD.child_model OR NEW.spawn_depth IS NOT OLD.spawn_depth OR NEW.evidence_kind IS NOT OLD.evidence_kind OR NEW.evidence_locator IS NOT OLD.evidence_locator OR NEW.evidence_ref IS NOT OLD.evidence_ref OR NEW.child_has_events IS NOT OLD.child_has_events OR NEW.spawned_at_ms IS NOT OLD.spawned_at_ms OR NEW.created_ms IS NOT OLD.created_ms OR NEW.updated_ms IS NOT OLD.updated_ms OR NEW.origin_session_id IS NOT OLD.origin_session_id) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.parent_session_id IS NOT NEW.parent_session_id OR OLD.relationship_uid IS NOT NEW.relationship_uid);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'relationship', OLD.source, OLD.parent_session_id, OLD.relationship_uid, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.parent_session_id IS NOT NEW.parent_session_id OR OLD.relationship_uid IS NOT NEW.relationship_uid;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_relationships SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'relationship' AND source = NEW.source AND session_id = NEW.parent_session_id AND record_key = NEW.relationship_uid;
         END;
CREATE TRIGGER change_feed_session_relationships_delete AFTER DELETE ON session_relationships BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('relationship', OLD.source, OLD.parent_session_id, OLD.relationship_uid, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_history_revision ON history(revision);
CREATE TRIGGER change_feed_history_insert AFTER INSERT ON history BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE history SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'history' AND source = NEW.source AND session_id = '' AND record_key = json_array(NEW.timestamp_ms, NEW.prompt);
         END;
CREATE TRIGGER change_feed_history_update AFTER UPDATE ON history
         WHEN NEW.revision = OLD.revision AND (NEW.id IS NOT OLD.id OR NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.project IS NOT OLD.project OR NEW.prompt IS NOT OLD.prompt OR NEW.prompt_hash IS NOT OLD.prompt_hash OR NEW.timestamp_ms IS NOT OLD.timestamp_ms OR NEW.git_branch IS NOT OLD.git_branch) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR json_array(OLD.timestamp_ms, OLD.prompt) IS NOT json_array(NEW.timestamp_ms, NEW.prompt));
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'history', OLD.source, '', json_array(OLD.timestamp_ms, OLD.prompt), (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR json_array(OLD.timestamp_ms, OLD.prompt) IS NOT json_array(NEW.timestamp_ms, NEW.prompt);
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE history SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'history' AND source = NEW.source AND session_id = '' AND record_key = json_array(NEW.timestamp_ms, NEW.prompt);
         END;
CREATE TRIGGER change_feed_history_delete AFTER DELETE ON history BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('history', OLD.source, '', json_array(OLD.timestamp_ms, OLD.prompt), (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_session_presences_revision ON session_presences(revision);
CREATE TRIGGER change_feed_session_presences_insert AFTER INSERT ON session_presences BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_presences SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'presence' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.location;
         END;
CREATE TRIGGER change_feed_session_presences_update AFTER UPDATE ON session_presences
         WHEN NEW.revision = OLD.revision AND (NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.location IS NOT OLD.location OR NEW.raw_locator IS NOT OLD.raw_locator OR NEW.source_stamp IS NOT OLD.source_stamp OR NEW.discovery_state IS NOT OLD.discovery_state) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.location IS NOT NEW.location);
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'presence', OLD.source, OLD.session_id, OLD.location, (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.location IS NOT NEW.location;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_presences SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'presence' AND source = NEW.source AND session_id = NEW.session_id AND record_key = NEW.location;
         END;
CREATE TRIGGER change_feed_session_presences_delete AFTER DELETE ON session_presences BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('presence', OLD.source, OLD.session_id, OLD.location, (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_session_commit_links_revision ON session_commit_links(revision);
CREATE TRIGGER change_feed_session_commit_links_insert AFTER INSERT ON session_commit_links BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_commit_links SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'commit_link' AND source = NEW.source AND session_id = NEW.session_id AND record_key = json_array(NEW.commit_sha, NEW.match_method);
         END;
CREATE TRIGGER change_feed_session_commit_links_update AFTER UPDATE ON session_commit_links
         WHEN NEW.revision = OLD.revision AND (NEW.id IS NOT OLD.id OR NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.repo IS NOT OLD.repo OR NEW.branch IS NOT OLD.branch OR NEW.commit_sha IS NOT OLD.commit_sha OR NEW.note_ref IS NOT OLD.note_ref OR NEW.match_method IS NOT OLD.match_method OR NEW.confidence IS NOT OLD.confidence OR NEW.files_json IS NOT OLD.files_json OR NEW.numstat_json IS NOT OLD.numstat_json OR NEW.evidence_json IS NOT OLD.evidence_json OR NEW.created_at_ms IS NOT OLD.created_at_ms) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR json_array(OLD.commit_sha, OLD.match_method) IS NOT json_array(NEW.commit_sha, NEW.match_method));
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'commit_link', OLD.source, OLD.session_id, json_array(OLD.commit_sha, OLD.match_method), (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR json_array(OLD.commit_sha, OLD.match_method) IS NOT json_array(NEW.commit_sha, NEW.match_method);
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_commit_links SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'commit_link' AND source = NEW.source AND session_id = NEW.session_id AND record_key = json_array(NEW.commit_sha, NEW.match_method);
         END;
CREATE TRIGGER change_feed_session_commit_links_delete AFTER DELETE ON session_commit_links BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('commit_link', OLD.source, OLD.session_id, json_array(OLD.commit_sha, OLD.match_method), (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_session_observations_revision ON session_observations(revision);
CREATE TRIGGER change_feed_session_observations_insert AFTER INSERT ON session_observations BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_observations SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'source_observation' AND source = NEW.source AND session_id = NEW.session_id AND record_key = json_array(NEW.location, NEW.connector_id, NEW.connector_instance);
         END;
CREATE TRIGGER change_feed_session_observations_update AFTER UPDATE ON session_observations
         WHEN NEW.revision = OLD.revision AND (NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.location IS NOT OLD.location OR NEW.connector_id IS NOT OLD.connector_id OR NEW.connector_instance IS NOT OLD.connector_instance OR NEW.raw_locator IS NOT OLD.raw_locator OR NEW.source_stamp IS NOT OLD.source_stamp OR NEW.discovery_state IS NOT OLD.discovery_state OR NEW.access_state IS NOT OLD.access_state OR NEW.updated_ms IS NOT OLD.updated_ms OR NEW.first_prompt IS NOT OLD.first_prompt OR NEW.last_assistant_text IS NOT OLD.last_assistant_text) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR json_array(OLD.location, OLD.connector_id, OLD.connector_instance) IS NOT json_array(NEW.location, NEW.connector_id, NEW.connector_instance));
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'source_observation', OLD.source, OLD.session_id, json_array(OLD.location, OLD.connector_id, OLD.connector_instance), (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR json_array(OLD.location, OLD.connector_id, OLD.connector_instance) IS NOT json_array(NEW.location, NEW.connector_id, NEW.connector_instance);
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE session_observations SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'source_observation' AND source = NEW.source AND session_id = NEW.session_id AND record_key = json_array(NEW.location, NEW.connector_id, NEW.connector_instance);
         END;
CREATE TRIGGER change_feed_session_observations_delete AFTER DELETE ON session_observations BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('source_observation', OLD.source, OLD.session_id, json_array(OLD.location, OLD.connector_id, OLD.connector_instance), (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE INDEX idx_observation_evidence_revision ON observation_evidence(revision);
CREATE TRIGGER change_feed_observation_evidence_insert AFTER INSERT ON observation_evidence BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE observation_evidence SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'observation_evidence' AND source = NEW.source AND session_id = NEW.session_id AND record_key = json_array(NEW.location, NEW.connector_id, NEW.connector_instance, NEW.evidence_uid);
         END;
CREATE TRIGGER change_feed_observation_evidence_update AFTER UPDATE ON observation_evidence
         WHEN NEW.revision = OLD.revision AND (NEW.source IS NOT OLD.source OR NEW.session_id IS NOT OLD.session_id OR NEW.location IS NOT OLD.location OR NEW.connector_id IS NOT OLD.connector_id OR NEW.connector_instance IS NOT OLD.connector_instance OR NEW.evidence_uid IS NOT OLD.evidence_uid OR NEW.payload_json IS NOT OLD.payload_json) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR json_array(OLD.location, OLD.connector_id, OLD.connector_instance, OLD.evidence_uid) IS NOT json_array(NEW.location, NEW.connector_id, NEW.connector_instance, NEW.evidence_uid));
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) SELECT 'observation_evidence', OLD.source, OLD.session_id, json_array(OLD.location, OLD.connector_id, OLD.connector_instance, OLD.evidence_uid), (SELECT version FROM observation_clock WHERE singleton = 1) WHERE OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR json_array(OLD.location, OLD.connector_id, OLD.connector_instance, OLD.evidence_uid) IS NOT json_array(NEW.location, NEW.connector_id, NEW.connector_instance, NEW.evidence_uid);
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE observation_evidence SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE rowid = NEW.rowid;
             DELETE FROM evidence_tombstones WHERE kind = 'observation_evidence' AND source = NEW.source AND session_id = NEW.session_id AND record_key = json_array(NEW.location, NEW.connector_id, NEW.connector_instance, NEW.evidence_uid);
         END;
CREATE TRIGGER change_feed_observation_evidence_delete AFTER DELETE ON observation_evidence BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             INSERT OR REPLACE INTO evidence_tombstones (kind, source, session_id, record_key, revision) VALUES ('observation_evidence', OLD.source, OLD.session_id, json_array(OLD.location, OLD.connector_id, OLD.connector_instance, OLD.evidence_uid), (SELECT version FROM observation_clock WHERE singleton = 1));
         END;
CREATE TRIGGER change_feed_session_locations_insert AFTER INSERT ON session_presences BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE source = NEW.source AND session_id = NEW.session_id;
         END;
CREATE TRIGGER change_feed_session_locations_update AFTER UPDATE ON session_presences WHEN NEW.revision = OLD.revision AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id OR OLD.location IS NOT NEW.location) BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE source = NEW.source AND session_id = NEW.session_id;
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
             UPDATE sessions SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE source = OLD.source AND session_id = OLD.session_id AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
         END;
CREATE TRIGGER change_feed_session_locations_delete AFTER DELETE ON session_presences BEGIN
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET revision = (SELECT version FROM observation_clock WHERE singleton = 1) WHERE source = OLD.source AND session_id = OLD.session_id;
         END;
PRAGMA writable_schema=OFF;
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('session_markers',8);
INSERT INTO "sqlite_sequence" VALUES('history',2);
INSERT INTO "sqlite_sequence" VALUES('session_events',9);
INSERT INTO "sqlite_sequence" VALUES('tool_calls',1);
COMMIT;
