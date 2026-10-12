BEGIN TRANSACTION;
CREATE TABLE schema_version (
    version INTEGER NOT NULL
);
CREATE TABLE system_prompts (
    hash TEXT PRIMARY KEY,
    prompt TEXT NOT NULL
);
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    created_source TEXT,
    user_id TEXT,
    session_key TEXT,
    chat_id TEXT,
    chat_type TEXT,
    thread_id TEXT,
    display_name TEXT,
    origin_json TEXT,
    expiry_finalized INTEGER DEFAULT 0,
    model TEXT,
    model_config TEXT,
    system_prompt TEXT,
    system_prompt_hash TEXT,
    parent_session_id TEXT,
    started_at REAL NOT NULL,
    ended_at REAL,
    end_reason TEXT,
    message_count INTEGER DEFAULT 0,
    tool_call_count INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    cache_write_tokens INTEGER DEFAULT 0,
    reasoning_tokens INTEGER DEFAULT 0,
    cwd TEXT,
    git_branch TEXT,
    git_repo_root TEXT,
    git_metadata_generation INTEGER NOT NULL DEFAULT 0,
    billing_provider TEXT,
    billing_base_url TEXT,
    billing_mode TEXT,
    estimated_cost_usd REAL,
    actual_cost_usd REAL,
    cost_status TEXT,
    cost_source TEXT,
    pricing_version TEXT,
    title TEXT,
    title_source TEXT,
    last_activity_at REAL,
    last_activity_description TEXT,
    last_activity_provenance TEXT,
    api_call_count INTEGER DEFAULT 0,
    handoff_state TEXT,
    handoff_platform TEXT,
    handoff_error TEXT,
    compression_failure_cooldown_until REAL,
    compression_failure_error TEXT,
    compression_fallback_streak INTEGER NOT NULL DEFAULT 0,
    compression_ineffective_count INTEGER NOT NULL DEFAULT 0,
    compression_recovery_deadline REAL,
    compression_overload_streak INTEGER NOT NULL DEFAULT 0,
    profile_name TEXT,
    transport_profile TEXT,
    rewind_count INTEGER NOT NULL DEFAULT 0,
    archived INTEGER NOT NULL DEFAULT 0,
    auto_archived INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0,
    hidden INTEGER NOT NULL DEFAULT 0,
    last_read_at REAL,
    tool_names TEXT,
    FOREIGN KEY (parent_session_id) REFERENCES sessions(id),
    FOREIGN KEY (system_prompt_hash) REFERENCES system_prompts(hash)
);
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    role TEXT NOT NULL,
    content TEXT,
    tool_call_id TEXT,
    tool_calls TEXT,
    tool_name TEXT,
    effect_disposition TEXT,
    timestamp REAL NOT NULL,
    token_count INTEGER,
    finish_reason TEXT,
    reasoning TEXT,
    reasoning_content TEXT,
    reasoning_details TEXT,
    codex_reasoning_items TEXT,
    codex_message_items TEXT,
    platform_message_id TEXT,
    observed INTEGER DEFAULT 0,
    _compressed_summary INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    compacted INTEGER NOT NULL DEFAULT 0,
    api_content TEXT,
    display_kind TEXT,
    display_metadata TEXT,
    display_identity BLOB,
    display_order INTEGER,
    message_uid TEXT,
    absorbed_message_uids TEXT,
    tool_call_uids TEXT,
    tool_call_uid TEXT
);
CREATE TABLE session_model_usage (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    model TEXT NOT NULL,
    billing_provider TEXT NOT NULL DEFAULT '',
    billing_base_url TEXT NOT NULL DEFAULT '',
    billing_mode TEXT NOT NULL DEFAULT '',
    task TEXT NOT NULL DEFAULT '',
    api_call_count INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    cache_write_tokens INTEGER NOT NULL DEFAULT 0,
    reasoning_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_cost_usd REAL NOT NULL DEFAULT 0,
    actual_cost_usd REAL NOT NULL DEFAULT 0,
    cost_status TEXT,
    cost_source TEXT,
    first_seen REAL,
    last_seen REAL,
    PRIMARY KEY (session_id, model, billing_provider, billing_base_url, billing_mode, task)
);
INSERT INTO "schema_version" VALUES(31);
INSERT INTO "sessions" VALUES('chat/not-a-uuid:α','cli','cli',NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,'fixture-model',NULL,NULL,NULL,NULL,1780000000.0,1780000000.0,'compression',1,0,1500,200,300,0,0,'/fixture/project',NULL,NULL,0,NULL,NULL,NULL,0.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2,NULL,NULL,NULL,NULL,NULL,0,0,NULL,0,'default',NULL,0,0,0,0,0,NULL,NULL);
INSERT INTO "sessions" VALUES('compressed-child','cli','cli',NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,'fixture-model',NULL,NULL,NULL,'chat/not-a-uuid:α',1780000000.0,NULL,NULL,3,0,0,0,0,0,0,'/fixture/project',NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,0,0,NULL,0,'default',NULL,0,0,0,0,0,NULL,NULL);
INSERT INTO "messages" VALUES(1,'chat/not-a-uuid:α','user','Find the Hermes fixture needle',NULL,NULL,NULL,NULL,1780000001.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,0,1,NULL,NULL,NULL,X'9C10857055C0943CD6CF7D40C9F7C1ACC78D8AC5EECC4460D2985197A6154568',1,'msg-user',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(2,'chat/not-a-uuid:α','assistant','I will inspect the file.',NULL,'[{"id": "call/native:1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"/fixture/project/notes.txt\"}"}}]',NULL,NULL,1780000002.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,0,1,NULL,NULL,NULL,X'7BF4520075BAA78265702249AB524A5D37139809589D3A8171FDFBF20D21A61B',2,'msg-call',NULL,'{"call/native:1": "0d6c7d592b6a4191aeb82b84a32a2cb1"}',NULL);
INSERT INTO "messages" VALUES(3,'chat/not-a-uuid:α','tool','The Hermes fixture needle is here.','call/native:1',NULL,'read_file',NULL,1780000003.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,0,1,NULL,NULL,NULL,X'9A844B04D69D45434A04120648E1032ECD83D1C1D45DB0A4128404F6E61B61AE',3,'msg-result',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(4,'chat/not-a-uuid:α','assistant','Found the Hermes fixture needle.',NULL,NULL,NULL,NULL,1780000004.0,NULL,'stop',NULL,'Inspect the recorded text.',NULL,NULL,NULL,NULL,0,0,0,1,NULL,NULL,NULL,X'B69F4159EC1E6C73D890970A2AC10D22643F55BBB7685FC5EE97E8D401548AE4',4,'msg-answer',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(5,'chat/not-a-uuid:α','user','Continue after restart.',NULL,NULL,NULL,NULL,1780000005.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,0,1,NULL,NULL,NULL,X'536E0760A96E861AA757A314A8D3AF8910501478B164AB2CE60F34B4AE81BF43',5,'msg-resume',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(6,'chat/not-a-uuid:α','assistant','The historical answer is still available.',NULL,NULL,NULL,NULL,1780000006.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,0,1,NULL,NULL,NULL,X'1D31D25A9AA3E8EF43BADDE613DA4FFEB14CA7FF7AB4B001B9F77B66EAE10E13',6,'msg-resumed-answer',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(7,'chat/not-a-uuid:α','user','Archived conversation summary.',NULL,NULL,NULL,NULL,1780000006.5,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,1,1,0,NULL,NULL,NULL,X'A1001FFCAD18CFF9513A8636160A0C1E4ACC83953EA712323959B16D47D887F0',7,'msg-inplace-summary',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(8,'compressed-child','user','Compressed prior context.',NULL,NULL,NULL,NULL,1780000007.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,1,1,0,NULL,NULL,NULL,X'8898996DA745FE70FBD5F5966B4DF15E82A8EFA9F1026344923266A8FDDEC63B',8,'msg-summary',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(9,'compressed-child','user','A fresh human question.',NULL,NULL,NULL,NULL,1780000008.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,1,0,NULL,NULL,NULL,X'5FAAC4036DC2E56DB62C9139847411B28ACA77A3AA674DE74B98EFDAE9139033',9,'msg-child-user',NULL,NULL,NULL);
INSERT INTO "messages" VALUES(10,'compressed-child','assistant','A fresh answer.',NULL,NULL,NULL,NULL,1780000009.0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,1,0,NULL,NULL,NULL,X'95AE3B177171ACD9484D3C4A0475207B9F63A1C7941B5BE7B12400362944E70E',10,'msg-child-answer',NULL,NULL,NULL);
INSERT INTO "session_model_usage" VALUES('chat/not-a-uuid:α','fixture-model','','','','',2,1500,200,300,0,0,0.0,0.0,NULL,NULL,1780000000.0,1780000000.0);
COMMIT;
