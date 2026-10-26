PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT,
                session_id TEXT NOT NULL REFERENCES sessions(id),
                role TEXT NOT NULL,
                content_json TEXT NOT NULL,
                created_timestamp INTEGER NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tokens INTEGER,
                metadata_json TEXT
            );
INSERT INTO "messages" VALUES(12,'user-native-1','20261022_1','user','[{"type":"text","text":"Find the Goose fixture needle"}]',1792643676,'2026-10-22 04:34:36',NULL,'{"userVisible":true,"agentVisible":true}');
INSERT INTO "messages" VALUES(13,'assistant-native-2','20261022_1','assistant','[{"type":"text","text":"The tool result was removed during rewrite."}]',1792643676,'2026-10-22 04:34:36',NULL,'{"userVisible":true,"agentVisible":true}');
INSERT INTO "messages" VALUES(14,'assistant-native-1','20261022_1','assistant','[{"type":"text","text":"I will inspect the fixture."},{"type":"toolRequest","id":"call-native-1","toolCall":{"status":"success","value":{"name":"read_file"}}}]',1792643676,'2026-10-22 04:34:36',NULL,'{"userVisible":true,"agentVisible":true}');
INSERT INTO "messages" VALUES(15,'user-native-2','20261022_1','user','[{"type":"text","text":"Continue after restart."}]',1792643676,'2026-10-22 04:34:36',NULL,'{"userVisible":true,"agentVisible":true}');
INSERT INTO "messages" VALUES(16,'assistant-native-3','20261022_1','assistant','[{"type":"text","text":"The revised historical answer is available."}]',1792643676,'2026-10-22 04:34:36',NULL,'{"userVisible":true,"agentVisible":true}');
CREATE TABLE provider_inventory_entries (
            inventory_key TEXT PRIMARY KEY,
            provider_id TEXT NOT NULL,
            provider_family TEXT NOT NULL,
            last_updated_at TEXT,
            last_refresh_attempt_at TEXT,
            last_refresh_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
CREATE TABLE provider_inventory_models (
            inventory_key TEXT NOT NULL REFERENCES provider_inventory_entries(inventory_key) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL,
            model_id TEXT NOT NULL,
            name TEXT NOT NULL,
            family TEXT,
            context_limit INTEGER,
            reasoning BOOLEAN,
            recommended BOOLEAN,
            PRIMARY KEY (inventory_key, ordinal)
        );
CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
INSERT INTO "schema_version" VALUES(17,'2026-10-22 04:20:21');
CREATE TABLE sessions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                user_set_name BOOLEAN DEFAULT FALSE,
                session_type TEXT NOT NULL DEFAULT 'user',
                working_dir TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                extension_data TEXT DEFAULT '{}',
                total_tokens INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cache_read_tokens INTEGER,
                cache_write_tokens INTEGER,
                accumulated_total_tokens INTEGER,
                accumulated_input_tokens INTEGER,
                accumulated_output_tokens INTEGER,
                accumulated_cache_read_tokens INTEGER,
                accumulated_cache_write_tokens INTEGER,
                accumulated_cost REAL,
                schedule_id TEXT,
                recipe_json TEXT,
                user_recipe_values_json TEXT,
                provider_name TEXT,
                model_config_json TEXT,
                goose_mode TEXT NOT NULL DEFAULT 'auto',
                archived_at TIMESTAMP,
                project_id TEXT,
                parent_session_id TEXT,
                system_prompt_override TEXT,
                system_prompt_extras_json TEXT,
                container_id TEXT
            );
INSERT INTO "sessions" VALUES('20261022_1','Synthetic Goose history','',0,'user','/synthetic/goose/workspace','2026-10-22 04:20:21','2026-10-22 04:20:24','{}',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'auto',NULL,NULL,NULL,NULL,NULL,NULL);
CREATE TABLE usage_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                created_timestamp INTEGER NOT NULL,
                model TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                total_tokens INTEGER,
                cache_read_tokens INTEGER,
                cache_write_tokens INTEGER,
                cost REAL,
                cost_source TEXT,
                is_compaction INTEGER DEFAULT 0
            );
CREATE INDEX idx_messages_session ON messages(session_id);
CREATE INDEX idx_messages_timestamp ON messages(timestamp);
CREATE INDEX idx_messages_message_id ON messages(message_id);
CREATE INDEX idx_messages_session_created ON messages(session_id, created_timestamp, id);
CREATE INDEX idx_sessions_updated ON sessions(updated_at DESC);
CREATE INDEX idx_sessions_type ON sessions(session_type);
CREATE INDEX idx_sessions_parent ON sessions(parent_session_id);
CREATE INDEX idx_usage_ledger_session ON usage_ledger(session_id);
CREATE INDEX idx_provider_inventory_provider_id ON provider_inventory_entries(provider_id);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('messages',16);
COMMIT;
PRAGMA foreign_keys=ON;
