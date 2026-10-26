-- Captured from @kilocode/cli 7.8.15 native import writer on macOS arm64.
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE `project` (
          `id` text PRIMARY KEY,
          `worktree` text NOT NULL,
          `vcs` text,
          `name` text,
          `icon_url` text,
          `icon_url_override` text,
          `icon_color` text,
          `time_created` integer NOT NULL,
          `time_updated` integer NOT NULL,
          `time_initialized` integer,
          `sandboxes` text NOT NULL,
          `commands` text
        );
INSERT INTO "project" (id,worktree,vcs,name,icon_url,icon_url_override,icon_color,time_created,time_updated,time_initialized,sandboxes,commands) VALUES ('global','/',NULL,NULL,NULL,NULL,NULL,1792653046756,1792653235914,NULL,'[]',NULL);
CREATE TABLE `session` (
          `id` text PRIMARY KEY,
          `project_id` text NOT NULL,
          `workspace_id` text,
          `parent_id` text,
          `slug` text NOT NULL,
          `directory` text NOT NULL,
          `path` text,
          `title` text NOT NULL,
          `version` text NOT NULL,
          `share_url` text,
          `summary_additions` integer,
          `summary_deletions` integer,
          `summary_files` integer,
          `summary_diffs` text,
          `metadata` text,
          `cost` real DEFAULT 0 NOT NULL,
          `tokens_input` integer DEFAULT 0 NOT NULL,
          `tokens_output` integer DEFAULT 0 NOT NULL,
          `tokens_reasoning` integer DEFAULT 0 NOT NULL,
          `tokens_cache_read` integer DEFAULT 0 NOT NULL,
          `tokens_cache_write` integer DEFAULT 0 NOT NULL,
          `revert` text,
          `permission` text,
          `agent` text,
          `model` text,
          `time_created` integer NOT NULL,
          `time_updated` integer NOT NULL,
          `time_compacting` integer,
          `time_archived` integer,
          CONSTRAINT `fk_session_project_id_project_id_fk` FOREIGN KEY (`project_id`) REFERENCES `project`(`id`) ON DELETE CASCADE
        );
INSERT INTO "session" (id,project_id,workspace_id,parent_id,slug,directory,path,title,version,share_url,summary_additions,summary_deletions,summary_files,summary_diffs,metadata,cost,tokens_input,tokens_output,tokens_reasoning,tokens_cache_read,tokens_cache_write,revert,permission,agent,model,time_created,time_updated,time_compacting,time_archived) VALUES ('ses_kiloTool002','global',NULL,NULL,'synthetic','/private/tmp/rh-kilo-capture','private/tmp/rh-kilo-capture','Synthetic Kilo tool history','7.8.15',NULL,NULL,NULL,NULL,NULL,NULL,0.0,0,0,0,0,0,NULL,NULL,NULL,NULL,1792653000000,1792653223919,NULL,NULL);
CREATE TABLE `message` (
          `id` text PRIMARY KEY,
          `session_id` text NOT NULL,
          `time_created` integer NOT NULL,
          `time_updated` integer NOT NULL,
          `data` text NOT NULL,
          CONSTRAINT `fk_message_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
        );
INSERT INTO "message" (id,session_id,time_created,time_updated,data) VALUES ('msg_kiloToolUser002','ses_kiloTool002',1792653001000,1792653182039,'{"role":"user","time":{"created":1792653001000},"agent":"build","model":{"providerID":"synthetic","modelID":"synthetic"}}');
INSERT INTO "message" (id,session_id,time_created,time_updated,data) VALUES ('msg_kiloToolAssistant002','ses_kiloTool002',1792653002000,1792653182040,'{"role":"assistant","time":{"created":1792653002000,"completed":1792653005000},"parentID":"msg_kiloToolUser002","modelID":"synthetic","providerID":"synthetic","mode":"build","agent":"build","path":{"cwd":"/private/tmp/rh-kilo-capture","root":"/private/tmp/rh-kilo-capture"},"cost":0,"tokens":{"input":0,"output":0,"reasoning":0,"cache":{"read":0,"write":0}}}');
INSERT INTO "message" (id,session_id,time_created,time_updated,data) VALUES ('msg_kiloResumeUser003','ses_kiloTool002',1792653010000,1792653223932,'{"role":"user","time":{"created":1792653010000},"agent":"build","model":{"providerID":"synthetic","modelID":"synthetic"}}');
INSERT INTO "message" (id,session_id,time_created,time_updated,data) VALUES ('msg_kiloResumeAssistant003','ses_kiloTool002',1792653011000,1792653223933,'{"role":"assistant","time":{"created":1792653011000,"completed":1792653012000},"parentID":"msg_kiloResumeUser003","modelID":"synthetic","providerID":"synthetic","mode":"build","agent":"build","path":{"cwd":"/private/tmp/rh-kilo-capture","root":"/private/tmp/rh-kilo-capture"},"cost":0,"tokens":{"input":0,"output":0,"reasoning":0,"cache":{"read":0,"write":0}}}');
CREATE TABLE `part` (
          `id` text PRIMARY KEY,
          `message_id` text NOT NULL,
          `session_id` text NOT NULL,
          `time_created` integer NOT NULL,
          `time_updated` integer NOT NULL,
          `data` text NOT NULL,
          CONSTRAINT `fk_part_message_id_message_id_fk` FOREIGN KEY (`message_id`) REFERENCES `message`(`id`) ON DELETE CASCADE
        );
INSERT INTO "part" (id,message_id,session_id,time_created,time_updated,data) VALUES ('prt_kiloToolUserText002','msg_kiloToolUser002','ses_kiloTool002',1792653182039,1792653182039,'{"type":"text","text":"Find the synthetic Kilo needle"}');
INSERT INTO "part" (id,message_id,session_id,time_created,time_updated,data) VALUES ('prt_kiloToolPart002','msg_kiloToolAssistant002','ses_kiloTool002',1792653182040,1792653182040,'{"type":"tool","callID":"call_kiloNative001","tool":"read","state":{"status":"completed","input":{"filePath":"/synthetic.txt"},"output":"Kilo tool result needle","title":"Read synthetic file","metadata":{},"time":{"start":1792653002000,"end":1792653003000}}}');
INSERT INTO "part" (id,message_id,session_id,time_created,time_updated,data) VALUES ('prt_kiloToolAssistantText002','msg_kiloToolAssistant002','ses_kiloTool002',1792653182040,1792653182040,'{"type":"text","text":"Found the synthetic Kilo needle."}');
INSERT INTO "part" (id,message_id,session_id,time_created,time_updated,data) VALUES ('prt_kiloResumeUserText003','msg_kiloResumeUser003','ses_kiloTool002',1792653223932,1792653223932,'{"type":"text","text":"Continue after restart"}');
INSERT INTO "part" (id,message_id,session_id,time_created,time_updated,data) VALUES ('prt_kiloResumeAssistantText003','msg_kiloResumeAssistant003','ses_kiloTool002',1792653223933,1792653223933,'{"type":"text","text":"Historical Kilo answer persists."}');
COMMIT;
PRAGMA foreign_keys=ON;
