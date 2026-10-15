-- Native OpenCode 1.18.31 import output; unrelated application tables omitted.
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
INSERT INTO project VALUES('global','/',NULL,NULL,NULL,NULL,NULL,1792044634336,1792044643806,NULL,'[]',NULL);
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
INSERT INTO session VALUES('ses_baseline11831','global',NULL,NULL,'synthetic-retry','/private/tmp/rh-oc-2hlvm69u/work','private/tmp/rh-oc-2hlvm69u/work','Synthetic retry baseline','1.18.31',NULL,NULL,NULL,NULL,NULL,NULL,0.0,0,0,0,0,0,NULL,NULL,NULL,NULL,1792018800000,1792018801000,NULL,NULL);
CREATE TABLE `message` (
          `id` text PRIMARY KEY,
          `session_id` text NOT NULL,
          `time_created` integer NOT NULL,
          `time_updated` integer NOT NULL,
          `data` text NOT NULL,
          CONSTRAINT `fk_message_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
        );
INSERT INTO message VALUES('msg_baseline11831a','ses_baseline11831',1792018801000,1792044636474,'{"role":"assistant","time":{"created":1792018801000},"parentID":"msg_baseline11831u","modelID":"fixture-model","providerID":"fixture","mode":"build","agent":"build","path":{"cwd":"/fixture/opencode","root":"/fixture/opencode"},"cost":0,"tokens":{"input":5,"output":7,"reasoning":0,"cache":{"read":0,"write":0}},"finish":"stop"}');
INSERT INTO message VALUES('msg_baseline11831u','ses_baseline11831',1792018800000,1792044636454,'{"role":"user","time":{"created":1792018800000},"agent":"build","model":{"providerID":"fixture","modelID":"fixture-model"}}');
CREATE TABLE `part` (
          `id` text PRIMARY KEY,
          `message_id` text NOT NULL,
          `session_id` text NOT NULL,
          `time_created` integer NOT NULL,
          `time_updated` integer NOT NULL,
          `data` text NOT NULL,
          CONSTRAINT `fk_part_message_id_message_id_fk` FOREIGN KEY (`message_id`) REFERENCES `message`(`id`) ON DELETE CASCADE
        );
INSERT INTO part VALUES('prt_baseline11831tool','msg_baseline11831a','ses_baseline11831',1792044636494,1792044636494,'{"type":"tool","callID":"call_baseline11831","tool":"bash","state":{"status":"completed","input":{"command":"printf synthetic"},"output":"synthetic","title":"Synthetic command","metadata":{"exit":0},"time":{"start":1792018800100,"end":1792018800200}}}');
INSERT INTO part VALUES('prt_msg_baseline11831a','msg_baseline11831a','ses_baseline11831',1792044636486,1792044636486,'{"type":"text","text":"The synthetic retry is bounded."}');
INSERT INTO part VALUES('prt_msg_baseline11831u','msg_baseline11831u','ses_baseline11831',1792044636455,1792044636455,'{"type":"text","text":"Inspect the synthetic retry fixture."}');
COMMIT;
