// Usage: node generate-delivery-native.mjs /path/to/built/relayhistory/sdk-ts
// Uses the client's synthetic gzip fixture and native contract 13+ public API.
import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";
import { DatabaseSync } from "node:sqlite";

const kinds = [
  "history",
  "session_event",
  "tool_call",
  "file_edit",
  "session",
  "presence",
  "relationship",
  "commit_link",
  "trajectory",
  "source_observation",
  "observation_evidence",
  "session_marker",
];
const sdk = resolve(process.argv[2]);
const api = await import(pathToFileURL(join(sdk, "dist/index.js")).href);
const root = await mkdtemp(join(tmpdir(), "delivery-wire-fixture-"));
try {
  const dbPath = join(root, "history.db");
  await writeFile(
    dbPath,
    gunzipSync(await readFile(join(sdk, "fixtures/offline-history.db.gz"))),
  );
  // Opening through the public API applies the current local schema before this
  // fixture adds one compact row for every versioned delivery evidence kind.
  await api.historyDeliveryStatus(undefined, { dbPath });
  const db = new DatabaseSync(dbPath);
  try {
    db.exec(`
      UPDATE session_events SET
        project_key='AgentWorkforce/relayhistory', project_key_method='git_remote',
        provider='anthropic', tool_use_id='fixture-tool', payload_bytes=2048,
        payload_truncated=0, payload_hash='fixture-payload-hash', call_index=1,
        event_index=2, result_status='completed', event_source='provider',
        error_signal='none', subagent_session_id='local-only', agent_id='agent-1',
        request_id='request-1', provider_message_id='message-1', stop_reason='end_turn',
        agent_version='1.2.3', is_sidechain=0, is_meta=0, turn_id='turn-1',
        request_span='request-1:0', raw_facts_version=2, raw_kind='user'
      WHERE source='claude' AND session_id='both';
      UPDATE sessions SET project_key='AgentWorkforce/relayhistory',
        project_key_method='git_remote' WHERE source='claude' AND session_id='both';
      INSERT INTO session_markers
        (source,session_id,marker_uid,ts_ms,message_id,parent_id,turn_id,kind,subkind,text,payload_json)
        VALUES ('claude','both','fixture-marker',1788256800001,'message-1',NULL,'turn-1',
          'compaction_boundary','summary','fixture marker','{"reason":"fixture"}');
      INSERT INTO tool_calls
        (source,session_id,message_id,tool_use_id,name,target,args_json,is_error,ts_ms)
        VALUES ('claude','both','message-1','fixture-tool','Read','fixture.ts',
          '{"path":"/fixture/project/fixture.ts"}',0,1788256800002);
      INSERT INTO file_edits
        (source,session_id,message_id,tool_use_id,file_path,tool_name,lines_added,lines_removed,
          structured_patch_json,user_modified,ts_ms,git_branch,cwd)
        VALUES ('claude','both','message-1','fixture-edit','/fixture/project/fixture.ts','Edit',
          1,0,'[{"new":"fixture"}]',1,1788256800003,'main','/fixture/project');
      INSERT INTO session_relationships
        (source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,
          child_agent_type,evidence_kind,evidence_ref,child_has_events,created_ms,updated_ms,
          origin_session_id)
        VALUES ('claude','both','fixture-relationship','local-only','delegated','observed',
          'worker','tool_use','fixture-tool',1,1788256800004,1788256800004,'remote-only');
      INSERT INTO session_commit_links
        (source,session_id,repo,branch,commit_sha,note_ref,match_method,confidence,files_json,
          numstat_json,evidence_json,created_at_ms)
        VALUES ('claude','both','AgentWorkforce/relayhistory','main','fixture-sha',NULL,'exact',1,
          '["fixture.ts"]','{"fixture.ts":{"added":1,"removed":0}}','{"kind":"fixture"}',
          1788256800005);
      INSERT INTO trajectories
        (id,version,persona_id,project_id,task_title,task_description,status,started_at,
          completed_at,decisions_json,retrospective_json,search_text,path,updated_ms,timestamp_ms)
        VALUES ('fixture-trajectory',1,'fixture','AgentWorkforce/relayhistory','Fixture task',
          'Exercise delivery','completed','2026-09-01T10:00:00Z','2026-09-01T10:01:00Z',
          '[]','{}','fixture delivery','/fixture/trajectory',1788256800006,1788256800006);
      INSERT OR IGNORE INTO session_observations
        (source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,
          discovery_state,access_state,updated_ms)
        VALUES ('claude','both','remote','fixture-connector','fixture','opaque:fixture','v1',
          'full','available',1788256800007);
      INSERT INTO observation_evidence
        (source,session_id,location,connector_id,connector_instance,evidence_uid,payload_json)
        VALUES ('claude','both','remote','fixture-connector','fixture','fixture-evidence',
          '{"event":{"role":"assistant","text":"fixture evidence"}}');
    `);
  } finally {
    db.close();
  }
  const job = await api.createHistoryDelivery(
    {
      destination_id: "relayhistory",
      instance_id: "synthetic-fixture",
      account_id:
        "relayhistory:" +
        createHash("sha256")
          .update(JSON.stringify(["org-fixture", "workspace-fixture"]))
          .digest("hex"),
      mapping_version: "relayhistory-delivery-v1",
      selection: {
        all_sources: true,
        sources: [],
        sessions: [],
        kinds,
        excluded_sessions: [],
      },
      limits: api.DEFAULT_DELIVERY_LIMITS,
    },
    { dbPath },
  );
  await api.deliveryRequest(
    { operation: "prepare_batch", job_id: job.job_id, now_ms: Date.now() },
    { dbPath },
  );
  const claimed = await api.deliveryRequest(
    {
      operation: "claim_batch",
      job_id: job.job_id,
      worker_id: "fixture-generator",
      lease_ms: 30000,
      now_ms: Date.now(),
    },
    { dbPath },
  );
  if (!claimed?.batch.records.length)
    throw new Error("synthetic native batch missing");
  await writeFile(
    new URL("./delivery-native-v1.json", import.meta.url),
    JSON.stringify({ protocolVersion: 1, batch: claimed.batch }, null, 2) +
      "\n",
  );
} finally {
  await rm(root, { recursive: true, force: true });
}
