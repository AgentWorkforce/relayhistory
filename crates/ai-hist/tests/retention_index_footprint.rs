//! Synthetic-only measurement of maintenance index storage and initial build cost.
#![cfg(feature = "unstable-internal")]
use ai_hist::open_db;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::time::Instant;

const SESSION: &str = "printf('00000000-0000-4000-8000-%012d',i%1000)";
fn seed(conn: &Connection) {
    let statements = [
        (1000, "sessions(source,session_id,last_activity_ms,raw_path)", format!("'codex',{SESSION},i,'/synthetic'")),
        (30000, "session_events(source,session_id,event_uid,role,kind,text,ts_ms)", format!("'codex',{SESSION},'event-'||i,'assistant','text',printf('%0128d',i),i")),
        (10000, "tool_calls(source,session_id,tool_use_id,name,args_json,ts_ms)", format!("'codex',{SESSION},'tool-'||i,'synthetic','{{}}',i")),
        (5000, "file_edits(source,session_id,tool_use_id,file_path,tool_name,ts_ms)", format!("'codex',{SESSION},'edit-'||i,'/synthetic/file.rs','edit',i")),
        (2000, "session_markers(source,session_id,marker_uid,kind,payload_json,ts_ms)", format!("'codex',{SESSION},'marker-'||i,'unknown','{{}}',i")),
        (6000, "history(source,session_id,prompt,timestamp_ms)", format!("'codex',{SESSION},printf('%064d',i),i")),
        (4000, "history(source,prompt,timestamp_ms)", "'codex',printf('%064d',i),i+100000".into()),
        (1000, "session_observations(source,session_id,location,connector_id,connector_instance,updated_ms)", format!("'codex',{SESSION},'remote','synthetic','fixture',i")),
        (5000, "observation_evidence(source,session_id,location,connector_id,connector_instance,evidence_uid,payload_json)", format!("'codex',{SESSION},'remote','synthetic','fixture','evidence-'||i,'{{}}'")),
        (1000, "session_commit_links(source,session_id,repo,commit_sha,match_method,confidence,created_at_ms)", format!("'codex',{SESSION},'/synthetic','commit-'||i,'synthetic',1.0,i")),
        (1000, "session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms)", format!("'codex',{SESSION},'edge-'||i,printf('00000000-0000-4000-8000-%012d',(i+1)%1000),'continuation','observed','synthetic',i,i")),
    ];
    let tx = conn.unchecked_transaction().unwrap();
    for (rows, table, values) in statements {
        tx.execute_batch(&format!("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<{rows}) INSERT INTO {table} SELECT {values} FROM n;")).unwrap();
    }
    // The observation API normally maintains these versions alongside rows.
    // Populate them here so upgrade timing does not include fixture repair.
    tx.execute_batch("INSERT INTO observation_versions(source,session_id,location,connector_id,connector_instance,version) SELECT source,session_id,location,connector_id,connector_instance,revision FROM session_observations;").unwrap();
    tx.commit().unwrap();
}
fn page_counts(conn: &Connection) -> (i64, i64) {
    let pages = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let free = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .unwrap();
    (pages, free)
}
fn head(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT version FROM observation_clock WHERE singleton=1",
        [],
        |r| r.get(0),
    )
    .unwrap()
}
fn index_pages(conn: &Connection) -> Option<Vec<Value>> {
    conn.prepare("SELECT name,SUM(pgsize) FROM dbstat WHERE name GLOB 'idx_retention_*' GROUP BY name ORDER BY name")
        .ok()?.query_map([], |r|Ok(json!({"name":r.get::<_,String>(0)?,"bytes":r.get::<_,i64>(1)?}))).ok()?
        .collect::<Result<Vec<_>,_>>().ok()
}

#[test]
#[ignore = "synthetic storage/build measurement; run release with --ignored --nocapture"]
fn retention_index_storage_and_build_measurement() {
    let mut runs = Vec::new();
    for run in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("synthetic.db");
        let mut conn = open_db(&path).unwrap();
        let definitions: Vec<(String,String)> = conn.prepare("SELECT name,sql FROM sqlite_master WHERE type='index' AND name GLOB 'idx_retention_*' ORDER BY name")
            .unwrap().query_map([], |r|Ok((r.get(0)?,r.get(1)?))).unwrap().collect::<Result<_,_>>().unwrap();
        assert_eq!(definitions.len(), 12);
        for (name, _) in &definitions {
            conn.execute_batch(&format!("DROP INDEX {name}")).unwrap();
        }
        seed(&conn);
        // All compaction/checkpoints affect only this test's temporary database;
        // they prepare comparable file sizes and are excluded from build timers.
        conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let (before_pages, before_free) = page_counts(&conn);
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        let before_head = head(&conn);
        let migration_path = dir.path().join("migration.db");
        std::fs::copy(&path, &migration_path).unwrap();
        let migration_started = Instant::now();
        let migrated = open_db(&migration_path).unwrap();
        let migration_ms = migration_started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            head(&migrated),
            before_head,
            "index migration must not restamp evidence"
        );
        assert_eq!(
            migrated
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name GLOB 'idx_retention_*'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            12
        );
        drop(migrated);
        let started = Instant::now();
        let tx = conn.transaction().unwrap();
        let mut builds = Vec::new();
        for (name, sql) in &definitions {
            let index_started = Instant::now();
            tx.execute_batch(sql).unwrap();
            builds.push(json!({"name":name,"ddl_ms":index_started.elapsed().as_secs_f64()*1000.0}));
        }
        tx.commit().unwrap();
        let build_ms = started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(head(&conn), before_head);
        let (after_pages, after_free) = page_counts(&conn);
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        runs.push(json!({"run":run,"page_size":page_size,"before_pages":before_pages,"before_free_pages":before_free,"after_pages":after_pages,"after_free_pages":after_free,
            "allocated_bytes_before":(before_pages-before_free)*page_size,"allocated_bytes_after":(after_pages-after_free)*page_size,
            "added_allocated_bytes":((after_pages-after_free)-(before_pages-before_free))*page_size,
            "index_ddl_and_commit_ms":build_ms,"sdk_writable_upgrade_open_ms":migration_ms,
            "per_index_ddl":builds,"per_index_dbstat":index_pages(&conn),"checkpointed_file_bytes":std::fs::metadata(&path).unwrap().len()}));
    }
    println!(
        "retention_index_footprint={}",
        json!({"fixture":{"sessions":1000,"session_id_bytes":36,"events":30000,"tools":10000,"edits":5000,"markers":2000,"linked_prompts":6000,"anonymous_prompts":4000,"observations":1000,"observation_evidence":5000,"commit_links":1000,"relationships":1000},"runs":runs})
    );
}
