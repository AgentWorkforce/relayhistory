//! Explicit operator maintenance over the existing store facade. Policy preview
//! never opens a writable store or estimates deletion counts.
use ai_hist::retention::{
    read_retention_generation, read_retention_policy, RetentionOptions, RetentionPolicy,
};
use ai_hist::{SessionStore, StoreOptions};
use serde::Deserialize;
use serde_json::{json, Value};

pub const OP_RETENTION: &str = "retention";
pub const OP_COMPACT: &str = "compact";

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Args {
    db_path: Option<String>,
    #[serde(default)]
    apply: bool,
    days: Option<u32>,
    #[serde(default)]
    allow_unuploaded: bool,
    max_rows: Option<u32>,
}
fn error(error: ai_hist::Error) -> napi::Error {
    crate::native_error(error.code(), error)
}

pub(crate) fn dispatch(op: &str, args_json: &str) -> napi::Result<String> {
    let args: Args =
        serde_json::from_str(args_json).map_err(|e| crate::native_error("INVALID_ARGUMENT", e))?;
    let path = crate::db_path(args.db_path);
    let result = if op == OP_COMPACT {
        if args.apply || args.days.is_some() || args.allow_unuploaded || args.max_rows.is_some() {
            return Err(crate::native_error(
                "INVALID_ARGUMENT",
                "compact accepts only dbPath",
            ));
        }
        if !path.is_file() {
            return Err(crate::native_error(
                "DATABASE_OPEN_FAILED",
                "no existing history database to compact",
            ));
        }
        // Opening a writable SessionStore here would migrate before the
        // compaction engine acquires its sync lock and checks free space.
        let started = std::time::Instant::now();
        let report = ai_hist::diagnostics::compact_database(&path).map_err(compact_error)?;
        json!({"dbBytesBefore":report.db_bytes_before,"walBytesBefore":report.wal_bytes_before,
            "dbBytesAfter":report.db_bytes_after,"walBytesAfter":report.wal_bytes_after,
            "reclaimableBefore":report.reclaimable_before,"walTruncated":report.wal_truncated,"elapsedMs":started.elapsed().as_millis() as u64})
    } else {
        retention(
            &path,
            args.apply,
            args.days,
            args.allow_unuploaded,
            args.max_rows,
        )?
    };
    serde_json::to_string(&result).map_err(|e| crate::native_error("INTERNAL_ERROR", e))
}

fn compact_error(error: anyhow::Error) -> napi::Error {
    let code = match error.downcast_ref::<ai_hist::diagnostics::CompactRefused>() {
        Some(ai_hist::diagnostics::CompactRefused::SyncRunning) => "SYNC_LOCKED",
        Some(_) => "INSUFFICIENT_SPACE",
        None => "DATABASE_QUERY_FAILED",
    };
    crate::native_error(code, format!("{error:#}"))
}

fn retention(
    path: &std::path::Path,
    apply: bool,
    days: Option<u32>,
    allow_unuploaded: bool,
    max_rows: Option<u32>,
) -> napi::Result<Value> {
    let max_rows = max_rows.unwrap_or(RetentionOptions::default().max_rows);
    if !(1..=1024).contains(&max_rows) || (allow_unuploaded && !apply) {
        return Err(crate::native_error(
            "INVALID_ARGUMENT",
            "maxRows must be 1..=1024; allowUnuploaded requires apply",
        ));
    }
    let stored = if path.exists() {
        read_retention_policy(path).map_err(error)?
    } else {
        RetentionPolicy::default()
    };
    let stored_days = stored.days.map_or(0, std::num::NonZeroU32::get);
    let effective = days.unwrap_or(stored_days);
    let mut report = Value::Null;
    let generation = if apply {
        let store = SessionStore::open(StoreOptions::default().db_path(path)).map_err(error)?;
        if days.is_some() {
            store
                .set_retention_policy(RetentionPolicy::from_days(effective))
                .map_err(error)?;
        }
        let result = store
            .prune_expired(RetentionOptions {
                max_rows,
                allow_unuploaded,
                ..RetentionOptions::default()
            })
            .map_err(error)?;
        report = json!({"rows":result.rows,"sessions":result.sessions,"protectedSessions":result.protected_sessions,
            "awaitingUploadAck":result.awaiting_upload_ack,"busy":result.busy,"deferred":result.deferred,"cycleComplete":result.cycle_complete});
        store.retention_generation().map_err(error)?
    } else if path.exists() {
        read_retention_generation(path).map_err(error)?
    } else {
        0
    };
    Ok(
        json!({"dryRun":!apply,"preview":"policy_only","storedDays":stored_days,"effectiveDays":effective,
        "maxRows":max_rows,"allowUnuploaded":allow_unuploaded,"generation":generation,"report":report}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn call(path: &std::path::Path, args: Value) -> Value {
        let mut args = args.as_object().unwrap().clone();
        args.insert("dbPath".into(), json!(path));
        serde_json::from_str(&dispatch(OP_RETENTION, &Value::Object(args).to_string()).unwrap())
            .unwrap()
    }
    #[test]
    fn policy_preview_never_creates_or_mutates_a_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        assert!(call(&path, json!({"days":7}))["dryRun"].as_bool().unwrap());
        assert!(!path.exists());
        let conn = ai_hist::open_db(&path).unwrap();
        let before = std::fs::read(&path).unwrap();
        let version = conn
            .pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
            .unwrap();
        let result = call(&path, json!({"days":7}));
        assert_eq!(result["effectiveDays"], 7);
        assert_eq!(result["storedDays"], 30);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(
            conn.pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            version
        );
        assert_eq!(
            read_retention_policy(&path).unwrap(),
            RetentionPolicy::default()
        );
        drop(conn);
    }
    #[test]
    fn apply_respects_ack_guard_and_bounds_each_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let conn = ai_hist::open_db(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id,last_activity_ms) VALUES('claude','old',1)",
            [],
        )
        .unwrap();
        for i in 0..8 {
            conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('claude','old',?1,1,'assistant','text','old')",[i.to_string()]).unwrap();
        }
        let guarded = call(&path, json!({"apply":true,"days":1,"maxRows":2}));
        assert_eq!(guarded["report"]["awaitingUploadAck"], true);
        assert_eq!(guarded["report"]["rows"], 0);
        let result = call(
            &path,
            json!({"apply":true,"allowUnuploaded":true,"maxRows":2}),
        );
        assert!(result["report"]["rows"].as_u64().unwrap() <= 2);
        let remaining: i64 = conn
            .query_row("SELECT count(*) FROM session_events", [], |r| r.get(0))
            .unwrap();
        assert!((6..8).contains(&remaining));
    }
    #[test]
    fn compact_refuses_a_held_sync_lock_without_migrating_an_old_schema() {
        use fs2::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE legacy_evidence(value TEXT); INSERT INTO legacy_evidence VALUES('kept');",
        )
        .unwrap();
        drop(conn);
        let before = std::fs::read(&path).unwrap();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join("history.db.sync.lock"))
            .unwrap();
        lock.try_lock_exclusive().unwrap();
        let failure =
            super::super::dispatch(OP_COMPACT, &json!({"dbPath":path}).to_string()).unwrap_err();
        assert!(failure.reason.contains("SYNC_LOCKED"), "{}", failure.reason);
        assert!(
            std::fs::read(&path).unwrap() == before,
            "refusal must precede schema migration"
        );
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT value FROM legacy_evidence", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "kept"
        );
    }

    #[test]
    fn malformed_and_unsafe_options_are_rejected_before_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        for mut args in [
            json!({"days":-1}),
            json!({"days":1.5}),
            json!({"maxRows":0}),
            json!({"maxRows":1025}),
            json!({"allowUnuploaded":true}),
            json!({"bogus":true}),
        ] {
            args["dbPath"] = json!(path);
            assert!(dispatch(OP_RETENTION, &args.to_string())
                .unwrap_err()
                .reason
                .contains("INVALID_ARGUMENT"));
        }
    }
}
