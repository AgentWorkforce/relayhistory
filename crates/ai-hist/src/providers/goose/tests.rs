use super::*;

fn roots(home: &Path) -> ProviderRoots {
    ProviderRoots::from_home(home.into(), home.join("opencode.db"))
}

#[test]
fn native_snapshot_reads_schema17_and_official_order() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join(".local/share/goose/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sessions.db");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(include_str!("../../../tests/fixtures/goose/schema17.sql"))
        .unwrap();
    let roots = roots(home.path());
    let context = AcquisitionContext::new(ID, &roots);
    let sessions = storage::all(&context, &path).unwrap();
    assert_eq!(sessions.len(), 1);
    let session = &sessions[0];
    assert_eq!(session.native_id, "20261022_1");
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/goose/schema17-official-export.json"
    ))
    .unwrap();
    let expected: Vec<_> = oracle["conversation"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    let actual: Vec<_> = session
        .messages
        .iter()
        .map(|m| m.native_id.as_deref().unwrap())
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn unsupported_schema_fails_before_presenting_empty_coverage() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join(".local/share/goose/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sessions.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_version(version INTEGER); INSERT INTO schema_version VALUES(99);",
    )
    .unwrap();
    let roots = roots(home.path());
    let context = AcquisitionContext::new(ID, &roots);
    assert!(storage::all(&context, &path)
        .unwrap_err()
        .to_string()
        .contains("unsupported Goose schema"));
}

#[test]
fn wal_only_append_is_in_snapshot_and_fingerprint() {
    let home = tempfile::tempdir().unwrap();
    let dir = home.path().join(".local/share/goose/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sessions.db");
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer
        .execute_batch(include_str!("../../../tests/fixtures/goose/schema17.sql"))
        .unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();
    writer.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    writer.execute("INSERT INTO messages(message_id,session_id,role,content_json,created_timestamp,metadata_json) VALUES(?1,?2,'user',?3,?4,?5)",rusqlite::params!["wal-native-1","20261022_1",r#"[{"type":"text","text":"WAL-only Goose turn"}]"#,1792642999,r#"{"userVisible":true,"agentVisible":true}"#]).unwrap();
    assert!(path.with_file_name("sessions.db-wal").exists());
    let roots = roots(home.path());
    let context = AcquisitionContext::new(ID, &roots);
    let captured = storage::all(&context, &path).unwrap();
    assert_eq!(captured[0].messages.len(), 7);
    assert_eq!(
        captured[0].messages[6].native_id.as_deref(),
        Some("wal-native-1")
    );
    assert!(storage::fingerprint_inputs(&roots)
        .unwrap()
        .iter()
        .any(|item| item.locator.ends_with("sessions.db-wal")));
}

#[cfg(unix)]
#[test]
fn escaped_database_symlink_is_rejected_before_open() {
    let home = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let dir = home.path().join(".local/share/goose/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let other = outside.path().join("sessions.db");
    rusqlite::Connection::open(&other).unwrap();
    std::os::unix::fs::symlink(&other, dir.join("sessions.db")).unwrap();
    assert!(storage::databases(&roots(home.path())).is_err());
}
