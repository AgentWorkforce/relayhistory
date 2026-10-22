use super::*;

#[test]
fn oversized_tool_result_keeps_decoded_search_prefix_and_encoded_page_bound() {
    let raw = format!("line one\nline two\n{}", "x".repeat(600_000));
    let payload = Value::String(raw);
    let facts = crate::ingest::tool_result_facts::ToolResultFacts::from_payload(&payload);
    let display = normalize::bounded_result(&payload, &facts);
    assert!(display.starts_with("line one\nline two\n"));
    assert!(display.contains("[Native Kilo result projection]"));
    assert!(Value::String(display).to_string().len() <= 512 * 1024);
}

#[test]
fn malformed_or_unsupported_database_schema_is_diagnostic() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE session(id TEXT); CREATE TABLE message(id TEXT); CREATE TABLE part(id TEXT);",
    )
    .unwrap();
    let error = storage::schema(&db).unwrap_err().to_string();
    assert!(error.contains("unsupported Kilo SQLite schema"));
}

#[test]
fn ancestor_scope_covers_database_and_sidecar() {
    let root = PathBuf::from("/synthetic/home/.local/share/kilo");
    let database = root.join("kilo.db");
    let ancestor = crate::ingest::path_scope::PathScope::from_paths([root.clone()]).unwrap();
    assert!(storage::in_scope(&database, Some(&ancestor)));
    let sidecar =
        crate::ingest::path_scope::PathScope::from_paths([root.join("kilo.db-wal")]).unwrap();
    assert!(storage::in_scope(&database, Some(&sidecar)));
}

#[test]
fn shallow_prompt_refuses_an_unbounded_run_of_ignored_parts() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE message(id TEXT,session_id TEXT,time_created INTEGER,data TEXT); CREATE TABLE part(id TEXT,message_id TEXT,data TEXT); INSERT INTO message VALUES('m','s',1,'{\"role\":\"user\"}');").unwrap();
    for index in 0..257 {
        db.execute("INSERT INTO part VALUES(?1,'m','{\"type\":\"text\",\"text\":\"ignored\",\"ignored\":true}')",[format!("p{index:03}")]).unwrap();
    }
    let error = shallow::first_prompt(&db, "s").unwrap_err();
    assert!(error.to_string().contains("bounded scan budget"));
}

#[test]
fn a_sole_broken_configured_root_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let broken = home.path().join("kilo.db");
    std::fs::create_dir(&broken).unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let mut wire = serde_json::to_value(roots).unwrap();
    wire["provider_roots"]["kilo"] = serde_json::json!([broken]);
    let roots: ProviderRoots = serde_json::from_value(wire).unwrap();
    let error = storage::enumerate(&roots, None).unwrap_err();
    assert!(error
        .to_string()
        .contains("configured database roots are unreadable"));
}
