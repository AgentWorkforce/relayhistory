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
