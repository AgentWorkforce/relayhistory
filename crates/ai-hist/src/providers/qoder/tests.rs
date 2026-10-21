use super::*;

#[test]
fn global_cli_fixture_matches_pinned_shape() {
    let bytes = include_bytes!("../../../tests/fixtures/qoder/global-cli/.qoder/projects/-synthetic-qoder-project/synthetic-tool.jsonl");
    let entries = format::parse(bytes, false).unwrap();
    assert_eq!(
        format::string(&entries[0], "sessionId").unwrap(),
        "synthetic-tool"
    );
    assert_eq!(
        format::first_prompt(&entries).as_deref(),
        Some("Read note.txt.")
    );
    assert_eq!(entries.iter().filter(|e| e["type"] == "user").count(), 2);
    assert!(entries.iter().any(|e| e["message"]["content"]
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(|block| block["tool_use_id"] == "call-1"))));
}

#[test]
fn relative_config_root_is_frozen_against_the_opening_cwd() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let opening_cwd = root.path().join("first-workspace");
    let config = expand(&home, &opening_cwd, "relative-qoder-config");
    assert_eq!(
        config.join("projects"),
        opening_cwd.join("relative-qoder-config/projects")
    );
    assert_eq!(expand(&home, &opening_cwd, "~/.qoder"), home.join(".qoder"));
    assert_eq!(expand(&home, &opening_cwd, home.to_str().unwrap()), home);
}
