use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
fn invoke(home: &Path, request: Value) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_history-provider-sources"))
        .env("HOME", home)
        .env("PATH", home.join("bin"))
        .env_remove("RELAYHISTORY_CLAUDE_CREDENTIALS")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&request).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn explicit_selection_rejects_commercial_connectors_and_does_not_create_history() {
    let home = tempfile::tempdir().unwrap();
    for connector in ["cloud", "relaycast", "unknown-secret-fixture"] {
        let response = invoke(
            home.path(),
            json!({"version":1,"operation":"discover","args":{"connectorId":connector}}),
        );
        assert_eq!(response["ok"], false);
        assert!(!response.to_string().contains("secret-fixture"));
    }
    assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
}
#[cfg(unix)]
#[test]
fn real_helper_lists_codex_and_normalizes_only_file_edit_capability() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::write(home.path().join(".codex/auth.json"), "{}").unwrap();
    std::fs::create_dir_all(home.path().join("bin")).unwrap();
    let script = home.path().join("bin/codex");
    std::fs::write(&script, r#"#!/bin/sh
if [ "$1 $2" = 'cloud list' ]; then
  printf '%s' '{"tasks":[{"id":"task_fixture","title":"Review fixture","updated_at":"2026-01-01T00:00:00Z","status":"ready"}]}'
elif [ "$1 $2 $3" = 'cloud diff task_fixture' ]; then
  printf '%s\n' 'diff --git a/example.ts b/example.ts' '--- a/example.ts' '+++ b/example.ts' '@@ -1 +1 @@' '-old' '+new'
else
  exit 2
fi
"#).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let discovered = invoke(
        home.path(),
        json!({"version":1,"operation":"discover","args":{"connectorId":"codex-cloud","connectorInstance":"work","source":"codex","limit":10}}),
    );
    assert_eq!(discovered["ok"], true, "{discovered}");
    let row = &discovered["value"]["observations"][0];
    assert_eq!(row["session_id"], "task_fixture");
    let observation = json!({"key":{"source":"codex","session_id":"task_fixture","location":"remote","connector_id":"codex-cloud","connector_instance":"work"},"raw_locator":row["raw_path"],"source_stamp":row["source_stamp"],"discovery_state":"shallow","access_state":"available","updated_ms":1});
    let hydrated = invoke(
        home.path(),
        json!({"version":1,"operation":"hydrate","args":{"connectorId":"codex-cloud","connectorInstance":"work","observation":observation}}),
    );
    assert_eq!(hydrated["ok"], true, "{hydrated}");
    assert_eq!(hydrated["value"]["covered_kinds"], json!(["file_edit"]));
    assert_eq!(hydrated["value"]["records"].as_array().unwrap().len(), 1);
    assert_eq!(
        hydrated["value"]["records"][0]["payload"]["source"],
        "codex"
    );
    assert_eq!(
        hydrated["value"]["records"][0]["payload"]["session_id"],
        "task_fixture"
    );
    assert!(hydrated["value"]["records"][0]["payload"]
        .get("id")
        .is_none());
    let rejected = invoke(
        home.path(),
        json!({"version":1,"operation":"hydrate","args":{"connectorId":"codex-cloud","connectorInstance":"other","observation":observation}}),
    );
    assert_eq!(rejected["ok"], false);
    assert!(!home.path().join(".ai-hist").exists());
}
