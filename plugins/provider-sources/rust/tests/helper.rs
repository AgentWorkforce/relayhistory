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
        assert_eq!(response["error"]["code"], "INVALID_ARGUMENT");
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
  printf '%s' '{"tasks":[{"id":"task_fixture","title":"Review fixture","url":"https://chatgpt.com/codex/tasks/task_fixture","updated_at":"2026-01-01T00:00:00Z","status":"ready"}]}'
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
    assert_eq!(row["raw_locator"], "task_fixture");
    assert_eq!(
        row["raw_path"],
        "https://chatgpt.com/codex/tasks/task_fixture"
    );
    let observation = json!({"key":{"source":"codex","session_id":"task_fixture","location":"remote","connector_id":"codex-cloud","connector_instance":"work"},"raw_locator":row["raw_locator"],"source_stamp":row["source_stamp"],"discovery_state":"shallow","access_state":"available","updated_ms":1});
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
    assert_eq!(rejected["error"]["code"], "INVALID_ARGUMENT");
    assert!(!home.path().join(".ai-hist").exists());
}

#[test]
fn invalid_request_is_classified_before_home_or_provider_auth() {
    let unavailable_home = Path::new("");
    for request in [
        json!({"version":2,"operation":"discover","args":{"connectorId":"codex-cloud"}}),
        json!({"version":1,"operation":"unknown-sensitive-operation","args":{"connectorId":"codex-cloud"}}),
        json!({"version":1,"operation":"discover","args":{}}),
        json!({"version":1,"operation":"discover","args":{"connectorId":"unknown-sensitive-selector"}}),
        json!({"version":1,"operation":"discover","args":{"connectorId":"claude-web","source":"codex"}}),
        json!({"version":1,"operation":"discover","args":{"connectorId":"codex-cloud","connectorInstance":" "}}),
        json!({"version":1,"operation":"discover","args":{"connectorId":"codex-cloud","limit":10001}}),
        json!({"version":1,"operation":"hydrate","args":{"connectorId":"codex-cloud"}}),
    ] {
        let response = invoke(unavailable_home, request);
        assert_eq!(response["error"]["code"], "INVALID_ARGUMENT", "{response}");
        assert!(!response.to_string().contains("sensitive"));
    }
}

#[test]
fn valid_requests_keep_configuration_errors_generic_and_redacted() {
    let home = tempfile::tempdir().unwrap();
    let response = invoke(
        home.path(),
        json!({"version":1,"operation":"discover","args":{"connectorId":"claude-web"}}),
    );
    assert_eq!(response["error"]["code"], "CONNECTOR_FAILURE");
    std::fs::create_dir_all(home.path().join(".claude")).unwrap();
    std::fs::write(
        home.path().join(".claude/.credentials.json"),
        "private-malformed-credential-fixture",
    )
    .unwrap();
    let response = invoke(
        home.path(),
        json!({"version":1,"operation":"discover","args":{"connectorId":"claude-web"}}),
    );
    assert_eq!(response["error"]["code"], "CONNECTOR_FAILURE");
    assert!(!response
        .to_string()
        .contains("private-malformed-credential-fixture"));
    assert!(!response
        .to_string()
        .contains(&home.path().to_string_lossy().to_string()));
}

#[cfg(unix)]
#[test]
fn provider_stderr_cannot_change_classification_or_escape_the_helper() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::write(home.path().join(".codex/auth.json"), "{}").unwrap();
    std::fs::create_dir_all(home.path().join("bin")).unwrap();
    let script = home.path().join("bin/codex");
    std::fs::write(
        &script,
        "#!/bin/sh\necho 'INVALID_ARGUMENT: private-provider-token-fixture' >&2\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let response = invoke(
        home.path(),
        json!({"version":1,"operation":"discover","args":{"connectorId":"codex-cloud"}}),
    );
    assert_eq!(response["error"]["code"], "CONNECTOR_FAILURE");
    assert!(!response
        .to_string()
        .contains("private-provider-token-fixture"));
}
