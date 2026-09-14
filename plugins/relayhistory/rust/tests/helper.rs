use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};
fn invoke(home: &std::path::Path, request: &Value) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_relayhistory-plugin"))
        .env("HOME", home)
        .env("RELAYHISTORY_HOME", home.join("auth"))
        .env_remove("RELAYHISTORY_BASE_URL")
        .env_remove("AI_HIST_BASE_URL")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(request).unwrap())
        .unwrap();
    child.wait_with_output().unwrap()
}
#[test]
fn absent_auth_returns_json_without_creating_history_or_credentials() {
    let home = tempfile::tempdir().unwrap();
    let output = invoke(
        home.path(),
        &json!({"version":1,"operation":"cloudLoadAuth","args":{"baseUrl":"https://history.agentrelay.com"}}),
    );
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"version":1,"ok":true,"value":null})
    );
    assert!(output.stderr.is_empty());
    assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
}
#[test]
fn helper_errors_do_not_echo_supplied_secrets_or_malformed_input() {
    let home = tempfile::tempdir().unwrap();
    for request in [
        json!({"version":1,"operation":"secret-bearer-fixture","args":{}}),
        json!({"version":1,"operation":"cloudLogin","args":{"baseUrl":"invalid-secret-fixture","relayAccessToken":"secret-bearer-fixture"}}),
    ] {
        let output = invoke(home.path(), &request);
        assert!(output.status.success());
        let response: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(response["ok"], false);
        let text =
            String::from_utf8(output.stdout).unwrap() + &String::from_utf8(output.stderr).unwrap();
        assert!(!text.contains("secret-bearer-fixture"));
        assert!(!text.contains("invalid-secret-fixture"));
    }
    let output = invoke(
        home.path(),
        &json!({"version":1,"operation":"accessToken","unexpected":"secret-bearer-fixture"}),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8(output.stderr)
        .unwrap()
        .contains("secret-bearer-fixture"));
}
#[test]
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn migration_status_without_home_fails_closed_without_reading_schedulers() {
    let output = invoke(
        std::path::Path::new(""),
        &json!({"version":1,"operation":"deliveryMigrationStatus"}),
    );
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"version":1,"ok":true,"value":{"state":"unknown","jobs":["home-unavailable"]}})
    );
    assert!(output.stderr.is_empty());
}
