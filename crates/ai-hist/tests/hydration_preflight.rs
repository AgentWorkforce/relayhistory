//! Native callers must get a capability error before creating the ledger or
//! consulting credentials for an adapter that cannot hydrate.
use ai_hist::{
    hydrate_session_at_with_connectors, remote::SourceConnectorSelection, HydrateSessionOptions,
    SessionScope,
};

#[test]
fn commercial_only_or_unconfigured_provider_hydration_leaves_fresh_database_absent() {
    let dir = tempfile::tempdir().unwrap();
    let auth = dir.path().join("commercial");
    std::fs::create_dir_all(&auth).unwrap();
    std::fs::write(auth.join("auth.json"), "{poisoned commercial auth").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "hydration_preflight_child", "--nocapture"])
        .env("RH_HYDRATION_PREFLIGHT_HOME", dir.path())
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .env("RELAYHISTORY_HOME", auth)
        .env(
            "RELAYHISTORY_CLAUDE_CREDENTIALS",
            dir.path().join("absent-credentials.json"),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.path().join("history.db").exists());
}

#[test]
fn hydration_preflight_child() {
    let Some(home) = std::env::var_os("RH_HYDRATION_PREFLIGHT_HOME") else {
        return;
    };
    let db = std::path::PathBuf::from(home).join("history.db");
    for ids in [
        vec!["cloud".into()],
        vec!["cloud".into(), "claude-web".into()],
    ] {
        let selection = SourceConnectorSelection::new(ids).unwrap();
        let error = hydrate_session_at_with_connectors(
            &db,
            &HydrateSessionOptions {
                source: "claude".into(),
                session_id: "session_01missing".into(),
                scope: SessionScope::Remote,
                include_related: false,
            },
            &selection,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("CONNECTOR_NOT_CONFIGURED"),
            "{error}"
        );
        assert!(!error.to_string().contains("poisoned"), "{error}");
        assert!(!db.exists());
    }
}
