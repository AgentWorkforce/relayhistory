use ai_hist::{
    hydrate_session_at, open_db,
    sources::{ConnectorIdentity, SourceRegistry},
    sync_scoped_at, HydrateSessionOptions, SessionScope, SessionStore, StoreOptions, SyncOptions,
};
use rusqlite::Connection;
use std::{fs, path::Path, process::Command};

fn write(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, body).unwrap();
}

#[test]
fn configured_provider_roots_drive_sync_discovery_and_hydration() {
    let dir = tempfile::tempdir().unwrap();
    let claude = dir.path().join("relocated-claude");
    let codex = dir.path().join("relocated-codex");
    let grok = dir.path().join("relocated-grok");
    let db = dir.path().join("history.db");

    write(
        &claude.join("projects/app/claude-env.jsonl"),
        concat!(
            r#"{"sessionId":"claude-env","uuid":"c1","cwd":"/work/app","type":"user","message":{"role":"user","content":"relocated claude session"},"timestamp":"2026-09-20T01:00:00Z"}"#,
            "\n",
            r#"{"sessionId":"claude-env","uuid":"c2","type":"assistant","message":{"role":"assistant","content":"done"},"timestamp":"2026-09-20T01:00:01Z"}"#,
            "\n",
        ),
    );
    write(
        &claude.join("history.jsonl"),
        "{\"display\":\"relocated claude history\",\"timestamp\":1,\"sessionId\":\"claude-history\"}\n",
    );
    write(
        &codex.join("sessions/2026/09/20/rollout-codex-env.jsonl"),
        concat!(
            r#"{"timestamp":"2026-09-20T02:00:00Z","type":"session_meta","payload":{"id":"codex-env","cwd":"/work/app","originator":"codex_cli_rs"}}"#,
            "\n",
            r#"{"timestamp":"2026-09-20T02:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"relocated codex session"}}"#,
            "\n",
            r#"{"timestamp":"2026-09-20T02:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#,
            "\n",
        ),
    );
    write(
        &codex.join("history.jsonl"),
        "{\"text\":\"relocated codex history\",\"ts\":2,\"session_id\":\"codex-history\"}\n",
    );

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "configured_provider_roots_child", "--nocapture"])
        .env("RH_ENV_ROOTS_DB", &db)
        .env("HOME", dir.path().join("empty-home"))
        .env("USERPROFILE", dir.path().join("empty-home"))
        .env("CLAUDE_CONFIG_DIR", &claude)
        .env("CODEX_HOME", &codex)
        .env("GROK_HOME", &grok)
        .env("OPENCODE_DB", dir.path().join("missing-opencode.db"))
        .env("XDG_DATA_HOME", dir.path().join("xdg"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn configured_provider_roots_child() {
    let Some(db) = std::env::var_os("RH_ENV_ROOTS_DB") else {
        return;
    };
    let db = Path::new(&db);
    sync_scoped_at(db, SessionScope::Local).unwrap();

    let conn = open_db(db).unwrap();
    for (source, session_id) in [("claude", "claude-env"), ("codex", "codex-env")] {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source=? AND session_id=?",
                [source, session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "{source} session was not discovered");
    }
    let history: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM history WHERE prompt IN ('relocated claude history', 'relocated codex history')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(history, 2, "relocated prompt histories were not ingested");
    drop(conn);

    for (source, session_id) in [("claude", "claude-env"), ("codex", "codex-env")] {
        let options = HydrateSessionOptions {
            source: source.into(),
            session_id: session_id.into(),
            scope: SessionScope::Local,
            include_related: false,
        };
        let result = hydrate_session_at(db, &options).unwrap();
        assert_eq!(result.status, "hydrated", "{source} hydration failed");
        let registry_result = SourceRegistry::local()
            .hydrate_at(db, &options, &ConnectorIdentity::new(source, "default"))
            .unwrap();
        assert_eq!(
            registry_result.status, "unchanged",
            "{source} registry hydration did not use the configured root"
        );
    }
}

#[test]
fn explicit_store_home_honors_provider_env_roots() {
    let dir = tempfile::tempdir().unwrap();
    let provider_db = dir.path().join("relocated-opencode.db");
    let codex = dir.path().join("relocated-codex");
    write(
        &codex.join("sessions/2026/09/20/rollout-codex-explicit-home.jsonl"),
        concat!(
            r#"{"timestamp":"2026-09-20T02:00:00Z","type":"session_meta","payload":{"id":"codex-explicit-home","cwd":"/work/app","originator":"codex_cli_rs"}}"#,
            "\n",
            r#"{"timestamp":"2026-09-20T02:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"relocated codex explicit home"}}"#,
            "\n",
        ),
    );
    let provider = Connection::open(&provider_db).unwrap();
    provider
        .execute_batch(
            r#"CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
               CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
               CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
               INSERT INTO session VALUES ('opencode-env', '/work/app', 1, 2);
               INSERT INTO message VALUES ('m1', 'opencode-env', 1, '{"role":"user","modelID":"test-model"}');
               INSERT INTO part VALUES ('p1', 'm1', 'opencode-env', 1, '{"type":"text","text":"relocated opencode session"}');"#,
        )
        .unwrap();
    drop(provider);

    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "explicit_store_home_honors_provider_env_roots_child",
            "--nocapture",
        ])
        .env("RH_EXPLICIT_HOME_DB", dir.path().join("history.db"))
        .env("RH_EXPLICIT_HOME", dir.path().join("empty-home"))
        .env("CODEX_HOME", codex)
        .env("OPENCODE_DB", provider_db)
        .env("XDG_DATA_HOME", dir.path().join("xdg"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn explicit_store_home_honors_provider_env_roots_child() {
    let (Some(db), Some(home)) = (
        std::env::var_os("RH_EXPLICIT_HOME_DB"),
        std::env::var_os("RH_EXPLICIT_HOME"),
    ) else {
        return;
    };
    let mut options = StoreOptions::default();
    options.db_path = Some(db.clone().into());
    options.home = Some(home.into());
    SessionStore::open(options)
        .unwrap()
        .sync(SyncOptions::default())
        .unwrap();

    let conn = open_db(Path::new(&db)).unwrap();
    for (source, session_id, variable) in [
        ("opencode", "opencode-env", "OPENCODE_DB"),
        ("codex", "codex-explicit-home", "CODEX_HOME"),
    ] {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source=? AND session_id=?",
                [source, session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "SessionStore ignored {variable}");
    }
}

#[test]
fn whitespace_provider_roots_fall_back_to_home() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "whitespace_provider_roots_child", "--nocapture"])
        .env("RH_ENV_ROOTS_HOME", dir.path())
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .env("CLAUDE_CONFIG_DIR", "  ")
        .env("CODEX_HOME", "\t")
        .env("GROK_HOME", "")
        .env("XDG_DATA_HOME", " ")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn whitespace_provider_roots_child() {
    let Some(home) = std::env::var_os("RH_ENV_ROOTS_HOME") else {
        return;
    };
    let home = Path::new(&home);
    assert_eq!(
        ai_hist::paths::claude_config_dir(home),
        home.join(".claude")
    );
    assert_eq!(ai_hist::paths::codex_home(home), home.join(".codex"));
    assert_eq!(ai_hist::paths::grok_home(home), home.join(".grok"));
    assert_eq!(
        ai_hist::paths::devin_cli_dir(home),
        home.join(".local/share/devin/cli")
    );
}
