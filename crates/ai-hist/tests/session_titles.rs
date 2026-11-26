//! The name a harness gave a session reaches the catalog's `title`, and a
//! rename replaces it.

use ai_hist::{
    ChangeKind, ChangeQuery, DiscoveryOptions, ProviderRoots, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, Watermark,
};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const CLAUDE_ID: &str = "11111111-1111-1111-1111-111111111111";
const CODEX_ID: &str = "sess_simple_1";

fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(path)
}

fn store(home: &Path) -> SessionStore {
    let options = StoreOptions::default()
        .db_path(home.join("ai-history.db"))
        .roots(ProviderRoots::from_home(
            home.to_path_buf(),
            home.join(".local/share/opencode/opencode.db"),
        ));
    SessionStore::open(options).expect("open")
}

fn append(path: &Path, line: &str) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    writeln!(file, "{line}").unwrap();
}

fn title(store: &SessionStore, source: Source, id: &str, include_text: bool) -> Option<String> {
    let mut query = SessionQuery::default();
    query.include_text = include_text;
    store
        .session(&SessionRef::id(source, id), query)
        .unwrap()
        .expect("catalogued")
        .session
        .title
}

#[test]
fn a_claude_title_is_catalogued_and_a_rename_replaces_it() {
    let home = tempfile::tempdir().unwrap();
    let project = home.path().join(".claude/projects/-tmp-project");
    fs::create_dir_all(&project).unwrap();
    let transcript = project.join(format!("{CLAUDE_ID}.jsonl"));
    fs::copy(fixture("claude/simple-turn.jsonl"), &transcript).unwrap();
    append(
        &transcript,
        &format!(r#"{{"type":"ai-title","aiTitle":"Say hello","sessionId":"{CLAUDE_ID}"}}"#),
    );
    let store = store(home.path());
    store.sync(Default::default()).unwrap();
    assert_eq!(
        title(&store, Source::Claude, CLAUDE_ID, true).as_deref(),
        Some("Say hello")
    );

    append(
        &transcript,
        &format!(
            r#"{{"type":"custom-title","customTitle":"Greeting test","sessionId":"{CLAUDE_ID}"}}"#
        ),
    );
    append(
        &transcript,
        &format!(
            r#"{{"type":"agent-name","agentName":"Greeting test","sessionId":"{CLAUDE_ID}"}}"#
        ),
    );
    store.sync(Default::default()).unwrap();
    assert_eq!(
        title(&store, Source::Claude, CLAUDE_ID, true).as_deref(),
        Some("Greeting test")
    );
    // The title is text, so a read that asked for none carries none.
    assert_eq!(title(&store, Source::Claude, CLAUDE_ID, false), None);
}

/// The Agent Relay worker name the session was addressed by.
const RELAY_AGENT: &str = "agent-relay-social-animation";

fn agent_name(store: &SessionStore, id: &str) -> Option<String> {
    store
        .session(&SessionRef::id(Source::Claude, id), SessionQuery::default())
        .unwrap()
        .expect("catalogued")
        .session
        .agent_name
}

fn claude_transcript_with_agent_name(home: &Path) -> PathBuf {
    let project = home.join(".claude/projects/-tmp-project");
    fs::create_dir_all(&project).unwrap();
    let transcript = project.join(format!("{CLAUDE_ID}.jsonl"));
    fs::copy(fixture("claude/simple-turn.jsonl"), &transcript).unwrap();
    append(
        &transcript,
        &format!(
            r#"{{"type":"agent-name","agentName":"{RELAY_AGENT}","sessionId":"{CLAUDE_ID}"}}"#
        ),
    );
    append(
        &transcript,
        &format!(
            r#"{{"type":"custom-title","customTitle":"Changelog videos","sessionId":"{CLAUDE_ID}"}}"#
        ),
    );
    transcript
}

/// A custom title outranks the agent name for `title`, so the agent name is
/// catalogued in its own column: it is what other agents look the session up by.
#[test]
fn a_claude_agent_name_is_catalogued_apart_from_the_title() {
    let home = tempfile::tempdir().unwrap();
    claude_transcript_with_agent_name(home.path());
    let store = store(home.path());
    store.sync(Default::default()).unwrap();
    assert_eq!(
        title(&store, Source::Claude, CLAUDE_ID, true).as_deref(),
        Some("Changelog videos")
    );
    assert_eq!(agent_name(&store, CLAUDE_ID).as_deref(), Some(RELAY_AGENT));
    // Like the title, it can carry transcript text, so a read that asked for no
    // text carries none.
    let mut no_text = SessionQuery::default();
    no_text.include_text = false;
    let content_off = store
        .session(&SessionRef::id(Source::Claude, CLAUDE_ID), no_text)
        .unwrap()
        .expect("catalogued")
        .session;
    assert_eq!(content_off.agent_name, None);

    // A later rename by the relay replaces it; a read that finds none keeps it.
    let transcript = home
        .path()
        .join(format!(".claude/projects/-tmp-project/{CLAUDE_ID}.jsonl"));
    append(
        &transcript,
        &format!(
            r#"{{"type":"agent-name","agentName":"renamed-worker","sessionId":"{CLAUDE_ID}"}}"#
        ),
    );
    store.sync(Default::default()).unwrap();
    assert_eq!(
        agent_name(&store, CLAUDE_ID).as_deref(),
        Some("renamed-worker")
    );
}

/// A rename that touches only `agent_name` is still a catalog change, even when
/// discovery stamped the transcript first and the title and activity are stable.
#[test]
fn an_agent_only_rename_is_reported_as_changed() {
    let home = tempfile::tempdir().unwrap();
    let transcript = claude_transcript_with_agent_name(home.path());
    let store = store(home.path());
    store.sync(Default::default()).unwrap();
    append(
        &transcript,
        &format!(
            r#"{{"type":"agent-name","agentName":"renamed-worker","sessionId":"{CLAUDE_ID}"}}"#
        ),
    );
    store.discover(DiscoveryOptions::default()).unwrap();
    let report = store.sync(Default::default()).unwrap();
    assert_eq!(
        agent_name(&store, CLAUDE_ID).as_deref(),
        Some("renamed-worker")
    );
    assert_eq!(
        title(&store, Source::Claude, CLAUDE_ID, true).as_deref(),
        Some("Changelog videos")
    );
    assert!(
        report
            .changed
            .contains(&SessionRef::id(Source::Claude, CLAUDE_ID)),
        "an agent-only rename is a catalog change: {:?}",
        report.changed
    );
}

#[test]
fn discovery_alone_catalogs_a_claude_agent_name() {
    let home = tempfile::tempdir().unwrap();
    claude_transcript_with_agent_name(home.path());
    let store = store(home.path());
    store.discover(DiscoveryOptions::default()).unwrap();
    assert_eq!(agent_name(&store, CLAUDE_ID).as_deref(), Some(RELAY_AGENT));
}

/// The delivered catalog record carries it, so a destination can index it.
#[test]
fn the_change_feed_session_record_carries_the_agent_name() {
    let home = tempfile::tempdir().unwrap();
    claude_transcript_with_agent_name(home.path());
    let store = store(home.path());
    store.sync(Default::default()).unwrap();
    let records: Vec<serde_json::Value> = store
        .changes_since(
            Watermark::START,
            ChangeQuery::default().kinds([ChangeKind::Session]),
        )
        .unwrap()
        .map(|change| change.unwrap())
        .filter(|change| change.session_id == CLAUDE_ID)
        .filter_map(|change| change.columns)
        .map(|columns| serde_json::to_value(columns).unwrap())
        .collect();
    assert!(
        records
            .iter()
            .any(|row| row["agent_name"] == serde_json::json!(RELAY_AGENT)),
        "session record carries agent_name: {records:?}"
    );
}

#[test]
fn a_codex_thread_name_is_catalogued_and_a_rename_replaces_it() {
    let home = tempfile::tempdir().unwrap();
    let codex = home.path().join(".codex");
    let day = codex.join("sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::copy(
        fixture("codex/simple-turn.jsonl"),
        day.join(format!("rollout-2026-04-20T00-00-00-{CODEX_ID}.jsonl")),
    )
    .unwrap();
    let index = codex.join("session_index.jsonl");
    fs::write(
        &index,
        format!("{{\"id\":\"{CODEX_ID}\",\"thread_name\":\"First name\"}}\n"),
    )
    .unwrap();
    let store = store(home.path());
    store.sync(Default::default()).unwrap();
    assert_eq!(
        title(&store, Source::Codex, CODEX_ID, true).as_deref(),
        Some("First name")
    );

    // Only the index moves on a rename; the sweep must still notice, and
    // report the session as changed.
    append(
        &index,
        &format!("{{\"id\":\"{CODEX_ID}\",\"thread_name\":\"Second name\"}}"),
    );
    let report = store.sync(Default::default()).unwrap();
    assert!(
        report
            .changed
            .contains(&SessionRef::id(Source::Codex, CODEX_ID)),
        "a rename is a catalog change: {:?}",
        report.changed
    );
    assert_eq!(
        title(&store, Source::Codex, CODEX_ID, true).as_deref(),
        Some("Second name")
    );
}

#[test]
fn discovery_alone_catalogs_a_codex_thread_name() {
    let home = tempfile::tempdir().unwrap();
    let codex = home.path().join(".codex");
    let day = codex.join("sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::copy(
        fixture("codex/simple-turn.jsonl"),
        day.join(format!("rollout-2026-04-20T00-00-00-{CODEX_ID}.jsonl")),
    )
    .unwrap();
    fs::write(
        codex.join("session_index.jsonl"),
        format!("{{\"id\":\"{CODEX_ID}\",\"thread_name\":\"Fix tests\"}}\n"),
    )
    .unwrap();
    let store = store(home.path());
    store.discover(DiscoveryOptions::default()).unwrap();
    assert_eq!(
        title(&store, Source::Codex, CODEX_ID, true).as_deref(),
        Some("Fix tests")
    );
}

#[test]
fn grok_cli_titles_reach_discovery_hydration_and_sync() {
    for operation in ["discover", "hydrate", "sync"] {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join(".grok/sessions/%2Fwork%2Fapp/grok-title");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("chat_history.jsonl"),
            "{\"type\":\"user\",\"content\":\"A prompt, not the title\"}\n",
        )
        .unwrap();
        let summary = dir.join("summary.json");
        let write_title = |name: &str| {
            fs::write(
                &summary,
                serde_json::json!({
                    "info": {"id": "grok-title", "cwd": "/work/app"},
                    "generated_title": name,
                    "session_summary": "Previous name",
                    "created_at": "2026-10-19T15:39:08.995237Z"
                })
                .to_string(),
            )
            .unwrap();
        };
        write_title("Drop Python SDK and open the PR");
        let store = store(home.path());
        let session = SessionRef::id(Source::Grok, "grok-title");
        let capture = || match operation {
            "discover" => {
                store.discover(DiscoveryOptions::default()).unwrap();
            }
            "hydrate" => {
                store.hydrate(&session, Default::default()).unwrap();
            }
            _ => {
                store.sync(Default::default()).unwrap();
            }
        };
        // Hydration targets a discovered identity; it does not enumerate.
        if operation == "hydrate" {
            store.discover(DiscoveryOptions::default()).unwrap();
        }
        capture();
        assert_eq!(
            title(&store, Source::Grok, "grok-title", true).as_deref(),
            Some("Drop Python SDK and open the PR"),
            "{operation}"
        );
        assert_eq!(title(&store, Source::Grok, "grok-title", false), None);

        // Only summary.json changes; transcript bytes remain untouched.
        write_title("Remove Python SDK");
        capture();
        assert_eq!(
            title(&store, Source::Grok, "grok-title", true).as_deref(),
            Some("Remove Python SDK"),
            "{operation} must notice summary-only renames"
        );

        if operation != "sync" {
            // The previous reader saved these same source bytes without the
            // Grok title-reader generation, and left title null. Both shallow
            // discovery and hydration must repair that cache without a file edit.
            let db = rusqlite::Connection::open(home.path().join("ai-history.db")).unwrap();
            db.execute("UPDATE sessions SET title=NULL WHERE source='grok'", [])
                .unwrap();
            for table in [
                "sessions",
                "session_presences",
                "session_observations",
                "session_hydration_checkpoints",
                "observation_hydration_checkpoints",
            ] {
                db.execute(&format!(
                    "UPDATE {table} SET source_stamp=replace(source_stamp,'|titles:v1','') WHERE source='grok'"
                ), []).unwrap();
            }
            capture();
            assert_eq!(
                title(&store, Source::Grok, "grok-title", true).as_deref(),
                Some("Remove Python SDK"),
                "{operation} must repair an older cache"
            );
        }
    }
}
