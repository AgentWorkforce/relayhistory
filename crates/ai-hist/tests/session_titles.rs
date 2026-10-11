//! The name a harness gave a session reaches the catalog's `title`, and a
//! rename replaces it.

use ai_hist::{
    DiscoveryOptions, ProviderRoots, SessionQuery, SessionRef, SessionStore, Source, StoreOptions,
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
