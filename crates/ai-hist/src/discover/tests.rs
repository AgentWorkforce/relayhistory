//! Fixture-backed tests for shallow session discovery.
//!
//! Every fixture is written inline into a temp directory and reached through an
//! explicit [`DiscoveryEnv`], so no test mutates process-wide environment
//! variables and the suite stays parallel-safe.
//!
//! Performance claims are asserted through [`DiscoveryCounters`] rather than a
//! wall clock: bounded reads mean a limited request opens a bounded number of
//! files and reads a bounded number of bytes no matter how large the archive
//! is, an unchanged rescan performs zero shallow reads, and the cache-only
//! listing performs no file I/O at all.

use super::*;
use crate::{init_db, mark_session_presence, upsert_session_presence};
use std::fs;
use std::time::{Duration, SystemTime};

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

fn catalog() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory database");
    init_db(&conn).expect("schema");
    conn
}

fn env_at<'a>(conn: &'a Connection, home: &Path) -> DiscoveryEnv<'a> {
    DiscoveryEnv::with_all_roots(
        conn,
        home.to_path_buf(),
        home.join(".claude"),
        home.join(".codex"),
        home.join(".grok"),
        home.join("opencode.db"),
    )
}

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    fs::write(path, contents).expect("write fixture");
}

/// Pin a file's mtime so recency ordering (and the change stamp) is
/// deterministic instead of depending on how fast the test ran.
fn set_mtime(path: &Path, ms: i64) {
    let file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open for mtime");
    let when = SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64);
    file.set_times(fs::FileTimes::new().set_modified(when))
        .expect("set mtime");
}

fn claude_session(home: &Path, id: &str, body: &str, mtime_ms: i64) -> PathBuf {
    let path = home.join(format!(".claude/projects/proj/{id}.jsonl"));
    write(&path, body);
    set_mtime(&path, mtime_ms);
    path
}

fn codex_rollout(home: &Path, id: &str, body: &str, mtime_ms: i64) -> PathBuf {
    let path = home.join(format!(".codex/sessions/2026/06/20/rollout-{id}.jsonl"));
    write(&path, body);
    set_mtime(&path, mtime_ms);
    path
}

fn cursor_session(home: &Path, project: &str, id: &str, body: &str, mtime_ms: i64) -> PathBuf {
    let path = home.join(format!(
        ".cursor/projects/{project}/agent-transcripts/{id}/{id}.jsonl"
    ));
    write(&path, body);
    set_mtime(&path, mtime_ms);
    path
}

fn grok_session(home: &Path, project: &str, id: &str, summary: &str, chat: &str, mtime_ms: i64) {
    let dir = home.join(format!(".grok/sessions/{project}/{id}"));
    write(&dir.join("summary.json"), summary);
    write(&dir.join("chat_history.jsonl"), chat);
    set_mtime(&dir.join("summary.json"), mtime_ms);
    set_mtime(&dir.join("chat_history.jsonl"), mtime_ms);
}

fn opencode_db(home: &Path, statements: &str) {
    fs::create_dir_all(home).expect("mkdir");
    let db = Connection::open(home.join("opencode.db")).expect("opencode db");
    db.execute_batch(&format!(
        "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
         CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE INDEX session_time_updated_id_idx ON session(time_updated DESC, id);
         CREATE INDEX message_session_time_created_id_idx ON message(session_id, time_created, id);
         CREATE INDEX part_session_idx ON part(session_id);
         CREATE INDEX part_message_id_id_idx ON part(message_id, id);
         {statements}"
    ))
    .expect("opencode fixture");
}

/// A realistic Claude transcript: a meta row, a slash-command wrapper, and a
/// sidechain (subagent) turn all precede the first real human prompt.
const CLAUDE_BODY: &str = concat!(
    r#"{"sessionId":"claude-1","cwd":"/work/app","gitBranch":"main","version":"1.2.3","type":"user","isMeta":true,"message":{"role":"user","content":"session bookkeeping"},"timestamp":"2026-06-20T10:00:00.000Z"}"#,
    "\n",
    r#"{"sessionId":"claude-1","type":"user","message":{"role":"user","content":"<command-name>/compact</command-name>"},"timestamp":"2026-06-20T10:00:01.000Z"}"#,
    "\n",
    r#"{"sessionId":"claude-1","type":"user","isSidechain":true,"message":{"role":"user","content":"subagent instruction"},"timestamp":"2026-06-20T10:00:02.000Z"}"#,
    "\n",
    r#"{"sessionId":"claude-1","type":"user","message":{"role":"user","content":[{"type":"text","text":"the real first prompt"}]},"timestamp":"2026-06-20T10:00:03.000Z"}"#,
    "\n",
    r#"{"sessionId":"claude-1","type":"assistant","message":{"role":"assistant","model":"claude-opus-4","content":[{"type":"text","text":"working on it"}]},"timestamp":"2026-06-20T10:05:00.000Z"}"#,
    "\n",
);

const CODEX_BODY: &str = concat!(
    r#"{"timestamp":"2026-06-20T11:00:00.000Z","type":"session_meta","payload":{"id":"codex-1","cwd":"/work/api","originator":"codex_cli_rs","cli_version":"0.148.0","git":{"branch":"feature","repository_url":"git@github.com:acme/api.git","commit_hash":"abc1234"}}}"#,
    "\n",
    r#"{"timestamp":"2026-06-20T11:00:01.000Z","type":"turn_context","payload":{"model":"gpt-5-codex"}}"#,
    "\n",
    r#"{"timestamp":"2026-06-20T11:00:02.000Z","type":"event_msg","payload":{"type":"user_message","message":"<environment_context>ignore me</environment_context>"}}"#,
    "\n",
    r#"{"timestamp":"2026-06-20T11:00:03.000Z","type":"event_msg","payload":{"type":"user_message","message":"add a retry to the client"}}"#,
    "\n",
    r#"{"timestamp":"2026-06-20T11:09:00.000Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#,
    "\n",
);

fn discover(conn: &Connection, home: &Path, options: &DiscoverOptions) -> DiscoveryResultForTest {
    let env = env_at(conn, home);
    let mut rows = Vec::new();
    let summary =
        discover_sessions_with_env(&env, options, |session| rows.push(session.clone())).unwrap();
    DiscoveryResultForTest { rows, summary }
}

struct DiscoveryResultForTest {
    rows: Vec<ShallowSession>,
    summary: DiscoverySummary,
}

impl DiscoveryResultForTest {
    fn ids(&self) -> Vec<String> {
        self.rows
            .iter()
            .map(|row| format!("{}:{}", row.source, row.session_id))
            .collect()
    }

    fn row(&self, session_id: &str) -> &ShallowSession {
        self.rows
            .iter()
            .find(|row| row.session_id == session_id)
            .unwrap_or_else(|| panic!("no {session_id} in {:?}", self.ids()))
    }
}

fn only(sources: &[&str]) -> DiscoverOptions {
    DiscoverOptions {
        sources: sources.iter().map(|s| s.to_string()).collect(),
        limit: None,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// registry
// ---------------------------------------------------------------------------

#[test]
fn every_source_is_either_discoverable_or_explicitly_exempt() {
    let adapters: BTreeSet<&str> = shallow_providers()
        .iter()
        .map(|provider| provider.source())
        .collect();
    let exempt: BTreeSet<&str> = DISCOVERY_EXEMPTIONS
        .iter()
        .map(|entry| entry.source)
        .collect();
    assert_eq!(
        adapters.len(),
        shallow_providers().len(),
        "an adapter is registered twice"
    );
    for source in SOURCE_CHOICES {
        let covered = usize::from(adapters.contains(source)) + usize::from(exempt.contains(source));
        assert_eq!(
            covered, 1,
            "source '{source}' must be covered by exactly one of the adapter or exemption lists; \
             a new provider needs a decision, not a default"
        );
    }
    for source in adapters.iter().chain(exempt.iter()) {
        assert!(
            SOURCE_CHOICES.contains(source),
            "'{source}' is registered but is not a known source"
        );
    }
    assert!(
        exempt.contains("trajectory"),
        "trajectories are derived records and must stay out of session discovery"
    );
}

#[test]
fn discovering_an_exempt_source_is_rejected_with_its_reason() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let env = env_at(&conn, home.path());
    let error = discover_sessions_with_env(&env, &only(&["trajectory"]), |_| {}).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains("exempt"), "{message}");
    assert!(message.contains("derived trajectory records"), "{message}");
}

#[test]
fn discovering_an_unknown_source_is_rejected() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let env = env_at(&conn, home.path());
    let error = discover_sessions_with_env(&env, &only(&["nope"]), |_| {}).unwrap_err();
    assert!(format!("{error:#}").contains("invalid source"));
}

// ---------------------------------------------------------------------------
// claude
// ---------------------------------------------------------------------------

#[test]
fn claude_cold_discovery_extracts_identity_and_skips_non_human_turns() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);

    let found = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(found.ids(), vec!["claude:claude-1"]);
    let row = found.row("claude-1");
    assert_eq!(row.cwd.as_deref(), Some("/work/app"));
    assert_eq!(row.git_branch.as_deref(), Some("main"));
    assert_eq!(row.agent_version.as_deref(), Some("1.2.3"));
    assert_eq!(row.models, vec!["claude-opus-4".to_string()]);
    assert_eq!(row.discovery_state, "shallow");
    assert!(!row.from_cache);
    // Meta, slash-command wrapper and sidechain turns are not human prompts.
    assert_eq!(row.first_prompt.as_deref(), Some("the real first prompt"));
    assert_eq!(
        row.first_activity_ms,
        crate::parse_iso_ms("2026-06-20T10:00:00.000Z")
    );
    assert_eq!(
        row.last_activity_ms,
        crate::parse_iso_ms("2026-06-20T10:05:00.000Z")
    );
    assert_eq!(found.summary.discovered, 1);
    assert_eq!(
        found.summary.contract_version,
        SESSION_CATALOG_CONTRACT_VERSION
    );
}

#[test]
fn claude_identity_is_stable_and_an_unchanged_rescan_reparses_nothing() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);

    let first = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(first.summary.counters.shallow_reads, 1);

    let second = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(second.ids(), first.ids(), "session identity must be stable");
    assert_eq!(
        second.summary.counters.shallow_reads, 0,
        "an unchanged stamp must skip the read entirely"
    );
    assert_eq!(second.summary.skipped_unchanged, 1);
    assert_eq!(second.summary.counters.files_opened, 0);
    assert!(second.rows[0].from_cache);

    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "a rescan must upsert, never duplicate");
}

#[test]
fn discovery_emits_rows_only_outside_write_transactions() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);

    for expected_cached in [false, true] {
        let env = env_at(&conn, home.path());
        let mut rows = Vec::new();
        discover_sessions_with_env(&env, &only(&["claude"]), |row| {
            assert!(
                conn.is_autocommit(),
                "callbacks must observe committed catalog rows"
            );
            rows.push(row.clone());
        })
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].from_cache, expected_cached);
    }
}

#[test]
fn claude_append_updates_the_existing_row_without_duplicating_it() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let path = claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    discover(&conn, home.path(), &only(&["claude"]));

    let mut appended = fs::read_to_string(&path).unwrap();
    appended.push_str(
        r#"{"sessionId":"claude-1","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"later"}]},"timestamp":"2026-06-20T12:00:00.000Z"}"#,
    );
    appended.push('\n');
    fs::write(&path, appended).unwrap();
    set_mtime(&path, 1_750_000_500_000);

    let second = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(second.summary.counters.shallow_reads, 1);
    assert_eq!(
        second.row("claude-1").last_activity_ms,
        crate::parse_iso_ms("2026-06-20T12:00:00.000Z")
    );
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn an_incomplete_trailing_record_is_ignored_but_the_session_still_appears() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    // A transcript being written right now: the final record has no newline.
    let body = format!(
        "{CLAUDE_BODY}{}",
        r#"{"sessionId":"claude-1","type":"user","message":{"role":"user","content":"half-written"#
    );
    claude_session(home.path(), "claude-1", &body, 1_750_000_000_000);

    let found = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(found.ids(), vec!["claude:claude-1"]);
    assert_eq!(
        found.row("claude-1").last_activity_ms,
        crate::parse_iso_ms("2026-06-20T10:05:00.000Z"),
        "a partial trailing record is not yet a record"
    );
}

/// A subagent sidecar is its own file whose records carry the *parent's*
/// sessionId. Enumerating it as a session emitted the parent twice and let the
/// two files fight over one row's raw_path/source_stamp, so one of them was
/// re-read on every run forever.
#[test]
fn a_subagent_sidecar_is_not_a_second_copy_of_its_parent_session() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    claude_session(
        home.path(),
        "agent-sub",
        concat!(
            r#"{"type":"user","uuid":"su1","sessionId":"claude-1","isSidechain":true,"cwd":"/work/app","timestamp":"2026-06-20T10:02:00.000Z","message":{"role":"user","content":"Research the repo."}}"#,
            "\n",
            r#"{"type":"assistant","uuid":"sa1","sessionId":"claude-1","isSidechain":true,"cwd":"/work/app","timestamp":"2026-06-20T10:03:00.000Z","message":{"role":"assistant","content":[{"type":"text","text":"Report."}]}}"#,
            "\n"
        ),
        1_750_000_050_000,
    );

    let first = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(
        first.ids(),
        vec!["claude:claude-1"],
        "the parent session must be emitted exactly once per run"
    );
    assert_eq!(first.summary.providers["claude"].candidates, 2);
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
    // The row points at the session's own transcript, not at the sidecar.
    let raw_path: String = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE session_id = 'claude-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(raw_path.ends_with("claude-1.jsonl"), "{raw_path}");

    let second = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(second.ids(), vec!["claude:claude-1"]);
    assert_eq!(
        second.summary.counters.shallow_reads, 0,
        "neither the transcript nor its sidecar may be re-read when nothing changed"
    );
    assert_eq!(second.summary.skipped_unchanged, 2);
    assert_eq!(
        second.rows[0].source_stamp, first.rows[0].source_stamp,
        "the stamp must be stable across rescans"
    );
}

/// The same defect class as the claude sidecar: a codex subagent thread is a
/// real rollout that is not a session, so "no catalog row" left nothing for the
/// stamp check to match and it was re-read on every run.
#[test]
fn a_non_session_source_is_remembered_so_rescans_do_not_reread_it() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);
    codex_rollout(
        home.path(),
        "codex-sub",
        concat!(
            r#"{"timestamp":"2026-06-20T11:02:00.000Z","type":"session_meta","payload":{"id":"codex-sub","cwd":"/work/api","thread_source":"subagent"}}"#,
            "\n"
        ),
        1_750_000_300_000,
    );

    let first = discover(&conn, home.path(), &only(&["codex"]));
    assert_eq!(first.summary.counters.shallow_reads, 2);
    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert_eq!(
        second.summary.counters.shallow_reads, 0,
        "a source already known not to be a session must not be re-read"
    );
    assert_eq!(second.ids(), vec!["codex:codex-1"]);

    // A file that later does become a session drops its marker.
    codex_rollout(
        home.path(),
        "codex-sub",
        &CODEX_BODY.replace("codex-1", "codex-sub"),
        1_750_000_400_000,
    );
    let third = discover(&conn, home.path(), &only(&["codex"]));
    assert!(third.ids().contains(&"codex:codex-sub".to_string()));
    let markers: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM observation_discovery_skips",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(markers, 0, "a stale non-session marker must be cleared");
}

#[test]
fn a_malformed_transcript_does_not_hide_its_healthy_neighbours() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    // Not JSON at all, and no session id anywhere.
    claude_session(
        home.path(),
        "broken",
        "}}}} not json at all\nalso not json\n",
        1_750_000_100_000,
    );

    let found = discover(&conn, home.path(), &only(&["claude"]));
    let ids = found.ids();
    assert_eq!(
        ids,
        vec!["claude:claude-1"],
        "a corrupt file must not be published as a session under its file name"
    );
    assert_eq!(found.summary.providers["claude"].candidates, 2);
    assert_eq!(found.summary.discovered, 1);
    // The corruption is named rather than silently absorbed.
    let diagnostic = found
        .summary
        .diagnostics
        .iter()
        .find(|entry| entry.source == "claude")
        .expect("a diagnostic for the corrupt transcript");
    assert!(
        diagnostic
            .locator
            .as_deref()
            .is_some_and(|locator| locator.ends_with("broken.jsonl")),
        "the diagnostic must name the broken file: {diagnostic:?}"
    );
    assert!(
        diagnostic.error.contains("no parseable JSON records"),
        "{diagnostic:?}"
    );
    // A row exists for neither the corrupt file's stem nor anything else.
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
}

/// An empty transcript is a session that has only just started, not a corrupt
/// one: nothing to catalog yet, and no diagnostic noise every run.
#[test]
fn an_empty_transcript_is_not_a_session_and_not_an_error() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    claude_session(home.path(), "starting", "", 1_750_000_100_000);

    let found = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(found.ids(), vec!["claude:claude-1"]);
    assert!(
        found.summary.diagnostics.is_empty(),
        "{:?}",
        found.summary.diagnostics
    );
}

#[test]
fn absent_optional_metadata_stays_null_instead_of_being_invented() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(
        home.path(),
        "bare",
        concat!(
            r#"{"sessionId":"bare","type":"user","message":{"role":"user","content":"hello"}}"#,
            "\n"
        ),
        1_750_000_000_000,
    );

    let found = discover(&conn, home.path(), &only(&["claude"]));
    let row = found.row("bare");
    assert_eq!(row.cwd, None);
    assert_eq!(row.git_branch, None);
    assert_eq!(row.agent_version, None);
    assert_eq!(row.repo_url, None);
    assert_eq!(row.initial_commit, None);
    assert_eq!(row.originator, None);
    assert_eq!(row.first_activity_ms, None);
    assert_eq!(row.last_activity_ms, None);
    assert!(row.models.is_empty());
    assert!(row.workspace_roots.is_empty());
    // The columns really are NULL, not empty JSON arrays.
    let (models, roots): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT models_json, workspace_roots_json FROM sessions WHERE session_id = 'bare'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(models, None);
    assert_eq!(roots, None);
}

// ---------------------------------------------------------------------------
// codex
// ---------------------------------------------------------------------------

#[test]
fn codex_session_meta_supplies_originator_version_and_git_provenance() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);

    let found = discover(&conn, home.path(), &only(&["codex"]));
    let row = found.row("codex-1");
    assert_eq!(row.cwd.as_deref(), Some("/work/api"));
    assert_eq!(row.git_branch.as_deref(), Some("feature"));
    assert_eq!(row.originator.as_deref(), Some("codex_cli_rs"));
    assert_eq!(row.agent_version.as_deref(), Some("0.148.0"));
    assert_eq!(row.repo_url.as_deref(), Some("git@github.com:acme/api.git"));
    assert_eq!(row.initial_commit.as_deref(), Some("abc1234"));
    assert_eq!(row.models, vec!["gpt-5-codex".to_string()]);
    assert_eq!(
        row.first_prompt.as_deref(),
        Some("add a retry to the client"),
        "the environment_context control turn is not a human prompt"
    );
    assert_eq!(
        row.last_activity_ms,
        crate::parse_iso_ms("2026-06-20T11:09:00.000Z")
    );
}

#[test]
fn codex_desktop_response_items_supply_the_first_substantive_prompt() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(
        home.path(),
        "codex-desktop",
        concat!(
            r#"{"timestamp":"2026-08-31T11:00:00.000Z","type":"session_meta","payload":{"id":"codex-desktop","cwd":"/work/api","thread_source":"user","source":"vscode","originator":"Codex Desktop"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T11:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<environment_context>injected</environment_context>"}]}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T11:00:02.000Z","type":"response_item","payload":{"type":"message","role":"user","id":"msg-human","content":[{"type":"input_text","text":"repair"},{"type":"input_text","text":"the parser"}]}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T11:00:02.100Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"message"}}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T11:00:03.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"working"}]}}"#,
            "\n",
        ),
        1_788_200_000_000,
    );

    let found = discover(&conn, home.path(), &only(&["codex"]));
    let row = found.row("codex-desktop");
    assert_eq!(row.first_prompt.as_deref(), Some("repair\nthe parser"));
    assert_eq!(row.originator.as_deref(), Some("Codex Desktop"));
}

#[test]
fn codex_subagent_threads_are_not_sessions() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);
    codex_rollout(
        home.path(),
        "codex-sub",
        concat!(
            r#"{"timestamp":"2026-06-20T11:02:00.000Z","type":"session_meta","payload":{"id":"codex-sub","cwd":"/work/api","thread_source":"subagent"}}"#,
            "\n"
        ),
        1_750_000_300_000,
    );

    let found = discover(&conn, home.path(), &only(&["codex"]));
    assert_eq!(found.ids(), vec!["codex:codex-1"]);
    assert_eq!(found.summary.providers["codex"].candidates, 2);
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn linked_codex_source_marked_subagent_is_not_a_root_session() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);
    codex_rollout(
        home.path(),
        "codex-linked-guardian",
        concat!(
            r#"{"timestamp":"2026-06-20T11:02:00.000Z","type":"session_meta","payload":{"id":"codex-linked-guardian","cwd":"/work/api","parent_thread_id":"codex-1","source":{"subagent":{"other":"guardian"}}}}"#,
            "\n"
        ),
        1_750_000_300_000,
    );

    let found = discover(&conn, home.path(), &only(&["codex"]));
    assert_eq!(found.ids(), vec!["codex:codex-1"]);
    assert_eq!(found.summary.providers["codex"].candidates, 2);
    assert_eq!(found.summary.discovered, 1);
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
}

#[test]
fn standalone_codex_guardian_source_marker_is_a_root_session() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);
    let guardian = codex_rollout(
        home.path(),
        "codex-guardian",
        concat!(
            r#"{"timestamp":"2026-06-20T11:02:00.000Z","type":"session_meta","payload":{"id":"codex-guardian","cwd":"/work/api","source":{"subagent":{"other":"guardian"}}}}"#,
            "\n",
            r#"{"timestamp":"2026-06-20T11:02:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"guardian prompt"}}"#,
            "\n",
            r#"{"timestamp":"2026-06-20T11:02:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"guardian answer"}}"#,
            "\n",
        ),
        1_750_000_300_000,
    );

    let found = discover(&conn, home.path(), &only(&["codex"]));
    assert!(found.ids().contains(&"codex:codex-guardian".to_string()));
    assert_eq!(found.summary.providers["codex"].candidates, 2);
    assert_eq!(found.summary.discovered, 2);
    let row = found.row("codex-guardian");
    assert_eq!(row.cwd.as_deref(), Some("/work/api"));
    assert_eq!(row.first_prompt.as_deref(), Some("guardian prompt"));
    assert_eq!(row.raw_path.as_deref(), Some(guardian.to_str().unwrap()));
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);
}

/// A guardian an older release classified as a subagent was remembered in
/// `discovery_skips`. Its bytes never change, so only the scanner version in the
/// stored stamp can invalidate that memory -- without a bump, upgraded installs
/// keep skipping the file and the catalog never gains the session.
#[test]
fn an_upgrade_re_reads_a_guardian_an_older_scanner_skipped() {
    const GUARDIAN: &str = concat!(
        r#"{"timestamp":"2026-06-20T11:02:00.000Z","type":"session_meta","payload":{"id":"codex-guardian","cwd":"/work/api","source":{"subagent":{"other":"guardian"}}}}"#,
        "\n",
        r#"{"timestamp":"2026-06-20T11:02:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"guardian prompt"}}"#,
        "\n",
    );
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);
    let guardian = codex_rollout(home.path(), "codex-guardian", GUARDIAN, 1_750_000_300_000);
    let locator = guardian.to_string_lossy().to_string();

    // Learn this fixture's raw stamp the way discovery computes it, so the seeded
    // skip below differs from a live one *only* by its version prefix.
    let first = discover(&conn, home.path(), &only(&["codex"]));
    assert!(first.ids().contains(&"codex:codex-guardian".to_string()));
    let stored: String = conn
        .query_row(
            "SELECT source_stamp FROM sessions WHERE source='codex' AND session_id='codex-guardian'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let raw = stored
        .split_once(':')
        .expect("stored stamps carry a version prefix")
        .1
        .to_string();

    let seed_skip = |stamp: String| {
        conn.execute("DELETE FROM sessions", []).unwrap();
        conn.execute(
            "INSERT INTO observation_discovery_skips (source, location, connector_id, connector_instance, locator, stamp, updated_ms) \
             VALUES ('codex', 'local', 'codex', 'default', ?, ?, 0) \
             ON CONFLICT(source, location, connector_id, connector_instance, locator) DO UPDATE SET stamp = excluded.stamp",
            params![locator, stamp],
        )
        .unwrap();
    };

    // Control: a skip at the current version does suppress the re-read, so the
    // assertion below is about the version prefix and nothing else.
    seed_skip(stored_stamp(&raw));
    let skipped = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        !skipped.ids().contains(&"codex:codex-guardian".to_string()),
        "a current-version skip is expected to suppress the read"
    );

    // What an older release left behind: same bytes, its own scanner version.
    seed_skip(format!("v{}:{raw}", SHALLOW_SCANNER_VERSION - 1));
    let after_upgrade = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        after_upgrade
            .ids()
            .contains(&"codex:codex-guardian".to_string()),
        "a skip written by an older scanner version must not survive the upgrade"
    );
}

#[test]
fn codex_rescan_is_stamp_guarded_and_identity_stable() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    codex_rollout(home.path(), "codex-1", CODEX_BODY, 1_750_000_200_000);
    let first = discover(&conn, home.path(), &only(&["codex"]));
    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert_eq!(first.ids(), second.ids());
    assert_eq!(second.summary.counters.shallow_reads, 0);
    assert_eq!(second.summary.skipped_unchanged, 1);
}

// ---------------------------------------------------------------------------
// cursor
// ---------------------------------------------------------------------------

#[test]
fn cursor_reports_mtime_as_last_activity_and_leaves_first_activity_null() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    cursor_session(
        home.path(),
        "work-app",
        "cursor-1",
        concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>\nfix the flaky test\n</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}"#,
            "\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let row = found.row("cursor-1");
    assert_eq!(row.first_prompt.as_deref(), Some("fix the flaky test"));
    assert_eq!(row.cwd.as_deref(), Some("/work/app"));
    // This transcript's turns carry no readable time, so mtime is the only
    // signal and it is reported as last activity only — never as a first
    // activity the provider did not record.
    assert_eq!(row.last_activity_ms, Some(1_750_000_400_000));
    assert_eq!(row.first_activity_ms, None);
    assert!(row.models.is_empty());
}

/// A turn time that only the head can see still sets the catalog's recency.
///
/// Reported by Devin as "head timestamp lost from recency". Past
/// `HEAD_SCAN_MAX_BYTES` the tail is a separate region, so a transcript whose
/// one dated human turn is followed by a long run of assistant and tool
/// records has a tail with no `<timestamp>` in it — only a human turn carries
/// one. `last_activity_ms` fell straight to the file mtime, so the catalog
/// reported the session as having last spoken "now", while full ingestion had
/// those records inheriting the open turn's time. Worse, the discovery upsert
/// merges `last_activity_ms` with `MAX`, so that mtime would also re-expand a
/// window a rebuild had just retracted.
///
/// Positive control: without the head fallback this failed at
/// `a head-only turn time must outrank the mtime: left: Some(1750000400000),
/// right: Some(1789587420000)` — the mtime, not the turn.
#[test]
fn cursor_recency_uses_a_turn_time_only_the_head_can_see() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    // One dated human turn, then enough assistant prose to push the tail
    // region past the head budget. The tail therefore holds only assistant
    // records, none of which carries a tag.
    let mut body = String::new();
    body.push_str(
        r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>start</user_query>"}]}}"#,
    );
    body.push('\n');
    let filler = "x".repeat(2048);
    while body.len() < (super::HEAD_SCAN_MAX_BYTES + super::TAIL_SCAN_MAX_BYTES) as usize * 2 {
        body.push_str(&format!(
            r#"{{"role":"assistant","message":{{"content":[{{"type":"text","text":"{filler}"}}]}}}}"#
        ));
        body.push('\n');
    }
    cursor_session(home.path(), "work-app", "cursor-head-time", &body, 1_750_000_400_000);

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let row = found.row("cursor-head-time");
    assert_eq!(
        row.last_activity_ms,
        Some(1_789_587_420_000),
        "a head-only turn time must outrank the mtime"
    );
    assert_eq!(row.first_activity_ms, Some(1_789_587_420_000));
}

/// The catalog's `first_prompt` and the indexed `history` row are the same
/// string for a turn Cursor split into several text blocks.
///
/// Reported by Devin as "multi-block first prompts stay truncated".
/// `parse_cursor_text` stopped at the first text block while the event parser
/// joins them all, so a two-block opening turn was stored whole in `history`
/// and truncated in the catalog — and hydration does not rewrite
/// `sessions.first_prompt`, so the short version survived full indexing.
/// Both now go through `cursor::human_turn_prompt`.
///
/// Positive control: with the `break` in `parse_cursor_text` this failed at
/// `discovery and ingestion must agree on the first prompt:
/// left: Some("now write the test"), right: Some("now write the
/// test\n\ninclude the timeout case")`.
#[test]
fn cursor_first_prompt_is_the_same_before_and_after_hydration() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let transcript = cursor_session(
        home.path(),
        "work-app",
        "cursor-split",
        concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>now write the test</user_query>"},{"type":"text","text":"include the timeout case"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"on it"}]}}"#,
            "\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let discovered = found.row("cursor-split").first_prompt.clone();

    let ingest_conn = Connection::open_in_memory().unwrap();
    init_db(&ingest_conn).unwrap();
    crate::ingest::ingest_cursor_transcript(
        &ingest_conn,
        &transcript,
        "cursor-split",
        Some("/work/app"),
        1_750_000_400_000,
        0,
        u64::MAX,
    )
    .unwrap();
    let indexed: Option<String> = ingest_conn
        .query_row(
            "SELECT prompt FROM history WHERE source = 'cursor' \
             AND session_id = 'cursor-split' ORDER BY id LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();

    assert_eq!(
        discovered, indexed,
        "discovery and ingestion must agree on the first prompt"
    );
    assert_eq!(
        discovered.as_deref(),
        Some("now write the test\n\ninclude the timeout case"),
    );
}

/// The catalog's activity window comes from human turns, not from an
/// assistant that quotes a `<timestamp>` tag back.
///
/// `cursor_record_time` ran the same unrestricted block scan the event parser
/// did, so a model explaining the transcript format moved `first_activity_ms`
/// and `last_activity_ms` and re-sorted the session in the catalog. Both paths
/// now share `cursor::injected_turn_time`, which reads the tag only out of a
/// human turn's own text blocks.
///
/// Positive control: with the scan unrestricted this failed at
/// `assistant prose must not move the catalog window: left: Some(1789587660000),
/// right: Some(1789587420000)` — the quoted instant became the session's last
/// activity.
#[test]
fn cursor_activity_ignores_a_timestamp_quoted_by_the_assistant() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    cursor_session(
        home.path(),
        "work-app",
        "cursor-quoted",
        concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>when was this?</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"Cursor writes <timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp> into the turn."}]}}"#,
            "\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let row = found.row("cursor-quoted");
    // Both endpoints are the human turn's own time. The mtime is not reached
    // either: a readable turn time outranks it.
    assert_eq!(
        row.last_activity_ms,
        Some(1_789_587_420_000),
        "assistant prose must not move the catalog window"
    );
    assert_eq!(row.first_activity_ms, Some(1_789_587_420_000));
}

/// Discovery's session summary and the one full ingestion writes have to be
/// the same string, or hydrating a session silently rewrites its summary.
///
/// A Cursor assistant record can hold several text blocks — prose, a tool
/// call, then more prose. Ingestion walks the blocks in order and keeps the
/// last non-empty one, so discovery must too.
///
/// Positive control: with `.find()` in `cursor_assistant_text` this failed
/// with `discovery and ingestion must agree on the session summary:
/// left: Some("Let me check the test."), right: Some("Fixed it.")` — the
/// catalog advertised the opening line and hydration replaced it with the
/// closing one.
#[test]
fn cursor_summary_is_the_same_before_and_after_hydration() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let transcript = cursor_session(
        home.path(),
        "work-app",
        "cursor-multi",
        concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>fix it</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"Let me check the test."},{"type":"tool_use","name":"Read","input":{"path":"t.rs"}},{"type":"text","text":"Fixed it."},{"type":"turn_ended","status":"success"}]}}"#,
            "\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let discovered = found.row("cursor-multi").last_assistant_text.clone();

    let ingest_conn = Connection::open_in_memory().unwrap();
    init_db(&ingest_conn).unwrap();
    let outcome = crate::ingest::ingest_cursor_transcript(
        &ingest_conn,
        &transcript,
        "cursor-multi",
        Some("/work/app"),
        1_750_000_400_000,
        0,
        u64::MAX,
    )
    .unwrap();

    assert_eq!(
        discovered, outcome.last_assistant_text,
        "discovery and ingestion must agree on the session summary"
    );
    assert_eq!(
        discovered.as_deref(),
        Some("Fixed it."),
        "the summary is the reply's last word, not its first"
    );
}

/// Discovery caps `last_assistant_text` at `EXCERPT_MAX_CHARS`. Ingestion
/// used to keep the full block, so hydrating a long Cursor reply rewrote the
/// catalog summary.
#[test]
fn cursor_summary_stays_capped_after_ingestion() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let long = "x".repeat(EXCERPT_MAX_CHARS + 80);
    let transcript = cursor_session(
        home.path(),
        "work-app",
        "cursor-long",
        &format!(
            "{{\"role\":\"user\",\"message\":{{\"content\":[{{\"type\":\"text\",\"text\":\"<user_query>go</user_query>\"}}]}}}}\n\
             {{\"role\":\"assistant\",\"message\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{long}\"}}]}}}}\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let discovered = found.row("cursor-long").last_assistant_text.clone();
    assert_eq!(
        discovered.as_ref().map(|text| text.chars().count()),
        Some(EXCERPT_MAX_CHARS)
    );

    let ingest_conn = Connection::open_in_memory().unwrap();
    init_db(&ingest_conn).unwrap();
    let outcome = crate::ingest::ingest_cursor_transcript(
        &ingest_conn,
        &transcript,
        "cursor-long",
        Some("/work/app"),
        1_750_000_400_000,
        0,
        u64::MAX,
    )
    .unwrap();
    assert_eq!(
        discovered, outcome.last_assistant_text,
        "discovery and ingestion must agree on a long session summary"
    );
}

#[test]
fn cursor_reports_the_injected_turn_times_when_the_build_writes_them() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    cursor_session(
        home.path(),
        "work-app",
        "cursor-2",
        concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>fix the flaky test</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"model":"claude-4.5-sonnet","content":[{"type":"text","text":"Reading the test."},{"type":"tool_use","name":"Read","input":{"path":"t.rs"}}]}}"#,
            "\n",
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp>\n<user_query>now ship it</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"Shipped."},{"type":"turn_ended","status":"success"}]}}"#,
            "\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    let row = found.row("cursor-2");
    // The injected tag is a real recorded time, so it is preferred over mtime
    // at both ends.
    assert_eq!(row.first_activity_ms, Some(1_789_587_420_000));
    assert_eq!(row.last_activity_ms, Some(1_789_587_660_000));
    assert_eq!(row.models, vec!["claude-4.5-sonnet".to_string()]);
    assert_eq!(row.last_assistant_text.as_deref(), Some("Shipped."));
}

/// A catalog row written by the scanner that shipped *before* this change
/// carries `v3:` and the fields that reader never extracted: no
/// `first_activity_ms`, no `models`, no `last_assistant_text`. The stamp is
/// compared before the provider reader is invoked, so if the scanner version
/// does not move, those rows are served from cache and stay null forever —
/// the transcript's bytes never change, so nothing else can ever invalidate
/// them.
///
/// Positive control: with `SHALLOW_SCANNER_VERSION` left at 3 this failed at
/// `a row from the previous scanner must be read again: left: None, right:
/// Some(1789587420000)` — the upgraded install kept serving the prompt-only
/// row.
#[test]
fn a_cursor_row_from_the_previous_scanner_is_read_again_after_the_upgrade() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    cursor_session(
        home.path(),
        "work-app",
        "cursor-upgrade",
        concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>fix the flaky test</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"model":"claude-4.5-sonnet","content":[{"type":"text","text":"Shipped."},{"type":"turn_ended","status":"success"}]}}"#,
            "\n"
        ),
        1_750_000_400_000,
    );

    let found = discover(&conn, home.path(), &only(&["cursor"]));
    assert_eq!(
        found.row("cursor-upgrade").first_activity_ms,
        Some(1_789_587_420_000)
    );

    // Rewrite the catalog into what the previous scanner left behind: the same
    // bytes, its own version prefix, and none of the fields this one learned
    // to extract. The stamp lives in three places and the cache check reads
    // the observation, so all three move together.
    let stored: String = conn
        .query_row(
            "SELECT source_stamp FROM sessions WHERE source = 'cursor' \
             AND session_id = 'cursor-upgrade'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let raw = stored
        .split_once(':')
        .expect("stored stamps carry a version prefix")
        .1
        .to_string();
    let previous = format!("v3:{raw}");
    conn.execute(
        "UPDATE sessions SET source_stamp = ?, first_activity_ms = NULL, \
         last_assistant_text = NULL WHERE source = 'cursor' \
         AND session_id = 'cursor-upgrade'",
        params![previous],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_presences SET source_stamp = ? WHERE source = 'cursor' \
         AND session_id = 'cursor-upgrade'",
        params![previous],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_observations SET source_stamp = ? WHERE source = 'cursor' \
         AND session_id = 'cursor-upgrade'",
        params![previous],
    )
    .unwrap();

    let upgraded = discover(&conn, home.path(), &only(&["cursor"]));
    let row = upgraded.row("cursor-upgrade");
    assert_eq!(
        row.first_activity_ms,
        Some(1_789_587_420_000),
        "a row from the previous scanner must be read again"
    );
    assert_eq!(row.last_assistant_text.as_deref(), Some("Shipped."));
    assert_eq!(row.models, vec!["claude-4.5-sonnet".to_string()]);
    assert_eq!(
        upgraded.summary.counters.shallow_reads, 1,
        "the re-read is the point: the row must not have come from cache"
    );

    // Control: the bump costs exactly one re-read. The pass after it is
    // cached again, so this is a one-time migration and not a permanent
    // rescan of every unchanged transcript.
    let settled = discover(&conn, home.path(), &only(&["cursor"]));
    assert_eq!(settled.summary.counters.shallow_reads, 0);
    assert_eq!(settled.summary.skipped_unchanged, 1);
    assert_eq!(
        settled.row("cursor-upgrade").first_activity_ms,
        Some(1_789_587_420_000)
    );
}

#[test]
fn cursor_rescan_keeps_one_row_per_session() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    cursor_session(
        home.path(),
        "work-app",
        "cursor-1",
        "{\"role\":\"user\",\"message\":{\"content\":\"hi\"}}\n",
        1_750_000_400_000,
    );
    discover(&conn, home.path(), &only(&["cursor"]));
    let second = discover(&conn, home.path(), &only(&["cursor"]));
    assert_eq!(second.summary.skipped_unchanged, 1);
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
}

// ---------------------------------------------------------------------------
// grok
// ---------------------------------------------------------------------------

#[test]
fn grok_summary_supplies_identity_and_the_chat_head_supplies_the_prompt() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-1",
        r#"{"info":{"id":"grok-1","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z","updated_at":"2026-06-20T09:30:00.000Z","head_branch":"trunk"}"#,
        concat!(
            r#"{"type":"user","synthetic_reason":"system_reminder","content":[{"type":"text","text":"synthetic"}]}"#,
            "\n",
            r#"{"type":"user","content":[{"type":"text","text":"summarize the diff"}]}"#,
            "\n",
            r#"{"type":"assistant","content":[{"type":"text","text":"sure"}]}"#,
            "\n"
        ),
        1_750_000_500_000,
    );

    let found = discover(&conn, home.path(), &only(&["grok"]));
    let row = found.row("grok-1");
    assert_eq!(row.cwd.as_deref(), Some("/work/grok"));
    assert_eq!(row.git_branch.as_deref(), Some("trunk"));
    assert_eq!(
        row.first_prompt.as_deref(),
        Some("summarize the diff"),
        "synthetic user turns are not human prompts"
    );
    assert_eq!(
        row.first_activity_ms,
        crate::parse_iso_ms("2026-06-20T09:00:00.000Z")
    );
    assert_eq!(
        row.last_activity_ms,
        crate::parse_iso_ms("2026-06-20T09:30:00.000Z")
    );
}

/// `summary.json` says when the session was opened and last touched;
/// `updates.jsonl` says when it actually did things. The stream wins, and a
/// session restored from a checkpoint — whose `created_at` predates its own
/// first event — is the case that makes the difference visible.
#[test]
fn grok_activity_comes_from_the_update_stream_when_there_is_one() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-stream",
        r#"{"info":{"id":"grok-stream","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z","updated_at":"2026-06-20T09:30:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"go\"}\n",
        1_750_000_500_000,
    );
    let updates = home
        .path()
        .join(".grok/sessions/%2Fwork%2Fgrok/grok-stream/updates.jsonl");
    write(
        &updates,
        concat!(
            r#"{"timestamp":1789560000,"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1789560000000}}}"#,
            "\n",
            r#"{"timestamp":1789560138,"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed","totalTokens":9210},"_meta":{"agentTimestampMs":1789560138000}}}"#,
            "\n"
        ),
    );
    set_mtime(&updates, 1_750_000_500_000);

    let found = discover(&conn, home.path(), &only(&["grok"]));
    let row = found.row("grok-stream");
    assert_eq!(row.first_activity_ms, Some(1_789_560_000_000));
    assert_eq!(row.last_activity_ms, Some(1_789_560_138_000));
}

/// Grok compaction can drop earlier and later stream records. Discovery
/// assigns the current snapshot's bounds rather than merging them with MIN/MAX,
/// which would keep activity the files no longer contain.
#[test]
fn grok_discovery_replaces_activity_bounds_after_compaction() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-compact",
        r#"{"info":{"id":"grok-compact","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z","updated_at":"2026-06-20T11:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"go\"}\n",
        1_750_000_500_000,
    );
    let updates = home
        .path()
        .join(".grok/sessions/%2Fwork%2Fgrok/grok-compact/updates.jsonl");
    write(
        &updates,
        concat!(
            r#"{"timestamp":1789560000,"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1789560000000}}}"#,
            "\n",
            r#"{"timestamp":1789563600,"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed"},"_meta":{"agentTimestampMs":1789563600000}}}"#,
            "\n"
        ),
    );
    set_mtime(&updates, 1_750_000_500_000);

    let found = discover(&conn, home.path(), &only(&["grok"]));
    let row = found.row("grok-compact");
    assert_eq!(row.first_activity_ms, Some(1_789_560_000_000));
    assert_eq!(row.last_activity_ms, Some(1_789_563_600_000));

    write(
        &updates,
        concat!(
            r#"{"timestamp":1789561800,"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1789561800000}}}"#,
            "\n",
            r#"{"timestamp":1789562700,"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed"},"_meta":{"agentTimestampMs":1789562700000}}}"#,
            "\n"
        ),
    );
    set_mtime(&updates, 1_750_000_600_000);

    let found = discover(&conn, home.path(), &only(&["grok"]));
    let row = found.row("grok-compact");
    assert_eq!(
        row.first_activity_ms,
        Some(1_789_561_800_000),
        "compaction moved the start later; the catalog has to follow"
    );
    assert_eq!(
        row.last_activity_ms,
        Some(1_789_562_700_000),
        "compaction moved the end earlier; MIN/MAX merge would have kept 11:00"
    );
}

/// An unreadable `summary.json` fails the shallow read instead of naming the
/// session from its folder. A sibling with a valid summary still catalogs.
#[cfg(unix)]
#[test]
fn grok_discovery_fails_an_unreadable_summary_rather_than_naming_the_folder() {
    use std::os::unix::fs::PermissionsExt;
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-ok",
        r#"{"info":{"id":"grok-ok","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"ok\"}\n",
        1_750_000_500_000,
    );
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-blocked",
        r#"{"info":{"id":"grok-blocked","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"secret\"}\n",
        1_750_000_500_000,
    );
    let summary = home
        .path()
        .join(".grok/sessions/%2Fwork%2Fgrok/grok-blocked/summary.json");
    // A file the walk can stat but cannot read: the stamp still covers it, so
    // the candidate reaches the shallow reader instead of being skipped, and
    // the identity sidecar is the one that must fail closed.
    let mut permissions = fs::metadata(&summary).unwrap().permissions();
    permissions.set_mode(0o000);
    fs::set_permissions(&summary, permissions.clone()).unwrap();

    let found = discover(&conn, home.path(), &only(&["grok"]));
    permissions.set_mode(0o644);
    fs::set_permissions(&summary, permissions).unwrap();
    assert_eq!(found.ids(), vec!["grok:grok-ok"]);
    assert!(
        found
            .summary
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.source == "grok"
                && diagnostic.locator.as_deref().is_some_and(|locator| {
                    locator.contains("grok-blocked")
                })),
        "unreadable identity has to be a diagnostic, not a folder-named row: {:?}",
        found.summary.diagnostics
    );
    assert!(
        found.rows.iter().all(|row| row.session_id != "grok-blocked"
            && row.session_id != "local-folder"),
        "the folder name must not become the session id: {:?}",
        found.ids()
    );
}

/// Discovery strips the `<user_query>` envelope, because hydration does.
///
/// Grok wraps a typed prompt in `<user_query>…</user_query>`. The full read
/// unwraps it; the shallow read did not, so `sessions.first_prompt` held the
/// XML envelope while `history.prompt` held the typed text -- for the *same*
/// prompt of the *same* session. Catalog search and the session list showed
/// the wrapper, and nothing said which of the two was the prompt.
#[test]
fn grok_discovery_unwraps_a_user_query_envelope_exactly_as_hydration_does() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-wrapped",
        r#"{"info":{"id":"grok-wrapped","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"<user_query>ship it</user_query>\"}\n",
        1_750_000_500_000,
    );
    // The positive control, in the same store: a prompt with no envelope is
    // passed through untouched, so the fix cannot be "strip angle brackets".
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-plain",
        r#"{"info":{"id":"grok-plain","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"ship it\"}\n",
        1_750_000_500_000,
    );
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-embedded",
        r#"{"info":{"id":"grok-embedded","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"Compare a <user_query>x</user_query> element with HTML\"}\n",
        1_750_000_500_000,
    );

    let found = discover(&conn, home.path(), &only(&["grok"]));
    assert_eq!(
        found.row("grok-wrapped").first_prompt.as_deref(),
        Some("ship it"),
        "the catalog stores what the person typed, not Grok's envelope"
    );
    assert_eq!(
        found.row("grok-plain").first_prompt.as_deref(),
        Some("ship it"),
        "and an unwrapped prompt is unchanged"
    );
    assert_eq!(
        found.row("grok-embedded").first_prompt.as_deref(),
        Some("Compare a <user_query>x</user_query> element with HTML"),
        "a tag inside the typed prompt is not the envelope"
    );

    // And the two readers agree, so they cannot drift apart again: the
    // record interpretation the full read uses gives the same answer.
    let wrapped = serde_json::json!({
        "type": "user",
        "content": "<user_query>ship it</user_query>",
    });
    let line = crate::ingest::grok::parse_chat_record(&wrapped);
    match line.record {
        crate::ingest::grok::GrokRecord::User { text, .. } => assert_eq!(
            text.as_deref(),
            found.row("grok-wrapped").first_prompt.as_deref(),
            "shallow and full reads must store the same prompt"
        ),
        other => panic!("expected a user record, got {other:?}"),
    }
}

/// A session whose update stream grew has new evidence even when the
/// transcript is byte-identical, so the change stamp has to cover it.
#[test]
fn grok_rescan_sees_a_grown_update_stream() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-grow",
        r#"{"info":{"id":"grok-grow","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"hey\"}\n",
        1_750_000_500_000,
    );
    let updates = home
        .path()
        .join(".grok/sessions/%2Fwork%2Fgrok/grok-grow/updates.jsonl");
    write(&updates, "{}\n");
    set_mtime(&updates, 1_750_000_500_000);
    discover(&conn, home.path(), &only(&["grok"]));

    write(&updates, "{}\n{}\n");
    set_mtime(&updates, 1_750_000_900_000);
    let second = discover(&conn, home.path(), &only(&["grok"]));
    assert_eq!(second.summary.skipped_unchanged, 0);
    assert_eq!(second.summary.counters.shallow_reads, 1);
}

#[test]
fn grok_rescan_is_stamp_guarded() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    grok_session(
        home.path(),
        "%2Fwork%2Fgrok",
        "grok-1",
        r#"{"info":{"id":"grok-1","cwd":"/work/grok"},"created_at":"2026-06-20T09:00:00.000Z"}"#,
        "{\"type\":\"user\",\"content\":\"hey\"}\n",
        1_750_000_500_000,
    );
    discover(&conn, home.path(), &only(&["grok"]));
    let second = discover(&conn, home.path(), &only(&["grok"]));
    assert_eq!(second.summary.counters.shallow_reads, 0);
    assert_eq!(second.summary.skipped_unchanged, 1);
}

// ---------------------------------------------------------------------------
// opencode
// ---------------------------------------------------------------------------

#[test]
fn opencode_sessions_come_from_the_session_table_with_a_first_prompt() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(
        home.path(),
        r#"INSERT INTO session VALUES ('oc-1', '/work/oc', 1750000600000, 1750000700000);
           INSERT INTO message VALUES ('m1', 'oc-1', 1750000600000, '{"role":"user","providerID":"openai","modelID":"gpt-5"}');
           INSERT INTO message VALUES ('m2', 'oc-1', NULL, '{"role":"assistant","time":{"created":200},"providerID":"anthropic","modelID":"claude-opus"}');
           INSERT INTO message VALUES ('m3', 'oc-1', 100, '{"role":"assistant","time":{"created":100},"providerID":"anthropic","modelID":"claude-sonnet"}');
           INSERT INTO part VALUES ('p1', 'm1', 'oc-1', 1750000600000, '{"type":"text","text":"port the parser"}');"#,
    );

    let found = discover(&conn, home.path(), &only(&["opencode"]));
    let row = found.row("oc-1");
    assert_eq!(row.cwd.as_deref(), Some("/work/oc"));
    assert_eq!(row.first_prompt.as_deref(), Some("port the parser"));
    assert_eq!(row.first_activity_ms, Some(1_750_000_600_000));
    assert_eq!(row.last_activity_ms, Some(1_750_000_700_000));
    assert_eq!(row.models, vec!["anthropic/claude-sonnet".to_string()]);
    assert_eq!(
        row.raw_path.as_deref(),
        Some(home.path().join("opencode.db").to_string_lossy().as_ref()),
        "opencode sessions retain their store provenance"
    );

    let second = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(second.summary.skipped_unchanged, 1);
    assert_eq!(second.summary.counters.shallow_reads, 0);
}

#[test]
fn opencode_model_order_rejects_payload_times_outside_i64() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(
        home.path(),
        r#"INSERT INTO session VALUES ('oc-overflow', '/work/oc', 1, 2);
           INSERT INTO message VALUES ('m1', 'oc-overflow', 1, '{"role":"assistant","time":{"created":9223372036854775808},"providerID":"anthropic","modelID":"claude-opus"}');
           INSERT INTO message VALUES ('m2', 'oc-overflow', 2, '{"role":"assistant","time":{"created":2},"providerID":"anthropic","modelID":"claude-sonnet"}');"#,
    );

    let found = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(
        found.row("oc-overflow").models,
        vec!["anthropic/claude-opus"],
        "the out-of-i64 payload time must fall back to relational time_created"
    );
}

/// A single opencode part can hold a whole pasted file. The excerpt is cut in
/// SQL so only the capped prefix ever crosses into Rust.
#[test]
fn a_huge_opencode_part_is_truncated_before_it_reaches_rust() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let huge = "x".repeat(EXCERPT_MAX_CHARS * 8);
    opencode_db(
        home.path(),
        &format!(
            r#"INSERT INTO session VALUES ('oc-big', '/work/oc', 1750000600000, 1750000700000);
               INSERT INTO message VALUES ('m1', 'oc-big', 1750000600000, '{{"role":"user"}}');
               INSERT INTO part VALUES ('p1', 'm1', 'oc-big', 1750000600000, json_object('type', 'text', 'text', '{huge}'));"#
        ),
    );

    let found = discover(&conn, home.path(), &only(&["opencode"]));
    let prompt = found.row("oc-big").first_prompt.clone().expect("a prompt");
    assert_eq!(
        prompt.chars().count(),
        EXCERPT_MAX_CHARS,
        "the excerpt must be capped at the documented bound"
    );
    let stored: String = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE session_id = 'oc-big'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored.chars().count(), EXCERPT_MAX_CHARS);
}

#[test]
fn opencode_sql_uses_the_same_whitespace_set_as_rust_excerpts() {
    let rust_whitespace: String = ('\0'..=char::MAX)
        .filter(|character| character.is_whitespace())
        .collect();
    assert_eq!(EXCERPT_TRIM_WHITESPACE, rust_whitespace);
}

#[test]
fn empty_and_minimal_opencode_schemas_are_tolerated() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(home.path(), "");
    let empty = discover(&conn, home.path(), &only(&["opencode"]));
    assert!(empty.rows.is_empty());
    assert_eq!(empty.summary.counters.provider_queries, 1);
    assert_eq!(empty.summary.counters.records_inspected, 0);

    fs::remove_file(home.path().join("opencode.db")).unwrap();
    let db = Connection::open(home.path().join("opencode.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY); INSERT INTO session VALUES ('ses_minimal');",
    )
    .unwrap();
    drop(db);
    let minimal = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(minimal.ids(), vec!["opencode:ses_minimal"]);
    let row = minimal.row("ses_minimal");
    assert_eq!(row.cwd, None);
    assert_eq!(row.first_prompt, None);
    assert!(row.models.is_empty());

    // `time_created` is optional in older message schemas. Discovery still
    // filters to assistant messages and deterministically falls back to id.
    fs::remove_file(home.path().join("opencode.db")).unwrap();
    let db = Connection::open(home.path().join("opencode.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY); \
         CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, data TEXT); \
         CREATE INDEX message_session_id_idx ON message(session_id, id); \
         INSERT INTO session VALUES ('ses_legacy_message'); \
         INSERT INTO message VALUES ('m1', 'ses_legacy_message', \
             '{\"role\":\"user\",\"time\":{\"created\":1},\"providerID\":\"openai\",\"modelID\":\"gpt-5\"}'); \
         INSERT INTO message VALUES ('m2', 'ses_legacy_message', \
             '{\"role\":\"assistant\",\"time\":{\"created\":2},\"providerID\":\"anthropic\",\"modelID\":\"claude-sonnet\"}');",
    )
    .unwrap();
    drop(db);
    let legacy = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(
        legacy.row("ses_legacy_message").models,
        vec!["anthropic/claude-sonnet"]
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn opencode_rejects_a_limit_that_sqlite_cannot_represent() {
    let catalog = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(home.path(), "");
    let env = env_at(&catalog, home.path());
    let error = OpencodeProvider::default()
        .enumerate(&env, Some(usize::MAX))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("limit exceeds SQLite's signed 64-bit range"),
        "{error:#}"
    );
}

#[test]
fn opencode_fixed_limit_does_not_inspect_unrelated_history() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let db = Connection::open(home.path().join("opencode.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
         CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE INDEX session_time_updated_id_idx ON session(time_updated DESC, id);
         CREATE INDEX message_session_time_created_id_idx ON message(session_id, time_created, id);
         CREATE INDEX part_session_idx ON part(session_id);
         CREATE INDEX part_message_id_id_idx ON part(message_id, id);
         BEGIN",
    )
    .unwrap();
    for index in 0..2_000_i64 {
        let id = format!("ses_{index:06}");
        let message = format!("msg_{index:06}");
        db.execute(
            "INSERT INTO session VALUES (?, '/work/oc', ?, ?)",
            params![id, index, index],
        )
        .unwrap();
        db.execute(
            "INSERT INTO message VALUES (?, ?, ?, '{\"role\":\"user\",\"modelID\":\"bounded-model\"}')",
            params![message, id, index],
        )
        .unwrap();
        db.execute(
            "INSERT INTO message VALUES (?, ?, ?, '{\"role\":\"assistant\",\"modelID\":\"bounded-model\"}')",
            params![format!("assistant_{index:06}"), id, index + 1],
        )
        .unwrap();
        db.execute(
            "INSERT INTO part VALUES (?, ?, ?, ?, '{\"type\":\"text\",\"text\":\"bounded prompt\"}')",
            params![format!("prt_{index:06}"), message, id, index],
        )
        .unwrap();
        // Large unrelated histories make an accidental scan expensive and
        // visible to the query-plan assertions below.
        for extra in 0..3 {
            db.execute(
                "INSERT INTO part VALUES (?, ?, ?, ?, '{\"type\":\"tool\"}')",
                params![
                    format!("prt_{index:06}_{extra}"),
                    message,
                    id,
                    index + extra
                ],
            )
            .unwrap();
        }
    }
    db.execute_batch("COMMIT").unwrap();
    drop(db);

    let found = discover(
        &conn,
        home.path(),
        &DiscoverOptions {
            sources: vec!["opencode".into()],
            limit: Some(20),
            ..Default::default()
        },
    );
    assert_eq!(found.rows.len(), 20);
    assert_eq!(found.summary.counters.candidates_enumerated, 20);
    assert_eq!(found.summary.counters.shallow_reads, 20);
    assert_eq!(found.summary.counters.provider_queries, 41);
    assert_eq!(found.summary.counters.records_inspected, 60);
    assert_eq!(found.summary.counters.bytes_read, 0);
    assert_eq!(found.summary.counters.files_opened, 1);

    let unchanged = discover(
        &conn,
        home.path(),
        &DiscoverOptions {
            sources: vec!["opencode".into()],
            limit: Some(20),
            ..Default::default()
        },
    );
    assert_eq!(unchanged.summary.counters.candidates_enumerated, 20);
    assert_eq!(unchanged.summary.counters.shallow_reads, 0);
    assert_eq!(unchanged.summary.counters.provider_queries, 1);
    assert_eq!(unchanged.summary.counters.records_inspected, 20);
    assert_eq!(unchanged.summary.counters.bytes_read, 0);
}

#[test]
fn opencode_applies_the_global_id_tiebreak_before_its_limit() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(
        home.path(),
        "INSERT INTO session VALUES ('ses_c', '/c', 1, 10);
         INSERT INTO session VALUES ('ses_a', '/a', 1, 10);
         INSERT INTO session VALUES ('ses_b', '/b', 1, 10);",
    );
    let found = discover(
        &conn,
        home.path(),
        &DiscoverOptions {
            sources: vec!["opencode".into()],
            limit: Some(2),
            ..Default::default()
        },
    );
    assert_eq!(found.ids(), vec!["opencode:ses_a", "opencode:ses_b"]);
}

fn explain_details<P: rusqlite::Params>(conn: &Connection, sql: &str, params: P) -> String {
    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map(params, |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n")
}

#[test]
fn opencode_selected_session_queries_use_provider_indexes() {
    let home = tempfile::tempdir().unwrap();
    opencode_db(home.path(), "");
    let db = Connection::open(home.path().join("opencode.db")).unwrap();
    let prompt = explain_details(
        &db,
        "SELECT substr(json_extract(p.data, '$.text'), 1, ?)
         FROM part p JOIN message m ON m.id = p.message_id
         WHERE p.session_id = ? AND json_valid(m.data) AND json_valid(p.data)
           AND json_extract(m.data, '$.role') = 'user'
           AND json_extract(p.data, '$.type') = 'text'
           AND json_type(p.data, '$.text') = 'text'
           AND trim(substr(json_extract(p.data, '$.text'), 1, ?), ?) <> ''
         ORDER BY COALESCE(p.time_created, m.time_created) ASC LIMIT 1",
        rusqlite::params![
            EXCERPT_MAX_CHARS as i64,
            "selected",
            EXCERPT_MAX_CHARS as i64,
            EXCERPT_TRIM_WHITESPACE
        ],
    );
    assert!(
        prompt.contains("SEARCH p USING INDEX part_session_idx"),
        "{prompt}"
    );
    assert!(prompt.contains("SEARCH m USING INDEX"), "{prompt}");
    assert!(!prompt.contains("SCAN p"), "{prompt}");
    assert!(!prompt.contains("SCAN m"), "{prompt}");

    let model = explain_details(
        &db,
        "SELECT json_extract(data, '$.providerID'),
                COALESCE(json_extract(data, '$.modelID'), json_extract(data, '$.model.modelID'))
         FROM message WHERE session_id = 'selected' AND json_valid(data)
         AND json_extract(data, '$.role') = 'assistant'
         AND COALESCE(
               CASE WHEN json_type(data, '$.time.created') = 'integer'
                    AND typeof(json_extract(data, '$.time.created')) = 'integer'
                    THEN json_extract(data, '$.time.created') END,
               time_created
             ) IS NOT NULL
         AND (NULLIF(json_extract(data, '$.providerID'), '') IS NOT NULL
              OR NULLIF(COALESCE(json_extract(data, '$.modelID'),
                                 json_extract(data, '$.model.modelID')), '') IS NOT NULL)
         ORDER BY COALESCE(
                    CASE WHEN json_type(data, '$.time.created') = 'integer'
                         AND typeof(json_extract(data, '$.time.created')) = 'integer'
                         THEN json_extract(data, '$.time.created') END,
                    time_created
                  ) ASC,
                  id ASC
         LIMIT 1",
        [],
    );
    assert!(
        model.contains("SEARCH message USING INDEX message_session_time_created_id_idx"),
        "{model}"
    );
    assert!(!model.contains("SCAN message"), "{model}");
}

#[test]
fn unindexed_opencode_history_is_not_scanned_or_mutated() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let db_path = home.path().join("opencode.db");
    let db = Connection::open(&db_path).unwrap();
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
         CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
         INSERT INTO session VALUES ('ses_ffffffffffffold', '/work/legacy', 1, 2);
         INSERT INTO message VALUES ('m1', 'ses_ffffffffffffold', 1, '{\"role\":\"user\",\"modelID\":\"would-require-scan\"}');
         INSERT INTO part VALUES ('p1', 'm1', 'ses_ffffffffffffold', 1, '{\"type\":\"text\",\"text\":\"would require scan\"}');",
    )
    .unwrap();
    let schema_before: String = db
        .query_row(
            "SELECT group_concat(sql, ';') FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let bytes_before = fs::read(&db_path).unwrap();
    let files_before: BTreeSet<_> = fs::read_dir(home.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    drop(db);

    let found = discover(&conn, home.path(), &only(&["opencode"]));
    let row = found.row("ses_ffffffffffffold");
    assert_eq!(row.cwd.as_deref(), Some("/work/legacy"));
    assert_eq!(row.first_prompt, None);
    assert!(row.models.is_empty());
    assert_eq!(found.summary.counters.provider_queries, 1);
    assert_eq!(found.summary.counters.records_inspected, 1);
    assert_eq!(found.summary.counters.bytes_read, 0);

    let db = Connection::open(&db_path).unwrap();
    let schema_after: String = db
        .query_row(
            "SELECT group_concat(sql, ';') FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        schema_after, schema_before,
        "discovery must issue no provider DDL"
    );
    drop(db);
    assert_eq!(
        fs::read(&db_path).unwrap(),
        bytes_before,
        "provider bytes changed"
    );
    let files_after: BTreeSet<_> = fs::read_dir(home.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        files_after, files_before,
        "discovery created a database copy or sidecar"
    );
}

#[test]
fn current_opencode_schema_without_recency_index_uses_primary_key_fallback() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let db = Connection::open(home.path().join("opencode.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
         CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE INDEX message_session_time_created_id_idx ON message(session_id, time_created, id);
         CREATE INDEX part_session_idx ON part(session_id);
         CREATE INDEX part_message_id_id_idx ON part(message_id, id);
         INSERT INTO session VALUES ('ses_ffffffffffffold', '/old', 1, 1000);
         INSERT INTO session VALUES ('ses_000000000000new', '/new', 2, 2);
         INSERT INTO message VALUES ('m_new_user', 'ses_000000000000new', 2, '{\"role\":\"user\",\"modelID\":\"requested-model\"}');
         INSERT INTO message VALUES ('m_new_assistant', 'ses_000000000000new', 3, '{\"role\":\"assistant\",\"modelID\":\"fallback-model\"}');
         INSERT INTO part VALUES ('p_new', 'm_new_user', 'ses_000000000000new', 2, '{\"type\":\"text\",\"text\":\"fallback prompt\"}');",
    )
    .unwrap();
    let plan = explain_details(
        &db,
        "SELECT id, directory, time_created, time_updated FROM session
         WHERE id IS NOT NULL AND id <> '' ORDER BY id ASC LIMIT 1",
        [],
    );
    assert!(plan.contains("sqlite_autoindex_session_1"), "{plan}");
    drop(db);

    let found = discover(
        &conn,
        home.path(),
        &DiscoverOptions {
            sources: vec!["opencode".into()],
            limit: Some(1),
            ..Default::default()
        },
    );
    assert_eq!(found.ids(), vec!["opencode:ses_000000000000new"]);
    assert_eq!(
        found.row("ses_000000000000new").first_prompt.as_deref(),
        Some("fallback prompt")
    );
    assert_eq!(
        found.row("ses_000000000000new").models,
        vec!["fallback-model"]
    );
}

#[test]
fn opencode_wal_append_does_not_tear_the_read_snapshot() {
    let catalog = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(
        home.path(),
        "INSERT INTO session VALUES ('ses_snapshot', '/work/oc', 10, 20);
         INSERT INTO message VALUES ('m_old_user', 'ses_snapshot', 10, '{\"role\":\"user\",\"modelID\":\"old-requested-model\"}');
         INSERT INTO message VALUES ('m_old_assistant', 'ses_snapshot', 11, '{\"role\":\"assistant\",\"modelID\":\"old-model\"}');
         INSERT INTO part VALUES ('p_old', 'm_old_user', 'ses_snapshot', 10, '{\"type\":\"text\",\"text\":\"coherent old prompt\"}');",
    );
    let writer = Connection::open(home.path().join("opencode.db")).unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();

    let env = env_at(&catalog, home.path());
    let provider = OpencodeProvider::default();
    let candidate = provider.enumerate(&env, Some(1)).unwrap().remove(0);
    writer
        .execute_batch(
            "INSERT INTO message VALUES ('m_new_user', 'ses_snapshot', 5, '{\"role\":\"user\",\"modelID\":\"new-requested-model\"}');
             INSERT INTO message VALUES ('m_new_assistant', 'ses_snapshot', 6, '{\"role\":\"assistant\",\"modelID\":\"new-model\"}');
             INSERT INTO part VALUES ('p_new', 'm_new_user', 'ses_snapshot', 5, '{\"type\":\"text\",\"text\":\"new prompt outside snapshot\"}');
             UPDATE session SET time_updated = 30 WHERE id = 'ses_snapshot';",
        )
        .unwrap();
    let row = provider
        .read_shallow(&env.scan(), None, &candidate)
        .unwrap()
        .unwrap();
    assert_eq!(row.first_prompt.as_deref(), Some("coherent old prompt"));
    assert_eq!(row.models, vec!["old-model"]);
    assert_eq!(row.last_activity_ms, Some(20));
}

#[test]
fn malformed_and_partial_opencode_rows_do_not_hide_an_older_complete_prompt() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(
        home.path(),
        "INSERT INTO session VALUES ('ses_partial', '/work/oc', 1, 4);
         INSERT INTO message VALUES ('m_ws', 'ses_partial', -1, '{\"role\":\"user\"}');
         INSERT INTO part VALUES ('p_ws', 'm_ws', 'ses_partial', -1, '{\"type\":\"text\",\"text\":\"\\t\\n\\r  \"}');
         INSERT INTO message VALUES ('m0', 'ses_partial', 0, '{\"role\":\"user\"}');
         INSERT INTO part VALUES ('p0', 'm0', 'ses_partial', 0, '{\"type\":\"text\"}');
         INSERT INTO message VALUES ('m1', 'ses_partial', 1, '{\"role\":\"user\",\"modelID\":\"ok\"}');
         INSERT INTO part VALUES ('p1', 'm1', 'ses_partial', 1, '{\"type\":\"text\",\"text\":\"complete prompt\"}');
         INSERT INTO message VALUES ('m2', 'ses_partial', 2, '{broken');
         INSERT INTO part VALUES ('p2', 'm2', 'ses_partial', 2, '{broken');
         INSERT INTO message VALUES ('m3', 'ses_partial', 3, '{\"role\":\"user\"}');",
    );
    let found = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(
        found.row("ses_partial").first_prompt.as_deref(),
        Some("complete prompt")
    );
    assert!(found.summary.diagnostics.is_empty());
}

#[test]
fn replacing_the_opencode_database_invalidates_same_timestamp_stamps() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(
        home.path(),
        "INSERT INTO session VALUES ('ses_replaced', '/old', 1, 2);
         INSERT INTO message VALUES ('m1', 'ses_replaced', 1, '{\"role\":\"user\"}');
         INSERT INTO part VALUES ('p1', 'm1', 'ses_replaced', 1, '{\"type\":\"text\",\"text\":\"old prompt\"}');",
    );
    let first = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(
        first.row("ses_replaced").first_prompt.as_deref(),
        Some("old prompt")
    );

    fs::remove_file(home.path().join("opencode.db")).unwrap();
    opencode_db(
        home.path(),
        "INSERT INTO session VALUES ('ses_replaced', '/new', 1, 2);
         INSERT INTO message VALUES ('m1', 'ses_replaced', 1, '{\"role\":\"user\"}');
         INSERT INTO part VALUES ('p1', 'm1', 'ses_replaced', 1, '{\"type\":\"text\",\"text\":\"new prompt\"}');",
    );
    let second = discover(&conn, home.path(), &only(&["opencode"]));
    assert_eq!(second.summary.counters.shallow_reads, 1);
    assert_eq!(second.row("ses_replaced").cwd.as_deref(), Some("/new"));
    assert_eq!(
        second.row("ses_replaced").first_prompt.as_deref(),
        Some("new prompt")
    );
}

#[test]
fn opencode_waits_for_a_transient_busy_writer() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    opencode_db(home.path(), "");
    let blocker = Connection::open(home.path().join("opencode.db")).unwrap();
    blocker
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(350));
        blocker.execute_batch("COMMIT").unwrap();
    });
    let env = env_at(&conn, home.path());
    let provider = OpencodeProvider::default();
    let started = std::time::Instant::now();
    assert!(provider.enumerate(&env, Some(1)).is_ok());
    let elapsed = started.elapsed();
    release.join().unwrap();
    assert!(
        elapsed >= std::time::Duration::from_millis(250),
        "{elapsed:?}"
    );
    assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
}

// ---------------------------------------------------------------------------
// relay
// ---------------------------------------------------------------------------

#[test]
fn relay_discovers_from_already_synced_rows_and_never_touches_the_network() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    for (id, prompt, ts) in [
        ("ch:general", "[ana] deploy is red", 1_750_000_800_000_i64),
        ("ch:general", "[bo] rolling back", 1_750_000_900_000),
    ] {
        crate::insert_history(
            &conn,
            &crate::HistoryEntry {
                id: 0,
                source: "relay".into(),
                session_id: Some(id.into()),
                project: Some("ws-1".into()),
                prompt: prompt.into(),
                prompt_hash: None,
                timestamp_ms: ts,
            },
        )
        .unwrap();
    }

    let found = discover(&conn, home.path(), &only(&["relay"]));
    let row = found.row("ch:general");
    assert_eq!(row.first_prompt.as_deref(), Some("[ana] deploy is red"));
    assert_eq!(row.first_activity_ms, Some(1_750_000_800_000));
    assert_eq!(row.last_activity_ms, Some(1_750_000_900_000));
    // Relay has no local working directory to report, and inventing one would
    // be a fabrication.
    assert_eq!(row.cwd, None);
    // No local files were opened at all: relay is a database-only adapter.
    assert_eq!(found.summary.counters.files_opened, 0);
}

#[test]
fn relay_with_nothing_synced_discovers_nothing_rather_than_failing() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let found = discover(&conn, home.path(), &only(&["relay"]));
    assert!(found.rows.is_empty());
    assert!(found.summary.diagnostics.is_empty());
    assert_eq!(found.summary.providers["relay"].candidates, 0);
}

// ---------------------------------------------------------------------------
// cross-provider
// ---------------------------------------------------------------------------

fn three_providers(home: &Path) {
    claude_session(home, "claude-old", CLAUDE_BODY, 1_000_000_000_000);
    codex_rollout(home, "codex-mid", CODEX_BODY, 2_000_000_000_000);
    cursor_session(
        home,
        "work-app",
        "cursor-new",
        "{\"role\":\"user\",\"message\":{\"content\":\"newest\"}}\n",
        3_000_000_000_000,
    );
}

#[test]
fn candidates_are_ordered_globally_by_recency_across_providers() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    three_providers(home.path());

    let found = discover(&conn, home.path(), &DiscoverOptions::default());
    assert_eq!(
        found.ids(),
        vec![
            "cursor:cursor-new".to_string(),
            "codex:codex-1".to_string(),
            "claude:claude-1".to_string(),
        ],
        "rows must arrive newest-first across providers, not provider by provider"
    );
}

#[test]
fn the_limit_is_global_not_per_provider() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    // Two providers, three sessions each, interleaved in time. The three
    // newest overall are two codex sessions and one claude session.
    for (index, id) in ["claude-a", "claude-b", "claude-c"].iter().enumerate() {
        let body = CLAUDE_BODY.replace("claude-1", id);
        claude_session(
            home.path(),
            id,
            &body,
            1_000_000_000_000 + index as i64 * 100,
        );
    }
    for (index, id) in ["codex-a", "codex-b", "codex-c"].iter().enumerate() {
        let body = CODEX_BODY.replace("codex-1", id);
        codex_rollout(
            home.path(),
            id,
            &body,
            1_000_000_000_050 + index as i64 * 100,
        );
    }

    let found = discover(
        &conn,
        home.path(),
        &DiscoverOptions {
            sources: Vec::new(),
            limit: Some(3),
            ..Default::default()
        },
    );
    assert_eq!(
        found.ids(),
        vec![
            "codex:codex-c".to_string(),
            "claude:claude-c".to_string(),
            "codex:codex-b".to_string(),
        ],
        "the newest three overall (two codex, one claude), not three from one provider"
    );
    assert_eq!(found.summary.counters.candidates_enumerated, 6);
    assert_eq!(found.summary.counters.shallow_reads, 3);
}

#[test]
fn the_same_native_id_under_two_providers_is_two_rows() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(
        home.path(),
        "shared",
        &CLAUDE_BODY.replace("claude-1", "shared"),
        1_000_000_000_000,
    );
    codex_rollout(
        home.path(),
        "shared",
        &CODEX_BODY.replace("codex-1", "shared"),
        1_000_000_000_100,
    );

    let found = discover(&conn, home.path(), &DiscoverOptions::default());
    assert_eq!(found.rows.len(), 2);
    let sources: BTreeSet<&str> = found.rows.iter().map(|row| row.source.as_str()).collect();
    assert_eq!(sources, ["claude", "codex"].into_iter().collect());
    let rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE session_id = 'shared'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rows, 2,
        "(source, session_id) is the identity, not id alone"
    );
}

#[test]
fn one_broken_provider_does_not_block_the_others() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    // Not a SQLite database at all: the opencode snapshot must fail.
    fs::write(home.path().join("opencode.db"), "definitely not sqlite").unwrap();

    let found = discover(&conn, home.path(), &DiscoverOptions::default());
    assert_eq!(found.ids(), vec!["claude:claude-1"]);
    assert!(found.summary.providers["opencode"].failed);
    assert!(!found.summary.providers["claude"].failed);
    assert_eq!(found.summary.diagnostics.len(), 1);
    assert_eq!(found.summary.diagnostics[0].source, "opencode");
}

/// A streaming caller may write its own records (a tag, a commit link)
/// through the same connection from `on_row`. Discovery's durability
/// relaxation covers only its own catalog transactions, so the callback — and
/// the caller after the run — must observe the connection's configured
/// synchronous level, not discovery's.
#[test]
fn on_row_callbacks_run_at_the_configured_durability_not_discoverys() {
    let conn = catalog();
    conn.pragma_update(None, "synchronous", "FULL").unwrap();
    let full: i64 = conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);

    let mut seen = Vec::new();
    let env = env_at(&conn, home.path());
    discover_sessions_with_env(&env, &only(&["claude"]), |_| {
        seen.push(
            conn.query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))
                .unwrap(),
        );
    })
    .unwrap();

    assert!(!seen.is_empty(), "the fixture session must be emitted");
    assert_eq!(seen, vec![full; seen.len()]);
    let after: i64 = conn
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, full);
}

#[test]
fn a_run_fails_only_when_every_provider_fails() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    fs::write(home.path().join("opencode.db"), "definitely not sqlite").unwrap();
    let env = env_at(&conn, home.path());
    let error = discover_sessions_with_env(&env, &only(&["opencode"]), |_| {}).unwrap_err();
    assert!(
        format!("{error:#}").contains("no provider made progress"),
        "{error:#}"
    );
}

#[test]
fn bounded_reads_do_not_grow_with_the_size_of_the_archive() {
    let conn = catalog();
    let small = tempfile::tempdir().unwrap();
    claude_session(small.path(), "a", &CLAUDE_BODY.replace("claude-1", "a"), 10);
    codex_rollout(small.path(), "b", &CODEX_BODY.replace("codex-1", "b"), 20);
    let limited = DiscoverOptions {
        sources: Vec::new(),
        limit: Some(2),
        ..Default::default()
    };
    let baseline = discover(&conn, small.path(), &limited);

    let conn = catalog();
    let big = tempfile::tempdir().unwrap();
    for index in 0..50 {
        let id = format!("bulk-{index:02}");
        claude_session(
            big.path(),
            &id,
            &CLAUDE_BODY.replace("claude-1", &id),
            1_000 + index as i64,
        );
    }
    claude_session(
        big.path(),
        "a",
        &CLAUDE_BODY.replace("claude-1", "a"),
        900_000,
    );
    codex_rollout(
        big.path(),
        "b",
        &CODEX_BODY.replace("codex-1", "b"),
        900_010,
    );
    let scaled = discover(&conn, big.path(), &limited);

    assert_eq!(scaled.summary.counters.candidates_enumerated, 52);
    assert_eq!(
        scaled.summary.counters.shallow_reads, 2,
        "a limit of 2 must read exactly two sources, whatever the archive holds"
    );
    assert_eq!(
        scaled.summary.counters.files_opened, baseline.summary.counters.files_opened,
        "file opens must not scale with the archive"
    );
    assert_eq!(
        scaled.summary.counters.bytes_read, baseline.summary.counters.bytes_read,
        "bytes read must not scale with the archive"
    );
    assert_eq!(
        scaled.ids(),
        vec!["codex:b".to_string(), "claude:a".to_string()]
    );
}

#[test]
fn a_head_read_stays_inside_its_budget_on_a_very_large_transcript() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let mut body = String::from(CLAUDE_BODY);
    let filler = r#"{"sessionId":"claude-1","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"PADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDINGPADDING"}]},"timestamp":"2026-06-20T10:06:00.000Z"}"#;
    while body.len() < 3 * HEAD_SCAN_MAX_BYTES as usize {
        body.push_str(filler);
        body.push('\n');
    }
    body.push_str(
        r#"{"sessionId":"claude-1","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"final"}]},"timestamp":"2026-06-20T23:00:00.000Z"}"#,
    );
    body.push('\n');
    let path = claude_session(home.path(), "claude-1", &body, 1_750_000_000_000);
    let size = fs::metadata(&path).unwrap().len();

    let found = discover(&conn, home.path(), &only(&["claude"]));
    assert!(
        found.summary.counters.bytes_read <= HEAD_SCAN_MAX_BYTES + TAIL_SCAN_MAX_BYTES,
        "read {} bytes of a {size}-byte transcript",
        found.summary.counters.bytes_read
    );
    assert!(found.summary.counters.bytes_read < size);
    let row = found.row("claude-1");
    assert_eq!(row.first_prompt.as_deref(), Some("the real first prompt"));
    assert_eq!(
        row.last_activity_ms,
        crate::parse_iso_ms("2026-06-20T23:00:00.000Z"),
        "the tail read must still find the last timestamp"
    );
}

// ---------------------------------------------------------------------------
// catalog listing
// ---------------------------------------------------------------------------

#[test]
fn the_cache_only_listing_survives_the_provider_files_disappearing() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    three_providers(home.path());
    discover(&conn, home.path(), &DiscoverOptions::default());

    // Nothing on disk any more: a cache-only list must not care.
    fs::remove_dir_all(home.path().join(".claude")).unwrap();
    fs::remove_dir_all(home.path().join(".codex")).unwrap();
    fs::remove_dir_all(home.path().join(".cursor")).unwrap();

    let rows = list_session_catalog(&conn, &CatalogListOptions::default()).unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row.from_cache));
    let recency: Vec<Option<i64>> = rows.iter().map(|row| row.last_activity_ms).collect();
    let mut sorted = recency.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(recency, sorted, "the catalog lists newest first");
}

#[test]
fn catalog_scope_filters_presences_without_duplicating_dual_sessions() {
    let conn = catalog();
    for (id, recency) in [
        ("legacy-local", 400),
        ("local-only", 300),
        ("remote-only", 200),
        ("dual", 100),
    ] {
        seed_row(&conn, "codex", id, Some(recency));
    }
    conn.execute(
        "DELETE FROM session_presences WHERE source = 'codex' AND session_id IN ('legacy-local', 'remote-only')",
        [],
    )
    .unwrap();
    mark_session_presence(&conn, "codex", "local-only", SessionLocation::Local).unwrap();
    mark_session_presence(&conn, "codex", "remote-only", SessionLocation::Remote).unwrap();
    upsert_session_presence(
        &conn,
        "codex",
        "remote-only",
        SessionLocation::Remote,
        Some("cloud://remote-only"),
        Some("v1:remote-stamp"),
        Some("shallow"),
    )
    .unwrap();
    mark_session_presence(&conn, "codex", "dual", SessionLocation::Local).unwrap();
    mark_session_presence(&conn, "codex", "dual", SessionLocation::Remote).unwrap();

    let ids = |scope| {
        list_session_catalog(
            &conn,
            &CatalogListOptions {
                scope,
                ..Default::default()
            },
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.session_id, row.locations))
        .collect::<Vec<_>>()
    };

    assert_eq!(
        ids(SessionScope::Local),
        vec![
            ("legacy-local".into(), vec![]),
            ("local-only".into(), vec!["local".into()]),
            ("dual".into(), vec!["local".into(), "remote".into()]),
        ]
    );
    assert_eq!(
        ids(SessionScope::Remote),
        vec![
            ("remote-only".into(), vec!["remote".into()]),
            ("dual".into(), vec!["local".into(), "remote".into()]),
        ]
    );
    assert_eq!(
        ids(SessionScope::All),
        vec![
            ("legacy-local".into(), vec![]),
            ("local-only".into(), vec!["local".into()]),
            ("remote-only".into(), vec!["remote".into()]),
            ("dual".into(), vec!["local".into(), "remote".into()]),
        ]
    );

    assert!(
        fetch_catalog_row_at_location(&conn, "codex", "remote-only", SessionLocation::Local,)
            .unwrap()
            .is_none()
    );
    let remote_cache =
        fetch_catalog_row_at_location(&conn, "codex", "remote-only", SessionLocation::Remote)
            .unwrap()
            .unwrap();
    assert_eq!(
        remote_cache.raw_path.as_deref(),
        Some("cloud://remote-only")
    );
    assert_eq!(
        remote_cache.source_stamp.as_deref(),
        Some("v1:remote-stamp")
    );
}

#[test]
fn remote_shallow_upsert_rolls_back_canonical_row_when_presence_write_fails() {
    let conn = catalog();
    conn.execute_batch(
        "CREATE TRIGGER reject_remote_presence BEFORE INSERT ON session_presences
         WHEN NEW.location = 'remote'
         BEGIN SELECT RAISE(ABORT, 'injected remote presence failure'); END;",
    )
    .unwrap();
    let session = ShallowSession {
        source: "codex".into(),
        session_id: "remote-atomic".into(),
        first_prompt: Some("remote prompt".into()),
        raw_path: Some("cloud://remote-atomic".into()),
        source_stamp: Some("v1:remote".into()),
        discovery_state: "shallow".into(),
        ..Default::default()
    };

    let error = upsert_shallow_session_at_location(&conn, &session, SessionLocation::Remote)
        .expect_err("presence failure must abort the canonical upsert");
    assert!(error
        .to_string()
        .contains("injected remote presence failure"));
    let session_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE source = 'codex' AND session_id = 'remote-atomic'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let presence_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_presences WHERE source = 'codex' AND session_id = 'remote-atomic'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(session_count, 0);
    assert_eq!(presence_count, 0);
}

#[test]
fn the_catalog_query_reads_only_the_sessions_table() {
    let (sql, _) = catalog_list_query(&CatalogListOptions::default());
    assert!(sql.contains("FROM sessions"), "{sql}");
    for forbidden in ["history", "session_events", "tool_calls", "file_edits"] {
        assert!(
            !sql.contains(forbidden),
            "the cache-only listing must not touch {forbidden}: {sql}"
        );
    }
}

#[test]
fn trajectory_rows_never_appear_in_a_session_listing() {
    let conn = catalog();
    conn.execute(
        "INSERT INTO sessions (session_id, source, last_activity_ms, discovery_state) \
         VALUES ('traj-1', 'trajectory', 9999999999999, 'full')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (session_id, source, last_activity_ms, discovery_state) \
         VALUES ('claude-1', 'claude', 1, 'shallow')",
        [],
    )
    .unwrap();
    mark_session_presence(&conn, "claude", "claude-1", SessionLocation::Local).unwrap();
    let rows = list_session_catalog(&conn, &CatalogListOptions::default()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source, "claude");
}

#[test]
fn the_catalog_listing_filters_by_source_and_paginates_by_recency() {
    let conn = catalog();
    for (source, id, ts) in [
        ("claude", "c1", 300_i64),
        ("claude", "c2", 200),
        ("codex", "x1", 250),
    ] {
        seed_row(&conn, source, id, Some(ts));
    }
    let claude_only = list_session_catalog(
        &conn,
        &CatalogListOptions {
            sources: vec!["claude".into()],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        claude_only
            .iter()
            .map(|row| row.session_id.clone())
            .collect::<Vec<_>>(),
        vec!["c1", "c2"]
    );

    let page = list_session_catalog(
        &conn,
        &CatalogListOptions {
            limit: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(page[0].session_id, "c1");
    let next = list_session_catalog(
        &conn,
        &CatalogListOptions {
            limit: Some(1),
            before_ms: page[0].last_activity_ms,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(next[0].session_id, "x1");
}

fn seed_row(conn: &Connection, source: &str, session_id: &str, last: Option<i64>) {
    conn.execute(
        "INSERT INTO sessions (session_id, source, last_activity_ms, discovery_state) \
         VALUES (?, ?, ?, 'shallow')",
        params![session_id, source, last],
    )
    .unwrap();
    mark_session_presence(conn, source, session_id, SessionLocation::Local).unwrap();
}

/// Walk the whole catalog one page at a time and assert the walk is a
/// partition: every row exactly once, in the catalog's total order.
fn walk_pages(conn: &Connection, page_size: i64) -> Vec<(String, String)> {
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = list_session_catalog_page(
            conn,
            &CatalogListOptions {
                limit: Some(page_size),
                after: after.clone(),
                ..Default::default()
            },
        )
        .unwrap();
        seen.extend(
            page.sessions
                .iter()
                .map(|row| (row.source.clone(), row.session_id.clone())),
        );
        match page.next_cursor {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
        assert!(seen.len() < 500, "pagination did not terminate");
    }
    seen
}

/// Recency alone is not a key: one discovery pass stamps many sessions with the
/// same mtime-derived millisecond, and a timestamp-only cursor drops every row
/// tied with the page boundary.
#[test]
fn pagination_walks_tied_timestamps_without_skipping_or_repeating_rows() {
    let conn = catalog();
    // Twelve sessions across three timestamps: every page boundary lands in
    // the middle of a tie group.
    for (source, index) in [("claude", 0), ("codex", 1), ("cursor", 2), ("grok", 3)] {
        for (tie, last) in [(0, 300_i64), (1, 200), (2, 100)] {
            seed_row(
                &conn,
                source,
                &format!("{source}-{tie}-{index}"),
                Some(last),
            );
        }
    }
    let total: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(total, 12);

    let all = walk_pages(&conn, 5);
    assert_eq!(all.len(), 12, "every row must appear exactly once: {all:?}");
    let unique: BTreeSet<_> = all.iter().cloned().collect();
    assert_eq!(unique.len(), 12, "no row may repeat across pages: {all:?}");

    // The paged walk must equal one unpaginated read of the whole catalog.
    let straight: Vec<(String, String)> = list_session_catalog(
        &conn,
        &CatalogListOptions {
            limit: Some(100),
            ..Default::default()
        },
    )
    .unwrap()
    .iter()
    .map(|row| (row.source.clone(), row.session_id.clone()))
    .collect();
    assert_eq!(all, straight, "paging must not reorder the catalog");
}

/// Sessions whose recency is unknown sort after every dated row. A cursor that
/// only carries a timestamp can never reach them, so the whole undated tail
/// would be invisible to a paginating client.
#[test]
fn pagination_reaches_the_undated_tail() {
    let conn = catalog();
    seed_row(&conn, "claude", "dated-a", Some(300));
    seed_row(&conn, "claude", "dated-b", Some(300));
    seed_row(&conn, "cursor", "undated-a", None);
    seed_row(&conn, "cursor", "undated-b", None);
    seed_row(&conn, "grok", "undated-c", None);

    let all = walk_pages(&conn, 2);
    assert_eq!(
        all,
        vec![
            ("claude".to_string(), "dated-a".to_string()),
            ("claude".to_string(), "dated-b".to_string()),
            ("cursor".to_string(), "undated-a".to_string()),
            ("cursor".to_string(), "undated-b".to_string()),
            ("grok".to_string(), "undated-c".to_string()),
        ],
        "undated rows sort last but must still be reachable"
    );

    // Stepping straight from a dated cursor into the undated tail works too.
    let page = list_session_catalog_page(
        &conn,
        &CatalogListOptions {
            limit: Some(10),
            after: Some(CatalogCursor {
                last_activity_ms: Some(300),
                source: "claude".into(),
                session_id: "dated-b".into(),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(page.sessions.len(), 3);
    assert!(page
        .sessions
        .iter()
        .all(|row| row.last_activity_ms.is_none()));
    assert!(
        page.next_cursor.is_none(),
        "a short page ends the walk rather than looping"
    );
}

#[test]
fn a_full_page_carries_a_cursor_and_a_short_one_does_not() {
    let conn = catalog();
    for index in 0..3 {
        seed_row(&conn, "claude", &format!("c{index}"), Some(100 - index));
    }
    let full = list_session_catalog_page(
        &conn,
        &CatalogListOptions {
            limit: Some(3),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(full.sessions.len(), 3);
    assert_eq!(
        full.next_cursor,
        Some(CatalogCursor {
            last_activity_ms: Some(98),
            source: "claude".into(),
            session_id: "c2".into(),
        }),
        "a page that fills its limit hands back its last row as the cursor"
    );
    let short = list_session_catalog_page(
        &conn,
        &CatalogListOptions {
            limit: Some(10),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(short.sessions.len(), 3);
    assert_eq!(short.next_cursor, None);
}

#[test]
fn the_catalog_listing_is_served_by_an_index_not_a_table_scan() {
    let conn = catalog();
    // The plan is taken from the *same* builder the listing runs, so this
    // cannot pass against a restated copy of the query while the real one
    // drifts into a table scan.
    let plan = |options: &CatalogListOptions| -> String {
        let (sql, args) = catalog_list_query(options);
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let params = rusqlite::params_from_iter(args.iter().map(|arg| arg.as_ref()));
        stmt.query_map(params, |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join(" | ")
    };

    let unfiltered = plan(&CatalogListOptions::default());
    assert!(
        unfiltered.contains("idx_sessions_recency"),
        "the catalog's total order must be served by idx_sessions_recency: {unfiltered}"
    );
    assert!(
        !unfiltered.contains("TEMP B-TREE"),
        "the listing must not sort the table: {unfiltered}"
    );

    // The paginated form is the one that runs on every page after the first;
    // it must stay indexed too, composite cursor predicate and all.
    let paginated = plan(&CatalogListOptions {
        limit: Some(10),
        after: Some(CatalogCursor {
            last_activity_ms: Some(500),
            source: "claude".into(),
            session_id: "c1".into(),
        }),
        ..Default::default()
    });
    assert!(
        paginated.contains("idx_sessions_recency") && !paginated.contains("TEMP B-TREE"),
        "a paginated listing must stay index-ordered: {paginated}"
    );

    let filtered = plan(&CatalogListOptions {
        sources: vec!["claude".into()],
        limit: Some(10),
        before_ms: Some(1),
        after: None,
        ..Default::default()
    });
    assert!(
        filtered.contains("idx_sessions_source_recency") && !filtered.contains("TEMP B-TREE"),
        "a source-filtered listing must be served by idx_sessions_source_recency: {filtered}"
    );
}

// ---------------------------------------------------------------------------
// full-ingest interaction
// ---------------------------------------------------------------------------

#[test]
fn a_shallow_rescan_never_downgrades_a_fully_indexed_row() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let path = claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    // Stand in for the full-sync path having already ingested this session.
    crate::upsert_session(
        &conn,
        "claude-1",
        "claude",
        Some("/work/app"),
        Some("main"),
        1,
        2,
        Some("the assistant's last word"),
        Some(&path.to_string_lossy()),
    )
    .unwrap();

    let found = discover(&conn, home.path(), &only(&["claude"]));
    let row = found.row("claude-1");
    assert_eq!(
        row.discovery_state, "full",
        "shallow discovery must never claim a fully indexed session is shallow"
    );
    assert_eq!(
        row.last_assistant_text.as_deref(),
        Some("the assistant's last word"),
        "a shallow pass must not null out what full indexing established"
    );
    assert_eq!(
        row.first_prompt.as_deref(),
        Some("the real first prompt"),
        "a shallow rescan of a full row still refreshes catalog metadata"
    );
    assert!(
        row.source_stamp
            .as_deref()
            .is_some_and(|stamp| stamp.starts_with(&format!("v{SHALLOW_SCANNER_VERSION}:"))),
        "the stamp records which scanner wrote it: {:?}",
        row.source_stamp
    );
}

/// A row written before `discovery_state` existed carries NULL, which readers
/// deliberately interpret as fully indexed. The upsert has to agree, or the
/// first discovery run on an upgraded database quietly demotes every legacy
/// session to `shallow`.
#[test]
fn a_legacy_row_with_no_discovery_state_is_not_demoted_to_shallow() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let path = claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    // Exactly what a pre-catalog database holds: the original nine columns
    // populated, every column this feature added still NULL.
    conn.execute(
        "INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, \
          last_assistant_text, raw_path, parser_version) \
         VALUES ('claude-1', 'claude', '/work/app', 'main', 1, 2, 'legacy tail', ?, 1)",
        [path.to_string_lossy().as_ref()],
    )
    .unwrap();
    let before: Option<String> = conn
        .query_row(
            "SELECT discovery_state FROM sessions WHERE session_id = 'claude-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before, None, "the fixture must start as a legacy row");

    let found = discover(&conn, home.path(), &only(&["claude"]));
    let row = found.row("claude-1");
    assert_eq!(
        row.discovery_state, "full",
        "a NULL discovery_state means fully indexed and must survive a shallow rescan"
    );
    let stored: Option<String> = conn
        .query_row(
            "SELECT discovery_state FROM sessions WHERE session_id = 'claude-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored.as_deref(), Some("full"));
    assert_eq!(
        row.last_assistant_text.as_deref(),
        Some("legacy tail"),
        "the legacy row's own evidence must survive too"
    );
    assert_eq!(row.first_prompt.as_deref(), Some("the real first prompt"));
}

/// The limit counts emitted sessions. Truncating candidates up front let a
/// codex subagent thread -- which is not a session -- eat a result slot.
#[test]
fn non_session_candidates_do_not_consume_limit_slots() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    // The two newest candidates are subagent threads; the real sessions are
    // older, so a candidate-truncating limit returned one session for a limit
    // of two.
    for (index, id) in ["sub-a", "sub-b"].iter().enumerate() {
        codex_rollout(
            home.path(),
            id,
            &format!(
                concat!(
                    r#"{{"timestamp":"2026-06-20T11:02:00.000Z","type":"session_meta","payload":{{"id":"{}","cwd":"/work/api","thread_source":"subagent"}}}}"#,
                    "\n"
                ),
                id
            ),
            1_750_000_900_000 + index as i64,
        );
    }
    for (index, id) in ["real-a", "real-b", "real-c"].iter().enumerate() {
        codex_rollout(
            home.path(),
            id,
            &CODEX_BODY.replace("codex-1", id),
            1_750_000_800_000 + index as i64,
        );
    }

    let found = discover(
        &conn,
        home.path(),
        &DiscoverOptions {
            sources: vec!["codex".into()],
            limit: Some(2),
            ..Default::default()
        },
    );
    assert_eq!(
        found.ids(),
        vec!["codex:real-c".to_string(), "codex:real-b".to_string()],
        "a limit of 2 must yield 2 real sessions, newest first"
    );
    assert_eq!(found.summary.discovered, 2);
    // The two subagent threads were read (and remembered) but did not count.
    assert_eq!(found.summary.counters.shallow_reads, 4);
}

#[test]
fn bumping_the_scanner_version_invalidates_stored_stamps() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    claude_session(home.path(), "claude-1", CLAUDE_BODY, 1_750_000_000_000);
    discover(&conn, home.path(), &only(&["claude"]));
    // Simulate a database stamped by an older scanner generation.
    conn.execute(
        "UPDATE session_observations SET source_stamp = 'v0:stale' \
         WHERE location = 'local' AND session_id = 'claude-1'",
        [],
    )
    .unwrap();
    let rescan = discover(&conn, home.path(), &only(&["claude"]));
    assert_eq!(rescan.summary.counters.shallow_reads, 1);
    assert_eq!(rescan.summary.skipped_unchanged, 0);
}

// ---------------------------------------------------------------------------
// project identity on the cached path
// ---------------------------------------------------------------------------

fn stored_key(conn: &Connection, source: &str, session_id: &str) -> (Option<String>, Option<String>) {
    conn.query_row(
        "SELECT project_key, project_key_method FROM sessions WHERE source = ? AND session_id = ?",
        params![source, session_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap_or_else(|error| panic!("reading {source}/{session_id}: {error}"))
}

/// A codex rollout whose only interesting property is where it ran.
fn codex_in(home: &Path, id: &str, cwd: &Path, mtime_ms: i64) {
    let body = format!(
        "{}\n{}\n",
        format_args!(
            r#"{{"timestamp":"2026-06-20T11:00:00.000Z","type":"session_meta","payload":{{"id":"{id}","cwd":"{}"}}}}"#,
            cwd.display()
        ),
        r#"{"timestamp":"2026-06-20T11:00:03.000Z","type":"event_msg","payload":{"type":"user_message","message":"delegated work"}}"#,
    );
    codex_rollout(home, id, &body, mtime_ms);
}

/// What discovery streams for a cached row must be what the pass then stores.
///
/// The cached-row upgrade resolves the working directory, which for a
/// delegated child is frequently not a repository at all — a path key. The
/// end-of-pass refresh then replaces that path with the parent's repository,
/// because a borrowed key beats a machine-local directory. If the upgrade
/// emits its own answer, the JSONL a consumer parses says one project and the
/// database it came from says another, with nothing anywhere recording the
/// disagreement.
#[test]
fn a_cached_child_is_streamed_the_key_the_refresh_pass_will_store() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let work = home.path().join("work/child");
    fs::create_dir_all(&work).unwrap();
    codex_in(home.path(), "child", &work, 1_750_000_000_000);

    // Nothing to inherit from yet, so the child's own directory is all there
    // is, and it is not a repository.
    let first = discover(&conn, home.path(), &only(&["codex"]));
    assert_eq!(
        first.row("child").project_key_method.as_deref(),
        Some(ProjectKeyMethod::PathFallback.as_str()),
        "the premise is a child that resolved to nothing canonical"
    );

    // The delegating parent lands, keyed to the repository it ran in.
    conn.execute(
        "INSERT INTO sessions (source, session_id, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'parent', 'github.com/acme/parent', 'remote', 1, 'shallow')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
         child_session_id, relationship, identity_status, evidence_kind, created_ms, updated_ms) \
         VALUES ('codex', 'parent', 'rel', 'child', 'delegation', 'observed', 'fixture', 1, 1)",
        [],
    )
    .unwrap();

    // The child's transcript is untouched, so this pass serves it by its stamp.
    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        second.summary.skipped_unchanged > 0,
        "the premise of this test is the cached branch; {:?}",
        second.summary
    );
    let emitted = second.row("child");
    let inherited = (
        Some("github.com/acme/parent".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    assert_eq!(
        (
            emitted.project_key.clone(),
            emitted.project_key_method.clone()
        ),
        inherited,
        "discovery streamed a path key for a child the refresh pass then made inherited"
    );
    assert_eq!(
        stored_key(&conn, "codex", "child"),
        inherited,
        "the streamed row and the stored row must not disagree"
    );
}

/// An upgrade the catalog refuses must not be reported as if it happened.
///
/// The cached-row upgrade reads the row, resolves, and writes — three steps
/// with no transaction around them, so a hydrate or a concurrent sync can
/// settle the same session in between. The write is guarded and correctly
/// declines to downgrade the settled key; the row streamed to the caller has
/// to decline with it. Reporting the key the write *wanted* invents a value no
/// row anywhere holds, which is worse than a stale one because nothing
/// downstream can tell it is wrong.
#[test]
fn a_cached_upgrade_the_catalog_refuses_streams_the_stored_key() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let work = home.path().join("work/api");
    fs::create_dir_all(&work).unwrap();
    // A row from before project identity existed: no key at all.
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, last_activity_ms, discovery_state) \
         VALUES ('codex', 'settled', ?, 1, 'shallow')",
        params![work.to_string_lossy()],
    )
    .unwrap();
    let mut row = fetch_catalog_row(&conn, "codex", "settled")
        .unwrap()
        .expect("the row was just inserted");

    // Between that read and the upgrade, a hydrate settles the session on the
    // repository it actually belongs to.
    conn.execute(
        "UPDATE sessions SET project_key = 'github.com/acme/api', project_key_method = 'remote' \
         WHERE source = 'codex' AND session_id = 'settled'",
        [],
    )
    .unwrap();

    crate::project_identity::begin_acquisition_pass();
    upgrade_cached_project_identity(&conn, &mut row).unwrap();

    let settled = (
        Some("github.com/acme/api".to_string()),
        Some(ProjectKeyMethod::Remote.as_str().to_string()),
    );
    assert_eq!(
        (row.project_key.clone(), row.project_key_method.clone()),
        settled,
        "the streamed row reported a key the catalog refused to store"
    );
    assert_eq!(
        stored_key(&conn, "codex", "settled"),
        settled,
        "a settled remote key must never be overwritten by a path"
    );
}

/// The streamed key must survive the *whole* refresh, not just its first pass.
///
/// A cached row is decided and handed to the caller before the end-of-pass
/// refresh runs at all. Asking the catalog what the parent holds right now is
/// not enough to agree with what that refresh will store: the same refresh can
/// promote the parent off a path and onto a `remote` of its own (pass 1) and
/// only then lend it down (pass 2) — by which time the child has been streamed
/// with a path key, and the JSONL a consumer parses disagrees with the
/// database it came from.
#[test]
fn a_cached_child_is_streamed_the_key_its_parent_is_about_to_resolve() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let child_dir = home.path().join("work/child");
    fs::create_dir_all(&child_dir).unwrap();
    codex_in(home.path(), "child", &child_dir, 1_750_000_000_000);
    discover(&conn, home.path(), &only(&["codex"]));

    // The delegating parent is a checkout that has an `origin` — but the
    // catalog still has it on a path key, exactly as a session first seen
    // before its checkout was cloned would be. Pass 1 is what fixes that, and
    // pass 1 has not run since.
    let parent_dir = home.path().join("work/parent");
    let git_dir = parent_dir.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/parent.git\n",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) VALUES ('codex', 'parent', ?1, ?1, 'path', 1, 'shallow')",
        params![parent_dir.to_string_lossy()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
         child_session_id, relationship, identity_status, evidence_kind, created_ms, updated_ms) \
         VALUES ('codex', 'parent', 'rel', 'child', 'delegation', 'observed', 'fixture', 1, 1)",
        [],
    )
    .unwrap();

    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        second.summary.skipped_unchanged > 0,
        "the premise of this test is the cached branch; {:?}",
        second.summary
    );
    let expected = (
        Some("github.com/acme/parent".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    let emitted = second.row("child");
    assert_eq!(
        (
            emitted.project_key.clone(),
            emitted.project_key_method.clone()
        ),
        expected,
        "the streamed key did not account for the promotion its own pass was about to make"
    );
    assert_eq!(
        stored_key(&conn, "codex", "child"),
        expected,
        "the streamed row and the stored row must not disagree"
    );
    assert_eq!(
        stored_key(&conn, "codex", "parent"),
        (
            Some("github.com/acme/parent".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "the parent was not promoted, so nothing above was actually tested"
    );
}

/// The streamed key must be the key the refresh will *leave*, which is not the
/// same as the key the filesystem resolves.
///
/// Pass 1 never rewrites a `remote`: a session that resolved its repository
/// once keeps that answer even if the checkout has since been re-pointed at a
/// fork, because a settled canonical key is not something a later pass may
/// quietly change underneath every consumer that has already grouped by it. A
/// read-time walk that asks the filesystem instead would hand the caller the
/// fork's key and then watch the refresh write the original back — the two
/// disagreeing on every single pass, not just once.
#[test]
fn a_cached_child_is_streamed_the_settled_key_its_parent_keeps() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let child_dir = home.path().join("work/child");
    fs::create_dir_all(&child_dir).unwrap();
    codex_in(home.path(), "child", &child_dir, 1_750_000_000_000);
    discover(&conn, home.path(), &only(&["codex"]));

    // The parent settled on `acme/parent` at some earlier pass; its checkout
    // now points somewhere else entirely.
    let parent_dir = home.path().join("work/parent");
    let git_dir = parent_dir.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/fork.git\n",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'parent', ?, 'github.com/acme/parent', 'remote', 1, 'shallow')",
        params![parent_dir.to_string_lossy()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
         child_session_id, relationship, identity_status, evidence_kind, created_ms, updated_ms) \
         VALUES ('codex', 'parent', 'rel', 'child', 'delegation', 'observed', 'fixture', 1, 1)",
        [],
    )
    .unwrap();

    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        second.summary.skipped_unchanged > 0,
        "the premise of this test is the cached branch; {:?}",
        second.summary
    );
    let settled = (
        Some("github.com/acme/parent".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    let emitted = second.row("child");
    assert_eq!(
        (
            emitted.project_key.clone(),
            emitted.project_key_method.clone()
        ),
        settled,
        "the streamed key came from the checkout rather than from the row the pass will leave"
    );
    assert_eq!(
        stored_key(&conn, "codex", "child"),
        settled,
        "the streamed row and the stored row must not disagree"
    );
    assert_eq!(
        stored_key(&conn, "codex", "parent").0.as_deref(),
        Some("github.com/acme/parent"),
        "a settled remote key must not be rewritten by a later pass"
    );
}

/// A stand-in follows the ancestor it stands in for, all the way down.
///
/// When pass 1 promotes a grandparent onto a `remote` of its own, pass 2
/// carries that key through every borrowed key beneath it. A read-time walk
/// that stops at the first ancestor already wearing a borrowed key answers
/// with the stand-in the refresh is about to replace — so the row goes out on
/// the wire naming one project while the database ends up naming another.
#[test]
fn a_cached_child_is_streamed_the_key_a_promoted_ancestor_will_lend() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let child_dir = home.path().join("work/child");
    fs::create_dir_all(&child_dir).unwrap();
    codex_in(home.path(), "child", &child_dir, 1_750_000_000_000);
    discover(&conn, home.path(), &only(&["codex"]));

    // The grandparent is about to be promoted: its checkout has an `origin`
    // but the catalog still has it wearing a key it borrowed.
    let grandparent_dir = home.path().join("work/grandparent");
    let git_dir = grandparent_dir.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/new.git\n",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'grandparent', ?, 'github.com/acme/old', 'inherited', 1, 'shallow')",
        params![grandparent_dir.to_string_lossy()],
    )
    .unwrap();
    // The middle generation and the child both borrowed that same old key.
    conn.execute(
        "INSERT INTO sessions (source, session_id, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'parent', 'github.com/acme/old', 'inherited', 1, 'shallow')",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE sessions SET project_key = 'github.com/acme/old', \
         project_key_method = 'inherited' WHERE source = 'codex' AND session_id = 'child'",
        [],
    )
    .unwrap();
    for (parent, child, uid) in [
        ("grandparent", "parent", "gp->p"),
        ("parent", "child", "p->c"),
    ] {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', 1, 1)",
            params![parent, uid, child],
        )
        .unwrap();
    }

    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        second.summary.skipped_unchanged > 0,
        "the premise of this test is the cached branch; {:?}",
        second.summary
    );
    let promoted = (
        Some("github.com/acme/new".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    let emitted = second.row("child");
    assert_eq!(
        (
            emitted.project_key.clone(),
            emitted.project_key_method.clone()
        ),
        promoted,
        "the streamed key was the stand-in the refresh was about to replace"
    );
    assert_eq!(
        stored_key(&conn, "codex", "child"),
        promoted,
        "the streamed row and the stored row must not disagree"
    );
    assert_eq!(
        stored_key(&conn, "codex", "grandparent"),
        (
            Some("github.com/acme/new".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "the grandparent was not promoted, so nothing above was actually tested"
    );
}

/// The streamed key must see what the stored key sees — including past a
/// generation the catalog does not hold.
///
/// A delegated thread is evidence, not a session, so the generation between a
/// root and its grandchild routinely has no catalog row. The refresh walks
/// past it; a read-time walk that inner-joined `sessions` could not, so a
/// cached grandchild was streamed with the stand-in it already wore while the
/// same pass stored the promoted ancestor's key. Two answers for one session,
/// one in `sessions discover --json` and one in the database, with nothing in
/// either recording that they disagree.
#[test]
fn a_cached_grandchild_is_streamed_the_key_the_refresh_lends_across_a_gap() {
    let conn = catalog();
    let home = tempfile::tempdir().unwrap();
    let child_dir = home.path().join("work/child");
    fs::create_dir_all(&child_dir).unwrap();
    codex_in(home.path(), "child", &child_dir, 1_750_000_000_000);
    discover(&conn, home.path(), &only(&["codex"]));

    // The grandparent is about to be promoted onto a `remote` of its own; the
    // catalog still has it wearing a borrowed key.
    let grandparent_dir = home.path().join("work/grandparent");
    let git_dir = grandparent_dir.join(".git");
    fs::create_dir_all(&git_dir).unwrap();
    fs::write(
        git_dir.join("config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/new.git\n",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, cwd, project_key, project_key_method, \
         last_activity_ms, discovery_state) \
         VALUES ('codex', 'grandparent', ?, 'github.com/acme/old', 'inherited', 1, 'shallow')",
        params![grandparent_dir.to_string_lossy()],
    )
    .unwrap();
    // The generation in between is evidence only: a relationship, no row.
    conn.execute(
        "UPDATE sessions SET project_key = 'github.com/acme/old', \
         project_key_method = 'inherited' WHERE source = 'codex' AND session_id = 'child'",
        [],
    )
    .unwrap();
    for (parent, child, uid) in [
        ("grandparent", "middle", "gp->m"),
        ("middle", "child", "m->c"),
    ] {
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) \
             VALUES ('codex', ?1, ?2, ?3, 'delegation', 'observed', 'fixture', 1, 1)",
            params![parent, uid, child],
        )
        .unwrap();
    }
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE source = 'codex' AND session_id = 'middle'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0,
        "the premise of this test is that the middle generation has no catalog row"
    );

    let second = discover(&conn, home.path(), &only(&["codex"]));
    assert!(
        second.summary.skipped_unchanged > 0,
        "the premise of this test is the cached branch; {:?}",
        second.summary
    );
    let promoted = (
        Some("github.com/acme/new".to_string()),
        Some(ProjectKeyMethod::Inherited.as_str().to_string()),
    );
    let emitted = second.row("child");
    assert_eq!(
        (
            emitted.project_key.clone(),
            emitted.project_key_method.clone()
        ),
        promoted,
        "the streamed walk stopped at the uncataloged generation the refresh walks past"
    );
    assert_eq!(
        stored_key(&conn, "codex", "child"),
        promoted,
        "the streamed row and the stored row must not disagree"
    );
    assert_eq!(
        stored_key(&conn, "codex", "grandparent"),
        (
            Some("github.com/acme/new".to_string()),
            Some(ProjectKeyMethod::Remote.as_str().to_string())
        ),
        "the grandparent was not promoted, so nothing above was actually tested"
    );
}
