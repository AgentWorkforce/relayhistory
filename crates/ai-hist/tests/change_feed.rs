//! The change feed through the public `SessionStore` surface, over real
//! provider files: what a downstream consumer sees when it ticks after each
//! sync, and that replaying the feed from the start rebuilds the tables.
//!
//! Public API only, so this runs in the `--no-default-features` job too. The
//! direct table reads the replay is checked against open the SQLite file
//! themselves, which is the one thing a consumer must never need to do.

use ai_hist::{
    Change, ChangeKind, ChangeOp, ChangeQuery, EvidenceRow, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions, Watermark,
};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// The environment is process-wide, so the tests in this binary take turns.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// An isolated provider home.
///
/// `StoreOptions::home` is not enough on its own: a local sweep resolves
/// each provider root through its override (`CLAUDE_CONFIG_DIR`,
/// `CODEX_HOME`, ...), and a runner that sets one would sync a live session
/// into a test that expects exactly the rows it staged. Every override is
/// pinned under the temporary home for as long as the `Home` lives.
struct Home {
    dir: tempfile::TempDir,
    _env: MutexGuard<'static, ()>,
}

impl Home {
    fn new() -> Self {
        let env = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        std::env::set_var("HOME", home);
        std::env::set_var("USERPROFILE", home);
        std::env::set_var("CLAUDE_CONFIG_DIR", home.join(".claude"));
        std::env::set_var("CODEX_HOME", home.join(".codex"));
        std::env::set_var("GROK_HOME", home.join(".grok"));
        std::env::set_var(
            "OPENCODE_DB",
            home.join(".local/share/opencode/opencode.db"),
        );
        std::env::set_var(
            "OPENCODE_STORAGE_DIR",
            home.join(".local/share/opencode/storage"),
        );
        std::env::set_var("TRAJECTORY_ROOT", home.join(".trajectories"));
        std::env::set_var("XDG_DATA_HOME", home.join(".local/share"));
        std::env::set_var("XDG_CONFIG_HOME", home.join(".config"));
        std::env::remove_var("AI_HIST_DB");
        Self { dir, _env: env }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn db(&self) -> PathBuf {
        self.path().join("ai-history.db")
    }

    fn store(&self) -> SessionStore {
        // Field by field: `StoreOptions` is `#[non_exhaustive]`, which is the
        // shape an outside crate sees.
        let mut options = StoreOptions::default();
        options.db_path = Some(self.db());
        options.home = Some(self.path().to_path_buf());
        SessionStore::open(options).unwrap()
    }

    fn claude_transcript(&self, name: &str) -> PathBuf {
        let project = self.path().join(".claude/projects/corpus");
        fs::create_dir_all(&project).unwrap();
        project.join(name)
    }

    fn stage_claude(&self, fixture: &str) -> PathBuf {
        let target = self.claude_transcript(fixture);
        fs::copy(fixtures_root().join("claude").join(fixture), &target).unwrap();
        target
    }

    fn stage_codex(&self, fixture: &str) {
        let day = self.path().join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let stem = fixture.trim_end_matches(".jsonl");
        fs::copy(
            fixtures_root().join("codex").join(fixture),
            day.join(format!("rollout-2026-04-20T00-00-00-{stem}.jsonl")),
        )
        .unwrap();
    }

    /// A raw read of the ledger for the replay check only.
    fn raw(&self) -> Connection {
        Connection::open_with_flags(self.db(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
    }

    /// A raw write, standing in for a migration or a writer this crate does
    /// not own.
    fn raw_writer(&self) -> Connection {
        Connection::open(self.db()).unwrap()
    }
}

fn drain(store: &SessionStore, from: Watermark) -> Vec<Change> {
    store
        .changes_since(from, ChangeQuery::default())
        .unwrap()
        .map(|change| change.unwrap())
        .collect()
}

const SESSION: &str = "feed-live-session";

fn user_line(uuid: &str, text: &str, ts: &str) -> String {
    format!(
        "{{\"parentUuid\":null,\"isSidechain\":false,\"type\":\"user\",\
         \"message\":{{\"role\":\"user\",\"content\":\"{text}\"}},\"uuid\":\"{uuid}\",\
         \"timestamp\":\"{ts}\",\"cwd\":\"/tmp/project\",\"sessionId\":\"{SESSION}\",\
         \"version\":\"2.1.96\"}}\n"
    )
}

fn assistant_line(uuid: &str, parent: &str, block: &str, stop: &str, ts: &str) -> String {
    format!(
        "{{\"parentUuid\":\"{parent}\",\"isSidechain\":false,\"message\":{{\"model\":\
         \"claude-opus-4-7\",\"id\":\"msg_live_1\",\"role\":\"assistant\",\"content\":[{block}],\
         \"stop_reason\":{stop},\"usage\":{{\"input_tokens\":3,\"output_tokens\":43}}}},\
         \"requestId\":\"req_live_1\",\"type\":\"assistant\",\"uuid\":\"{uuid}\",\
         \"timestamp\":\"{ts}\",\"cwd\":\"/tmp/project\",\"sessionId\":\"{SESSION}\",\
         \"version\":\"2.1.96\"}}\n"
    )
}

/// A message the provider is still writing is not in the feed. When it
/// completes, every block of it arrives at once, each exactly once, and the
/// request the blocks make up is readable with its final usage.
#[test]
fn an_in_progress_message_reaches_the_feed_only_once_complete() {
    let home = Home::new();
    let transcript = home.claude_transcript(format!("{SESSION}.jsonl").as_str());
    let prompt = user_line("u-1", "go", "2026-08-31T10:00:00.000Z");
    let streaming = assistant_line(
        "a-1",
        "u-1",
        r#"{"type":"text","text":"thinking about it"}"#,
        "null",
        "2026-08-31T10:00:01.000Z",
    );
    fs::write(&transcript, format!("{prompt}{streaming}")).unwrap();

    let store = home.store();
    let first = store.sync(Default::default()).unwrap();
    assert_eq!(
        first.head_revision,
        store.head_revision().unwrap().revision,
        "the report carries the head the store answers"
    );
    let tick = drain(&store, Watermark::START);
    let events: Vec<&Change> = tick
        .iter()
        .filter(|change| change.kind == ChangeKind::SessionEvent)
        .collect();
    assert_eq!(
        events.len(),
        1,
        "the prompt is indexed, the streaming message is held: {events:?}"
    );
    assert!(matches!(
        &events[0].op,
        ChangeOp::Upsert(EvidenceRow::SessionEvent(event)) if event.role == "user"
    ));
    assert!(
        tick.iter().any(|change| change.kind == ChangeKind::Session),
        "the catalog row is part of the feed"
    );
    assert!(tick
        .iter()
        .all(|change| change.source == Some(Source::Claude) && change.session_id == SESSION));

    // Nothing changed: a tick is empty, and the head stays put.
    let head = store.head_revision().unwrap();
    store.sync(Default::default()).unwrap();
    assert_eq!(store.head_revision().unwrap(), head);
    assert!(drain(&store, head).is_empty());

    // The message completes with a second block; the whole message lands.
    let done = assistant_line(
        "a-2",
        "a-1",
        r#"{"type":"tool_use","id":"toolu_live_1","name":"Bash","input":{"command":"ls"}}"#,
        "\"tool_use\"",
        "2026-08-31T10:00:02.000Z",
    );
    let mut contents = fs::read_to_string(&transcript).unwrap();
    contents.push_str(&done);
    fs::write(&transcript, contents).unwrap();
    // A rewrite inside the same second is invisible to a stat-only check on
    // some filesystems; make the change unambiguous.
    let later = filetime_now_plus(2);
    fs::File::open(&transcript)
        .unwrap()
        .set_modified(later)
        .unwrap();
    store.sync(Default::default()).unwrap();

    let tick = drain(&store, head);
    let mut uids: Vec<String> = tick
        .iter()
        .filter_map(|change| match &change.op {
            ChangeOp::Upsert(EvidenceRow::SessionEvent(event)) => Some(event.event_uid.clone()),
            _ => None,
        })
        .collect();
    uids.sort();
    assert_eq!(
        uids,
        vec!["a-1:0".to_string(), "a-2:0".to_string()],
        "one upsert per block of the completed message, and no repeat of the prompt"
    );
    assert!(
        tick.iter().any(
            |change| change.kind == ChangeKind::ToolCall && change.record_key == "toolu_live_1"
        ),
        "the tool use the completing block carried is in the same tick"
    );
    assert!(tick.iter().all(|change| change.op != ChangeOp::Delete));
    let evidence = store
        .session(
            &SessionRef::id(Source::Claude, SESSION),
            SessionQuery::default(),
        )
        .unwrap()
        .expect("the completed session is catalogued");
    assert_eq!(
        evidence.requests.len(),
        1,
        "the blocks are one request, readable once the message is complete"
    );
}

fn filetime_now_plus(seconds: u64) -> std::time::SystemTime {
    std::time::SystemTime::now() + std::time::Duration::from_secs(seconds)
}

/// A record as the feed and the tables both name it: its kind and
/// [`Change::key`], serialized.
type Key = (ChangeKind, String);
/// A stored row: every column but `revision`, in table order.
type Row = Vec<(String, Value)>;

/// Apply a feed in order: an upsert replaces, a delete removes.
fn replay(changes: &[Change]) -> BTreeMap<Key, Row> {
    let mut state = BTreeMap::new();
    let mut last = 0;
    for change in changes {
        assert!(
            change.revision > last,
            "the feed is strictly ordered by revision: {change:?}"
        );
        last = change.revision;
        let key = (change.kind, serde_json::to_string(&change.key).unwrap());
        match &change.op {
            ChangeOp::Upsert(_) => {
                let columns = change.columns.as_ref().expect("an upsert carries its row");
                state.insert(
                    key,
                    columns
                        .iter()
                        .map(|(name, value)| (name.to_string(), value.clone()))
                        .collect(),
                );
            }
            ChangeOp::Delete => {
                state.remove(&key);
            }
            other => panic!("an op this consumer does not know: {other:?}"),
        }
    }
    state
}

/// Each kind's table, its source (a column, or the literal a table without
/// one shares) and the columns of its identity after the source.
const TABLES: &[(ChangeKind, &str, Option<&str>, &[&str])] = &[
    (ChangeKind::Session, "sessions", None, &["session_id"]),
    (
        ChangeKind::SessionEvent,
        "session_events",
        None,
        &["session_id", "event_uid"],
    ),
    (
        ChangeKind::ToolCall,
        "tool_calls",
        None,
        &["session_id", "tool_use_id"],
    ),
    (
        ChangeKind::FileEdit,
        "file_edits",
        None,
        &["session_id", "tool_use_id"],
    ),
    (
        ChangeKind::SessionMarker,
        "session_markers",
        None,
        &["session_id", "marker_uid"],
    ),
    (
        ChangeKind::Relationship,
        "session_relationships",
        None,
        &["parent_session_id", "relationship_uid"],
    ),
    (
        ChangeKind::History,
        "history",
        None,
        &["timestamp_ms", "prompt"],
    ),
    (
        ChangeKind::Presence,
        "session_presences",
        None,
        &["session_id", "location"],
    ),
    (
        ChangeKind::CommitLink,
        "session_commit_links",
        None,
        &["session_id", "commit_sha", "match_method"],
    ),
    (
        ChangeKind::Trajectory,
        "trajectories",
        Some("trajectory"),
        &["id"],
    ),
    (
        ChangeKind::SourceObservation,
        "session_observations",
        None,
        &[
            "session_id",
            "location",
            "connector_id",
            "connector_instance",
        ],
    ),
    (
        ChangeKind::ObservationEvidence,
        "observation_evidence",
        None,
        &[
            "session_id",
            "location",
            "connector_id",
            "connector_instance",
            "evidence_uid",
        ],
    ),
];

fn sqlite_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(integer) => Value::from(integer),
        ValueRef::Real(real) => Value::from(real),
        ValueRef::Text(text) => Value::from(String::from_utf8(text.to_vec()).unwrap()),
        ValueRef::Blob(bytes) => Value::from(bytes.to_vec()),
    }
}

/// Every row of one table as stored, every column but `revision`.
fn stored_rows(conn: &Connection, table: &str) -> Vec<Row> {
    let mut statement = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
    let names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let rows = statement
        .query_map([], |row| {
            let mut stored = Row::new();
            for (index, name) in names.iter().enumerate() {
                if name != "revision" {
                    stored.push((name.clone(), sqlite_value(row.get_ref(index)?)));
                }
            }
            Ok(stored)
        })
        .unwrap();
    rows.collect::<rusqlite::Result<_>>().unwrap()
}

/// Every fed table as it stands, keyed the way the feed keys it.
fn direct(conn: &Connection) -> BTreeMap<Key, Row> {
    let mut state = BTreeMap::new();
    for (kind, table, literal_source, identity) in TABLES {
        for row in stored_rows(conn, table) {
            let value = |column: &str| {
                row.iter()
                    .find(|(name, _)| name == column)
                    .map(|(_, value)| value.clone())
                    .unwrap()
            };
            let mut key = vec![Value::from(kind.as_str())];
            if literal_source.is_none() {
                key.push(value("source"));
            }
            key.extend(identity.iter().map(|column| value(column)));
            state.insert((*kind, serde_json::to_string(&key).unwrap()), row);
        }
    }
    state
}

fn assert_replay_matches_tables(home: &Home, store: &SessionStore, step: &str) {
    let replayed = replay(&drain(store, Watermark::START));
    let tables = direct(&home.raw());
    let replayed_keys: BTreeSet<&Key> = replayed.keys().collect();
    let table_keys: BTreeSet<&Key> = tables.keys().collect();
    assert_eq!(
        replayed_keys, table_keys,
        "{step}: replaying the feed from START names exactly the rows the tables hold"
    );
    for (key, row) in &replayed {
        assert_eq!(
            row, &tables[key],
            "{step}: the replayed row for {key:?} is the row the table holds, column for column"
        );
    }
    assert!(
        !replayed.is_empty(),
        "{step}: the check must run over something"
    );
}

/// For a sequence of syncs over the fixture corpus — files arriving, a file
/// growing, a provider joining, a sync that finds nothing new — replaying
/// `changes_since(START)` reconstructs the same table contents as a direct
/// read, and a consumer resuming from its own commit sees exactly the delta.
#[test]
fn replaying_the_feed_reconstructs_the_tables_after_every_sync() {
    let home = Home::new();
    let store = home.store();
    let consumer = || ChangeQuery::default().consumer("burn");
    let mut kinds_seen = BTreeSet::new();

    // Step 1: one transcript.
    let simple = home.stage_claude("simple-turn.jsonl");
    store.sync(Default::default()).unwrap();
    assert_replay_matches_tables(&home, &store, "one transcript");
    let mut first = store
        .changes_since(Watermark::CONSUMER, consumer())
        .unwrap();
    let first_count = first.by_ref().count();
    assert!(first_count > 0);
    kinds_seen.extend(
        drain(&store, Watermark::START)
            .iter()
            .map(|change| change.kind),
    );
    let committed = first.commit().unwrap();
    assert_eq!(committed, first.head());

    // Step 2: more transcripts, with tool calls, file edits, markers and a
    // delegation.
    for fixture in [
        "multi-block-turn.jsonl",
        "edit-revert.jsonl",
        "compact-boundary.jsonl",
        "resume-marker.jsonl",
    ] {
        home.stage_claude(fixture);
    }
    store.sync(Default::default()).unwrap();
    assert_replay_matches_tables(&home, &store, "several transcripts");
    let delta = store
        .changes_since(Watermark::CONSUMER, consumer())
        .unwrap();
    assert_eq!(delta.position(), committed, "resumes from the commit");
    let delta: Vec<Change> = delta.map(|change| change.unwrap()).collect();
    // The resumed session gains a relationship row and its catalog row is
    // touched; its events, which nothing rewrote, are not re-reported.
    let re_reported: Vec<&str> = delta
        .iter()
        .filter(|change| {
            change.session_id == "11111111-1111-1111-1111-111111111111"
                && change.kind == ChangeKind::SessionEvent
        })
        .map(|change| change.record_key.as_str())
        .collect();
    assert!(
        re_reported.is_empty(),
        "an unchanged transcript is not re-reported: {re_reported:?}"
    );
    kinds_seen.extend(delta.iter().map(|change| change.kind));

    // Step 3: a transcript grows by one complete turn.
    let mut contents = fs::read_to_string(&simple).unwrap();
    contents.push_str(
        r#"{"parentUuid":null,"isSidechain":false,"type":"user","message":{"role":"user","content":"and again"},"uuid":"u-late-1","timestamp":"2026-04-21T00:00:00.000Z","cwd":"/tmp/project","sessionId":"11111111-1111-1111-1111-111111111111","version":"2.1.96"}"#,
    );
    contents.push('\n');
    fs::write(&simple, contents).unwrap();
    fs::File::open(&simple)
        .unwrap()
        .set_modified(filetime_now_plus(2))
        .unwrap();
    let before = store.head_revision().unwrap();
    store.sync(Default::default()).unwrap();
    assert_replay_matches_tables(&home, &store, "a transcript grew");
    let grown = drain(&store, before);
    assert!(
        grown.iter().any(|change| matches!(
            &change.op,
            ChangeOp::Upsert(EvidenceRow::SessionEvent(event)) if event.event_uid == "u-late-1:0"
        )),
        "the appended turn is in the delta: {grown:?}"
    );

    // Step 4: a second provider.
    home.stage_codex("compaction.jsonl");
    home.stage_codex("with-tool-call.jsonl");
    store.sync(Default::default()).unwrap();
    assert_replay_matches_tables(&home, &store, "a second provider");
    let codex: Vec<Change> = drain(&store, Watermark::START)
        .into_iter()
        .filter(|change| change.source == Some(Source::Codex))
        .collect();
    assert!(!codex.is_empty());
    kinds_seen.extend(codex.iter().map(|change| change.kind));

    // Step 5: nothing changed.
    let head = store.head_revision().unwrap();
    store.sync(Default::default()).unwrap();
    assert_eq!(
        store.head_revision().unwrap(),
        head,
        "no write, no revision"
    );
    assert!(drain(&store, head).is_empty());
    assert_replay_matches_tables(&home, &store, "an idle sync");

    // The corpus exercised every kind it writes, so the replay check above
    // covered every table the syncs filled rather than only the easy ones.
    let filled: BTreeSet<ChangeKind> = direct(&home.raw()).keys().map(|(kind, _)| *kind).collect();
    assert_eq!(kinds_seen, filled, "every kind the corpus wrote was fed");
    for kind in [
        ChangeKind::Session,
        ChangeKind::SessionEvent,
        ChangeKind::ToolCall,
        ChangeKind::FileEdit,
        ChangeKind::SessionMarker,
        ChangeKind::Relationship,
        ChangeKind::Presence,
    ] {
        assert!(filled.contains(&kind), "the corpus writes {kind:?}");
    }

    // A second consumer, starting now, replays everything independently of
    // the first one's commit.
    let fresh = store
        .changes_since(
            Watermark::CONSUMER,
            ChangeQuery::default().consumer("other"),
        )
        .unwrap();
    assert_eq!(fresh.position().revision, 0);
    assert_eq!(
        replay(&fresh.map(|c| c.unwrap()).collect::<Vec<_>>()).len(),
        direct(&home.raw()).len()
    );
}

fn only(store: &SessionStore, from: Watermark, kind: ChangeKind) -> Vec<Change> {
    store
        .changes_since(from, ChangeQuery::default().kinds([kind]))
        .unwrap()
        .map(|change| change.unwrap())
        .collect()
}

/// A prompt from a provider's prompt log is a `history` change, keyed the
/// way the prompt log is unique, with its typed entry and its stored row.
#[test]
fn a_history_prompt_reaches_the_feed_with_its_stored_row() {
    let home = Home::new();
    fs::create_dir_all(home.path().join(".claude")).unwrap();
    fs::write(
        home.path().join(".claude/history.jsonl"),
        "{\"display\":\"ship the feed\",\"timestamp\":1756634400000,\
         \"project\":\"/tmp/project\",\"sessionId\":\"history-session\"}\n",
    )
    .unwrap();
    let store = home.store();
    store.sync(Default::default()).unwrap();

    let history = only(&store, Watermark::START, ChangeKind::History);
    assert_eq!(history.len(), 1, "{history:?}");
    let change = &history[0];
    assert_eq!(change.source, Some(Source::Claude));
    assert_eq!(change.source_name, "claude");
    assert_eq!(change.session_id, "history-session");
    assert_eq!(
        change.key,
        vec![
            Value::from("history"),
            Value::from("claude"),
            Value::from(1_756_634_400_000i64),
            Value::from("ship the feed"),
        ]
    );
    match &change.op {
        ChangeOp::Upsert(EvidenceRow::History(entry)) => {
            assert_eq!(entry.prompt, "ship the feed");
            assert_eq!(entry.timestamp_ms, 1_756_634_400_000);
        }
        other => panic!("a typed history row: {other:?}"),
    }
    let columns = change.columns.as_ref().unwrap();
    let stored = stored_rows(&home.raw(), "history");
    let row: Row = columns
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect();
    assert_eq!(vec![row], stored, "the stored row, column for column");
    assert_eq!(columns.get("project"), Some(&Value::from("/tmp/project")));
    assert!(columns.get("revision").is_none());
}

/// A column a table gains after the feed was built is carried as soon as it
/// exists, with every other column exactly as stored: JSON text stays text,
/// integers stay integers, NULL stays null.
#[test]
fn a_column_a_table_gains_is_carried_verbatim() {
    let home = Home::new();
    home.stage_claude("multi-block-turn.jsonl");
    let store = home.store();
    store.sync(Default::default()).unwrap();

    let head = store.head_revision().unwrap();
    let writer = home.raw_writer();
    writer
        .execute_batch(
            "ALTER TABLE tool_calls ADD COLUMN review_note TEXT; \
             UPDATE tool_calls SET review_note = 'looked fine' \
                 WHERE rowid = (SELECT MIN(rowid) FROM tool_calls);",
        )
        .unwrap();
    let changes = only(&store, head, ChangeKind::ToolCall);
    assert_eq!(changes.len(), 1, "{changes:?}");
    let columns = changes[0].columns.as_ref().unwrap();
    assert_eq!(
        columns.get("review_note"),
        Some(&Value::from("looked fine"))
    );
    let names: Vec<&str> = columns.iter().map(|(name, _)| name).collect();
    assert_eq!(names.last(), Some(&"review_note"), "in table order");
    let row: Row = columns
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect();
    let stored = stored_rows(&home.raw(), "tool_calls");
    assert!(stored.contains(&row), "the stored row, column for column");
    assert!(
        columns.get("args_json").is_some_and(Value::is_string),
        "JSON text stays text"
    );
    assert!(columns.get("id").is_some_and(Value::is_i64));

    // The session row carries every catalog column, `parser_version`
    // included, and its JSON columns unparsed.
    let sessions = only(&store, Watermark::START, ChangeKind::Session);
    let catalog = sessions[0].columns.as_ref().unwrap();
    assert!(catalog.get("parser_version").is_some_and(Value::is_i64));
    assert!(catalog
        .get("models_json")
        .is_some_and(|models| models.is_string() || models.is_null()));
    assert!(catalog.get("locations").is_none(), "no derived column");
}

/// A delete carries the key the record's upserts carried, including for a
/// kind whose identity spans several columns of different types.
#[test]
fn a_tombstone_carries_the_record_key() {
    let home = Home::new();
    home.stage_claude("multi-block-turn.jsonl");
    fs::write(
        home.path().join(".claude/history.jsonl"),
        "{\"display\":\"to be removed\",\"timestamp\":42,\"sessionId\":\"gone\"}\n",
    )
    .unwrap();
    let store = home.store();
    store.sync(Default::default()).unwrap();
    let upserts: Vec<Change> = drain(&store, Watermark::START);
    let call = upserts
        .iter()
        .find(|change| change.kind == ChangeKind::ToolCall)
        .unwrap();
    let prompt = upserts
        .iter()
        .find(|change| change.kind == ChangeKind::History)
        .unwrap();

    let head = store.head_revision().unwrap();
    let writer = home.raw_writer();
    writer
        .execute(
            "DELETE FROM tool_calls WHERE tool_use_id = ?",
            [&call.record_key],
        )
        .unwrap();
    writer
        .execute("DELETE FROM history WHERE timestamp_ms = 42", [])
        .unwrap();
    let deletes = drain(&store, head);
    assert_eq!(deletes.len(), 2, "{deletes:?}");
    for (delete, upsert) in deletes.iter().zip([call, prompt]) {
        assert_eq!(delete.op, ChangeOp::Delete);
        assert!(delete.columns.is_none());
        assert_eq!(delete.kind, upsert.kind);
        assert_eq!(delete.key, upsert.key, "the same identity");
        assert_eq!(delete.record_key, upsert.record_key);
        assert_eq!(delete.source_name, upsert.source_name);
    }
    assert_eq!(deletes[1].key[2], Value::from(42), "an integer stays one");
}

/// A row from a source this build does not know is carried, not a failed
/// drain: `source` is `None` and `source_name` names it.
#[test]
fn a_row_from_an_unknown_source_is_carried() {
    let home = Home::new();
    let store = home.store();
    home.raw_writer()
        .execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
             VALUES ('some-new-agent', 's1', 'hi', 7)",
            [],
        )
        .unwrap();
    let changes = drain(&store, Watermark::START);
    assert_eq!(changes.len(), 1, "{changes:?}");
    assert_eq!(changes[0].source, None);
    assert_eq!(changes[0].source_name, "some-new-agent");
    assert_eq!(changes[0].key[1], Value::from("some-new-agent"));
    home.raw_writer()
        .execute("DELETE FROM history", [])
        .unwrap();
    let changes = drain(&store, Watermark::START);
    assert_eq!(changes[0].op, ChangeOp::Delete);
    assert_eq!(changes[0].source_name, "some-new-agent");
}

fn session_drain(
    store: &SessionStore,
    from: Watermark,
    source: &str,
    session: &str,
) -> Vec<Change> {
    store
        .changes_since(from, ChangeQuery::default().session(source, session))
        .unwrap()
        .map(|change| change.unwrap())
        .collect()
}

/// Every session the unfiltered drain names.
fn sessions_in(changes: &[Change]) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| !change.session_id.is_empty())
        .map(|change| (change.source_name.clone(), change.session_id.clone()))
        .collect()
}

fn assert_session_drains_match(store: &SessionStore, from: Watermark, step: &str) {
    let everything = drain(store, from);
    let sessions = sessions_in(&everything);
    assert!(sessions.len() > 1, "{step}: several sessions to tell apart");
    for (source, session) in &sessions {
        let expected: Vec<&Change> = everything
            .iter()
            .filter(|change| &change.source_name == source && &change.session_id == session)
            .collect();
        let filtered = session_drain(store, from, source, session);
        assert_eq!(
            filtered.iter().collect::<Vec<_>>(),
            expected,
            "{step}: {source} {session} is the whole feed restricted to that session"
        );
        assert!(!filtered.is_empty());
    }
}

/// A drain restricted to one session is the unfiltered drain restricted to
/// that session -- the same changes, rows, keys and revisions, tombstones
/// included -- and names no other session, after syncs, updates and deletes.
#[test]
fn a_session_drain_is_the_feed_restricted_to_that_session() {
    let home = Home::new();
    for fixture in [
        "simple-turn.jsonl",
        "multi-block-turn.jsonl",
        "edit-revert.jsonl",
        "compact-boundary.jsonl",
        "resume-marker.jsonl",
    ] {
        home.stage_claude(fixture);
    }
    home.stage_codex("compaction.jsonl");
    home.stage_codex("with-tool-call.jsonl");
    fs::write(
        home.path().join(".claude/history.jsonl"),
        "{\"display\":\"a prompt with a session\",\"timestamp\":5,\
         \"sessionId\":\"11111111-1111-1111-1111-111111111111\"}\n\
         {\"display\":\"a prompt without one\",\"timestamp\":6}\n",
    )
    .unwrap();
    let store = home.store();
    store.sync(Default::default()).unwrap();
    assert_session_drains_match(&store, Watermark::START, "after a sync");

    // Updates, a deleted row, and a whole session deleted with its cascade.
    let head = store.head_revision().unwrap();
    let writer = home.raw_writer();
    let doomed: String = writer
        .query_row(
            "SELECT session_id FROM sessions WHERE source = 'claude' \
             AND session_id <> '11111111-1111-1111-1111-111111111111' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    writer
        .execute_batch(&format!(
            "UPDATE session_events SET text = 'edited' \
                 WHERE rowid = (SELECT MIN(rowid) FROM session_events); \
             DELETE FROM tool_calls WHERE rowid = (SELECT MIN(rowid) FROM tool_calls); \
             UPDATE history SET project = '/elsewhere' WHERE timestamp_ms = 5; \
             DELETE FROM sessions WHERE source = 'claude' AND session_id = '{doomed}';"
        ))
        .unwrap();
    assert_session_drains_match(&store, Watermark::START, "after updates and deletes");
    assert_session_drains_match(&store, head, "from a watermark");
    let gone = session_drain(&store, head, "claude", &doomed);
    assert!(
        gone.iter()
            .any(|change| change.kind == ChangeKind::Session && change.op == ChangeOp::Delete),
        "the deleted session's own tombstones reach its drain: {gone:?}"
    );

    // The drain is bounded to the head at open, like the unfiltered one.
    let changes = store
        .changes_since(
            Watermark::START,
            ChangeQuery::default().session("claude", "11111111-1111-1111-1111-111111111111"),
        )
        .unwrap();
    assert_eq!(changes.head(), store.head_revision().unwrap());

    // A prompt that names no session is in the feed, and in no session's
    // drain.
    let prompts = only(&store, Watermark::START, ChangeKind::History);
    assert!(prompts.iter().any(|change| change.session_id.is_empty()));
    for (source, session) in sessions_in(&drain(&store, Watermark::START)) {
        assert!(session_drain(&store, Watermark::START, &source, &session)
            .iter()
            .all(|change| !change.session_id.is_empty()));
    }
}

/// A session under a source this build does not know drains like any other,
/// and a trajectory is the session its id names.
#[test]
fn a_session_drain_takes_any_stored_source() {
    let home = Home::new();
    let store = home.store();
    home.raw_writer()
        .execute_batch(
            "INSERT INTO sessions (session_id, source) VALUES ('n1', 'some-new-agent'); \
             INSERT INTO sessions (session_id, source) VALUES ('n2', 'some-new-agent'); \
             INSERT INTO history (source, session_id, prompt, timestamp_ms) \
                 VALUES ('some-new-agent', 'n1', 'hi', 7); \
             INSERT INTO trajectories (id, decisions_json, retrospective_json, search_text, \
                 updated_ms, timestamp_ms) VALUES ('traj-1', '[]', '{}', 'x', 1, 1);",
        )
        .unwrap();
    let changes = session_drain(&store, Watermark::START, "some-new-agent", "n1");
    let kinds: Vec<ChangeKind> = changes.iter().map(|change| change.kind).collect();
    assert_eq!(kinds, vec![ChangeKind::Session, ChangeKind::History]);
    assert!(changes.iter().all(|change| change.source.is_none()));

    let trajectory = session_drain(&store, Watermark::START, "trajectory", "traj-1");
    assert_eq!(trajectory.len(), 1);
    assert_eq!(trajectory[0].kind, ChangeKind::Trajectory);
    assert!(session_drain(&store, Watermark::START, "claude", "traj-1").is_empty());
}

/// A session drain is a one-shot read: it cannot name a consumer, so it
/// can never move one, and an empty identity is refused rather than read.
#[test]
fn a_session_drain_refuses_a_consumer() {
    let home = Home::new();
    let store = home.store();
    for (from, query) in [
        (
            Watermark::CONSUMER,
            ChangeQuery::default()
                .consumer("probe")
                .session("claude", "s1"),
        ),
        (
            Watermark::START,
            ChangeQuery::default()
                .consumer("probe")
                .session("claude", "s1"),
        ),
        (
            Watermark::START,
            ChangeQuery::default().session("claude", ""),
        ),
        (Watermark::START, ChangeQuery::default().session("", "s1")),
    ] {
        let error = store.changes_since(from, query).unwrap_err();
        assert_eq!(error.code(), "INVALID_ARGUMENT", "{error}");
    }
    let changes = store
        .changes_since(
            Watermark::START,
            ChangeQuery::default().session("claude", "s1"),
        )
        .unwrap();
    assert_eq!(changes.commit().unwrap_err().code(), "INVALID_ARGUMENT");
}

/// One session's backfill against a store of many: run with `--ignored
/// --nocapture` in release to print the timing.
#[test]
#[ignore]
fn a_session_drain_reads_one_session_of_many() {
    const SESSIONS: usize = 2_000;
    const EVENTS: usize = 50;
    let home = Home::new();
    let store = home.store();
    let mut writer = home.raw_writer();
    let tx = writer.transaction().unwrap();
    for session in 0..SESSIONS {
        let id = format!("s{session:05}");
        tx.execute(
            "INSERT INTO sessions (session_id, source) VALUES (?, 'claude')",
            [&id],
        )
        .unwrap();
        for event in 0..EVENTS {
            tx.execute(
                "INSERT INTO session_events (source, session_id, message_id, ts_ms, role, \
                 kind, text, event_uid) VALUES ('claude', ?1, 'm', ?2, 'assistant', 'text', \
                 'some text', ?3)",
                rusqlite::params![id, event as i64, format!("e{event}")],
            )
            .unwrap();
        }
        tx.execute(
            "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
             VALUES ('claude', ?, 't1', 'Bash')",
            [&id],
        )
        .unwrap();
    }
    // And one long session, whose backfill pages many times over.
    const LONG: usize = 50_000;
    tx.execute(
        "INSERT INTO sessions (session_id, source) VALUES ('long', 'claude')",
        [],
    )
    .unwrap();
    for event in 0..LONG {
        tx.execute(
            "INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, \
             text, event_uid) VALUES ('claude', 'long', 'm', ?1, 'assistant', 'text', \
             'some text', ?2)",
            rusqlite::params![event as i64, format!("e{event}")],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let rows = SESSIONS * (EVENTS + 2) + LONG + 1;

    let started = std::time::Instant::now();
    let everything = drain(&store, Watermark::START).len();
    let full = started.elapsed();
    let started = std::time::Instant::now();
    let one = session_drain(&store, Watermark::START, "claude", "s01000").len();
    let filtered = started.elapsed();
    let started = std::time::Instant::now();
    let unfiltered_one = drain(&store, Watermark::START)
        .into_iter()
        .filter(|change| change.session_id == "s01000")
        .count();
    let replay = started.elapsed();
    assert_eq!(everything, rows);
    assert_eq!(one, EVENTS + 2);
    assert_eq!(unfiltered_one, one);
    eprintln!(
        "{SESSIONS} sessions, {rows} rows: one-session drain {one} changes in {filtered:?}; \
         full drain {everything} changes in {full:?}; full replay filtered to the session \
         in {replay:?}"
    );
    assert!(filtered * 20 < full, "{filtered:?} vs {full:?}");

    let started = std::time::Instant::now();
    let long = session_drain(&store, Watermark::START, "claude", "long").len();
    let long_elapsed = started.elapsed();
    assert_eq!(long, LONG + 1);
    eprintln!("one session of {long} changes, default batch: {long_elapsed:?}");
    let started = std::time::Instant::now();
    let long = store
        .changes_since(
            Watermark::START,
            ChangeQuery::default()
                .session("claude", "long")
                .batch(ai_hist::MAX_CHANGE_BATCH),
        )
        .unwrap()
        .count();
    eprintln!(
        "one session of {long} changes, batch {}: {:?}",
        ai_hist::MAX_CHANGE_BATCH,
        started.elapsed()
    );

    // Every identity, paged at the default size.
    let started = std::time::Instant::now();
    let mut identities: Vec<ai_hist::SessionIdentity> = Vec::new();
    loop {
        let mut query = ai_hist::IdentityQuery::default();
        if let Some(last) = identities.last() {
            query = query.after(last.clone());
        }
        let page = store.session_identities(query).unwrap();
        if page.is_empty() {
            break;
        }
        identities.extend(page);
    }
    assert_eq!(identities.len(), SESSIONS + 1);
    eprintln!(
        "{} identities over {rows} rows, paged at 1,000: {:?}",
        identities.len(),
        started.elapsed()
    );

    // The same pages as a three-table UNION over the whole tables, for scale.
    let started = std::time::Instant::now();
    let reader = home.raw();
    let mut union: Vec<(String, String)> = Vec::new();
    loop {
        let last = union.last().cloned();
        let page: Vec<(String, String)> = reader
            .prepare(
                "SELECT source, session_id FROM (
                    SELECT source, session_id FROM sessions
                    UNION SELECT source, session_id FROM history WHERE session_id IS NOT NULL
                    UNION SELECT source, session_id FROM session_events
                ) WHERE ?1 IS NULL OR (source, session_id) > (?1, ?2)
                ORDER BY source, session_id LIMIT 1000",
            )
            .unwrap()
            .query_map(
                rusqlite::params![
                    last.as_ref().map(|(source, _)| source.clone()),
                    last.as_ref().map(|(_, session)| session.clone())
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        if page.is_empty() {
            break;
        }
        union.extend(page);
    }
    assert_eq!(union.len(), identities.len());
    eprintln!(
        "the same identities as a three-table UNION: {:?}",
        started.elapsed()
    );
}
