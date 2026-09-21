//! The change feed through the public `SessionStore` surface, over real
//! provider files: what a downstream consumer sees when it ticks after each
//! sync, and that replaying the feed from the start rebuilds the tables.
//!
//! Public API only, so this runs in the `--no-default-features` job too. The
//! direct table reads the replay is checked against open the SQLite file
//! themselves, which is the one thing a consumer must never need to do.

use ai_hist::{
    Change, ChangeKind, ChangeOp, ChangeQuery, EvidenceRow, SessionStore, Source, StoreOptions,
    Watermark,
};
use rusqlite::{Connection, OpenFlags};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

struct Home {
    dir: tempfile::TempDir,
}

impl Home {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
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
        .all(|change| change.source == Source::Claude && change.session_id == SESSION));

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
    let requests = store
        .session_requests_page(Source::Claude, SESSION, 10, None)
        .unwrap();
    assert_eq!(
        requests.requests.len(),
        1,
        "the blocks are one request, readable once the message is complete"
    );
}

fn filetime_now_plus(seconds: u64) -> std::time::SystemTime {
    std::time::SystemTime::now() + std::time::Duration::from_secs(seconds)
}

type Key = (ChangeKind, String, String, String);

/// Apply a feed in order: an upsert replaces, a delete removes.
fn replay(changes: &[Change]) -> BTreeMap<Key, EvidenceRow> {
    let mut state = BTreeMap::new();
    let mut last = 0;
    for change in changes {
        assert!(
            change.revision > last,
            "the feed is strictly ordered by revision: {change:?}"
        );
        last = change.revision;
        let key = (
            change.kind,
            change.source.as_str().to_string(),
            change.session_id.clone(),
            change.record_key.clone(),
        );
        match &change.op {
            ChangeOp::Upsert(row) => {
                state.insert(key, row.clone());
            }
            ChangeOp::Delete => {
                state.remove(&key);
            }
            other => panic!("an op this consumer does not know: {other:?}"),
        }
    }
    state
}

/// The tables as they stand, keyed the way the feed keys them.
fn direct(conn: &Connection) -> BTreeMap<Key, Option<String>> {
    let mut rows = BTreeMap::new();
    let reads: [(ChangeKind, &str); 6] = [
        (
            ChangeKind::Session,
            "SELECT source, session_id, session_id, cwd FROM sessions",
        ),
        (
            ChangeKind::SessionEvent,
            "SELECT source, session_id, event_uid, text FROM session_events",
        ),
        (
            ChangeKind::ToolCall,
            "SELECT source, session_id, tool_use_id, name FROM tool_calls",
        ),
        (
            ChangeKind::FileEdit,
            "SELECT source, session_id, tool_use_id, file_path FROM file_edits",
        ),
        (
            ChangeKind::SessionMarker,
            "SELECT source, session_id, marker_uid, kind FROM session_markers",
        ),
        (
            ChangeKind::Relationship,
            "SELECT source, parent_session_id, relationship_uid, relationship \
             FROM session_relationships",
        ),
    ];
    for (kind, sql) in reads {
        let mut statement = conn.prepare(sql).unwrap();
        let read = statement
            .query_map([], |row| {
                Ok((
                    (
                        kind,
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ),
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .unwrap();
        for entry in read {
            let (key, value) = entry.unwrap();
            rows.insert(key, value);
        }
    }
    rows
}

fn value_of(row: &EvidenceRow) -> Option<String> {
    match row {
        EvidenceRow::Session(session) => session.cwd.clone(),
        EvidenceRow::SessionEvent(event) => event.text.clone(),
        EvidenceRow::ToolCall(call) => Some(call.name.clone()),
        EvidenceRow::FileEdit(edit) => Some(edit.file_path.clone()),
        EvidenceRow::SessionMarker(marker) => Some(marker.kind.clone()),
        EvidenceRow::Relationship(relationship) => Some(relationship.relationship.clone()),
        other => panic!("a row this consumer does not know: {other:?}"),
    }
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
            &value_of(row),
            &tables[key],
            "{step}: the replayed row for {key:?} is the row the table holds"
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
        .filter(|change| change.source == Source::Codex)
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

    // The corpus exercised every kind the feed reports, so the replay check
    // above covered every table rather than only the easy ones.
    let expected: BTreeSet<ChangeKind> = ChangeKind::ALL.iter().copied().collect();
    assert_eq!(kinds_seen, expected, "every kind was fed by the corpus");

    // A second consumer, starting now, replays everything independently of
    // the first one's commit.
    let fresh = store
        .changes_since(
            Watermark::CONSUMER,
            ChangeQuery::default().consumer("other"),
        )
        .unwrap();
    assert_eq!(fresh.position(), Watermark::START);
    assert_eq!(
        replay(&fresh.map(|c| c.unwrap()).collect::<Vec<_>>()).len(),
        direct(&home.raw()).len()
    );
}
