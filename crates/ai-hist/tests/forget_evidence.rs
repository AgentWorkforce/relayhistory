//! `SessionStore::forget_evidence` and `SessionStore::compact`, on the
//! crate's default features: an embedder that keeps evidence for a chosen set
//! of sessions drops the rest, keeps the catalog, and gets the space back.

use ai_hist::{
    CatalogQuery, ChangeKind, ChangeOp, ChangeQuery, CompactOptions, DiscoveryOptions,
    DiscoveryState, Error, ForgetOptions, ForgetScope, HydrateOptions, HydrateStatus,
    ProviderRoots, SessionEvidence, SessionQuery, SessionRef, SessionStore, Source, StopToken,
    StoreOptions, SyncOptions,
};
use std::fs;
use std::path::Path;

fn fixtures() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures"))
}

/// A provider home holding one Claude transcript and one Codex rollout.
fn stage(home: &Path) {
    let project = home.join(".claude/projects/corpus");
    fs::create_dir_all(&project).unwrap();
    fs::copy(
        fixtures().join("claude/files-touched.jsonl"),
        project.join("files-touched.jsonl"),
    )
    .unwrap();
    let day = home.join(".codex/sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::copy(
        fixtures().join("codex/with-tool-call.jsonl"),
        day.join("rollout-2026-04-20T00-00-00-with-tool-call.jsonl"),
    )
    .unwrap();
}

fn open(home: &Path, read_only: bool) -> SessionStore {
    let options = StoreOptions::default()
        .db_path(home.join("ai-history.db"))
        .roots(ProviderRoots::from_home(
            home.to_path_buf(),
            home.join(".local/share/opencode/opencode.db"),
        ))
        .read_only(read_only);
    SessionStore::open(options).expect("open")
}

fn session_of(store: &SessionStore, source: Source) -> SessionRef {
    store
        .sessions(CatalogQuery::default())
        .map(Result::unwrap)
        .find(|row| row.source == source)
        .expect("catalogued")
        .session_ref()
}

fn state_of(store: &SessionStore, session: &SessionRef) -> DiscoveryState {
    store
        .sessions(CatalogQuery::default())
        .map(Result::unwrap)
        .find(|row| &row.session_ref() == session)
        .expect("still catalogued")
        .discovery_state
}

fn evidence(store: &SessionStore, session: &SessionRef) -> SessionEvidence {
    store
        .session(session, SessionQuery::default())
        .unwrap()
        .expect("catalogued")
}

fn count(home: &Path, sql: &str) -> i64 {
    rusqlite::Connection::open(home.join("ai-history.db"))
        .unwrap()
        .query_row(sql, [], |row| row.get(0))
        .unwrap()
}

/// Discover both sessions and hydrate them, the way a selective embedder
/// fills its store.
fn hydrated() -> (tempfile::TempDir, SessionStore, SessionRef, SessionRef) {
    let dir = tempfile::tempdir().unwrap();
    stage(dir.path());
    let store = open(dir.path(), false);
    store.discover(DiscoveryOptions::default()).unwrap();
    let claude = session_of(&store, Source::Claude);
    let codex = session_of(&store, Source::Codex);
    for session in [&claude, &codex] {
        let report = store.hydrate(session, HydrateOptions::default()).unwrap();
        assert_eq!(report.status, HydrateStatus::Hydrated);
    }
    (dir, store, claude, codex)
}

#[test]
fn a_forgotten_session_stays_catalogued_shallow_and_hydrates_back_in_full() {
    let (dir, store, claude, codex) = hydrated();
    let before = evidence(&store, &claude);
    assert!(!before.messages.is_empty() && !before.tool_calls.is_empty());
    let kept = evidence(&store, &codex);
    let head = store.head_revision().unwrap();

    let report = store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(report.sessions, 1);
    assert!(report.events > 0 && report.tool_calls > 0);

    // Still listed, now shallow, with nothing behind it; the other session
    // is untouched.
    assert_eq!(state_of(&store, &claude), DiscoveryState::Shallow);
    assert_eq!(state_of(&store, &codex), DiscoveryState::Full);
    let forgotten = evidence(&store, &claude);
    assert!(forgotten.messages.is_empty());
    assert!(forgotten.tool_calls.is_empty() && forgotten.file_edits.is_empty());
    assert_eq!(evidence(&store, &codex), kept);
    let SessionRef::Id { session_id, .. } = &claude else {
        unreachable!()
    };
    assert_eq!(
        count(
            dir.path(),
            &format!("SELECT count(*) FROM session_events_fts WHERE rowid IN (SELECT id FROM session_events WHERE session_id = '{session_id}')"),
        ),
        0
    );
    assert_eq!(
        count(
            dir.path(),
            "SELECT count(*) FROM session_hydration_checkpoints WHERE source = 'claude'"
        ),
        0
    );

    // The feed reports the catalog row moving to shallow, not a tombstone per
    // record: the provider still holds every one of them.
    let changes: Vec<_> = store
        .changes_since(head, ChangeQuery::default())
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        changes
            .iter()
            .all(|change| !matches!(change.op, ChangeOp::Delete)),
        "{changes:?}"
    );
    assert!(changes
        .iter()
        .any(|change| change.kind == ChangeKind::Session && &change.session_id == session_id));

    // A hydration reads it back in full, then settles.
    let again = store.hydrate(&claude, HydrateOptions::default()).unwrap();
    assert_eq!(again.status, HydrateStatus::Hydrated, "{again:?}");
    assert_eq!(state_of(&store, &claude), DiscoveryState::Full);
    let restored = evidence(&store, &claude);
    assert_eq!(restored.messages, before.messages);
    assert_eq!(restored.tool_calls, before.tool_calls);
    assert_eq!(restored.file_edits, before.file_edits);
    assert_eq!(restored.markers, before.markers);
    let settled = store.hydrate(&claude, HydrateOptions::default()).unwrap();
    assert_eq!(settled.status, HydrateStatus::Unchanged);
}

#[test]
fn all_except_keeps_only_the_named_sessions_and_never_user_data() {
    let (dir, store, claude, codex) = hydrated();
    let db = rusqlite::Connection::open(dir.path().join("ai-history.db")).unwrap();
    let SessionRef::Id { session_id, .. } = &claude else {
        unreachable!()
    };
    db.execute_batch(&format!(
        "INSERT INTO tags (name, display_name, created_ms, updated_ms) VALUES ('keep', 'Keep', 1, 1);
         INSERT INTO session_tags (source, session_id, tag_id, created_ms)
           SELECT 'claude', '{session_id}', id, 1 FROM tags WHERE name = 'keep';
         INSERT INTO session_commit_links (source, session_id, repo, commit_sha, match_method,
           confidence, created_at_ms)
           VALUES ('claude', '{session_id}', '/repo', 'abc123', 'manual', 1.0, 1);"
    ))
    .unwrap();
    let kept = evidence(&store, &codex);

    let report = store
        .forget_evidence(
            ForgetScope::AllExcept(vec![codex.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(report.sessions, 1, "{report:?}");
    assert_eq!(state_of(&store, &claude), DiscoveryState::Shallow);
    assert_eq!(evidence(&store, &codex), kept);
    assert_eq!(
        count(
            dir.path(),
            "SELECT count(*) FROM session_events WHERE source = 'claude'"
        ),
        0
    );
    assert_eq!(count(dir.path(), "SELECT count(*) FROM session_tags"), 1);
    assert_eq!(
        count(dir.path(), "SELECT count(*) FROM session_commit_links"),
        1
    );

    // Nothing left to forget is a no-op, not an error.
    let again = store
        .forget_evidence(
            ForgetScope::AllExcept(vec![codex.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(again.sessions, 0);
    assert_eq!(again.events, 0);
}

/// A session a full sweep indexed (no hydration checkpoint, the cursor in
/// the sync walk's own table) hydrates back too.
#[test]
fn a_swept_session_forgets_and_hydrates_back() {
    let dir = tempfile::tempdir().unwrap();
    stage(dir.path());
    let store = open(dir.path(), false);
    store.sync(SyncOptions::default()).unwrap();
    let codex = session_of(&store, Source::Codex);
    let before = evidence(&store, &codex);
    assert!(!before.messages.is_empty());

    store
        .forget_evidence(
            ForgetScope::Sessions(vec![codex.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(state_of(&store, &codex), DiscoveryState::Shallow);
    assert!(evidence(&store, &codex).messages.is_empty());
    assert_eq!(
        count(
            dir.path(),
            "SELECT count(*) FROM transcript_cursors WHERE source = 'codex'"
        ),
        0,
        "a cursor into forgotten evidence would resume past it"
    );

    let again = store.hydrate(&codex, HydrateOptions::default()).unwrap();
    assert_eq!(again.status, HydrateStatus::Hydrated, "{again:?}");
    assert_eq!(evidence(&store, &codex).messages, before.messages);
}

#[test]
fn compact_returns_the_freed_space_and_keeps_what_remains() {
    let (dir, store, claude, codex) = hydrated();
    let kept = evidence(&store, &codex);
    store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    let report = store.compact(CompactOptions::default()).unwrap();
    assert!(
        report.db_bytes_after + report.wal_bytes_after
            <= report.db_bytes_before + report.wal_bytes_before,
        "{report:?}"
    );
    assert_eq!(
        count(dir.path(), "PRAGMA freelist_count"),
        0,
        "the rewrite leaves no free pages"
    );
    assert_eq!(evidence(&store, &codex), kept);
    // The full-text index still covers exactly the rows that remain.
    rusqlite::Connection::open(dir.path().join("ai-history.db"))
        .unwrap()
        .execute_batch(
            "INSERT INTO session_events_fts(session_events_fts) VALUES('integrity-check')",
        )
        .unwrap();
    assert_eq!(
        count(
            dir.path(),
            "SELECT count(*) FROM session_events_fts_docsize"
        ),
        count(dir.path(), "SELECT count(*) FROM session_events"),
    );
}

#[test]
fn forget_and_compact_refuse_what_they_cannot_do() {
    let (dir, store, claude, _) = hydrated();

    let reader = open(dir.path(), true);
    let refused = reader
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap_err();
    assert!(
        matches!(refused, Error::UnsupportedOperation(_)),
        "{refused}"
    );
    let refused = reader.compact(CompactOptions::default()).unwrap_err();
    assert!(
        matches!(refused, Error::UnsupportedOperation(_)),
        "{refused}"
    );

    let by_path = store
        .forget_evidence(
            ForgetScope::Sessions(vec![SessionRef::path(
                Source::Claude,
                dir.path().join("x.jsonl"),
            )]),
            ForgetOptions::default(),
        )
        .unwrap_err();
    assert!(matches!(by_path, Error::InvalidArgument(_)), "{by_path}");

    let stop = StopToken::new();
    stop.stop();
    let mut stopped = ForgetOptions::default();
    stopped.stop = Some(stop);
    let cancelled = store
        .forget_evidence(ForgetScope::Sessions(vec![claude.clone()]), stopped)
        .unwrap_err();
    assert!(matches!(cancelled, Error::Cancelled(_)), "{cancelled}");
    assert_eq!(state_of(&store, &claude), DiscoveryState::Full);
}

#[cfg(unix)]
#[test]
fn a_held_sync_lock_is_waited_for_then_reported() {
    use std::os::unix::io::AsRawFd;

    let (dir, store, claude, _) = hydrated();
    let holder = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.path().join("ai-history.db.sync.lock"))
        .unwrap();
    // SAFETY: `holder` owns the descriptor for the whole test.
    assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX) }, 0);

    let mut patient = ForgetOptions::default();
    patient.lock_timeout_ms = 200;
    let error = store
        .forget_evidence(ForgetScope::Sessions(vec![claude.clone()]), patient)
        .unwrap_err();
    assert!(
        matches!(error, Error::SyncLocked { waited_ms, .. } if waited_ms >= 200),
        "{error}"
    );
    let error = store.compact(CompactOptions::default()).unwrap_err();
    assert_eq!(error.code(), "SYNC_LOCKED");
    assert_eq!(state_of(&store, &claude), DiscoveryState::Full);

    // SAFETY: as above.
    assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_UN) }, 0);
    store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(state_of(&store, &claude), DiscoveryState::Shallow);
}

// ---------------------------------------------------------------------------
// delegated children and unrecoverable sessions
// ---------------------------------------------------------------------------

const CLAUDE_PARENT: &str = "claude-sidecar-parent";
const CLAUDE_CHILD: &str = "plan01";
const CODEX_PARENT: &str = "019da866-c280-7000-8000-0000000000a0";
const CODEX_CHILD: &str = "019da867-37b0-7000-8000-0000000000b0";

/// A Claude session whose subagent transcript sits in its sidecar
/// directory; the subagent has no catalog row of its own.
fn stage_claude_family(home: &Path) {
    let from = fixtures().join("claude/sidecar-subagent/.claude/projects/corpus");
    let project = home.join(".claude/projects/corpus");
    fs::create_dir_all(project.join("claude-sidecar-parent/subagents")).unwrap();
    fs::copy(
        from.join("claude-sidecar-parent.jsonl"),
        project.join("claude-sidecar-parent.jsonl"),
    )
    .unwrap();
    for file in ["agent-plan01.jsonl", "agent-plan01.meta.json"] {
        fs::copy(
            from.join("claude-sidecar-parent/subagents").join(file),
            project.join("claude-sidecar-parent/subagents").join(file),
        )
        .unwrap();
    }
}

/// A Codex root rollout and the child thread it spawned.
fn stage_codex_family(home: &Path) {
    let day = home.join(".codex/sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::copy(
        fixtures().join("codex/fork-subagent/root.jsonl"),
        day.join(format!("rollout-2026-04-20T01-00-00-{CODEX_PARENT}.jsonl")),
    )
    .unwrap();
    fs::copy(
        fixtures().join("codex/fork-subagent/subagent.jsonl"),
        day.join(format!("rollout-2026-04-20T01-00-30-{CODEX_CHILD}.jsonl")),
    )
    .unwrap();
}

type Staging = fn(&Path);

fn family(stage: Staging, parent: SessionRef) -> (tempfile::TempDir, SessionStore, SessionRef) {
    let dir = tempfile::tempdir().unwrap();
    stage(dir.path());
    let store = open(dir.path(), false);
    store.discover(DiscoveryOptions::default()).unwrap();
    let report = store.hydrate(&parent, HydrateOptions::default()).unwrap();
    assert_eq!(report.status, HydrateStatus::Hydrated);
    (dir, store, parent)
}

fn families() -> [(Staging, SessionRef, &'static str); 2] {
    [
        (
            stage_claude_family,
            SessionRef::id(Source::Claude, CLAUDE_PARENT),
            CLAUDE_CHILD,
        ),
        (
            stage_codex_family,
            SessionRef::id(Source::Codex, CODEX_PARENT),
            CODEX_CHILD,
        ),
    ]
}

fn child_events(home: &Path, child: &str) -> i64 {
    count(
        home,
        &format!("SELECT count(*) FROM session_events WHERE session_id = '{child}'"),
    )
}

fn child_has_events(home: &Path, child: &str) -> i64 {
    count(
        home,
        &format!(
            "SELECT coalesce(max(child_has_events), -1) FROM session_relationships \
             WHERE child_session_id = '{child}'"
        ),
    )
}

#[test]
fn keeping_a_session_keeps_its_catalog_less_children() {
    for (stage, parent, child) in families() {
        let (dir, store, parent) = family(stage, parent);
        let before = child_events(dir.path(), child);
        assert!(before > 0, "{child}");
        let report = store
            .forget_evidence(
                ForgetScope::AllExcept(vec![parent.clone()]),
                ForgetOptions::default(),
            )
            .unwrap();
        assert_eq!(report.events, 0, "{child}: {report:?}");
        assert_eq!(child_events(dir.path(), child), before, "{child}");
        let again = store.hydrate(&parent, HydrateOptions::default()).unwrap();
        assert_eq!(again.status, HydrateStatus::Unchanged);
    }
}

#[test]
fn forgetting_a_session_forgets_its_children_and_hydration_restores_them() {
    for (stage, parent, child) in families() {
        let (dir, store, parent) = family(stage, parent);
        let before = child_events(dir.path(), child);
        assert_eq!(child_has_events(dir.path(), child), 1, "{child}");
        let report = store
            .forget_evidence(
                ForgetScope::Sessions(vec![parent.clone()]),
                ForgetOptions::default(),
            )
            .unwrap();
        assert_eq!(report.sessions, 2, "{child}: {report:?}");
        assert_eq!(child_events(dir.path(), child), 0, "{child}");
        assert_eq!(child_has_events(dir.path(), child), 0, "{child}");

        let again = store.hydrate(&parent, HydrateOptions::default()).unwrap();
        assert_eq!(again.status, HydrateStatus::Hydrated, "{child}");
        assert_eq!(child_events(dir.path(), child), before, "{child}");
        assert_eq!(child_has_events(dir.path(), child), 1, "{child}");
    }
}

/// Forgetting only a child marks its kept parent shallow, so an embedder
/// that re-hydrates shallow sessions re-reads the parent's related
/// transcripts instead of getting `Unchanged` over the gap.
#[test]
fn forgetting_a_child_of_a_kept_parent_rehydrates_through_the_parent() {
    for (stage, parent, child) in families() {
        let (dir, store, parent) = family(stage, parent);
        let before = child_events(dir.path(), child);
        let parent_events = evidence(&store, &parent).messages.len();
        store
            .forget_evidence(
                ForgetScope::Sessions(vec![SessionRef::id(parent.source(), child)]),
                ForgetOptions::default(),
            )
            .unwrap();
        assert_eq!(child_events(dir.path(), child), 0, "{child}");
        assert_eq!(state_of(&store, &parent), DiscoveryState::Shallow);
        assert_eq!(evidence(&store, &parent).messages.len(), parent_events);
        let again = store.hydrate(&parent, HydrateOptions::default()).unwrap();
        assert_ne!(again.status, HydrateStatus::Unchanged, "{child}");
        assert_eq!(child_events(dir.path(), child), before, "{child}");
        assert_eq!(state_of(&store, &parent), DiscoveryState::Full);
    }
}

/// A session whose transcript the provider has deleted cannot be hydrated
/// back, so it is skipped unless the caller opts in.
#[test]
fn a_session_whose_transcript_is_gone_is_skipped_unless_opted_in() {
    let (dir, store, claude, codex) = hydrated();
    fs::remove_file(
        dir.path()
            .join(".claude/projects/corpus/files-touched.jsonl"),
    )
    .unwrap();
    let before = evidence(&store, &claude);

    let report = store
        .forget_evidence(
            ForgetScope::AllExcept(vec![codex.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(report.skipped_unrecoverable, 1, "{report:?}");
    assert_eq!(report.sessions, 0);
    assert_eq!(evidence(&store, &claude), before);
    assert_eq!(state_of(&store, &claude), DiscoveryState::Full);

    let anyway = ForgetOptions::default().include_unrecoverable(true);
    let report = store
        .forget_evidence(ForgetScope::Sessions(vec![claude.clone()]), anyway)
        .unwrap();
    assert_eq!((report.sessions, report.skipped_unrecoverable), (1, 0));
    assert!(evidence(&store, &claude).messages.is_empty());
}

/// Evidence a remote connector supplied is not re-read by a local
/// hydration, so its session is unrecoverable.
#[test]
fn a_session_with_remote_evidence_is_skipped() {
    let (dir, store, claude, _) = hydrated();
    let SessionRef::Id { session_id, .. } = &claude else {
        unreachable!()
    };
    rusqlite::Connection::open(dir.path().join("ai-history.db"))
        .unwrap()
        .execute(
            "INSERT INTO session_presences (source, session_id, location, discovery_state) \
             VALUES ('claude', ?, 'remote', 'full')",
            [session_id],
        )
        .unwrap();
    let report = store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!((report.sessions, report.skipped_unrecoverable), (0, 1));
}

/// A parent hydration can no longer read is skipped with its catalog-less
/// children, whose evidence only that parent could bring back; nothing of
/// the family changes.
#[test]
fn a_parent_whose_transcript_is_gone_is_skipped_with_its_children() {
    for (stage, parent, child) in families() {
        let (dir, store, parent) = family(stage, parent);
        let SessionRef::Id { session_id, .. } = &parent else {
            unreachable!()
        };
        let raw: String = rusqlite::Connection::open(dir.path().join("ai-history.db"))
            .unwrap()
            .query_row(
                "SELECT raw_path FROM sessions WHERE session_id = ?",
                [session_id],
                |row| row.get(0),
            )
            .unwrap();
        fs::remove_file(&raw).unwrap();
        let before = child_events(dir.path(), child);
        let checkpoints = "SELECT count(*) FROM session_hydration_checkpoints";
        let held = count(dir.path(), checkpoints);

        let report = store
            .forget_evidence(
                ForgetScope::Sessions(vec![parent.clone()]),
                ForgetOptions::default(),
            )
            .unwrap();
        assert_eq!(
            (report.sessions, report.skipped_unrecoverable),
            (0, 2),
            "{child}"
        );
        assert_eq!(child_events(dir.path(), child), before, "{child}");
        assert_eq!(count(dir.path(), checkpoints), held, "{child}");
        assert_eq!(state_of(&store, &parent), DiscoveryState::Full);
    }
}

/// An archived Codex parent sits in the flat `archived_sessions/` while its
/// child stays under its dated directory. Hydrating the parent reads the
/// child through the recorded delegation, so forgetting the pair and
/// hydrating the parent restores both.
#[test]
fn an_archived_codex_parent_restores_its_dated_child() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let archived = home.join(".codex/archived_sessions");
    fs::create_dir_all(&archived).unwrap();
    fs::copy(
        fixtures().join("codex/fork-subagent/root.jsonl"),
        archived.join(format!("rollout-2026-04-20T01-00-00-{CODEX_PARENT}.jsonl")),
    )
    .unwrap();
    let day = home.join(".codex/sessions/2026/04/20");
    fs::create_dir_all(&day).unwrap();
    fs::copy(
        fixtures().join("codex/fork-subagent/subagent.jsonl"),
        day.join(format!("rollout-2026-04-20T01-00-30-{CODEX_CHILD}.jsonl")),
    )
    .unwrap();
    let store = open(home, false);
    store.sync(SyncOptions::default()).unwrap();
    let parent = SessionRef::id(Source::Codex, CODEX_PARENT);
    let hydrated = store.hydrate(&parent, HydrateOptions::default()).unwrap();
    assert_eq!(
        hydrated.related,
        vec![SessionRef::id(Source::Codex, CODEX_CHILD)]
    );
    let before = child_events(home, CODEX_CHILD);
    assert!(before > 0);

    let report = store
        .forget_evidence(
            ForgetScope::Sessions(vec![parent.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!((report.sessions, report.skipped_unrecoverable), (2, 0));
    assert_eq!(child_events(home, CODEX_CHILD), 0);
    let again = store.hydrate(&parent, HydrateOptions::default()).unwrap();
    assert_eq!(again.status, HydrateStatus::Hydrated);
    assert_eq!(child_events(home, CODEX_CHILD), before);
    assert_eq!(child_has_events(home, CODEX_CHILD), 1);
}

/// A Claude conversation forked into branch files keeps its id in each. A
/// sweep indexes every branch under it; hydration reads only the catalogued
/// file, so a swept fork is unrecoverable, while a hydrated one round-trips.
#[test]
fn a_claude_session_spread_over_several_transcripts() {
    for swept in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join(".claude/projects/corpus");
        fs::create_dir_all(&project).unwrap();
        for file in [
            "original-session.jsonl",
            "fork-branch-a.jsonl",
            "fork-branch-b.jsonl",
        ] {
            fs::copy(fixtures().join("claude").join(file), project.join(file)).unwrap();
        }
        let store = open(dir.path(), false);
        let fork = SessionRef::id(Source::Claude, "00000000-0000-0000-0000-000000000fff");
        if swept {
            store.sync(SyncOptions::default()).unwrap();
        } else {
            store.discover(DiscoveryOptions::default()).unwrap();
            store.hydrate(&fork, HydrateOptions::default()).unwrap();
        }
        let before = evidence(&store, &fork).messages;
        let report = store
            .forget_evidence(
                ForgetScope::Sessions(vec![fork.clone()]),
                ForgetOptions::default(),
            )
            .unwrap();
        if swept {
            assert_eq!((report.sessions, report.skipped_unrecoverable), (0, 1));
        } else {
            assert_eq!((report.sessions, report.skipped_unrecoverable), (1, 0));
            store.hydrate(&fork, HydrateOptions::default()).unwrap();
        }
        assert_eq!(evidence(&store, &fork).messages, before, "swept={swept}");
    }
}

const DEVIN_SCHEMA: &str = "
CREATE TABLE sessions (
  id TEXT PRIMARY KEY, working_directory TEXT NOT NULL, backend_type TEXT NOT NULL,
  model TEXT NOT NULL, agent_mode TEXT NOT NULL, created_at INTEGER NOT NULL,
  last_activity_at INTEGER NOT NULL, title TEXT, main_chain_id INTEGER,
  shell_last_seen_index INTEGER DEFAULT 0, cogs_json TEXT, workspace_dirs TEXT,
  hidden INTEGER NOT NULL DEFAULT 0, metadata TEXT);
CREATE TABLE message_nodes (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, node_id INTEGER NOT NULL,
  parent_node_id INTEGER, chat_message TEXT NOT NULL, created_at INTEGER NOT NULL,
  metadata TEXT, UNIQUE(session_id, node_id));
CREATE TABLE tool_call_state (
  session_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, tool_call_json TEXT,
  tool_call_update_json TEXT, PRIMARY KEY (session_id, tool_call_id));
INSERT INTO sessions (id, working_directory, backend_type, model, agent_mode, created_at,
  last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES ('devin-test', '/work/repo', 'devin', 'test-model', 'normal', 1776643200, 1776643202,
  'Test session', '[\"/work/repo\"]', 0, NULL);
INSERT INTO message_nodes (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('devin-test', 0, NULL, '{\"message_id\":\"u0\",\"role\":\"user\",\"content\":\"first prompt\",\"metadata\":{\"is_user_input\":true},\"tool_calls\":null,\"thinking\":null,\"tool_call_id\":null,\"phase\":null}', 1776643201, NULL),
  ('devin-test', 1, 0, '{\"message_id\":\"a0\",\"role\":\"assistant\",\"content\":\"first answer\",\"thinking\":null,\"metadata\":{\"num_tokens\":8,\"generation_model\":\"test-model\"},\"tool_calls\":null,\"tool_call_id\":null,\"phase\":null}', 1776643202, NULL);
";

/// The provider store still exists, but no longer holds the session (or
/// hides it): hydration refuses it, so forgetting skips it.
#[test]
fn a_devin_session_gone_from_its_store_is_skipped() {
    for gone in [
        "DELETE FROM message_nodes; DELETE FROM sessions;",
        "UPDATE sessions SET hidden = 1;",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let cli = home.join(".local/share/devin/cli");
        fs::create_dir_all(&cli).unwrap();
        rusqlite::Connection::open(cli.join("sessions.db"))
            .unwrap()
            .execute_batch(DEVIN_SCHEMA)
            .unwrap();
        let store = open(home, false);
        store.discover(DiscoveryOptions::default()).unwrap();
        let session = SessionRef::id(Source::Devin, "devin-test");
        store.hydrate(&session, HydrateOptions::default()).unwrap();
        let events = "SELECT count(*) FROM session_events WHERE source = 'devin'";
        let before = count(home, events);
        assert!(before > 0);
        rusqlite::Connection::open(cli.join("sessions.db"))
            .unwrap()
            .execute_batch(gone)
            .unwrap();
        let report = store
            .forget_evidence(
                ForgetScope::Sessions(vec![session.clone()]),
                ForgetOptions::default(),
            )
            .unwrap();
        assert_eq!(
            (report.sessions, report.skipped_unrecoverable),
            (0, 1),
            "{gone}"
        );
        assert_eq!(count(home, events), before, "{gone}");
    }
}

fn copy_tree(from: &Path, to: &Path) {
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir_all(&target).unwrap();
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

type Census = std::collections::BTreeMap<(String, String), [i64; 5]>;

/// Evidence rows per session, per table.
fn census(home: &Path) -> Census {
    let db = rusqlite::Connection::open(home.join("ai-history.db")).unwrap();
    let mut out = Census::new();
    for (column, table) in [
        "session_events",
        "tool_calls",
        "file_edits",
        "session_markers",
        "observation_evidence",
    ]
    .into_iter()
    .enumerate()
    {
        let mut statement = db
            .prepare(&format!(
                "SELECT source, session_id, count(*) FROM {table} GROUP BY 1, 2"
            ))
            .unwrap();
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap();
        for row in rows {
            let (source, session_id, rows): (String, String, i64) = row.unwrap();
            out.entry((source, session_id)).or_default()[column] = rows;
        }
    }
    out
}

/// Every single-file fixture of every file-backed provider in one home.
fn stage_every_fixture(home: &Path) {
    let project = home.join(".claude/projects/corpus");
    fs::create_dir_all(&project).unwrap();
    for entry in fs::read_dir(fixtures().join("claude")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "jsonl") {
            fs::copy(&path, project.join(path.file_name().unwrap())).unwrap();
        }
    }
    stage_claude_family(home);
    stage_codex_family(home);
    for tree in [
        "grok/events-session",
        "grok/full-session",
        "grok/unified-usage",
        "muse/tools-session",
        "muse/cli-capture",
        "cursor/prompt-transcript",
    ] {
        copy_tree(&fixtures().join(tree), home);
    }
    let day = home.join(".codex/sessions/2026/04/20");
    for entry in fs::read_dir(fixtures().join("codex")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "jsonl") {
            let name = path.file_name().unwrap().to_string_lossy();
            fs::copy(
                &path,
                day.join(format!("rollout-2026-04-20T02-00-00-{name}")),
            )
            .unwrap();
        }
    }
}

/// Across every fixture, captured by a sweep or by hydration: forgetting
/// everything and hydrating every catalogued session brings back exactly the
/// evidence there was. What cannot come back is skipped, never lost.
#[test]
fn every_fixture_comes_back_as_it_was() {
    for swept in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        stage_every_fixture(home);
        let store = open(home, false);
        if swept {
            store.sync(SyncOptions::default()).unwrap();
        } else {
            store.discover(DiscoveryOptions::default()).unwrap();
        }
        let sessions: Vec<SessionRef> = store
            .sessions(CatalogQuery::default())
            .map(|row| row.unwrap().session_ref())
            .collect();
        if !swept {
            for session in &sessions {
                let _ = store.hydrate(session, HydrateOptions::default());
            }
        }
        let before = census(home);
        let report = store
            .forget_evidence(ForgetScope::AllExcept(vec![]), ForgetOptions::default())
            .unwrap();
        assert!(report.sessions > 0, "swept={swept}");
        for session in &sessions {
            store.hydrate(session, HydrateOptions::default()).unwrap();
        }
        assert_eq!(census(home), before, "swept={swept}: {report:?}");
    }
}

/// OpenCode in both layouts: a session the store still holds round-trips;
/// one gone from it -- a row deleted from `opencode.db`, a session file
/// removed from the legacy tree -- is skipped.
#[test]
fn opencode_sessions_round_trip_and_gone_ones_are_skipped() {
    for sqlite in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let db = home.join(".local/share/opencode/opencode.db");
        let (session, gone) = if sqlite {
            fs::create_dir_all(db.parent().unwrap()).unwrap();
            rusqlite::Connection::open(&db)
                .unwrap()
                .execute_batch(
                    &fs::read_to_string(fixtures().join("opencode/sqlite-store.sql")).unwrap(),
                )
                .unwrap();
            (
                "ses_sqlite_root",
                "DELETE FROM part; DELETE FROM message; DELETE FROM session;",
            )
        } else {
            copy_tree(
                &fixtures().join("opencode/legacy-json-simple"),
                &home.join(".local/share/opencode"),
            );
            ("ses_simple", "")
        };
        let store = open(home, false);
        store.discover(DiscoveryOptions::default()).unwrap();
        let session = SessionRef::id(Source::OpenCode, session);
        store.hydrate(&session, HydrateOptions::default()).unwrap();
        let before = census(home);
        assert!(!before.is_empty(), "sqlite={sqlite}");

        let report = store
            .forget_evidence(ForgetScope::AllExcept(vec![]), ForgetOptions::default())
            .unwrap();
        assert_eq!(
            report.skipped_unrecoverable, 0,
            "sqlite={sqlite}: {report:?}"
        );
        store.hydrate(&session, HydrateOptions::default()).unwrap();
        assert_eq!(census(home), before, "sqlite={sqlite}");

        if sqlite {
            rusqlite::Connection::open(&db)
                .unwrap()
                .execute_batch(gone)
                .unwrap();
        } else {
            fs::remove_file(
                home.join(".local/share/opencode/storage/session/global/ses_simple.json"),
            )
            .unwrap();
        }
        let report = store
            .forget_evidence(ForgetScope::AllExcept(vec![]), ForgetOptions::default())
            .unwrap();
        assert_eq!(report.sessions, 0, "sqlite={sqlite}: {report:?}");
        assert!(
            report.skipped_unrecoverable >= 1,
            "sqlite={sqlite}: {report:?}"
        );
        assert_eq!(census(home), before, "sqlite={sqlite}");
    }
}

/// Claude writes `file-history-snapshot` records with no `sessionId`, often
/// ahead of the first record that names one. A sweep keeps them as markers
/// of the file's session; a hydration reading the transcript from zero keeps
/// them too, so they survive forgetting.
#[test]
fn sessionless_records_ahead_of_the_session_id_come_back() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let project = home.join(".claude/projects/corpus");
    fs::create_dir_all(&project).unwrap();
    let snapshot = "{\"type\":\"file-history-snapshot\",\"messageId\":\"snap-1\",\
                    \"snapshot\":{\"messageId\":\"snap-1\",\"trackedFileBackups\":{},\
                    \"timestamp\":\"2026-04-20T00:00:00.000Z\"},\"isSnapshotUpdate\":false}\n";
    let body = fs::read_to_string(fixtures().join("claude/files-touched.jsonl")).unwrap();
    fs::write(
        project.join("files-touched.jsonl"),
        format!("{snapshot}{body}"),
    )
    .unwrap();
    let store = open(home, false);
    store.sync(SyncOptions::default()).unwrap();
    let claude = session_of(&store, Source::Claude);
    let before = census(home);
    assert!(
        count(
            home,
            "SELECT count(*) FROM session_markers WHERE subkind = 'file-history-snapshot'"
        ) > 0
    );
    store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    store.hydrate(&claude, HydrateOptions::default()).unwrap();
    assert_eq!(census(home), before);
}

/// The records a hydration holds until the session id appears are the
/// reader's own: a line that is not UTF-8 and a record over the reader's
/// ceiling ahead of the session id are skipped exactly as a sweep skips
/// them, and the sessionless marker after them is still kept.
#[test]
fn undecodable_and_oversized_records_ahead_of_the_session_id() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let project = home.join(".claude/projects/corpus");
    fs::create_dir_all(&project).unwrap();
    let mut transcript: Vec<u8> =
        b"{\"type\":\"summary\",\"summary\":\"\xff\xfe broken\"}\n".to_vec();
    transcript.extend_from_slice(b"{\"type\":\"file-history-snapshot\",\"pad\":\"");
    transcript.extend(std::iter::repeat_n(b'x', 17 * 1024 * 1024));
    transcript.extend_from_slice(b"\"}\n");
    transcript.extend_from_slice(
        b"{\"type\":\"file-history-snapshot\",\"messageId\":\"snap-2\",\"snapshot\":{\"messageId\":\"snap-2\",\"trackedFileBackups\":{},\"timestamp\":\"2026-04-20T00:00:00.000Z\"},\"isSnapshotUpdate\":false}\n",
    );
    transcript.extend(fs::read(fixtures().join("claude/files-touched.jsonl")).unwrap());
    fs::write(project.join("files-touched.jsonl"), transcript).unwrap();
    let store = open(home, false);
    store.sync(SyncOptions::default()).unwrap();
    let claude = session_of(&store, Source::Claude);
    let before = census(home);
    let snapshots = "SELECT count(*) FROM session_markers WHERE subkind = 'file-history-snapshot'";
    assert_eq!(count(home, snapshots), 1);
    store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    store.hydrate(&claude, HydrateOptions::default()).unwrap();
    assert_eq!(census(home), before);
    assert_eq!(count(home, snapshots), 1);
}

const CODEX_GRANDCHILD: &str = "019da868-37b0-7000-8000-0000000000c0";

/// Root → child → grandchild, each a Codex thread the one above spawned;
/// neither descendant has a catalog row.
fn stage_codex_three_generations(home: &Path) {
    stage_codex_family(home);
    let child = fs::read_to_string(fixtures().join("codex/fork-subagent/subagent.jsonl")).unwrap();
    let mut lines = child.lines();
    let meta = lines
        .next()
        .unwrap()
        .replace(CODEX_CHILD, CODEX_GRANDCHILD)
        .replace(CODEX_PARENT, CODEX_CHILD)
        .replace("\"depth\":1", "\"depth\":2");
    let rest: Vec<String> = lines
        .map(|line| {
            line.replace("0000000000a1", "0000000000c1")
                .replace("0000000000b1", "0000000000c2")
                .replace("review", "re-review")
        })
        .collect();
    fs::write(
        home.join(format!(
            ".codex/sessions/2026/04/20/rollout-2026-04-20T01-00-50-{CODEX_GRANDCHILD}.jsonl"
        )),
        format!("{meta}\n{}\n", rest.join("\n")),
    )
    .unwrap();
}

fn grandchild_rollout(home: &Path) -> std::path::PathBuf {
    home.join(format!(
        ".codex/sessions/2026/04/20/rollout-2026-04-20T01-00-50-{CODEX_GRANDCHILD}.jsonl"
    ))
}

/// The grandchild is judged by its own transcript, not its parent's: with
/// its rollout gone it is skipped even though the child it hangs off comes
/// back, and with the rollout present the whole line round-trips.
#[test]
fn a_nested_codex_descendant_is_judged_by_its_own_transcript() {
    for rollout_gone in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        stage_codex_three_generations(home);
        let store = open(home, false);
        store.sync(SyncOptions::default()).unwrap();
        let root = SessionRef::id(Source::Codex, CODEX_PARENT);
        store.hydrate(&root, HydrateOptions::default()).unwrap();
        let before = census(home);
        let grandchild = child_events(home, CODEX_GRANDCHILD);
        assert!(grandchild > 0, "the grandchild has evidence of its own");
        if rollout_gone {
            fs::remove_file(grandchild_rollout(home)).unwrap();
        }

        let report = store
            .forget_evidence(
                ForgetScope::Sessions(vec![root.clone()]),
                ForgetOptions::default(),
            )
            .unwrap();
        if rollout_gone {
            assert_eq!(
                (report.sessions, report.skipped_unrecoverable),
                (2, 1),
                "{report:?}"
            );
            assert_eq!(child_events(home, CODEX_GRANDCHILD), grandchild);
        } else {
            assert_eq!(
                (report.sessions, report.skipped_unrecoverable),
                (3, 0),
                "{report:?}"
            );
            store.hydrate(&root, HydrateOptions::default()).unwrap();
            assert_eq!(census(home), before);
        }
    }
}

/// Forgetting only the grandchild invalidates the catalogued root above the
/// catalog-less child it hangs off: the root's hydration is what re-reads
/// it.
#[test]
fn forgetting_a_nested_descendant_reopens_its_catalogued_ancestor() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_codex_three_generations(home);
    let store = open(home, false);
    store.sync(SyncOptions::default()).unwrap();
    let root = SessionRef::id(Source::Codex, CODEX_PARENT);
    store.hydrate(&root, HydrateOptions::default()).unwrap();
    let before = census(home);

    let report = store
        .forget_evidence(
            ForgetScope::Sessions(vec![SessionRef::id(Source::Codex, CODEX_GRANDCHILD)]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(report.sessions, 1, "{report:?}");
    assert_eq!(child_events(home, CODEX_GRANDCHILD), 0);
    assert_eq!(state_of(&store, &root), DiscoveryState::Shallow);
    let again = store.hydrate(&root, HydrateOptions::default()).unwrap();
    assert_ne!(again.status, HydrateStatus::Unchanged);
    assert_eq!(census(home), before);
}

/// A child whose evidence came from two transcripts comes back only when a
/// hydration reads both: one transcript gone makes it unrecoverable.
#[test]
fn a_child_linked_from_several_transcripts_needs_all_of_them() {
    let (dir, store, parent) = family(
        stage_claude_family,
        SessionRef::id(Source::Claude, CLAUDE_PARENT),
    );
    let home = dir.path();
    let db = rusqlite::Connection::open(home.join("ai-history.db")).unwrap();
    let elsewhere = home.join(".claude/projects/corpus/elsewhere/agent-plan01.jsonl");
    db.execute(
        "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
           child_session_id, relationship, identity_status, evidence_kind, evidence_locator, \
           child_has_events, created_ms, updated_ms) \
         SELECT source, parent_session_id, relationship_uid || '-2', child_session_id, \
           relationship, identity_status, evidence_kind, ?, child_has_events, created_ms, updated_ms \
         FROM session_relationships WHERE child_session_id = ?",
        rusqlite::params![elsewhere.to_string_lossy(), CLAUDE_CHILD],
    )
    .unwrap();
    let before = child_events(home, CLAUDE_CHILD);
    let report = store
        .forget_evidence(
            ForgetScope::Sessions(vec![SessionRef::id(Source::Claude, CLAUDE_CHILD)]),
            ForgetOptions::default(),
        )
        .unwrap();
    assert_eq!(
        (report.sessions, report.skipped_unrecoverable),
        (0, 1),
        "{report:?}"
    );
    assert_eq!(child_events(home, CLAUDE_CHILD), before);
    assert_eq!(state_of(&store, &parent), DiscoveryState::Full);
}

/// A long sessionless prefix is not held while the reader looks for the
/// session id: only where it begins is remembered, and the span is read again
/// a record at a time. Every one of its records still comes back.
#[test]
fn a_long_sessionless_prefix_comes_back_record_for_record() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let project = home.join(".claude/projects/corpus");
    fs::create_dir_all(&project).unwrap();
    let mut transcript = String::new();
    for index in 0..5_000 {
        transcript.push_str(&format!(
            "{{\"type\":\"file-history-snapshot\",\"messageId\":\"snap-{index}\",\
             \"snapshot\":{{\"messageId\":\"snap-{index}\",\"trackedFileBackups\":{{}},\
             \"timestamp\":\"2026-04-20T00:00:00.000Z\"}},\"isSnapshotUpdate\":false}}\n"
        ));
    }
    transcript
        .push_str(&fs::read_to_string(fixtures().join("claude/files-touched.jsonl")).unwrap());
    fs::write(project.join("files-touched.jsonl"), transcript).unwrap();
    let store = open(home, false);
    store.sync(SyncOptions::default()).unwrap();
    let claude = session_of(&store, Source::Claude);
    let snapshots = "SELECT count(*) FROM session_markers WHERE subkind = 'file-history-snapshot'";
    assert_eq!(count(home, snapshots), 5_000);
    let before = census(home);
    store
        .forget_evidence(
            ForgetScope::Sessions(vec![claude.clone()]),
            ForgetOptions::default(),
        )
        .unwrap();
    let hydrated = store.hydrate(&claude, HydrateOptions::default()).unwrap();
    assert_eq!(census(home), before);
    let size = fs::metadata(project.join("files-touched.jsonl"))
        .unwrap()
        .len() as i64;
    assert!(
        hydrated.bytes_read >= size,
        "the re-read prefix is counted: {} < {size}",
        hydrated.bytes_read
    );
}
