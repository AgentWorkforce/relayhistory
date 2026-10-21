//! Junie CLI 3110.7 native writer capture and post-restart official reader.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions,
};
use provider_support::{assert_history_conformance, HistoryCase};
use std::path::{Path, PathBuf};

const SID: &str = "session-261021-120000-demo";

fn stage(home: &Path) -> PathBuf {
    fn copy(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/junie/native");
    copy(&fixture, home);
    home.join(".junie/sessions").join(SID).join("events.jsonl")
}

fn open(home: &Path) -> SessionStore {
    open_with_db(home, home.join("junie.db"))
}

fn open_with_db(home: &Path, db: PathBuf) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(db)
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}

fn evidence(store: &SessionStore) -> ai_hist::SessionEvidence {
    store
        .session(&SessionRef::id(Source::Junie, SID), SessionQuery::default())
        .unwrap()
        .unwrap()
}

fn project_facts(db: &Path) -> (Option<String>, Option<String>, Option<String>) {
    let conn = rusqlite::Connection::open(db).unwrap();
    let cwd = conn
        .query_row(
            "SELECT cwd FROM sessions WHERE source='junie' AND session_id=?1",
            [SID],
            |row| row.get(0),
        )
        .unwrap();
    let history_project = conn
        .query_row(
            "SELECT project FROM history WHERE source='junie' AND session_id=?1 LIMIT 1",
            [SID],
            |row| row.get(0),
        )
        .unwrap();
    let event_project = conn.query_row("SELECT project FROM session_events WHERE source='junie' AND session_id=?1 AND role='user' LIMIT 1", [SID], |row| row.get(0)).unwrap();
    (cwd, history_project, event_project)
}

fn project_keys(db: &Path) -> (Option<String>, Option<String>) {
    let conn = rusqlite::Connection::open(db).unwrap();
    let catalog = conn
        .query_row(
            "SELECT project_key FROM sessions WHERE source='junie' AND session_id=?1",
            [SID],
            |row| row.get(0),
        )
        .unwrap();
    let event = conn
        .query_row(
            "SELECT project_key FROM session_events WHERE source='junie' \
             AND session_id=?1 AND role='user' LIMIT 1",
            [SID],
            |row| row.get(0),
        )
        .unwrap();
    (catalog, event)
}

#[test]
fn junie_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Junie,
            session_id: SID,
            messages: &[
                (Role::User, "Hello from synthetic Junie"),
                (Role::Assistant, "The synthetic file is blue."),
                (Role::User, "Continue after restart"),
                (Role::Assistant, "The child confirmed blue."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "step-tool-1");
    assert_eq!(evidence.tool_results.len(), 1);
    assert!(evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("blue")
            && b.tool_use_id.as_deref() == Some("step-tool-1")));
    assert!(evidence
        .requests
        .iter()
        .all(|request| request.usage.is_none()));
    assert_eq!(evidence.markers.len(), 8);
}

#[test]
fn append_rewrite_partial_tail_and_index_metadata_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store).messages;
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{original}{{\"kind\":")).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).messages, before);
    std::fs::write(
        &path,
        original.replace("The child confirmed blue.", "A rewritten native result."),
    )
    .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(evidence(&store)
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("A rewritten native result.")));
    let index = home.path().join(".junie/sessions/index.jsonl");
    let text = std::fs::read_to_string(&index)
        .unwrap()
        .replace("Synthetic Junie capture", "Renamed native title");
    std::fs::write(index, text).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let row = store.sessions(query).next().unwrap().unwrap();
    assert_eq!(row.title.as_deref(), Some("Renamed native title"));
}

#[test]
fn invalid_native_row_preserves_prior_evidence() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store).messages;
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        original.replace("\"timestampMs\":", "\"invalidTimestamp\":"),
    )
    .unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert_eq!(evidence(&store).messages, before);
}

#[test]
fn terminal_native_tool_status_without_output_keeps_observed_success() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let rows = std::fs::read_to_string(&path)
        .unwrap()
        .replace(",\"output\":\"blue\"", "");
    std::fs::write(&path, rows).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let result = evidence(&store);
    assert_eq!(result.tool_calls[0].is_error, Some(false));
    assert!(result.tool_results.is_empty());
}

#[test]
fn terminal_tool_error_rewrite_to_unknown_matches_fresh_capture() {
    for (status, output) in [(Some("IN_PROGRESS"), Some("blue")), (None, None)] {
        let rewritten = tempfile::tempdir().unwrap();
        let path = stage(rewritten.path());
        let original = std::fs::read_to_string(&path).unwrap();
        let failed = original.replace("\"status\":\"COMPLETED\"", "\"status\":\"FAILED\"");
        std::fs::write(&path, failed).unwrap();
        let store = open(rewritten.path());
        let reference = SessionRef::id(Source::Junie, SID);
        store
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap();
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        assert_eq!(evidence(&store).tool_calls[0].is_error, Some(true));

        let mut lines = original.lines().map(str::to_owned).collect::<Vec<_>>();
        let mut row: serde_json::Value = serde_json::from_str(&lines[3]).unwrap();
        let agent = row["event"]["agentEvent"].as_object_mut().unwrap();
        match status {
            Some(value) => agent.insert("status".into(), value.into()),
            None => agent.remove("status"),
        };
        match output {
            Some(value) => agent.insert("output".into(), value.into()),
            None => agent.remove("output"),
        };
        lines[3] = row.to_string();
        let target = format!("{}\n", lines.join("\n"));
        std::fs::write(&path, &target).unwrap();
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        let after = evidence(&store);

        let fresh = tempfile::tempdir().unwrap();
        let fresh_path = stage(fresh.path());
        std::fs::write(fresh_path, target).unwrap();
        let fresh_store = open(fresh.path());
        fresh_store
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap();
        fresh_store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        let expected = evidence(&fresh_store);
        assert_eq!(after.tool_calls[0].is_error, None);
        assert_eq!(after.tool_calls, expected.tool_calls);
        assert_eq!(after.tool_results, expected.tool_results);
    }
}

#[cfg(feature = "unstable-internal")]
#[test]
fn bounded_discovery_refreshes_index_activity_and_reads_shared_index_once() {
    let home = tempfile::tempdir().unwrap();
    let first_path = stage(home.path());
    let conn = ai_hist::open_db(&home.path().join("discovery.db")).unwrap();
    let env = ai_hist::DiscoveryEnv::with_roots(
        &conn,
        home.path().to_owned(),
        home.path().join("missing-opencode.db"),
    );
    let options =
        ai_hist::DiscoverOptions::new(ai_hist::SessionScope::Local, vec!["junie".into()], None);
    ai_hist::discover_sessions_with_env(&env, &options, |_| {}).unwrap();

    let journal = std::fs::read_to_string(&first_path).unwrap();
    let padded = format!(
        "{journal}{}",
        "{\"kind\":\"FutureEvent\",\"timestampMs\":1792609205000}\n".repeat(1400)
    );
    assert!(padded.len() > 64 * 1024);
    std::fs::write(&first_path, &padded).unwrap();
    let second_id = "session-261021-120001-demo";
    let second_path = first_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(second_id)
        .join("events.jsonl");
    std::fs::create_dir_all(second_path.parent().unwrap()).unwrap();
    std::fs::write(&second_path, &padded).unwrap();
    let index_path = home.path().join(".junie/sessions/index.jsonl");
    let mut first_index: serde_json::Value =
        serde_json::from_str(std::fs::read_to_string(&index_path).unwrap().trim()).unwrap();
    first_index["updatedAt"] = 1792609300000_i64.into();
    first_index["taskName"] = "Recently active Junie session".into();
    let mut second_index = first_index.clone();
    second_index["sessionId"] = second_id.into();
    std::fs::write(&index_path, format!("{first_index}\n{second_index}\n")).unwrap();

    let env = ai_hist::DiscoveryEnv::with_roots(
        &conn,
        home.path().to_owned(),
        home.path().join("missing-opencode.db"),
    );
    let mut rows = Vec::new();
    let report =
        ai_hist::discover_sessions_with_env(&env, &options, |row| rows.push(row.clone())).unwrap();
    assert!(report.diagnostics.is_empty(), "{:?}", report.diagnostics);
    assert_eq!(rows.len(), 2);
    let first = rows.iter().find(|row| row.session_id == SID).unwrap();
    assert_eq!(first.last_activity_ms, Some(1792609300000));
    assert_eq!(
        first.title.as_deref(),
        Some("Recently active Junie session")
    );
    assert_eq!(
        first.first_prompt.as_deref(),
        Some("Hello from synthetic Junie")
    );
    assert_eq!(
        report.counters.files_opened, 3,
        "one index and two journals"
    );
    assert_eq!(
        report.counters.bytes_read,
        std::fs::metadata(&index_path).unwrap().len() + 2 * (64 * 1024 + 1)
    );
}

#[test]
fn oversized_opening_prompt_remains_selectable_until_full_hydration() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let original = std::fs::read_to_string(&path).unwrap();
    let mut lines = original.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut opening: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    let prompt = "P".repeat(70_000);
    opening["prompt"] = prompt.clone().into();
    lines[0] = opening.to_string();
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap()
            .discovered,
        1
    );
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let catalog = store.sessions(query).next().unwrap().unwrap();
    assert_eq!(catalog.session_id, SID);
    assert_eq!(catalog.title.as_deref(), Some("Synthetic Junie capture"));
    assert_eq!(
        catalog.first_prompt, None,
        "the bounded read saw no full row"
    );
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(evidence(&store)
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .any(|block| block.text.as_deref() == Some(prompt.as_str())));

    let updated = "Q".repeat(70_000);
    opening["prompt"] = updated.clone().into();
    lines[0] = opening.to_string();
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    assert_eq!(
        store.sessions(query).next().unwrap().unwrap().first_prompt,
        Some("P".repeat(4096)),
        "bounded discovery keeps the prior preview until full hydration"
    );
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    assert_eq!(
        store.sessions(query).next().unwrap().unwrap().first_prompt,
        Some("Q".repeat(4096)),
        "selected hydration replaces the cached preview with native text"
    );
}

#[test]
fn optional_index_partial_tail_keeps_native_journal_readable_and_oversize_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let index = home.path().join(".junie/sessions/index.jsonl");
    let original = std::fs::read_to_string(&index).unwrap();
    std::fs::write(&index, format!("{original}{{\"sessionId\":")).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
    std::fs::write(&index, vec![b' '; 8 * 1024 * 1024 + 1]).unwrap();
    let discovery = store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    assert!(
        discovery.diagnostics.is_empty(),
        "{:?}",
        discovery.diagnostics
    );
    let report = store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.code == "JUNIE_INDEX_TOO_LARGE"));
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    assert_eq!(
        store
            .sessions(query)
            .next()
            .unwrap()
            .unwrap()
            .title
            .as_deref(),
        Some("Synthetic Junie capture")
    );
}

#[test]
fn malformed_optional_index_keeps_verified_journal_and_prior_title() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let index = home.path().join(".junie/sessions/index.jsonl");
    std::fs::write(index, b"{bad complete row}\n").unwrap();
    let discovered = store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    assert!(
        discovered.diagnostics.is_empty(),
        "{:?}",
        discovered.diagnostics
    );
    let report = store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.code == "JUNIE_INVALID_INDEX"));
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    assert_eq!(
        store
            .sessions(query)
            .next()
            .unwrap()
            .unwrap()
            .title
            .as_deref(),
        Some("Synthetic Junie capture")
    );
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
}

#[test]
fn missing_optional_index_preserves_verified_local_project_facts() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Junie, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let db = home.path().join("junie.db");
    let before = project_facts(&db);
    assert_eq!(
        before,
        (
            Some("/synthetic/project".into()),
            Some("/synthetic/project".into()),
            Some("/synthetic/project".into())
        )
    );
    std::fs::remove_file(home.path().join(".junie/sessions/index.jsonl")).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(project_facts(&db), before);
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
}

#[test]
fn relocated_journal_without_index_does_not_inherit_old_project() {
    let old_home = tempfile::tempdir().unwrap();
    let new_home = tempfile::tempdir().unwrap();
    let catalog = tempfile::tempdir().unwrap();
    let db = catalog.path().join("junie.db");
    let old_path = stage(old_home.path());
    let reference = SessionRef::id(Source::Junie, SID);
    let old = open_with_db(old_home.path(), db.clone());
    old.discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    old.hydrate(&reference, HydrateOptions::default()).unwrap();
    assert_eq!(project_facts(&db).1.as_deref(), Some("/synthetic/project"));
    assert!(project_keys(&db).0.is_some());
    assert!(project_keys(&db).1.is_some());
    drop(old);

    let new_path = stage(new_home.path());
    std::fs::remove_file(new_home.path().join(".junie/sessions/index.jsonl")).unwrap();
    std::fs::remove_file(old_path).unwrap();
    let moved = open_with_db(new_home.path(), db.clone());
    // Before rediscovery, an ID hydration can only target the old locator or
    // its cached checkpoint; it cannot select the new journal on its own.
    let before_discovery = project_facts(&db);
    let _ = moved.hydrate(&reference, HydrateOptions::default());
    assert_eq!(project_facts(&db), before_discovery);
    for _ in 0..2 {
        moved
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap();
        let conn = rusqlite::Connection::open(&db).unwrap();
        let (locator, state): (String, String) = conn
            .query_row(
                "SELECT raw_locator, discovery_state FROM session_presences \
                 WHERE source='junie' AND session_id=?1 AND location='local'",
                [SID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(locator, new_path.to_string_lossy());
        assert_eq!(state, "shallow");
    }
    moved
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(project_facts(&db), (None, None, None));
    assert_eq!(project_keys(&db), (None, None));
}

#[test]
fn native_index_rename_refreshes_full_catalog_on_discovery() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let index = home.path().join(".junie/sessions/index.jsonl");
    let renamed = std::fs::read_to_string(&index).unwrap().replace(
        "Synthetic Junie capture",
        "Renamed native title before hydration",
    );
    std::fs::write(index, renamed).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let row = store.sessions(query).next().unwrap().unwrap();
    assert_eq!(
        row.title.as_deref(),
        Some("Renamed native title before hydration")
    );
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
}

#[test]
fn discovery_limit_selects_newest_journal_after_enumerating_all_directories() {
    let home = tempfile::tempdir().unwrap();
    let original = stage(home.path());
    let second = original
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("session-zz-newer");
    std::fs::create_dir_all(&second).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    std::fs::write(
        second.join("events.jsonl"),
        std::fs::read(&original).unwrap(),
    )
    .unwrap();
    let store = open(home.path());
    store
        .discover(
            DiscoveryOptions::default()
                .sources([Source::Junie])
                .limit(1),
        )
        .unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let rows = store
        .sessions(query)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].session_id, "session-zz-newer");
}

#[test]
fn explicit_roots_isolate_equal_native_ids_and_empty_default_is_safe() {
    let home = tempfile::tempdir().unwrap();
    let empty = open(home.path());
    assert_eq!(
        empty
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap()
            .discovered,
        0
    );
    let native = stage(home.path());
    let root_a = home.path().join("alternate-a/sessions");
    let root_b = home.path().join("alternate-b/sessions");
    for root in [&root_a, &root_b] {
        let target = root.join(SID).join("events.jsonl");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(&native, target).unwrap();
        assert!(
            !root.join("index.jsonl").exists(),
            "optional metadata is absent"
        );
    }
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        home.path().into(),
        home.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["junie"] = serde_json::json!([root_a, root_b]);
    roots["provider_default_roots"]
        .as_object_mut()
        .unwrap()
        .remove("junie");
    let roots: ProviderRoots = serde_json::from_value(roots).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("isolated.db"))
            .roots(roots),
    )
    .unwrap();
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Junie]))
            .unwrap()
            .discovered,
        2
    );
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Junie]);
    let ids = store
        .sessions(query)
        .map(|row| row.unwrap().session_id)
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert!(ids
        .iter()
        .all(|id| id.starts_with("root:") && id.ends_with(SID)));
    for id in ids {
        store
            .hydrate(
                &SessionRef::id(Source::Junie, id.clone()),
                HydrateOptions::default(),
            )
            .unwrap();
        assert!(!store
            .session(&SessionRef::id(Source::Junie, id), SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages
            .is_empty());
    }
}

#[cfg(feature = "unstable-internal")]
#[test]
fn remote_evidence_accepts_matching_local_share_but_rejects_conflicting_rewrite() {
    use ai_hist::{SessionLocation, ShallowSession, SourceObservation, SourceObservationBatch};
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let reference = SessionRef::id(Source::Junie, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let conn = ai_hist::open_db(&home.path().join("junie.db")).unwrap();
    for table in ["session_events", "session_markers", "tool_calls"] {
        conn.execute(
            &format!("UPDATE {table} SET location='remote' WHERE source='junie' AND session_id=?1"),
            [SID],
        )
        .unwrap();
    }
    let before = evidence(&store);
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"\n")
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(evidence(&store).messages, before.messages);
    for table in ["session_events", "session_markers", "tool_calls"] {
        let count: i64 = conn.query_row(&format!("SELECT count(*) FROM {table} WHERE source='junie' AND session_id=?1 AND location!='both'"), [SID], |r| r.get(0)).unwrap();
        assert_eq!(count, 0, "{table} did not gain a local share");
    }
    let before_projects = project_facts(&home.path().join("junie.db"));
    store
        .apply_source_observations(SourceObservationBatch::new(
            "different-project-fixture",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "junie".into(),
                    session_id: SID.into(),
                    cwd: Some("/remote/different-project".into()),
                    source_stamp: Some("remote-project-only".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    assert_eq!(
        project_facts(&home.path().join("junie.db")).0.as_deref(),
        Some("/remote/different-project")
    );
    std::fs::remove_file(home.path().join(".junie/sessions/index.jsonl")).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(
        project_facts(&home.path().join("junie.db")),
        before_projects
    );
    assert_eq!(evidence(&store).messages, before.messages);
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        original.replace("The child confirmed blue.", "A conflicting local rewrite."),
    )
    .unwrap();
    assert!(store
        .hydrate(&reference, HydrateOptions::default())
        .is_err());
    assert_eq!(evidence(&store).messages, before.messages);
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn exported_junie_evidence_roundtrips_through_public_intake_then_local_hydrate() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let origin = tempfile::tempdir().unwrap();
    let origin_path = stage(origin.path());
    // Extend the pinned native row shape only for this ordering regression:
    // a same-millisecond bucket crossing ordinal 9 must survive public transport.
    let base = std::fs::read_to_string(&origin_path).unwrap();
    let mut added = String::new();
    let last: serde_json::Value = serde_json::from_str(base.lines().last().unwrap()).unwrap();
    for n in 8..14 {
        let mut row = last.clone();
        row["event"]["agentEvent"]["stepId"] = format!("same-ms-step-{n}").into();
        row["event"]["agentEvent"]["result"] = format!("Same-ms result {n}").into();
        added.push_str(&serde_json::to_string(&row).unwrap());
        added.push('\n');
    }
    std::fs::write(&origin_path, format!("{base}{added}")).unwrap();
    let source = open(origin.path());
    source
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    source
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(origin.path().join("junie.db")).unwrap(),
        &ExportSelection {
            sources: vec!["junie".into()],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        for record in page.records {
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            records.push(EvidenceRecord {
                kind: serde_json::from_value(serde_json::json!(record.kind)).unwrap(),
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            });
        }
    }
    assert!(records
        .iter()
        .any(|record| record.kind == EvidenceKind::ToolCall));
    let receiver = tempfile::tempdir().unwrap();
    let store = open(receiver.path());
    let key = ObservationKey {
        source: "junie".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "fixture-remote".into(),
        connector_instance: "default".into(),
    };
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fixture-remote",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "junie".into(),
                    session_id: SID.into(),
                    source_stamp: Some("remote-junie-fixture".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote-junie-fixture".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    assert_eq!(evidence(&store).tool_calls[0].tool_use_id, "step-tool-1");
    let receiver_path = stage(receiver.path());
    std::fs::copy(&origin_path, receiver_path).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Junie]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Junie, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).messages, evidence(&source).messages);
    assert_eq!(evidence(&store).tool_calls, evidence(&source).tool_calls);
}
