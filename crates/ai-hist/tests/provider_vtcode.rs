//! Public VT Code 0.180.2 canonical-journal acquisition acceptance.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const NATIVE: &str = "vt-synthetic-session";
const FIXTURE: &str =
    include_str!("fixtures/vtcode/basic/.vtcode/sessions/vt-synthetic-session/events.jsonl");

fn stage(home: &Path) -> PathBuf {
    let path = home
        .join(".vtcode/sessions")
        .join(NATIVE)
        .join("events.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    path
}
fn sid(home: &Path) -> String {
    sid_named(home, NATIVE)
}
fn sid_named(home: &Path, native: &str) -> String {
    // Existing workspace aliases share the provider's canonical namespace.
    let home = std::fs::canonicalize(home).expect("existing synthetic workspace");
    format!(
        "root:{:x}:{native}",
        Sha256::digest(home.join(".vtcode/sessions").to_string_lossy().as_bytes())
    )
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("conformance.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}
fn evidence(store: &SessionStore, id: &str) -> ai_hist::SessionEvidence {
    store
        .session(&SessionRef::id(Source::Vtcode, id), SessionQuery::default())
        .unwrap()
        .unwrap()
}
fn contains(evidence: &ai_hist::SessionEvidence, text: &str) -> bool {
    evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|block| block.text.as_deref() == Some(text))
}

#[cfg(unix)]
#[test]
fn workspace_alias_discovers_and_hydrates_the_canonical_native_identity() {
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let alias = parent.path().join("workspace-alias");
    std::os::unix::fs::symlink(&workspace, &alias).unwrap();
    let staged = stage(&alias);
    let store = open(&alias);
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 1, "{report:?}");
    assert!(report.diagnostics.is_empty(), "{report:?}");
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Vtcode]);
    let rows = store
        .sessions(query)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    let canonical = std::fs::canonicalize(&workspace).unwrap();
    let expected = format!(
        "root:{:x}:{NATIVE}",
        Sha256::digest(
            canonical
                .join(".vtcode/sessions")
                .to_string_lossy()
                .as_bytes()
        )
    );
    assert_eq!(rows[0].session_id, expected);
    assert_eq!(
        rows[0].raw_path,
        Some(std::fs::canonicalize(staged).unwrap())
    );
    eprintln!(
        "VTCode alias discovery registered one canonical row: {:?}; raw_path={:?}",
        rows[0].session_id, rows[0].raw_path
    );
    assert_eq!(
        sid(&alias),
        rows[0].session_id,
        "the fixture must hash the provider's canonical workspace root"
    );
    store
        .hydrate(&rows[0].session_ref(), HydrateOptions::default())
        .unwrap();
    assert!(contains(
        &evidence(&store, &expected),
        "I found one safe fixture file."
    ));
    assert_eq!(
        sid(&workspace),
        expected,
        "alias and target must share one fixture identity"
    );
}

#[test]
fn discover_hydrate_search_read_reopen_change_feed_export_and_filter() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let id = sid(home.path());
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Vtcode,
            session_id: &id,
            messages: &[
                (Role::User, "Inspect the synthetic fixture"),
                (Role::Assistant, "I found one safe fixture file."),
                (Role::User, "Run the synthetic verification"),
                (Role::Assistant, "Verification returned fixture."),
            ],
        },
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plans/history-providers/fixtures/vtcode/native-reader.json"
    ))
    .unwrap();
    let native_rows = oracle["events"].as_array().unwrap();
    let native_ids = native_rows
        .iter()
        .filter_map(|row| {
            let event = &row["event"];
            if event["type"] == "turn.started" && event["context"]["origin"] == "user" {
                event["context"]["turn_id"].as_str()
            } else if event["type"] == "item.completed" {
                event["item"]["id"].as_str()
            } else {
                None
            }
        })
        .map(|id| format!("{id}:0"))
        .collect::<std::collections::BTreeSet<_>>();
    let public_ids = e
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .map(|block| block.event_uid.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(public_ids, native_ids, "native reader and public event IDs");
    let native_usage = native_rows
        .iter()
        .filter(|row| row["event"]["type"] == "turn.completed")
        .map(|row| &row["event"]["usage"])
        .collect::<Vec<_>>();
    let public_usage = e
        .messages
        .iter()
        .filter_map(|message| message.usage.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(public_usage.len(), native_usage.len());
    for (actual, native) in public_usage.into_iter().zip(native_usage) {
        assert_eq!(
            actual.input_tokens + actual.cache_read_tokens + actual.cache_write_tokens,
            native["input_tokens"].as_u64().unwrap()
        );
        assert_eq!(
            actual.output_tokens,
            native["output_tokens"].as_u64().unwrap()
        );
    }
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_calls[0].tool_use_id, "call-b");
    assert!(e
        .tool_results
        .iter()
        .any(|r| r.tool_use_id.as_deref() == Some("call-b")));
    assert_eq!(
        e.messages.iter().filter_map(|m| m.usage.as_ref()).count(),
        2,
        "turn.completed usage is not counted again from thread.completed"
    );
    assert!(e
        .markers
        .iter()
        .any(|m| m.kind == "thread.compact_boundary"));
    assert!(e.file_edits.is_empty());
    assert!(e.relationships.is_empty());
    let store = open(home.path());
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Vtcode]);
    assert_eq!(store.sessions(query).count(), 1);
}

#[test]
fn partial_tail_append_rewrite_and_empty_roots_do_not_claim_missing_history() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
            .unwrap()
            .discovered,
        0
    );
    let path = stage(home.path());
    let id = sid(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let original = evidence(&store, &id).messages;
    std::fs::write(&path, format!("{FIXTURE}{{\"schema_version\":")).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store, &id).messages, original);
    let rewritten = FIXTURE.replace("I found one safe fixture file.", "Changed fixture answer.");
    std::fs::write(&path, rewritten).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(contains(&evidence(&store, &id), "Changed fixture answer."));
    assert!(!contains(
        &evidence(&store, &id),
        "I found one safe fixture file."
    ));
    std::fs::write(&path, FIXTURE).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(contains(
        &evidence(&store, &id),
        "I found one safe fixture file."
    ));
    let mut turn: serde_json::Value =
        serde_json::from_str(FIXTURE.lines().nth(1).unwrap()).unwrap();
    turn["event"]["context"]["turn_id"] = "turn-c".into();
    turn["event"]["context"]["task_id"] = "task-c".into();
    turn["event"]["context"]["timestamp"] = "2026-10-20T12:02:00Z".into();
    turn["event"]["context"]["goal"] = "Appended synthetic turn".into();
    let mut answer: serde_json::Value =
        serde_json::from_str(FIXTURE.lines().nth(2).unwrap()).unwrap();
    answer["event"]["item"]["id"] = "assistant-c".into();
    answer["event"]["item"]["context"]["turn_id"] = "turn-c".into();
    answer["event"]["item"]["context"]["task_id"] = "task-c".into();
    answer["event"]["item"]["context"]["timestamp"] = "2026-10-20T12:02:01Z".into();
    answer["event"]["item"]["text"] = "Appended synthetic answer".into();
    let appended = format!("{FIXTURE}{turn}\n{answer}\n");
    std::fs::write(&path, appended).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(contains(&evidence(&store, &id), "Appended synthetic turn"));
    assert!(contains(
        &evidence(&store, &id),
        "Appended synthetic answer"
    ));
    std::fs::write(&path, FIXTURE).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(!contains(&evidence(&store, &id), "Appended synthetic turn"));
}

#[test]
fn unsupported_and_malformed_canonical_rows_are_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    std::fs::write(&path, FIXTURE.replacen("0.18.0", "9.0.0", 1)).unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 0);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("VTCODE_UNSUPPORTED_SCHEMA")));
    std::fs::write(&path, format!("{FIXTURE}{{invalid}}\n")).unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 0);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("VTCODE_INVALID_RECORD")));
}

#[test]
fn metadata_only_journal_change_updates_marker_without_rewriting_messages() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let id = sid(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store, &id);
    let mut lines = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut meta: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    meta["event"]["fixture_metadata"] = "changed".into();
    lines[0] = meta.to_string();
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let after = evidence(&store, &id);
    assert_eq!(after.messages, before.messages);
    assert_ne!(after.markers, before.markers);
}

#[test]
fn missing_canonical_journal_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let valid = stage(home.path());
    std::fs::create_dir_all(home.path().join(".vtcode/sessions/missing")).unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("VTCODE_MISSING_JOURNAL")));
    let id = sid(home.path());
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    std::fs::remove_file(valid).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(contains(
        &evidence(&store, &id),
        "I found one safe fixture file."
    ));
}

#[test]
fn partial_live_journal_does_not_block_retirement_of_deleted_sibling() {
    let home = tempfile::tempdir().unwrap();
    let deleted = stage(home.path());
    let native = "partial-sibling";
    let sibling = home
        .path()
        .join(".vtcode/sessions")
        .join(native)
        .join("events.jsonl");
    std::fs::create_dir_all(sibling.parent().unwrap()).unwrap();
    let sibling_fixture = FIXTURE.replace(NATIVE, native);
    std::fs::write(&sibling, &sibling_fixture).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let deleted_id = sid(home.path());
    let sibling_id = sid_named(home.path(), native);
    let original = evidence(&store, &sibling_id).messages;
    assert!(!original.is_empty());

    std::fs::remove_dir_all(deleted.parent().unwrap()).unwrap();
    std::fs::write(&sibling, format!("{sibling_fixture}{{\"schema_version\":")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(
            &SessionRef::id(Source::Vtcode, deleted_id),
            SessionQuery::default(),
        )
        .unwrap()
        .is_none());
    assert_eq!(evidence(&store, &sibling_id).messages, original);
}

#[test]
fn shallow_tail_never_replaces_the_opening_prompt() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let opening = "A".repeat(70_000);
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut first: serde_json::Value = serde_json::from_str(&rows[1]).unwrap();
    first["event"]["context"]["goal"] = opening.clone().into();
    rows[1] = first.to_string();
    std::fs::write(&path, format!("{}\n", rows.join("\n"))).unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    let conn = rusqlite::Connection::open(home.path().join("conformance.db")).unwrap();
    let first: Option<String> = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='vtcode'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first, None, "tail turn is not an opening prompt");
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, sid(home.path())),
            HydrateOptions::default(),
        )
        .unwrap();
    // Change the stamp so the next discovery must reconsider shallow input.
    std::fs::write(
        &path,
        format!("{}\n{}\n", rows.join("\n"), FIXTURE.lines().last().unwrap()),
    )
    .unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    let first: Option<String> = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='vtcode'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first.as_deref(), Some(&opening[..4096]));
    assert!(contains(&evidence(&store, &sid(home.path())), &opening));
}

#[test]
fn empty_native_text_is_evidence_not_a_session_error() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut prompt: serde_json::Value = serde_json::from_str(&rows[1]).unwrap();
    prompt["event"]["context"]["goal"] = "".into();
    rows[1] = prompt.to_string();
    let mut reply: serde_json::Value = serde_json::from_str(&rows[2]).unwrap();
    reply["event"]["item"]["text"] = "".into();
    rows[2] = reply.to_string();
    std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let evidence = evidence(&store, &sid(home.path()));
    assert!(evidence
        .messages
        .iter()
        .any(|m| m.role == Role::User && m.blocks.iter().any(|b| b.text.as_deref() == Some(""))));
    assert!(evidence.messages.iter().any(
        |m| m.role == Role::Assistant && m.blocks.iter().any(|b| b.text.as_deref() == Some(""))
    ));
    assert!(contains(&evidence, "Run the synthetic verification"));
}

#[test]
fn tool_output_call_id_resolves_native_invocation_tool_id() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut output: serde_json::Value = serde_json::from_str(&rows[7]).unwrap();
    output["event"]["item"]
        .as_object_mut()
        .unwrap()
        .remove("tool_call_id");
    output["event"]["item"]["status"] = "failed".into();
    rows[7] = output.to_string();
    std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let evidence = evidence(&store, &sid(home.path()));
    assert_eq!(evidence.tool_calls[0].tool_use_id, "call-b");
    assert_eq!(evidence.tool_calls[0].is_error, Some(true));
    assert!(evidence
        .tool_results
        .iter()
        .any(|r| r.tool_use_id.as_deref() == Some("call-b")));
}

fn stage_tool_only(home: &Path, status: &str) {
    let path = stage(home);
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut invocation: serde_json::Value = serde_json::from_str(&rows[6]).unwrap();
    invocation["event"]["item"]["status"] = status.into();
    rows[6] = invocation.to_string();
    let retained = [0, 4, 5, 6, 9]
        .into_iter()
        .map(|index| rows[index].as_str())
        .collect::<Vec<_>>();
    std::fs::write(path, format!("{}\n", retained.join("\n"))).unwrap();
}

#[test]
fn invocation_failure_without_output_survives_retirement() {
    let home = tempfile::tempdir().unwrap();
    stage_tool_only(home.path(), "failed");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = evidence(&store, &sid(home.path()));
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_calls[0].is_error, Some(true));
    assert!(e.tool_results.is_empty());
}

#[test]
fn tool_only_turn_usage_attaches_to_recorded_invocation() {
    let home = tempfile::tempdir().unwrap();
    stage_tool_only(home.path(), "completed");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = evidence(&store, &sid(home.path()));
    let assistant = e
        .messages
        .iter()
        .find(|message| message.role == Role::Assistant)
        .unwrap();
    let usage = assistant
        .usage
        .as_ref()
        .expect("native turn usage on tool use");
    assert_eq!(usage.input_tokens, 16);
    assert_eq!(usage.cache_read_tokens, 4);
    assert_eq!(usage.output_tokens, 10);
    assert_eq!(e.messages.iter().filter(|m| m.usage.is_some()).count(), 1);
}

#[test]
fn rewritten_invocation_id_joins_its_current_output() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    for index in [5, 6] {
        let mut invocation: serde_json::Value = serde_json::from_str(&rows[index]).unwrap();
        invocation["event"]["item"]["tool_call_id"] = "call-new".into();
        rows[index] = invocation.to_string();
    }
    let mut output: serde_json::Value = serde_json::from_str(&rows[7]).unwrap();
    output["event"]["item"]
        .as_object_mut()
        .unwrap()
        .remove("tool_call_id");
    output["event"]["item"]["status"] = "failed".into();
    rows[7] = output.to_string();
    std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = evidence(&store, &sid(home.path()));
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_calls[0].tool_use_id, "call-new");
    assert_eq!(e.tool_calls[0].is_error, Some(true));
    assert_eq!(e.tool_results.len(), 1);
    assert_eq!(e.tool_results[0].tool_use_id.as_deref(), Some("call-new"));
}

#[test]
fn same_millisecond_order_repair_keeps_failed_tool_result() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut output: serde_json::Value = serde_json::from_str(&rows[7]).unwrap();
    output["event"]["item"]["status"] = "failed".into();
    rows[7] = output.to_string();
    std::fs::write(&path, format!("{}\n", rows.join("\n"))).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = evidence(&store, &sid(home.path()));
    assert_eq!(before.tool_calls[0].is_error, Some(true));

    let mut assistant: serde_json::Value = serde_json::from_str(&rows[8]).unwrap();
    assistant["event"]["item"]["id"] = "assistant-before-call".into();
    assistant["event"]["item"]["text"] = "Synthetic same-time reply".into();
    assistant["event"]["item"]["context"]["timestamp"] = "2026-10-20T12:01:02Z".into();
    rows.insert(6, assistant.to_string());
    std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let after = evidence(&store, &sid(home.path()));
    assert!(contains(&after, "Synthetic same-time reply"));
    assert_eq!(after.tool_calls[0].is_error, Some(true));
    assert_eq!(after.tool_results.len(), 1);
    assert_eq!(
        after.tool_results[0].result_status.as_deref(),
        Some("errored")
    );
}

#[test]
fn reordered_earlier_output_keeps_later_final_tool_status() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let mut rows = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut first_output: serde_json::Value = serde_json::from_str(&rows[7]).unwrap();
    first_output["event"]["item"]["status"] = "failed".into();
    rows[7] = first_output.to_string();
    let mut final_output = first_output;
    final_output["event"]["item"]["id"] = "output-final".into();
    final_output["event"]["item"]["status"] = "completed".into();
    final_output["event"]["item"]["context"]["timestamp"] = "2026-10-20T12:01:05Z".into();
    rows.insert(9, final_output.to_string());
    std::fs::write(&path, format!("{}\n", rows.join("\n"))).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        evidence(&store, &sid(home.path())).tool_calls[0].is_error,
        Some(false)
    );

    let mut assistant: serde_json::Value = serde_json::from_str(&rows[8]).unwrap();
    assistant["event"]["item"]["id"] = "assistant-before-output".into();
    assistant["event"]["item"]["text"] = "Synthetic same-time result preface".into();
    assistant["event"]["item"]["context"]["timestamp"] = "2026-10-20T12:01:03Z".into();
    rows.insert(7, assistant.to_string());
    std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let after = evidence(&store, &sid(home.path()));
    assert_eq!(after.tool_calls[0].is_error, Some(false));
    assert_eq!(after.tool_results.len(), 2);
    assert!(after
        .tool_results
        .iter()
        .any(|r| r.result_status.as_deref() == Some("errored")));
    assert!(after
        .tool_results
        .iter()
        .any(|r| r.result_status.as_deref() == Some("completed")));
}

#[test]
fn thread_started_only_keeps_unknown_activity_time() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::write(path, format!("{}\n", FIXTURE.lines().next().unwrap())).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Vtcode]);
    let row = store.sessions(query).next().unwrap().unwrap();
    assert_eq!(row.first_activity_ms, None);
    assert_eq!(row.last_activity_ms, None);
    let evidence = evidence(&store, &sid(home.path()));
    assert!(evidence.messages.is_empty());
    assert!(evidence.markers.iter().any(|m| m.kind == "thread.started"));
}

#[cfg(feature = "unstable-internal")]
#[test]
fn untimed_local_start_keeps_remote_catalog_metadata() {
    use ai_hist::{SessionLocation, ShallowSession, SourceObservation, SourceObservationBatch};
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::write(path, format!("{}\n", FIXTURE.lines().next().unwrap())).unwrap();
    let store = open(home.path());
    let id = sid(home.path());
    store
        .apply_source_observations(SourceObservationBatch::new(
            "synthetic-remote",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "vtcode".into(),
                    session_id: id.clone(),
                    first_activity_ms: Some(1_700_000_000_000),
                    last_activity_ms: Some(1_700_000_000_001),
                    first_prompt: Some("Remote observed prompt".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Vtcode]);
    let row = store.sessions(query).next().unwrap().unwrap();
    assert_eq!(row.first_activity_ms, Some(1_700_000_000_000));
    assert_eq!(row.last_activity_ms, Some(1_700_000_000_001));
    assert_eq!(row.first_prompt.as_deref(), Some("Remote observed prompt"));
    let conn = rusqlite::Connection::open(home.path().join("conformance.db")).unwrap();
    let state: String = conn
        .query_row(
            "SELECT discovery_state FROM sessions WHERE source='vtcode' AND session_id=?1",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "full");
    assert!(evidence(&store, &id)
        .markers
        .iter()
        .any(|marker| marker.kind == "thread.started"));
}

#[cfg(feature = "unstable-internal")]
#[test]
fn remote_observation_without_events_does_not_keep_retired_local_activity() {
    use ai_hist::{SessionLocation, ShallowSession, SourceObservation, SourceObservationBatch};
    for (remote_bounds, expected) in [
        (None, (1_792_497_660_000, 1_792_497_666_000)),
        (
            Some((1_792_497_540_000, 1_792_497_720_000)),
            (1_792_497_540_000, 1_792_497_720_000),
        ),
    ] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path());
        let store = open(home.path());
        let id = sid(home.path());
        store.sync(SyncOptions::default().force(true)).unwrap();
        store
            .apply_source_observations(SourceObservationBatch::new(
                "synthetic-remote",
                "default",
                SessionLocation::Remote,
                vec![SourceObservation {
                    session: ShallowSession {
                        source: "vtcode".into(),
                        session_id: id.clone(),
                        first_activity_ms: remote_bounds.map(|bounds| bounds.0),
                        last_activity_ms: remote_bounds.map(|bounds| bounds.1),
                        first_prompt: Some("Remote observed preview".into()),
                        ..Default::default()
                    },
                    raw_locator: None,
                }],
            ))
            .unwrap();
        let rows = FIXTURE
            .lines()
            .enumerate()
            .filter(|(index, _)| ![1, 2, 3].contains(index))
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
        store.sync(SyncOptions::default().force(true)).unwrap();
        let mut query = CatalogQuery::default();
        query.sources = Some(vec![Source::Vtcode]);
        let row = store.sessions(query).next().unwrap().unwrap();
        assert_eq!(row.first_activity_ms, Some(expected.0));
        assert_eq!(row.last_activity_ms, Some(expected.1));
        assert_eq!(
            row.first_prompt.as_deref(),
            Some("Run the synthetic verification")
        );
        assert!(!contains(
            &evidence(&store, &id),
            "Inspect the synthetic fixture"
        ));
        store.sync(SyncOptions::default().force(true)).unwrap();
        let mut repeated = CatalogQuery::default();
        repeated.sources = Some(vec![Source::Vtcode]);
        let stable = store.sessions(repeated).next().unwrap().unwrap();
        assert_eq!(stable.first_activity_ms, Some(expected.0));
        assert_eq!(stable.last_activity_ms, Some(expected.1));
    }
}

#[test]
fn complete_rewrite_without_user_turn_clears_first_prompt() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let id = sid(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let conn = rusqlite::Connection::open(home.path().join("conformance.db")).unwrap();
    let first: Option<String> = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='vtcode'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first.as_deref(), Some("Inspect the synthetic fixture"));
    let rows = FIXTURE
        .lines()
        .filter(|line| !line.contains("\"type\":\"turn.started\""))
        .collect::<Vec<_>>();
    std::fs::write(path, format!("{}\n", rows.join("\n"))).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let first: Option<String> = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='vtcode'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first, None);
    assert!(evidence(&store, &id)
        .messages
        .iter()
        .all(|m| m.role != Role::User));
}

#[cfg(unix)]
#[test]
fn symlink_to_outside_evidence_is_rejected() {
    let home = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let secret = outside.path().join("events.jsonl");
    std::fs::write(&secret, FIXTURE).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(secret, &path).unwrap();
    let store = open(home.path());
    assert!(store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .is_err());
}

#[cfg(feature = "unstable-internal")]
#[test]
fn remote_backed_native_rewrite_rolls_back_catalog_and_evidence() {
    for ownership in ["remote", "both"] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path());
        let id = sid(home.path());
        let store = open(home.path());
        store
            .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
            .unwrap();
        let reference = SessionRef::id(Source::Vtcode, &id);
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        let conn = ai_hist::open_db(&home.path().join("conformance.db")).unwrap();
        conn.execute("UPDATE session_events SET location=?1 WHERE source='vtcode' AND session_id=?2 AND event_uid='assistant-a:0'", rusqlite::params![ownership,id]).unwrap();
        let before = evidence(&store, &id);
        let revision = store.head_revision().unwrap();
        std::fs::write(
            &path,
            FIXTURE.replace("I found one safe fixture file.", "Remote backed rewrite."),
        )
        .unwrap();
        let error = store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("VTCODE_SHARED_EVIDENCE_CONFLICT"),
            "{error}"
        );
        let after = evidence(&store, &id);
        assert_eq!(after.session, before.session);
        assert_eq!(after.messages, before.messages);
        assert_eq!(after.tool_calls, before.tool_calls);
        assert_eq!(after.markers, before.markers);
        assert_eq!(store.head_revision().unwrap(), revision);
    }
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn native_export_remote_intake_then_local_hydrate_preserves_identities() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let id = sid(home.path());
    let source = open(home.path());
    source.sync(SyncOptions::default().force(true)).unwrap();
    let native = evidence(&source, &id);
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(home.path().join("conformance.db")).unwrap(),
        &ExportSelection {
            sources: vec!["vtcode".into()],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits {
            max_batch_records: 1,
            ..Default::default()
        },
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
    assert!(records.len() >= native.messages.len());
    let receiver = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("remote.db"))
            .roots(ProviderRoots::from_home(
                home.path().into(),
                home.path().join("opencode.db"),
            )),
    )
    .unwrap();
    let key = ObservationKey {
        source: "vtcode".into(),
        session_id: id.clone(),
        location: SessionLocation::Remote,
        connector_id: "synthetic-remote".into(),
        connector_instance: "default".into(),
    };
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "synthetic-remote",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "vtcode".into(),
                    session_id: id.clone(),
                    source_stamp: Some("synthetic-remote-v1".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "synthetic-remote-v1".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    let before = evidence(&receiver, &id);
    assert_eq!(before.messages, native.messages);
    assert_eq!(before.tool_calls, native.tool_calls);
    receiver
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    receiver
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let after = evidence(&receiver, &id);
    assert_eq!(after.messages, native.messages);
    assert_eq!(after.tool_calls, native.tool_calls);
    assert_eq!(after.markers, native.markers);
    let control_only = [
        FIXTURE.lines().next().unwrap(),
        FIXTURE.lines().last().unwrap(),
    ];
    std::fs::write(&path, format!("{}\n", control_only.join("\n"))).unwrap();
    receiver
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let after_control = evidence(&receiver, &id);
    assert_eq!(after_control.messages, native.messages);
    assert_eq!(after_control.tool_calls, native.tool_calls);
    let mut catalog_query = CatalogQuery::default();
    catalog_query.sources = Some(vec![Source::Vtcode]);
    let catalog = receiver.sessions(catalog_query).next().unwrap().unwrap();
    assert_eq!(
        catalog.first_prompt.as_deref(),
        Some("Inspect the synthetic fixture")
    );
    assert_eq!(
        catalog.last_assistant_text.as_deref(),
        Some("Verification returned fixture.")
    );
    assert_eq!(catalog.first_activity_ms, Some(1_792_497_600_000));
    assert_eq!(catalog.last_activity_ms, Some(1_792_497_666_000));
    let revision = receiver.head_revision().unwrap();
    std::fs::write(
        &path,
        FIXTURE.replace(
            "I found one safe fixture file.",
            "Conflicting native answer.",
        ),
    )
    .unwrap();
    let error = receiver
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("VTCODE_SHARED_EVIDENCE_CONFLICT"),
        "{error}"
    );
    let retained = evidence(&receiver, &id);
    assert_eq!(retained.messages, native.messages);
    assert_eq!(retained.tool_calls, native.tool_calls);
    assert_eq!(retained.markers, after_control.markers);
    assert_eq!(receiver.head_revision().unwrap(), revision);
}
