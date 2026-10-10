//! A delegated child's evidence, read through the public facade.
//!
//! Claude Code writes each Agent/Task subagent as a sidecar beside its
//! parent's transcript: `<sessionId>/subagents/agent-<id>.jsonl` with an
//! `agent-<id>.meta.json` that names no model. The fixture is that real
//! layout — a main transcript that spawns `a1` (Explore), and `a2`
//! (code-reviewer) spawned by a tool use inside `a1` — staged into an
//! isolated HOME and read on the crate's default features.

use ai_hist::{
    CatalogQuery, DiscoveryOptions, DiscoveryState, HydrateOptions, IdentityQuery, ProviderRoots,
    Relationship, RelationshipSide, SessionEvidence, SessionIdentity, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use std::fs;
use std::path::{Path, PathBuf};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/claude/nested-sidecars"
);

const MAIN: &str = "sidecar-session";

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn subagents(home: &Path) -> PathBuf {
    home.join(".claude/projects/-tmp-project/sidecar-session/subagents")
}

fn open(home: &Path) -> SessionStore {
    let mut options = StoreOptions::default();
    options.db_path = Some(home.join("ai-history.db"));
    options.roots = Some(ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    ));
    SessionStore::open(options).expect("open")
}

fn hydrate(store: &SessionStore) {
    let mut options = HydrateOptions::default();
    options.include_related = true;
    store
        .hydrate(&SessionRef::id(Source::Claude, MAIN), options)
        .expect("hydrate");
}

/// The fixture after the acquisition path a host runs: sync, then a targeted
/// hydration with related transcripts.
fn staged(home: &Path) -> SessionStore {
    copy_tree(Path::new(FIXTURE), home);
    let store = open(home);
    store.sync(SyncOptions::default()).expect("sync");
    hydrate(&store);
    store
}

fn read(store: &SessionStore, session_id: &str) -> Option<SessionEvidence> {
    store
        .session(
            &SessionRef::id(Source::Claude, session_id),
            SessionQuery::default(),
        )
        .expect("session")
}

fn delegation<'a>(
    evidence: &'a SessionEvidence,
    side: RelationshipSide,
    child: &str,
) -> &'a Relationship {
    evidence
        .relationships
        .iter()
        .find(|edge| {
            edge.side == side
                && edge.relationship == "delegated"
                && edge.child_session_id.as_deref() == Some(child)
        })
        .unwrap_or_else(|| {
            panic!(
                "no {side:?} delegation to {child} on {}: {:#?}",
                evidence.session.session_id, evidence.relationships
            )
        })
}

/// Every delegated edge as `(parent, child)`, from the main session down.
fn delegation_tree(store: &SessionStore) -> Vec<(String, String)> {
    let mut edges = Vec::new();
    let mut frontier = vec![MAIN.to_string()];
    while let Some(id) = frontier.pop() {
        let evidence = read(store, &id).unwrap_or_else(|| panic!("{id} is readable"));
        for edge in &evidence.relationships {
            if edge.side == RelationshipSide::Parent && edge.relationship == "delegated" {
                let child = edge.child_session_id.clone().expect("observed child");
                edges.push((edge.parent_session_id.clone(), child.clone()));
                frontier.push(child);
            }
        }
    }
    edges.sort();
    edges
}

#[test]
fn a_delegated_childs_evidence_is_readable_by_its_id() {
    let temp = tempfile::tempdir().unwrap();
    let store = staged(temp.path());

    let a1 = read(&store, "a1").expect("a1 is readable by its id");
    assert_eq!(a1.session.discovery_state, DiscoveryState::Delegated);
    assert_eq!(a1.session.source, Source::Claude);
    assert_eq!(a1.session.cwd.as_deref(), Some("/tmp/project"));
    assert_eq!(a1.session.models, vec!["claude-haiku-4-5".to_string()]);
    assert_eq!(a1.session.agent_version.as_deref(), Some("2.1.120"));
    assert!(a1.session.locations.is_empty());
    assert_eq!(
        a1.session.raw_path.as_deref(),
        Some(subagents(temp.path()).join("agent-a1.jsonl").as_path())
    );
    assert!(a1.session.first_activity_ms.is_some());
    assert!(a1.session.first_activity_ms <= a1.session.last_activity_ms);

    // Its own messages with the provider's usage verbatim, its tool calls,
    // its requests.
    let assistant: Vec<_> = a1
        .messages
        .iter()
        .filter(|message| message.provider_message_id.is_some())
        .collect();
    assert_eq!(
        assistant
            .iter()
            .map(|message| message.provider_message_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["msg_sub1_1", "msg_sub1_2"]
    );
    let usage: serde_json::Value =
        serde_json::from_str(assistant[0].raw_usage().expect("raw usage")).unwrap();
    assert_eq!(usage["input_tokens"], 300);
    assert_eq!(usage["output_tokens"], 30);
    assert_eq!(
        a1.tool_calls
            .iter()
            .map(|call| (call.tool_use_id.as_str(), call.name.as_str()))
            .collect::<Vec<_>>(),
        vec![("toolu_review", "Agent")]
    );
    assert_eq!(a1.requests.len(), 2);

    // Who spawned it, on the evidence itself.
    let spawned = delegation(&a1, RelationshipSide::Child, "a1");
    assert_eq!(spawned.parent_session_id, MAIN);
    assert_eq!(spawned.evidence_ref.as_deref(), Some("toolu_explore"));

    let a2 = read(&store, "a2").expect("a2 is readable by its id");
    assert_eq!(a2.session.discovery_state, DiscoveryState::Delegated);
    assert_eq!(a2.session.models, vec!["claude-sonnet-4-6".to_string()]);
    assert_eq!(a2.requests.len(), 1);
    let usage: serde_json::Value = serde_json::from_str(
        a2.messages
            .iter()
            .find_map(|message| message.raw_usage())
            .expect("raw usage"),
    )
    .unwrap();
    assert_eq!(usage["output_tokens"], 25);
}

#[test]
fn delegated_children_stay_out_of_the_catalog() {
    let temp = tempfile::tempdir().unwrap();
    let store = staged(temp.path());

    let listed: Vec<String> = store
        .sessions(CatalogQuery::default())
        .map(|row| row.unwrap().session_id)
        .collect();
    assert_eq!(listed, vec![MAIN.to_string()]);
    assert_eq!(
        read(&store, MAIN).unwrap().session.discovery_state,
        DiscoveryState::Full
    );

    let own: Vec<String> = store
        .session_identities(IdentityQuery::default().exclude_delegated())
        .unwrap()
        .into_iter()
        .map(|identity| identity.session_id)
        .collect();
    assert_eq!(own, vec![MAIN.to_string()]);

    // An id nothing delegated to and nothing is stored under is no session.
    assert!(read(&store, "a9").is_none());
    // A sidecar is named by its id, not by its path.
    assert!(store
        .session(
            &SessionRef::path(
                Source::Claude,
                subagents(temp.path()).join("agent-a1.jsonl")
            ),
            SessionQuery::default(),
        )
        .unwrap()
        .is_none());
}

#[test]
fn every_childs_evidence_is_reached_from_the_root() {
    let temp = tempfile::tempdir().unwrap();
    let store = staged(temp.path());

    let children = store
        .delegated_descendants(&[SessionIdentity::new("claude", MAIN)])
        .unwrap();
    let output: Vec<(String, i64)> = children
        .iter()
        .map(|child| {
            let evidence = read(&store, &child.session_id).expect("child is readable");
            let output = evidence
                .messages
                .iter()
                .filter_map(|message| message.raw_usage())
                .map(|raw| {
                    serde_json::from_str::<serde_json::Value>(raw).unwrap()["output_tokens"]
                        .as_i64()
                        .unwrap()
                })
                .sum();
            (child.session_id.clone(), output)
        })
        .collect();
    assert_eq!(output, vec![("a1".to_string(), 70), ("a2".to_string(), 25)]);
}

#[test]
fn a_nested_sidecar_hangs_under_the_subagent_that_spawned_it() {
    let expected = vec![
        ("a1".to_string(), "a2".to_string()),
        (MAIN.to_string(), "a1".to_string()),
    ];

    // Sync alone, then discovery and hydration alone, each on a fresh store.
    let temp = tempfile::tempdir().unwrap();
    copy_tree(Path::new(FIXTURE), temp.path());
    let store = open(temp.path());
    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(delegation_tree(&store), expected, "after sync");

    let temp = tempfile::tempdir().unwrap();
    copy_tree(Path::new(FIXTURE), temp.path());
    let store = open(temp.path());
    store.discover(DiscoveryOptions::default()).unwrap();
    hydrate(&store);
    assert_eq!(delegation_tree(&store), expected, "after hydration");

    // The nested sidecar read before the one holding its tool use: a file
    // name is not an identity, so `a2` sorting first must not change who
    // spawned it.
    let temp = tempfile::tempdir().unwrap();
    copy_tree(Path::new(FIXTURE), temp.path());
    let dir = subagents(temp.path());
    fs::rename(dir.join("agent-a2.jsonl"), dir.join("agent-0.jsonl")).unwrap();
    fs::rename(
        dir.join("agent-a2.meta.json"),
        dir.join("agent-0.meta.json"),
    )
    .unwrap();
    let store = open(temp.path());
    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(delegation_tree(&store), expected, "a2 read first, sync");
    hydrate(&store);
    assert_eq!(
        delegation_tree(&store),
        expected,
        "a2 read first, hydration"
    );

    let edge = read(&store, "a2").unwrap();
    let spawned = delegation(&edge, RelationshipSide::Child, "a2");
    assert_eq!(spawned.parent_session_id, "a1");
    assert_eq!(spawned.spawn_depth, Some(2));
    assert_eq!(spawned.evidence_ref.as_deref(), Some("toolu_review"));
    assert_eq!(spawned.child_agent_type.as_deref(), Some("code-reviewer"));
}

#[test]
fn a_childs_model_comes_from_its_own_records_when_the_meta_names_none() {
    let temp = tempfile::tempdir().unwrap();
    let store = staged(temp.path());

    let main = read(&store, MAIN).unwrap();
    assert_eq!(
        delegation(&main, RelationshipSide::Parent, "a1")
            .child_model
            .as_deref(),
        Some("claude-haiku-4-5")
    );
    let a1 = read(&store, "a1").unwrap();
    assert_eq!(
        delegation(&a1, RelationshipSide::Parent, "a2")
            .child_model
            .as_deref(),
        Some("claude-sonnet-4-6")
    );
}

#[test]
fn a_spawn_result_names_the_child_it_reports_on() {
    let temp = tempfile::tempdir().unwrap();
    let store = staged(temp.path());

    let main = read(&store, MAIN).unwrap();
    let result = main
        .tool_results
        .iter()
        .find(|result| result.tool_use_id.as_deref() == Some("toolu_explore"))
        .expect("spawn result");
    assert_eq!(result.agent_id.as_deref(), Some("a1"));
}

#[test]
fn a_codex_child_thread_is_readable_by_its_id() {
    let temp = tempfile::tempdir().unwrap();
    let day = temp.path().join(".codex/sessions/2026/08/31");
    fs::create_dir_all(&day).unwrap();
    fs::write(
        day.join("rollout-root.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:00Z","type":"session_meta","payload":{"id":"root","cwd":"/work/app"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"root prompt"}}"#,
            "\n",
        ),
    )
    .unwrap();
    fs::write(
        day.join("rollout-child.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:03Z","type":"session_meta","payload":{"id":"child","session_id":"root","parent_thread_id":"root","cwd":"/work/app","thread_source":"subagent"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:05Z","type":"event_msg","payload":{"type":"agent_message","message":"child answer"}}"#,
            "\n",
        ),
    )
    .unwrap();
    let store = open(temp.path());
    store.sync(SyncOptions::default()).unwrap();

    let listed: Vec<String> = store
        .sessions(CatalogQuery::default())
        .map(|row| row.unwrap().session_id)
        .collect();
    assert_eq!(listed, vec!["root".to_string()]);
    let child = store
        .session(
            &SessionRef::id(Source::Codex, "child"),
            SessionQuery::default(),
        )
        .unwrap()
        .expect("the child thread is readable by its id");
    assert_eq!(child.session.discovery_state, DiscoveryState::Delegated);
    assert_eq!(child.session.cwd.as_deref(), Some("/work/app"));
    assert!(!child.messages.is_empty());
    let spawned = delegation(&child, RelationshipSide::Child, "child");
    assert_eq!(spawned.parent_session_id, "root");
}

/// A standalone Codex guardian / auto-review thread is marked
/// `thread_source: "subagent"` but names no parent, so nothing links it to
/// another session: it is a catalogued session of its own, under
/// `sessions/` and `archived_sessions/` alike, readable by its id.
#[test]
fn a_codex_subagent_thread_naming_no_parent_is_a_catalogued_session() {
    let temp = tempfile::tempdir().unwrap();
    for (tree, id) in [
        ("sessions", "guardian"),
        ("archived_sessions", "archived-guardian"),
    ] {
        let day = temp.path().join(format!(".codex/{tree}/2026/08/31"));
        fs::create_dir_all(&day).unwrap();
        fs::write(
            day.join(format!("rollout-{id}.jsonl")),
            format!(
                concat!(
                    r#"{{"timestamp":"2026-08-31T10:00:00Z","type":"session_meta","payload":{{"id":"{id}","session_id":"{id}","cwd":"/work/app","source":{{"subagent":{{"other":"guardian"}}}},"thread_source":"subagent"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-08-31T10:00:01Z","type":"event_msg","payload":{{"type":"user_message","message":"review this"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-08-31T10:00:02Z","type":"event_msg","payload":{{"type":"agent_message","message":"approved"}}}}"#,
                    "\n",
                ),
                id = id
            ),
        )
        .unwrap();
    }
    let store = open(temp.path());
    store.sync(SyncOptions::default()).unwrap();

    let mut listed: Vec<String> = store
        .sessions(CatalogQuery::default())
        .map(|row| row.unwrap().session_id)
        .collect();
    listed.sort();
    assert_eq!(
        listed,
        vec!["archived-guardian".to_string(), "guardian".to_string()]
    );
    for id in ["guardian", "archived-guardian"] {
        let evidence = store
            .session(&SessionRef::id(Source::Codex, id), SessionQuery::default())
            .unwrap()
            .expect("the standalone thread is readable by its id");
        assert_ne!(evidence.session.discovery_state, DiscoveryState::Delegated);
        assert!(!evidence.messages.is_empty(), "{id}");
        assert!(evidence.relationships.is_empty(), "{id}");
    }
}

/// A child's classification comes from its own `session_meta`, so a child
/// synced before its parent's rollout exists is already a delegated child:
/// it never enters the catalog, and once the parent arrives the parent's
/// descendants reach it.
#[test]
fn a_codex_child_synced_before_its_parent_is_linked_when_the_parent_arrives() {
    let temp = tempfile::tempdir().unwrap();
    let day = temp.path().join(".codex/sessions/2026/08/31");
    fs::create_dir_all(&day).unwrap();
    fs::write(
        day.join("rollout-child.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:03Z","type":"session_meta","payload":{"id":"child","session_id":"root","parent_thread_id":"root","cwd":"/work/app","source":{"subagent":{"other":"guardian"}},"thread_source":"subagent"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:05Z","type":"event_msg","payload":{"type":"agent_message","message":"child answer"}}"#,
            "\n",
        ),
    )
    .unwrap();
    let store = open(temp.path());
    store.sync(SyncOptions::default()).unwrap();

    assert_eq!(store.sessions(CatalogQuery::default()).count(), 0);
    let child = store
        .session(
            &SessionRef::id(Source::Codex, "child"),
            SessionQuery::default(),
        )
        .unwrap()
        .expect("the child is readable before its parent is captured");
    assert_eq!(child.session.discovery_state, DiscoveryState::Delegated);

    fs::write(
        day.join("rollout-root.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:00Z","type":"session_meta","payload":{"id":"root","cwd":"/work/app"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"root prompt"}}"#,
            "\n",
        ),
    )
    .unwrap();
    store.sync(SyncOptions::default()).unwrap();

    let listed: Vec<String> = store
        .sessions(CatalogQuery::default())
        .map(|row| row.unwrap().session_id)
        .collect();
    assert_eq!(listed, vec!["root".to_string()]);
    let children: Vec<String> = store
        .delegated_descendants(&[SessionIdentity::new("codex", "root")])
        .unwrap()
        .into_iter()
        .map(|child| child.session_id)
        .collect();
    assert_eq!(children, vec!["child".to_string()]);
    let child = store
        .session(
            &SessionRef::id(Source::Codex, "child"),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(child.session.discovery_state, DiscoveryState::Delegated);
    assert!(!child.messages.is_empty());
}

#[test]
fn a_nested_sidecar_whose_meta_is_gone_keeps_its_spawner() {
    let temp = tempfile::tempdir().unwrap();
    let store = staged(temp.path());
    fs::remove_file(subagents(temp.path()).join("agent-a2.meta.json")).unwrap();
    store.sync(SyncOptions::default()).unwrap();
    hydrate(&store);

    let a2 = read(&store, "a2").unwrap();
    let spawned = delegation(&a2, RelationshipSide::Child, "a2");
    // The meta was the only record of the tool use; its absence says nothing
    // about who spawned the child, so the recorded spawner stands.
    assert_eq!(spawned.parent_session_id, "a1");
    assert_eq!(spawned.evidence_ref, None);
    assert_eq!(spawned.child_agent_type, None);
}
