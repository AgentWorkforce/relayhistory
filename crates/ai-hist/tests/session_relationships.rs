//! End-to-end delegation topology over real provider fixtures.
//!
//! One isolated HOME holding a Codex root with two subagent threads and a
//! grandchild, plus a Claude session with an `agentId`-carrying subagent, is
//! taken through the acquisition path a host actually uses (sync, discovery,
//! targeted hydration) and then queried through the public relationship API.

use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, sync_scoped_at, DiscoverOptions,
    HydrateSessionOptions, SessionScope,
};
use ai_hist::{open_db, session_relationships, session_tree, SessionTreeOptions};
use std::fs;
use std::path::Path;

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn codex_root(home: &Path) {
    let day = home.join(".codex/sessions/2026/08/31");
    write(
        &day.join("rollout-root.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:00Z","type":"session_meta","payload":{"id":"root","cwd":"/work/app"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"root prompt"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"root answer"}}"#,
            "\n",
        ),
    );
    write(
        &day.join("rollout-child-a.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:03Z","type":"session_meta","payload":{"id":"child-a","session_id":"root","parent_thread_id":"root","cwd":"/work/app","thread_source":"subagent","source":{"subagent":{"other":"guardian"}}}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:04Z","type":"event_msg","payload":{"type":"user_message","message":"delegated task a"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:05Z","type":"event_msg","payload":{"type":"agent_message","message":"child a answer"}}"#,
            "\n",
        ),
    );
    write(
        &day.join("rollout-child-b.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:06Z","type":"session_meta","payload":{"id":"child-b","session_id":"root","cwd":"/work/app","thread_source":"subagent"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:07Z","type":"event_msg","payload":{"type":"agent_message","message":"child b answer"}}"#,
            "\n",
        ),
    );
    write(
        &day.join("rollout-grandchild.jsonl"),
        concat!(
            r#"{"timestamp":"2026-08-31T10:00:08Z","type":"session_meta","payload":{"id":"grandchild","session_id":"child-a","cwd":"/work/app","thread_source":"subagent"}}"#,
            "\n",
            r#"{"timestamp":"2026-08-31T10:00:09Z","type":"event_msg","payload":{"type":"agent_message","message":"grandchild answer"}}"#,
            "\n",
        ),
    );
}

fn claude_root(home: &Path) {
    write(
        &home.join(".claude/projects/app/claude-root.jsonl"),
        concat!(
            r#"{"sessionId":"claude-root","uuid":"u1","cwd":"/work/app","type":"user","message":{"role":"user","content":"human prompt"},"timestamp":"2026-08-31T11:00:00Z"}"#,
            "\n",
            r#"{"sessionId":"claude-root","uuid":"a1","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Agent","input":{"prompt":"plan it"}}]},"timestamp":"2026-08-31T11:00:01Z"}"#,
            "\n",
        ),
    );
    let subagents = home.join(".claude/projects/app/claude-root/subagents");
    write(
        &subagents.join("agent-abc.jsonl"),
        concat!(
            r#"{"sessionId":"claude-root","agentId":"abc","isSidechain":true,"uuid":"side-u","cwd":"/work/app","type":"user","message":{"role":"user","content":"delegated instruction"},"timestamp":"2026-08-31T11:00:02Z"}"#,
            "\n",
            r#"{"sessionId":"claude-root","agentId":"abc","isSidechain":true,"uuid":"side-a","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":"child result"},"timestamp":"2026-08-31T11:00:03Z"}"#,
            "\n",
        ),
    );
    write(
        &subagents.join("agent-abc.meta.json"),
        r#"{"agentType":"Plan","description":"plan the work","toolUseId":"toolu_1","spawnDepth":1,"model":"opus"}"#,
    );
}

fn hydrate(db: &Path, source: &str, session_id: &str) {
    hydrate_session_at(
        db,
        &HydrateSessionOptions {
            source: source.to_string(),
            session_id: session_id.to_string(),
            scope: SessionScope::Local,
            include_related: true,
        },
    )
    .unwrap_or_else(|error| panic!("hydrating {source}/{session_id}: {error:#}"));
}

/// The only test in this binary: it sets `HOME` for the process, which is
/// safe exactly because nothing else here runs beside it.
#[test]
fn delegation_topology_survives_the_whole_acquisition_path() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    codex_root(home);
    claude_root(home);
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("OPENCODE_DB", home.join("missing-opencode.db"));
    std::env::remove_var("CLAUDE_CONFIG_DIR");
    std::env::remove_var("CODEX_HOME");
    std::env::remove_var("GROK_HOME");
    std::env::remove_var("AI_HIST_DB");
    let db = home.join("history.db");

    // A plain sync records the topology the provider files already describe,
    // including the edge no single hydration would reach: the grandchild
    // hangs off a subagent thread, which is never a hydration target.
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    discover_sessions_scoped_at(
        &db,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: Vec::new(),
            limit: None,
        },
    )
    .unwrap();
    hydrate(&db, "codex", "root");
    hydrate(&db, "claude", "claude-root");

    let conn = open_db(&db).unwrap();

    let tree = session_tree(&conn, "codex", "root", &SessionTreeOptions::default()).unwrap();
    assert_eq!(
        tree.nodes
            .iter()
            .map(|node| (node.session_id.as_str(), node.depth))
            .collect::<Vec<_>>(),
        vec![
            ("root", 0),
            ("child-a", 1),
            ("grandchild", 2),
            ("child-b", 1)
        ]
    );
    assert!(tree.nodes.iter().all(|node| node.has_events));
    assert!(!tree.truncated);
    assert_eq!(tree.max_depth_reached, 2);
    assert!(tree.unlinked.is_empty());
    assert_eq!(tree.capabilities.stable_child_identity, "always");
    assert_eq!(
        tree.nodes[1]
            .relationship
            .as_ref()
            .and_then(|edge| edge.child_agent_type.clone()),
        Some("guardian".to_string())
    );

    let middle = session_relationships(&conn, "codex", "child-a").unwrap();
    assert_eq!(middle.as_child.len(), 1);
    assert_eq!(middle.as_child[0].parent_session_id, "root");
    assert_eq!(
        middle
            .as_parent
            .iter()
            .map(|edge| edge.child_session_id.clone().unwrap())
            .collect::<Vec<_>>(),
        vec!["grandchild"]
    );

    let claude = session_relationships(&conn, "claude", "claude-root").unwrap();
    assert_eq!(claude.capabilities.stable_child_identity, "sometimes");
    assert_eq!(claude.as_parent.len(), 1);
    assert_eq!(claude.as_parent[0].child_session_id.as_deref(), Some("abc"));
    assert_eq!(claude.as_parent[0].identity_status, "observed");
    assert_eq!(
        claude.as_parent[0].child_agent_name.as_deref(),
        Some("plan the work")
    );
    assert!(claude.as_parent[0].child_has_events);

    // Delegated threads are evidence, not sessions: only the two roots a
    // human actually ran are in the catalog.
    let sessions: Vec<(String, String)> = conn
        .prepare("SELECT source, session_id FROM sessions ORDER BY source, session_id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        sessions,
        vec![
            ("claude".to_string(), "claude-root".to_string()),
            ("codex".to_string(), "root".to_string()),
        ]
    );

    // And a delegated instruction is nobody's prompt.
    let prompts: Vec<(String, Option<String>)> = conn
        .prepare("SELECT source, session_id FROM history ORDER BY source, id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        prompts,
        vec![
            ("claude".to_string(), Some("claude-root".to_string())),
            ("codex".to_string(), Some("root".to_string())),
        ]
    );

    // The child's own output is addressable under the child, and under the
    // child only: hydration healed the parent-attributed row the earlier
    // parser version (and the sync path) wrote.
    let placement: (i64, i64) = conn
        .query_row(
            "SELECT \
               (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'), \
               (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='claude-root' AND event_uid LIKE 'side-%')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(placement, (1, 0));
    drop(conn);

    // A later full sync walks the same provider files again. It must not undo
    // what hydration established: no re-attribution of the child's output, no
    // catalog row for a delegated thread, no lost topology.
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let after: (i64, i64, i64, i64) = conn
        .query_row(
            "SELECT \
               (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'), \
               (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='claude-root' AND event_uid LIKE 'side-%'), \
               (SELECT COUNT(*) FROM sessions), \
               (SELECT COUNT(*) FROM session_relationships)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(after, (1, 0, 2, 4));
    let raw_path: String = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='claude' AND session_id='claude-root'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(raw_path.ends_with("claude-root.jsonl"));
    assert_eq!(
        session_tree(&conn, "codex", "root", &SessionTreeOptions::default())
            .unwrap()
            .nodes
            .len(),
        4
    );
}
