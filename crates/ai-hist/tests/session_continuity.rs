//! End-to-end continuity topology over burn's Claude fixture corpus.
//!
//! One isolated HOME holding an origin transcript, a continuation of it, two
//! branches sharing one in-log session id, and a `/resume` transcript, taken
//! through the acquisition path a host actually uses. The point of doing it
//! here rather than in a unit test is that nothing is called directly: the
//! edges have to survive a plain `sync`, and reach the public API from there.
//!
//! Deliberately one test: `sync_scoped_at` reads `HOME` from the process
//! environment, so two of these running in parallel would each see the
//! other's corpus.

use ai_hist::{
    open_db, session_relationships, session_tree, sync_scoped_at, RelationshipKinds, SessionScope,
    SessionTreeOptions, CONTINUITY_RELATIONSHIPS, RELATIONSHIP_CONTINUATION, RELATIONSHIP_FORK,
    RELATIONSHIP_RESUME,
};
use std::fs;
use std::path::{Path, PathBuf};

const ORIGINAL: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const CROSS: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const SHARED_FORK: &str = "00000000-0000-0000-0000-000000000fff";
const RESUMED: &str = "99999999-9999-9999-9999-999999999999";
const RESUME_TARGET: &str = "11111111-1111-1111-1111-111111111111";

/// burn's fixtures, copied verbatim and kept under their original names: the
/// file name is what tells two branches of one conversation apart.
fn install(home: &Path, names: &[&str]) {
    let projects = home.join(".claude/projects/app");
    fs::create_dir_all(&projects).unwrap();
    for name in names {
        fs::copy(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/claude")
                .join(name),
            projects.join(name),
        )
        .unwrap();
    }
}

fn continuity_rows(db: &Path) -> Vec<(String, String, String)> {
    let conn = open_db(db).unwrap();
    let rows = conn
        .prepare(
            "SELECT relationship, parent_session_id, relationship_uid \
             FROM session_relationships \
             WHERE relationship IN ('continuation', 'fork', 'resume') \
             ORDER BY relationship, parent_session_id, relationship_uid",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    rows
}

#[test]
fn a_plain_sync_records_continuity_and_leaves_delegation_untouched() {
    let home = tempfile::tempdir().unwrap();
    install(
        home.path(),
        &[
            "original-session.jsonl",
            "cross-file-parent.jsonl",
            "fork-branch-a.jsonl",
            "fork-branch-b.jsonl",
            "resume-marker.jsonl",
        ],
    );
    std::env::set_var("HOME", home.path());
    std::env::set_var("USERPROFILE", home.path());
    std::env::set_var("OPENCODE_DB", home.path().join("missing-opencode.db"));
    std::env::remove_var("AI_HIST_DB");
    let db = home.path().join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();

    // A transcript that opens by answering a record it does not contain is a
    // continuation of whichever session holds that record.
    let continuation = session_relationships(&conn, "claude", CROSS).unwrap();
    let edge = continuation
        .continuity
        .iter()
        .find(|edge| edge.relationship == RELATIONSHIP_CONTINUATION)
        .expect("the cross-file parent uuid resolved to the session holding it");
    assert_eq!(edge.parent_session_id, ORIGINAL);
    assert_eq!(edge.child_session_id.as_deref(), Some(CROSS));
    assert_eq!(edge.evidence_ref.as_deref(), Some("u-original-asst"));

    // Two transcripts carrying one in-log session id are its branches. Neither
    // has a provider identity of its own, so both are unlinked evidence keyed
    // on the transcript rather than on a name taken from a file.
    let forks = session_relationships(&conn, "claude", SHARED_FORK).unwrap();
    let branches: Vec<(&str, Option<&str>)> = forks
        .continuity
        .iter()
        .filter(|edge| edge.relationship == RELATIONSHIP_FORK)
        .map(|edge| {
            (
                edge.relationship_uid.as_str(),
                edge.child_session_id.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        branches,
        vec![("fork:fork-branch-a", None), ("fork:fork-branch-b", None),]
    );

    let resumed = session_relationships(&conn, "claude", RESUMED).unwrap();
    let resume = resumed
        .continuity
        .iter()
        .find(|edge| edge.relationship == RELATIONSHIP_RESUME)
        .expect("the /resume marker named its prior session");
    assert_eq!(resume.parent_session_id, RESUME_TARGET);
    assert_eq!(resume.child_session_id.as_deref(), Some(RESUMED));

    // None of this reaches the delegation view.
    let origin = session_relationships(&conn, "claude", ORIGINAL).unwrap();
    assert!(origin.as_parent.is_empty());
    assert!(origin.as_child.is_empty());
    assert_eq!(origin.continuity.len(), 1);

    // The default tree is delegation only, so it is the lone root it would
    // have been before continuity existed.
    let tree = session_tree(&conn, "claude", ORIGINAL, &SessionTreeOptions::default()).unwrap();
    assert_eq!(
        tree.nodes
            .iter()
            .map(|node| node.session_id.as_str())
            .collect::<Vec<_>>(),
        vec![ORIGINAL]
    );
    assert_eq!(tree.nodes[0].child_count, 0);
    assert!(!tree.truncated);

    // Asking for continuity walks the same graph and reaches the child.
    let rolled_up = session_tree(
        &conn,
        "claude",
        ORIGINAL,
        &SessionTreeOptions {
            relationship_kinds: RelationshipKinds::only(CONTINUITY_RELATIONSHIPS.iter().copied()),
            ..SessionTreeOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        rolled_up
            .nodes
            .iter()
            .map(|node| node.session_id.as_str())
            .collect::<Vec<_>>(),
        vec![ORIGINAL, CROSS]
    );
    assert_eq!(
        rolled_up.nodes[1]
            .relationship
            .as_ref()
            .map(|edge| edge.relationship.as_str()),
        Some(RELATIONSHIP_CONTINUATION)
    );

    // And a second sync changes nothing it already recorded.
    let before = continuity_rows(&db);
    assert!(!before.is_empty());
    drop(conn);
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    assert_eq!(continuity_rows(&db), before);
}
