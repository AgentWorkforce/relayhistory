//! A resumed or forked Claude conversation is a new transcript, named after
//! the new session, that opens with a copy of its origin's records. The file
//! stem owns the transcript; the copied records are inherited history.

use ai_hist::{
    DiscoveryOptions, ProviderRoots, SessionQuery, SessionRef, SessionStore, Source, StoreOptions,
};
use std::fs;
use std::path::Path;

const PARENT: &str = "aaaaaaaa-0000-4000-8000-00000000000a";
const CHILD: &str = "cccccccc-0000-4000-8000-00000000000c";
/// 2026-11-02T09:00:00.000Z, the child's first own record.
const CHILD_STARTED: i64 = 1_793_610_000_000;

fn stage(home: &Path, names: &[&str]) {
    let project = home.join(".claude/projects/-tmp-project");
    fs::create_dir_all(&project).unwrap();
    for name in names {
        let from = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/claude")
            .join(name);
        fs::copy(from, project.join(name)).unwrap();
    }
}

fn store(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("ai-history.db"))
            .roots(ProviderRoots::from_home(
                home.to_path_buf(),
                home.join(".local/share/opencode/opencode.db"),
            )),
    )
    .expect("open")
}

fn evidence(store: &SessionStore, id: &str) -> ai_hist::SessionEvidence {
    store
        .session(&SessionRef::id(Source::Claude, id), SessionQuery::default())
        .unwrap()
        .unwrap_or_else(|| panic!("{id} was not captured"))
}

fn message_ids(evidence: &ai_hist::SessionEvidence) -> Vec<String> {
    let mut ids: Vec<String> = evidence
        .messages
        .iter()
        .filter_map(|message| message.message_id.clone())
        .collect();
    ids.sort();
    ids
}

#[test]
fn the_file_stem_owns_a_forked_transcript_and_inherits_its_origin() {
    let dir = tempfile::tempdir().unwrap();
    stage(
        dir.path(),
        &[&format!("{PARENT}.jsonl"), &format!("{CHILD}.jsonl")],
    );
    let store = store(dir.path());
    store
        .sync(Default::default())
        .expect("a forked transcript syncs");

    let child = evidence(&store, CHILD);
    assert_eq!(child.session.first_activity_ms, Some(CHILD_STARTED));
    assert_eq!(
        child.session.first_prompt.as_deref(),
        Some("child first prompt")
    );
    assert_eq!(child.session.title.as_deref(), Some("forked work"));
    assert_eq!(
        child.session.raw_path.as_deref().and_then(Path::file_stem),
        Some(std::ffi::OsStr::new(CHILD))
    );
    assert_eq!(message_ids(&child), ["c-a1", "c-u1"]);
    let fork = child
        .relationships
        .iter()
        .find(|edge| edge.relationship == "fork")
        .expect("the inherited session is the fork's parent");
    assert_eq!(fork.parent_session_id, PARENT);
    assert_eq!(fork.child_session_id.as_deref(), Some(CHILD));
    assert_eq!(fork.evidence_ref.as_deref(), Some("inheritedSessionId"));
    assert_eq!(fork.spawned_at_ms, Some(CHILD_STARTED));

    // The inherited records stay the origin's, which its own file also holds.
    let parent = evidence(&store, PARENT);
    assert_eq!(message_ids(&parent), ["p-a1", "p-u1", "p-u2"]);
    assert_eq!(
        parent.session.raw_path.as_deref().and_then(Path::file_stem),
        Some(std::ffi::OsStr::new(PARENT))
    );

    let again = store.sync(Default::default()).expect("resync");
    assert!(again.changed.is_empty(), "{:?}", again.changed);
}

#[test]
fn shallow_discovery_catalogs_a_forked_transcript_under_its_stem() {
    let dir = tempfile::tempdir().unwrap();
    stage(dir.path(), &[&format!("{CHILD}.jsonl")]);
    let store = store(dir.path());
    store
        .discover(DiscoveryOptions::default())
        .expect("a forked transcript is discoverable");
    let rows: Vec<_> = store
        .sessions(Default::default())
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].session_id, CHILD);
    assert_eq!(rows[0].first_activity_ms, Some(CHILD_STARTED));
    assert_eq!(rows[0].title.as_deref(), Some("forked work"));
}
