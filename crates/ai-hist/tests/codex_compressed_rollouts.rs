//! Codex rollouts Codex has compressed to `rollout-….jsonl.zst`, through the
//! public `SessionStore` surface: discovery, sync, the catalog, `session`,
//! hydration and the change feed treat them as the rollout they were, under
//! the same identity before and after compression.

use ai_hist::{
    ChangeKind, ChangeQuery, DiscoveryOptions, HydrateOptions, HydrateStatus, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, Watermark,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

/// The environment is process-wide, so the tests in this binary take turns.
static ENV_LOCK: Mutex<()> = Mutex::new(());

const SESSION: &str = "sess_multi_1";

/// An isolated provider home with every provider override pinned under it.
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
        std::env::set_var("XDG_DATA_HOME", home.join(".local/share"));
        std::env::set_var("XDG_CONFIG_HOME", home.join(".config"));
        std::env::remove_var("AI_HIST_DB");
        Self { dir, _env: env }
    }

    fn store(&self) -> SessionStore {
        self.store_named("ai-history.db")
    }

    /// A second store over the same provider files.
    fn store_named(&self, db: &str) -> SessionStore {
        let mut options = StoreOptions::default();
        options.db_path = Some(self.dir.path().join(db));
        options.home = Some(self.dir.path().to_path_buf());
        SessionStore::open(options).unwrap()
    }

    /// The `multi-turn` fixture as a plain rollout, at its locator.
    fn stage_rollout(&self) -> PathBuf {
        let day = self.dir.path().join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-2026-04-20T00-00-00-multi-turn.jsonl");
        fs::copy(fixture("multi-turn.jsonl"), &rollout).unwrap();
        rollout
    }
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/codex")
        .join(name)
}

/// Replace a rollout with its compressed copy, as Codex does: same records,
/// same mtime, `.jsonl.zst` beside the name it replaces.
fn compress(rollout: &Path) -> PathBuf {
    let modified = fs::metadata(rollout).unwrap().modified().unwrap();
    let mut name = rollout.as_os_str().to_os_string();
    name.push(".zst");
    let compressed = PathBuf::from(name);
    let bytes = zstd::bulk::compress(&fs::read(rollout).unwrap(), 3).unwrap();
    fs::write(&compressed, bytes).unwrap();
    fs::File::options()
        .write(true)
        .open(&compressed)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    fs::remove_file(rollout).unwrap();
    compressed
}

fn session_ref() -> SessionRef {
    SessionRef::id(Source::Codex, SESSION)
}

/// What a consumer reads back for the session: its catalog locator and the
/// counts of each kind of evidence.
fn evidence(store: &SessionStore) -> (Option<PathBuf>, usize, usize, usize) {
    let evidence = store
        .session(&session_ref(), SessionQuery::default())
        .unwrap()
        .expect("the session is catalogued");
    (
        evidence.session.raw_path,
        evidence.prompts.len(),
        evidence.messages.len(),
        evidence.requests.len(),
    )
}

/// A rollout that was already compressed when the store first looked is
/// discovered, synced and hydrated like any other, under its plain name, and
/// yields the evidence the plain rollout does.
#[test]
fn a_compressed_rollout_is_first_class_evidence() {
    let home = Home::new();
    let rollout = home.stage_rollout();
    let plain = home.store_named("plain.db");
    plain.sync(Default::default()).unwrap();
    let expected = evidence(&plain);
    assert!(expected.2 > 0 && expected.3 > 0, "{expected:?}");
    compress(&rollout);
    let store = home.store();

    let discovery = store.discover(DiscoveryOptions::default()).unwrap();
    assert!(
        discovery.diagnostics.is_empty(),
        "{:?}",
        discovery.diagnostics
    );
    assert_eq!(discovery.discovered, 1);

    store.sync(Default::default()).unwrap();
    assert_eq!(evidence(&store), expected);
    assert_eq!(expected.0.as_deref(), Some(rollout.as_path()));

    let feed: Vec<_> = store
        .changes_since(Watermark::START, ChangeQuery::default())
        .unwrap()
        .map(|change| change.unwrap())
        .filter(|change| change.session_id == SESSION && change.kind == ChangeKind::SessionEvent)
        .collect();
    assert_eq!(
        feed.len(),
        expected.2,
        "the session's events reach the feed"
    );

    let report = store
        .hydrate(&session_ref(), HydrateOptions::default())
        .unwrap();
    assert!(
        matches!(
            report.status,
            HydrateStatus::Hydrated | HydrateStatus::Updated | HydrateStatus::Unchanged
        ),
        "{:?}",
        report.status
    );
    assert_eq!(evidence(&store), expected, "hydration adds no rows");
}

/// Compressing a rollout after it was indexed changes nothing a consumer
/// can see: the same session under the same locator, the same evidence, no
/// change in the feed, and hydration still finds its source.
#[test]
fn a_rollout_compressed_after_ingest_keeps_its_identity() {
    let home = Home::new();
    let rollout = home.stage_rollout();
    let store = home.store();
    store.sync(Default::default()).unwrap();
    store
        .hydrate(&session_ref(), HydrateOptions::default())
        .unwrap();
    let before = evidence(&store);
    let head = store.head_revision().unwrap();

    compress(&rollout);
    store.sync(Default::default()).unwrap();
    let discovery = store.discover(DiscoveryOptions::default()).unwrap();
    assert!(
        discovery.diagnostics.is_empty(),
        "{:?}",
        discovery.diagnostics
    );
    let report = store
        .hydrate(&session_ref(), HydrateOptions::default())
        .unwrap();
    assert!(
        matches!(
            report.status,
            HydrateStatus::Updated | HydrateStatus::Unchanged
        ),
        "{:?}",
        report.status
    );

    assert_eq!(evidence(&store), before);
    assert_eq!(before.0.as_deref(), Some(rollout.as_path()));
    // The catalog row and the presence carry the source's change stamp,
    // which a compressed copy really does move. No evidence moves with it.
    let evidence_changes: Vec<_> = store
        .changes_since(head, ChangeQuery::default())
        .unwrap()
        .map(|change| change.unwrap())
        .filter(|change| {
            !matches!(
                change.kind,
                ChangeKind::Session | ChangeKind::Presence | ChangeKind::SourceObservation
            )
        })
        .collect();
    assert!(
        evidence_changes.is_empty(),
        "compression is not new evidence, yet it was written again: {evidence_changes:?}"
    );
    let codex_sessions = store
        .sessions(Default::default())
        .map(|session| session.unwrap())
        .filter(|session| session.source == Source::Codex)
        .count();
    assert_eq!(codex_sessions, 1);
}
