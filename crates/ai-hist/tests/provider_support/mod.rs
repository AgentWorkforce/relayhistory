//! Shared acceptance checks for provider-owned integration tests.
//!
//! Each provider stages its own versioned fixture under an isolated home,
//! then calls `assert_history_conformance`. No central case list is needed.
//! Only the optional export check opens a raw connection, for the crate-owned
//! export API; acquisition and evidence reads use the default public facade.

use ai_hist::{
    CatalogQuery, ChangeKind, ChangeQuery, DiscoveryOptions, HydrateOptions, HydrateStatus,
    ProviderRoots, Role, SessionEvidence, SessionQuery, SessionRef, SessionStore, Source,
    StoreOptions, SyncOptions, Watermark,
};
use std::collections::BTreeSet;
use std::path::Path;

#[cfg(feature = "export")]
mod export;

pub struct HistoryCase<'a> {
    pub source: Source,
    pub session_id: &'a str,
    /// At least one real user and assistant text, in their native roles.
    pub messages: &'a [(Role, &'a str)],
}

fn open(home: &Path) -> SessionStore {
    // `from_home` deliberately ignores the operator's environment overrides.
    // Using StoreOptions::home alone can accidentally read private history.
    let roots = ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    );
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("conformance.db"))
            .roots(roots),
    )
    .expect("open isolated conformance store")
}

fn read(store: &SessionStore, case: &HistoryCase<'_>) -> SessionEvidence {
    let reference = SessionRef::id(case.source, case.session_id);
    let evidence = store
        .session(&reference, SessionQuery::default())
        .expect("read public evidence")
        .expect("fixture session is present");
    assert_eq!(evidence.session.source, case.source);
    assert_eq!(evidence.session.session_id, case.session_id);
    assert!(case.messages.iter().any(|(role, _)| *role == Role::User));
    assert!(case
        .messages
        .iter()
        .any(|(role, _)| *role == Role::Assistant));
    for (role, text) in case.messages {
        assert!(!text.is_empty(), "an empty string proves no acquisition");
        assert!(
            evidence.messages.iter().any(|message| {
                message.role == *role
                    && message
                        .blocks
                        .iter()
                        .any(|block| block.text.as_deref() == Some(text))
            }),
            "{} / {} missing {role:?} text {text:?}",
            case.source,
            case.session_id
        );
    }
    let ids: Vec<_> = evidence
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .map(|block| &block.event_uid)
        .collect();
    assert_eq!(ids.len(), ids.iter().collect::<BTreeSet<_>>().len());
    evidence
}

fn event_changes(store: &SessionStore, from: Watermark, case: &HistoryCase<'_>) -> usize {
    store
        .changes_since(
            from,
            ChangeQuery::default()
                .session(case.source.as_str(), case.session_id)
                .kinds([ChangeKind::SessionEvent])
                .batch(1),
        )
        .expect("open paged change feed")
        .fold(0, |count, change| {
            change.expect("read change feed row");
            count + 1
        })
}

fn discover_and_hydrate(store: &SessionStore, case: &HistoryCase<'_>) {
    store
        .discover(DiscoveryOptions::default().sources([case.source]))
        .expect("discover fixture source");
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![case.source]);
    let matches = store
        .sessions(query)
        .map(|row| row.expect("read catalog row"))
        .filter(|row| row.session_id == case.session_id)
        .count();
    assert_eq!(matches, 1, "discovery registers this native identity once");
    let before = store.head_revision().expect("initial feed watermark");
    let report = store
        .hydrate(
            &SessionRef::id(case.source, case.session_id),
            HydrateOptions::default(),
        )
        .expect("hydrate selected fixture");
    assert_eq!(report.status, HydrateStatus::Hydrated);
    assert!(
        event_changes(store, before, case) > 0,
        "hydration emits evidence"
    );
}

/// Discover, hydrate, read, restart, rehydrate and force a full sweep of one
/// fixture. Assert stable messages and a silent event feed on repeated reads.
/// With `export`, also prove paged export preserves each native event identity.
/// Return the evidence for provider-specific assertions (tools, usage, forks).
pub fn assert_history_conformance(home: &Path, case: HistoryCase<'_>) -> SessionEvidence {
    let store = open(home);
    discover_and_hydrate(&store, &case);
    let original = read(&store, &case);
    let before = store.head_revision().expect("persisted feed watermark");
    drop(store);

    let store = open(home);
    assert_eq!(read(&store, &case).messages, original.messages, "restart");
    let report = store
        .hydrate(
            &SessionRef::id(case.source, case.session_id),
            HydrateOptions::default(),
        )
        .expect("rehydrate unchanged fixture");
    assert_eq!(report.status, HydrateStatus::Unchanged);
    assert_eq!(
        event_changes(&store, before, &case),
        0,
        "unchanged hydration"
    );
    store
        .sync(SyncOptions::default().force(true))
        .expect("full sweep");
    assert_eq!(
        read(&store, &case).messages,
        original.messages,
        "full sync parity"
    );
    let after_sync = store.head_revision().expect("sync feed watermark");
    store
        .sync(SyncOptions::default().force(true))
        .expect("repeat full sweep");
    assert_eq!(
        event_changes(&store, after_sync, &case),
        0,
        "idempotent full sync"
    );
    assert_eq!(
        read(&store, &case).messages,
        original.messages,
        "repeated sync"
    );

    #[cfg(feature = "export")]
    export::assert_export(&store, &case, &original);
    original
}
