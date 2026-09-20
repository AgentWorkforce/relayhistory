//! Local coding-agent session history.
//!
//! The default public surface is [`SessionStore`] plus the evidence types an
//! embedder reads back. Workspace crates enable `unstable-internal` for the
//! maintenance APIs that still take a raw database connection.

macro_rules! workspace_mod {
    ($name:ident) => {
        #[cfg(feature = "unstable-internal")]
        pub mod $name;
        #[cfg(not(feature = "unstable-internal"))]
        mod $name;
    };
}

/// Canonical project identity shared with burn. Always public: an embedder
/// that groups by project needs the same rules the ingest path stamps with,
/// and a second implementation is exactly the drift this module exists to
/// prevent.
pub mod project_identity;
mod store;
workspace_mod!(storage);
workspace_mod!(observations);
workspace_mod!(privacy);
workspace_mod!(source_evidence);
workspace_mod!(relationship_graph);
mod ingest;
mod relationship_capture;
workspace_mod!(diagnostics);
workspace_mod!(history_search);
workspace_mod!(paths);
workspace_mod!(discover);
workspace_mod!(remote);
workspace_mod!(source_intake);
workspace_mod!(sources);
workspace_mod!(watch);
mod file_lock;
mod jsonl_temp;
mod session_store;

#[cfg(all(feature = "delivery", feature = "unstable-internal"))]
pub mod delivery;
#[cfg(all(feature = "delivery", not(feature = "unstable-internal")))]
pub(crate) mod delivery;

#[cfg(all(feature = "git-hooks", feature = "unstable-internal"))]
pub mod git_helpers;
#[cfg(all(feature = "git-hooks", not(feature = "unstable-internal")))]
mod git_helpers;
#[cfg(all(feature = "git-hooks", feature = "unstable-internal"))]
pub mod git_sdk;
#[cfg(all(feature = "git-hooks", not(feature = "unstable-internal")))]
mod git_sdk;

pub(crate) use paths::{default_opencode_db_path, home_dir};
pub(crate) use relationship_capture::now_ms;
#[cfg(feature = "unstable-internal")]
pub use relationship_graph as relationships;
#[cfg(not(feature = "unstable-internal"))]
pub(crate) use relationship_graph as relationships;

#[cfg(feature = "unstable-internal")]
pub use ingest::*;
#[cfg(not(feature = "unstable-internal"))]
pub(crate) use ingest::*;

#[cfg(feature = "unstable-internal")]
pub use store::*;
#[cfg(not(feature = "unstable-internal"))]
pub(crate) use store::*;

pub use session_store::{
    Error, SessionRef, SessionStore, Source, StoreOptions, SyncOptions, SyncReport,
};
pub use source_evidence::{EvidenceKind, EvidenceRecord, FULL_SESSION_KINDS};

#[cfg(not(feature = "unstable-internal"))]
pub use store::{
    HistoryEntry, SessionEvent, SessionFileEdit, SessionLocation, SessionScope, SessionToolCall,
};

#[cfg(feature = "unstable-internal")]
#[doc(hidden)]
pub mod internal {
    pub use crate::ingest::*;
    pub use crate::relationship_capture::{record_relationship, ObservedRelationship};
    pub use crate::store::*;
}
