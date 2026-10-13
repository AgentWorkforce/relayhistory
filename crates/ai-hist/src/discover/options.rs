//! Options for one discovery run.

use crate::SessionScope;

/// Options for one discovery run.
#[derive(Debug, Clone, Default)]
pub struct DiscoverOptions {
    /// Provider-presence scope. Defaults to local for compatibility.
    pub scope: SessionScope,
    /// Restrict to these sources. Empty means every adapter.
    pub sources: Vec<String>,
    /// Global cap on emitted rows, applied across providers by recency.
    /// `None` means no cap.
    pub limit: Option<usize>,
    /// See [`crate::DiscoveryOptions::skip_protected_folder_identity`].
    pub skip_protected_folder_identity: bool,
}

impl DiscoverOptions {
    /// A run for `scope` and `sources`. `limit: None` reads every row.
    pub fn new(scope: SessionScope, sources: Vec<String>, limit: Option<usize>) -> Self {
        Self {
            scope,
            sources,
            limit,
            ..Self::default()
        }
    }
}
