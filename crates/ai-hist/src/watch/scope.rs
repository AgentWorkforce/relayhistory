//! Which watched roots the changes behind a tick fell under.
//!
//! The loop already decides, for every filesystem event, which [`WatchRoot`]
//! it belongs to — that is how it drops the events no root asked for. Keeping
//! the answer, rather than collapsing it to "something changed", lets the
//! sweep read only the providers whose roots fired: a Claude session that
//! appends every couple of seconds no longer re-walks every other provider,
//! and every trajectory root, each time.
//!
//! A scope only ever widens. Everything that fired inside one debounce window
//! is one tick, so its scope is the union of what fired; a forced tick that
//! is deferred behind a running sweep, or owed after the store's lock turned
//! it away, carries the scope it stood for into the tick that pays it; and
//! anything the loop cannot place — a rescan notice, a pathless event, a
//! host's own [`super::WatchLoop::notify_change`] — is [`ChangeScope::Everything`].
//!
//! `paths` is the files the events named, rewritten onto each root's own
//! spelling. Empty means the roots fired but the files could not be placed,
//! and the sweep walks those providers whole.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;

use super::{TickOutcome, TickTrigger};

/// What a tick's changes covered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ChangeScope {
    /// Anything may have changed: a tick no event drove (startup, poll,
    /// manual), or one the loop could not place under a root.
    #[default]
    Everything,
    /// Only these roots fired, each named by its [`WatchRoot::path`]. `roots`
    /// is never empty. `paths` is empty when a change could not be placed
    /// under a file: the providers of `roots` are then walked whole.
    ///
    /// [`WatchRoot::path`]: crate::discover::WatchRoot::path
    Roots {
        roots: BTreeSet<PathBuf>,
        paths: BTreeSet<PathBuf>,
    },
}

impl ChangeScope {
    /// Both scopes at once: the union of their roots, or everything.
    ///
    /// Path detail survives only when both sides still have it. One side
    /// that lost track of its files widens the tick to those roots' whole
    /// trees, which is the direction a scope is allowed to move.
    pub fn union(self, other: ChangeScope) -> ChangeScope {
        match (self, other) {
            (
                ChangeScope::Roots {
                    mut roots,
                    mut paths,
                },
                ChangeScope::Roots {
                    roots: theirs,
                    paths: their_paths,
                },
            ) => {
                roots.extend(theirs);
                if paths.is_empty() || their_paths.is_empty() {
                    paths.clear();
                } else {
                    paths.extend(their_paths);
                }
                ChangeScope::Roots { roots, paths }
            }
            _ => ChangeScope::Everything,
        }
    }
}

/// Fold `scope` into what is already held, if anything is.
pub(super) fn widen(held: Option<ChangeScope>, scope: ChangeScope) -> ChangeScope {
    match held {
        Some(held) => held.union(scope),
        None => scope,
    }
}

/// One tick, as handed to a [`ScopedTickFn`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TickRequest {
    pub trigger: TickTrigger,
    /// Bypass the stat-only fingerprint; set for filesystem-event ticks.
    pub force: bool,
    /// What the changes behind this tick covered. Always
    /// [`ChangeScope::Everything`] for an unforced tick.
    pub scope: ChangeScope,
}

/// A sweep that is told what woke it, so it can read only what changed. See
/// [`super::WatchLoop::scoped`].
pub type ScopedTickFn = Arc<dyn Fn(&TickRequest) -> Result<TickOutcome> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    fn roots(paths: &[&str]) -> ChangeScope {
        ChangeScope::Roots {
            roots: paths.iter().map(PathBuf::from).collect(),
            paths: BTreeSet::new(),
        }
    }

    fn rooted(roots: &[&str], paths: &[&str]) -> ChangeScope {
        ChangeScope::Roots {
            roots: roots.iter().map(PathBuf::from).collect(),
            paths: paths.iter().map(PathBuf::from).collect(),
        }
    }

    #[test]
    fn a_scope_only_widens() {
        assert_eq!(roots(&["/a"]).union(roots(&["/b"])), roots(&["/a", "/b"]));
        assert_eq!(
            roots(&["/a"]).union(ChangeScope::Everything),
            ChangeScope::Everything
        );
        assert_eq!(
            ChangeScope::Everything.union(roots(&["/a"])),
            ChangeScope::Everything
        );
        assert_eq!(widen(None, roots(&["/a"])), roots(&["/a"]));
        assert_eq!(
            widen(Some(roots(&["/a"])), roots(&["/a"])),
            roots(&["/a"])
        );
    }

    #[test]
    fn path_detail_survives_a_union_until_one_side_loses_it() {
        assert_eq!(
            rooted(&["/a"], &["/a/x"]).union(rooted(&["/a"], &["/a/y"])),
            rooted(&["/a"], &["/a/x", "/a/y"])
        );
        assert_eq!(
            rooted(&["/a"], &["/a/x"]).union(rooted(&["/b"], &[])),
            rooted(&["/a", "/b"], &[])
        );
    }
}
