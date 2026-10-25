//! Translate native event spellings back to the configured provider roots.
use super::{ProviderRoots, Source};
use crate::ingest::path_scope::PathScope;
use std::path::{Component, Path, PathBuf};

pub(super) fn scope(
    roots: &ProviderRoots,
    paths: Option<&[PathBuf]>,
    sources: Option<&[Source]>,
) -> Option<PathScope> {
    let paths = paths?;
    let roots: Vec<_> = sources
        .unwrap_or(Source::ALL)
        .iter()
        .flat_map(|source| source.capabilities().watch_roots(roots))
        .map(|root| (resolved(&root.path), root.path))
        .collect();
    PathScope::from_paths(paths.iter().map(|path| {
        let path = resolved(path);
        roots
            .iter()
            .find_map(|(resolved, configured)| {
                path.strip_prefix(resolved)
                    .ok()
                    .map(|suffix| configured.join(suffix))
            })
            .unwrap_or(path)
    }))
}

pub(super) fn selected_sources(
    roots: &ProviderRoots,
    paths: Option<&[PathBuf]>,
    sources: Option<Vec<Source>>,
) -> Option<Vec<Source>> {
    if sources.is_some() {
        return sources;
    }
    let paths = paths?;
    if paths.is_empty() {
        return Some(Vec::new());
    }
    let candidates: Vec<_> = Source::ALL
        .iter()
        .copied()
        .map(|source| {
            let roots = source
                .capabilities()
                .watch_roots(roots)
                .into_iter()
                .map(|root| {
                    let base = resolved(&root.path);
                    (root, base)
                })
                .collect::<Vec<_>>();
            (source, roots)
        })
        .collect();
    let mut selected = std::collections::BTreeSet::new();
    for path in paths {
        let path = resolved(path);
        let owners: Vec<_> = candidates
            .iter()
            .filter(|(_, roots)| {
                roots.iter().any(|(root, base)| {
                    if base.starts_with(&path) {
                        return true;
                    }
                    match root.scope {
                        crate::WatchScope::File => &path == base,
                        crate::WatchScope::Directory => {
                            path.parent() == Some(base.as_path())
                                && path.file_name().is_some_and(|name| root.admits(name))
                        }
                        _ => path.starts_with(base),
                    }
                })
            })
            .map(|(source, _)| *source)
            .collect();
        // Watch roots describe currently active inputs, not every legacy or
        // deleted profile locator. Unknown ownership must retain the prior
        // unrestricted source selection rather than silently skip cleanup.
        if owners.is_empty() {
            return None;
        }
        selected.extend(owners);
    }
    Some(selected.into_iter().collect())
}

/// Deleted/renamed paths still resolve their existing ancestor. Resolve links
/// before restoring absent suffixes so /var and /private/var stay one identity.
fn resolved(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            component => normalized.push(component.as_os_str()),
        }
    }
    let mut parent = normalized.as_path();
    let mut tail = Vec::new();
    loop {
        if let Ok(mut resolved) = parent.canonicalize() {
            for name in tail.iter().rev() {
                resolved.push(name);
            }
            return resolved;
        }
        match (parent.file_name(), parent.parent()) {
            (Some(name), Some(ancestor)) => {
                tail.push(name);
                parent = ancestor;
            }
            _ => return normalized,
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn native_aliases_and_deleted_paths_use_the_configured_root() {
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        std::fs::create_dir_all(real.join(".claude/projects/p")).unwrap();
        let link = temp.path().join("alias");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let roots = ProviderRoots::from_home(link.clone(), link.join("opencode.db"));
        let source = [Source::Claude];
        let file = real.join(".claude/projects/p/deleted.jsonl");
        let scope = scope(&roots, Some(&[file]), Some(&source)).unwrap();
        assert_eq!(
            scope.paths(),
            &[link.join(".claude/projects/p/deleted.jsonl")]
        );
        let outside = real.join(".claude/projects/../../private.jsonl");
        let scope = super::scope(&roots, Some(&[outside]), Some(&source)).unwrap();
        assert!(!scope.paths()[0].starts_with(roots.claude.join("projects")));
    }
}
