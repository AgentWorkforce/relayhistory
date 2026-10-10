//! Where trajectory records live, and how they are found.
//!
//! A machine that names no `TRAJECTORY_ROOT` has its trajectory roots derived
//! by walking `<home>/Projects` for `.trajectories` directories. That walk is
//! the most expensive stat-only work a sweep does: on a developer's disk it is
//! tens of thousands of directories even after pruning, about two seconds
//! warm, and it used to run twice per sweep — once for the source fingerprint
//! and once for the trajectory phase — on every filesystem-event tick of a
//! live watch. A sweep now enumerates once ([`trajectory_files`] is called by
//! the sweep and its result shared), the walk stops at
//! [`TRAJECTORY_SCAN_DEPTH`], and a watch keeps the roots between its full
//! sweeps (see `SessionStore::watch`).

use super::*;

/// How deep below `<home>/Projects` a derived `.trajectories` directory may
/// sit: the directory holding it is at most this many levels down.
///
/// A trajectory store lives at the root of a project, or of a package inside
/// a monorepo — `Projects/<repo>/.trajectories`,
/// `Projects/<group>/<repo>/packages/<pkg>/.trajectories` — and every level
/// below that is a source tree the walk would read for nothing. Measured on a
/// developer's `~/Projects`, the deepest store was four levels down and the
/// walk read 33,363 directories unbounded against 5,825 at this bound. A
/// store deeper than this is named through `TRAJECTORY_ROOT`, which is read
/// as given and never walked.
pub(crate) const TRAJECTORY_SCAN_DEPTH: usize = 5;

#[cfg(test)]
thread_local! {
    /// `<home>/Projects` walks made on this thread, so a test can say how
    /// many a sweep or a watch tick paid for.
    static TRAJECTORY_WALKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many `<home>/Projects` walks this thread has made so far.
#[cfg(test)]
pub(crate) fn trajectory_walks() -> usize {
    TRAJECTORY_WALKS.with(std::cell::Cell::get)
}

/// The trajectory roots these provider roots name, whether or not anything
/// exists inside them yet: the explicit list when one was given
/// (`TRAJECTORY_ROOT`, read once when the roots were built), otherwise every
/// `.trajectories` directory under `<home>/Projects` as of now, to
/// [`TRAJECTORY_SCAN_DEPTH`].
///
/// Split out from [`trajectory_files`] because the watcher and the file walk
/// need different answers. A root that is empty, or that does not exist at
/// all, contributes no files — but it is exactly what has to be watched, so
/// the first trajectory written into it wakes live capture instead of waiting
/// for the backstop.
///
/// The environment is not consulted here: an embedder that built its roots
/// without it must not have a host's `TRAJECTORY_ROOT` redirect its sweep and
/// its watcher outside the home it named.
pub(crate) fn trajectory_roots(provider_roots: &crate::ProviderRoots) -> Result<Vec<PathBuf>> {
    let mut roots = match &provider_roots.trajectory_roots {
        Some(explicit) => explicit.clone(),
        None => {
            #[cfg(test)]
            TRAJECTORY_WALKS.with(|walks| walks.set(walks.get() + 1));
            let mut derived = Vec::new();
            let projects = provider_roots.home.join("Projects");
            if projects.exists() {
                collect_named_dirs(
                    &projects,
                    ".trajectories",
                    TRAJECTORY_SCAN_DEPTH,
                    &mut derived,
                )?;
            }
            derived
        }
    };
    roots.sort();
    roots.dedup();
    Ok(roots)
}

/// Every trajectory record file under the roots, sorted. One sweep calls this
/// once and hands the list to both the source fingerprint and the trajectory
/// phase, so the two cannot disagree and the walk is not paid twice.
pub(super) fn trajectory_files(provider_roots: &crate::ProviderRoots) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for root in trajectory_roots(provider_roots)? {
        check_capture_cancelled()?;
        if root.is_file() && root.extension().and_then(|s| s.to_str()) == Some("json") {
            files.push(root);
            continue;
        }
        if !root.exists() {
            continue;
        }
        // Recursively collect every trajectory JSON under the `.trajectories` root.
        // The parser decides whether each file is a per-run trajectory or compacted roll-up.
        collect_trajectory_json(&root, &mut files)?;
    }
    files.sort();
    files.dedup();
    Ok(files)
}

/// Directory names never worth descending into when looking for a project's
/// `.trajectories`, and expensive enough to matter: a dependency tree or an
/// object store can be most of the files on the disk.
const SKIP_PROJECT_SCAN_DIRS: &[&str] = &["node_modules", "target", "vendor"];

/// Find directories named `name` under `root`, reading directories at most
/// `depth` levels below it.
///
/// The pruning is load-bearing, not a micro-optimisation. An unpruned walk of
/// `~/Projects` means walking every dependency tree and every `.git` object
/// store on the machine before deciding that nothing has changed — which is
/// the opposite of what a fast path is for.
///
/// Pruned: the names above, any hidden directory that is not the one being
/// looked for, and everything deeper than `depth`. A match is not descended
/// into either; nothing nests a `.trajectories` inside another one.
pub(super) fn collect_named_dirs(
    root: &Path,
    name: &str,
    depth: usize,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        check_capture_cancelled()?;
        let entry = entry?;
        // Never follow symlinks: dependency links can revisit the same tree or cycle.
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        let Some(entry_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if entry_name == name {
            out.push(path);
            continue;
        }
        if depth == 0 || skipped_project_dir(entry_name) {
            continue;
        }
        collect_named_dirs(&path, name, depth - 1, out)?;
    }
    Ok(())
}

/// Whether the project walk leaves a directory of this name unread.
fn skipped_project_dir(entry_name: &str) -> bool {
    entry_name.starts_with('.')
        || SKIP_PROJECT_SCAN_DIRS.contains(&entry_name)
        || matches!(
            entry_name,
            ".next" | ".venv" | "venv" | "__pycache__" | ".cache"
        )
}

/// Recursively collect trajectory JSON under a `.trajectories` root: `completed/<month>/`
/// individual runs, `compacted/` roll-ups, `active/`. Skips index/state/trace sidecars;
/// `parse_trajectory_file` decides per-file what's mappable.
pub(super) fn collect_trajectory_json(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        check_capture_cancelled()?;
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_trajectory_json(&path, out)?;
        } else if file_type.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if name != "index.json" && name != ".sync-state.json" && !name.ends_with(".trace.json")
            {
                out.push(path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_derived_walk_stops_at_the_scan_depth() {
        let home = tempfile::tempdir().unwrap();
        let projects = home.path().join("Projects");
        let mut expected = Vec::new();
        // `Projects/l1/.../l<n>/.trajectories`: the holder is `n` levels down.
        for levels in 1..=TRAJECTORY_SCAN_DEPTH + 2 {
            let holder = (1..=levels).fold(projects.join(format!("d{levels}")), |path, level| {
                if level == 1 {
                    path
                } else {
                    path.join(format!("l{level}"))
                }
            });
            let store = holder.join(".trajectories");
            fs::create_dir_all(&store).unwrap();
            if levels <= TRAJECTORY_SCAN_DEPTH {
                expected.push(store);
            }
        }
        expected.sort();
        let roots = trajectory_roots(&crate::ProviderRoots::from_home(
            home.path().to_path_buf(),
            home.path().join("opencode.db"),
        ))
        .unwrap();
        assert_eq!(roots, expected);
    }

    #[test]
    fn an_explicit_root_is_never_walked_or_bounded() {
        let home = tempfile::tempdir().unwrap();
        let deep = home.path().join("a/b/c/d/e/f/g/.trajectories");
        let mut roots = crate::ProviderRoots::from_home(
            home.path().to_path_buf(),
            home.path().join("opencode.db"),
        );
        roots.trajectory_roots = Some(vec![deep.clone()]);
        let walks = trajectory_walks();
        assert_eq!(trajectory_roots(&roots).unwrap(), vec![deep]);
        assert_eq!(trajectory_walks(), walks, "nothing was walked");
    }
}
