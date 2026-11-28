use super::*;

pub(super) const SUMMARY_DB: &str = "conversation_summaries.db";

pub(super) fn resolve(home: &Path, _: bool) -> ResolvedProviderRoots {
    ["antigravity", "antigravity-cli", "antigravity-ide"]
        .into_iter()
        .map(|variant| home.join(".gemini").join(variant).join("brain"))
        .collect::<Vec<_>>()
        .into()
}

pub(super) fn summary_dir_for_brain(brain_root: &Path) -> Option<PathBuf> {
    (brain_root.file_name() == Some(std::ffi::OsStr::new("brain")))
        .then(|| brain_root.parent().map(Path::to_path_buf))
        .flatten()
}

pub(super) fn summary_db_for_brain(brain_root: &Path) -> Option<PathBuf> {
    summary_dir_for_brain(brain_root).map(|dir| dir.join(SUMMARY_DB))
}

pub(super) fn summary_db_paths(brain_root: &Path) -> Vec<PathBuf> {
    let Some(db) = summary_db_for_brain(brain_root) else {
        return Vec::new();
    };
    ["", "-wal", "-shm", "-journal"]
        .into_iter()
        .map(|suffix| {
            let mut path = db.as_os_str().to_os_string();
            path.push(suffix);
            PathBuf::from(path)
        })
        .collect()
}

pub(super) fn transcript_roots(roots: &ProviderRoots) -> Vec<PathBuf> {
    let mut paths = roots.for_provider(ID).to_vec();
    for brain in roots.for_provider(ID) {
        paths.extend(summary_db_paths(brain));
    }
    paths
}

pub(super) fn locate<'a>(roots: &'a ProviderRoots, path: &Path) -> Result<(&'a Path, String)> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|r| path.starts_with(r))
        .max_by_key(|r| r.components().count())
        .context("ANTIGRAVITY_INVALID_PATH: outside brain roots")?;
    let relative = path.strip_prefix(root)?;
    let parts: Vec<_> = relative.components().collect();
    anyhow::ensure!(
        parts.len() == 4
            && parts
                .iter()
                .all(|p| matches!(p, std::path::Component::Normal(_))),
        "ANTIGRAVITY_INVALID_PATH: expected conversation/.system_generated/logs/transcript JSONL"
    );
    anyhow::ensure!(
        parts[1].as_os_str() == ".system_generated" && parts[2].as_os_str() == "logs",
        "ANTIGRAVITY_INVALID_PATH: invalid transcript directory"
    );
    let native = parts[0]
        .as_os_str()
        .to_str()
        .context("ANTIGRAVITY_INVALID_PATH: non-UTF8 conversation ID")?;
    anyhow::ensure!(
        !native.is_empty(),
        "ANTIGRAVITY_INVALID_PATH: empty conversation ID"
    );
    Ok((root.as_path(), native.to_owned()))
}

pub(super) fn identity_for_native(
    roots: &ProviderRoots,
    root: &Path,
    native: &str,
) -> Result<String> {
    // A root namespace prevents App, CLI, IDE and explicitly supplied roots
    // from collapsing distinct conversations that happen to reuse an ID.
    Ok(format!(
        "root:{:x}:{native}",
        Sha256::digest(
            root.strip_prefix(&roots.home)
                .unwrap_or(root)
                .to_str()
                .context("ANTIGRAVITY_INVALID_PATH: non-UTF8 root")?
                .replace('\\', "/")
                .as_bytes()
        )
    ))
}

pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let (root, native) = locate(roots, path)?;
    identity_for_native(roots, root, &native)
}
