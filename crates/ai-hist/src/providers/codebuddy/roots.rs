use super::*;

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let value = use_env
        .then(|| std::env::var_os("CODEBUDDY_CONFIG_DIR"))
        .flatten();
    let root = from_value(home, value.as_deref(), Path::new(""));
    if root.is_absolute() {
        return vec![root.join("projects")].into();
    }
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(error) => {
            return ResolvedProviderRoots {
                error: Some(format!("CODEBUDDY_INVALID_ROOT: {error}")),
                ..Default::default()
            }
        }
    };
    let root = from_value(home, value.as_deref(), &cwd);
    vec![root.join("projects")].into()
}
pub(super) fn from_value(home: &Path, value: Option<&std::ffi::OsStr>, cwd: &Path) -> PathBuf {
    let root = value
        .filter(|s| !s.to_string_lossy().trim().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codebuddy"));
    if root.is_absolute() {
        root
    } else {
        cwd.join(root)
    }
}
pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|r| path.starts_with(r))
        .max_by_key(|r| r.components().count())
        .context("CODEBUDDY_INVALID_PATH: outside projects roots")?;
    let relative = path.strip_prefix(root)?;
    let parts: Vec<_> = relative.components().collect();
    anyhow::ensure!(
        parts.len() == 2
            && parts
                .iter()
                .all(|p| matches!(p, std::path::Component::Normal(_))),
        "CODEBUDDY_INVALID_PATH: expected project/session.jsonl"
    );
    anyhow::ensure!(
        path.extension().is_some_and(|x| x == "jsonl"),
        "CODEBUDDY_INVALID_PATH: not JSONL"
    );
    let native = path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .context("CODEBUDDY_INVALID_PATH: invalid session ID")?;
    let namespace = root.strip_prefix(&roots.home).unwrap_or(root);
    let key = serde_json::to_vec(&(
        namespace.to_string_lossy(),
        parts[0].as_os_str().to_string_lossy(),
    ))?;
    Ok(format!("project:{:x}:{native}", Sha256::digest(key)))
}
