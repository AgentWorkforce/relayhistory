use super::*;
use sha2::{Digest, Sha256};

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let override_home = use_env.then(|| std::env::var_os("JUNIE_HOME")).flatten();
    let configured = override_home.filter(|value| !value.is_empty());
    let root = configured
        .as_ref()
        .map(|value| PathBuf::from(value.as_os_str()))
        .unwrap_or_else(|| home.join(".junie"));
    let root = if root.is_absolute() {
        root
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(root),
            Err(error) => {
                return ResolvedProviderRoots {
                    error: Some(format!(
                        "JUNIE_INVALID_ROOT: cannot freeze relative JUNIE_HOME: {error}"
                    )),
                    ..Default::default()
                }
            }
        }
    };
    let sessions = root.join("sessions");
    ResolvedProviderRoots {
        paths: vec![sessions.clone()],
        default_root: configured.is_none().then_some(sessions),
        error: None,
    }
}

pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.components().count())
        .context("JUNIE_INVALID_PATH: outside sessions root")?;
    let relative = path.strip_prefix(root)?;
    let parts = relative.components().collect::<Vec<_>>();
    anyhow::ensure!(
        parts.len() == 2
            && parts
                .iter()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            && parts[1].as_os_str() == "events.jsonl",
        "JUNIE_INVALID_PATH: expected <sessionId>/events.jsonl"
    );
    let native = parts[0]
        .as_os_str()
        .to_str()
        .context("JUNIE_INVALID_ID: non-UTF8 session ID")?;
    anyhow::ensure!(
        !native.is_empty() && native != "." && native != "..",
        "JUNIE_INVALID_ID: empty session ID"
    );
    if roots.default_for_provider(ID) == Some(root.as_path()) {
        return Ok(native.into());
    }
    // Explicit roots can contain identical native IDs. Scope their ledger IDs
    // without changing the native identity stored in raw event markers.
    Ok(format!(
        "root:{:x}:{native}",
        Sha256::digest(root.to_string_lossy().as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_and_override_roots_are_frozen() {
        let home = PathBuf::from("/synthetic/home");
        let default = resolve(&home, false);
        assert_eq!(default.paths, vec![home.join(".junie/sessions")]);
        assert_eq!(default.default_root, Some(home.join(".junie/sessions")));
    }
}
