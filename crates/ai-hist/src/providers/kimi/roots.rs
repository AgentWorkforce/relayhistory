use super::*;
pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let value = use_env
        .then(|| std::env::var_os("KIMI_CODE_HOME"))
        .flatten();
    resolve_with(home, value)
}
fn resolve_with(home: &Path, value: Option<std::ffi::OsString>) -> ResolvedProviderRoots {
    let root = value
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".kimi-code"));
    let root = if root.is_absolute() {
        root
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(root),
            Err(error) => {
                return ResolvedProviderRoots {
                    error: Some(format!(
                        "KIMI_INVALID_ROOT: cannot freeze relative data root: {error}"
                    )),
                    ..Default::default()
                }
            }
        }
    };
    vec![root.join("sessions")].into()
}
pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|r| path.starts_with(r))
        .max_by_key(|r| r.components().count())
        .context("KIMI_INVALID_PATH: outside sessions roots")?;
    let parts = path.strip_prefix(root)?.components().collect::<Vec<_>>();
    anyhow::ensure!(
        parts.len() == 5
            && parts
                .iter()
                .all(|p| matches!(p, std::path::Component::Normal(_)))
            && parts[2].as_os_str() == "agents"
            && parts[4].as_os_str() == "wire.jsonl",
        "KIMI_INVALID_PATH: expected bucket/session/agents/agent/wire.jsonl"
    );
    let namespace = root
        .strip_prefix(&roots.home)
        .unwrap_or(root)
        .to_string_lossy()
        .replace('\\', "/");
    Ok(format!(
        "root:{:x}:{}:{}:{}",
        Sha256::digest(namespace.as_bytes()),
        parts[0].as_os_str().to_string_lossy(),
        parts[1].as_os_str().to_string_lossy(),
        parts[3].as_os_str().to_string_lossy()
    ))
}

#[cfg(test)]
mod root_tests {
    use super::*;
    #[test]
    fn override_is_frozen_absolute_and_tilde_is_literal() {
        let home = std::env::current_dir().unwrap().join("fixture-home");
        let r = resolve_with(&home, Some("~/literal".into()));
        assert_eq!(
            r.paths,
            vec![std::env::current_dir().unwrap().join("~/literal/sessions")]
        );
        let absolute = std::env::current_dir().unwrap().join("custom/data");
        let r = resolve_with(&home, Some(absolute.clone().into_os_string()));
        assert_eq!(r.paths, vec![absolute.join("sessions")]);
    }
    #[test]
    fn empty_override_uses_canonical_default() {
        let home = std::env::current_dir().unwrap().join("fixture-home");
        assert_eq!(
            resolve_with(&home, Some("".into())).paths,
            vec![home.join(".kimi-code/sessions")]
        );
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    #[test]
    fn default_namespace_uses_portable_relative_slashes() {
        let home = std::env::current_dir().unwrap().join("fixture-home");
        let roots = ProviderRoots::from_home(home.clone(), home.join("opencode.db"));
        let path = home.join(".kimi-code/sessions/bucket/session/agents/main/wire.jsonl");
        assert_eq!(
            identity(&roots, &path).unwrap(),
            format!(
                "root:{:x}:bucket:session:main",
                Sha256::digest(b".kimi-code/sessions")
            )
        );
    }
}
