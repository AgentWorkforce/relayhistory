use super::*;
use std::ffi::OsString;

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    resolve_with(home, use_env, |key| std::env::var_os(key))
}
fn resolve_with(
    home: &Path,
    use_env: bool,
    env: impl Fn(&str) -> Option<OsString>,
) -> ResolvedProviderRoots {
    let default = home.join(".deepagents/.state/sessions.db");
    if !use_env {
        return ResolvedProviderRoots {
            paths: vec![default.clone()],
            default_root: Some(default),
            error: None,
        };
    }
    // This variable is an inspector/import override at the pinned revision;
    // dcode's own writer uses DEEPAGENTS_HOME/.state/sessions.db.
    if let Some(value) = env("DEEPAGENTS_SESSIONS_DB").filter(|value| !value.is_empty()) {
        let value = PathBuf::from(value);
        let path = if let Ok(rest) = value.strip_prefix("~") {
            home.join(rest)
        } else {
            value
        };
        return ResolvedProviderRoots {
            paths: vec![path],
            default_root: None,
            error: None,
        };
    }
    if let Some(value) = env("DEEPAGENTS_HOME").filter(|value| !value.is_empty()) {
        let value = PathBuf::from(value);
        let profile = if let Ok(rest) = value.strip_prefix("~") {
            home.join(rest)
        } else {
            value
        };
        if !profile.is_absolute() {
            return ResolvedProviderRoots {
                paths: vec![],
                default_root: None,
                error: Some("DEEPAGENTS_HOME must be absolute or start with ~/".into()),
            };
        }
        let path = profile.join(".state/sessions.db");
        return ResolvedProviderRoots {
            paths: vec![path],
            default_root: None,
            error: None,
        };
    }
    ResolvedProviderRoots {
        paths: vec![default.clone()],
        default_root: Some(default),
        error: None,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn override_precedence_is_frozen_and_home_only_ignores_process_env() {
        let home = Path::new("/synthetic/home");
        let both = |k: &str| match k {
            "DEEPAGENTS_SESSIONS_DB" => Some("/db/custom.db".into()),
            "DEEPAGENTS_HOME" => Some("/profile".into()),
            _ => None,
        };
        assert_eq!(
            resolve_with(home, true, both).paths,
            vec![PathBuf::from("/db/custom.db")]
        );
        assert_eq!(
            resolve_with(home, false, both).paths,
            vec![home.join(".deepagents/.state/sessions.db")]
        );
    }
}
