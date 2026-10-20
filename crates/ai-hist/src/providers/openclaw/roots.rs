//! OpenClaw's state-directory resolver (`src/config/state-dir.ts` and
//! `src/infra/home-dir.ts`): `OPENCLAW_STATE_DIR`, else `<home>/.openclaw`
//! (or `<home>/.openclaw-<profile>` under `OPENCLAW_PROFILE`), where
//! `OPENCLAW_HOME` replaces the OS home.
use super::*;
pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    resolve_with(home, use_env, |key| std::env::var_os(key))
}
pub(super) fn resolve_with(
    home: &Path,
    use_env: bool,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> ResolvedProviderRoots {
    let default = || {
        let root = home.join(".openclaw");
        ResolvedProviderRoots {
            paths: vec![root.clone()],
            default_root: Some(root),
        }
    };
    if !use_env {
        return default();
    }
    let value = |key: &str| {
        env(key)
            .map(|v| v.to_string_lossy().trim().to_owned())
            .filter(|v| !v.is_empty() && v != "undefined" && v != "null")
    };
    // OPENCLAW_HOME relocates the home every other OpenClaw path hangs from.
    let effective_home = value("OPENCLAW_HOME").map(|explicit| absolute(expand(home, &explicit)));
    if let Some(state) = value("OPENCLAW_STATE_DIR") {
        let base = effective_home.as_deref().unwrap_or(home);
        return vec![absolute(expand(base, &state))].into();
    }
    // `--profile <name>` exports OPENCLAW_PROFILE and selects ~/.openclaw-<name>
    // unless OPENCLAW_STATE_DIR is set (`src/cli/profile.ts`).
    let profile = value("OPENCLAW_PROFILE")
        .filter(|p| valid_profile(p) && !p.eq_ignore_ascii_case("default"));
    match (effective_home, profile) {
        (None, None) => default(),
        (home_override, profile) => {
            let base = home_override.unwrap_or_else(|| home.to_path_buf());
            let dir = profile.map_or(".openclaw".into(), |p| format!(".openclaw-{p}"));
            vec![base.join(dir)].into()
        }
    }
}
/// `^[a-z0-9][a-z0-9_-]{0,63}$`, case-insensitive (`src/cli/profile-utils.ts`).
fn valid_profile(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && name.len() <= 64
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
fn expand(home: &Path, value: &str) -> PathBuf {
    if value == "~" {
        home.into()
    } else if let Some(rest) = value.strip_prefix("~/").or(value.strip_prefix("~\\")) {
        home.join(rest)
    } else {
        value.into()
    }
}
/// Upstream resolves relative overrides against the invoking process's cwd.
fn absolute(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(&path))
        .unwrap_or(path)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn resolve_env(home: &Path, vars: &[(&str, &str)]) -> ResolvedProviderRoots {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| ((*k).into(), (*v).into()))
            .collect();
        resolve_with(home, true, |key| {
            vars.iter().find(|(k, _)| k == key).map(|(_, v)| v.into())
        })
    }
    #[test]
    fn state_dir_and_home_overrides_match_upstream_resolver() {
        let home = Path::new("/isolated/home");
        let plain = resolve_env(home, &[]);
        assert_eq!(plain.paths, vec![home.join(".openclaw")]);
        assert_eq!(plain.default_root, Some(home.join(".openclaw")));
        let state = resolve_env(home, &[("OPENCLAW_STATE_DIR", "  ~/state  ")]);
        assert_eq!(state.paths, vec![home.join("state")]);
        assert_eq!(state.default_root, None, "an override is path-namespaced");
        let relocated = resolve_env(home, &[("OPENCLAW_HOME", "/srv/claw")]);
        assert_eq!(relocated.paths, vec![PathBuf::from("/srv/claw/.openclaw")]);
        let both = resolve_env(
            home,
            &[("OPENCLAW_HOME", "~/claw"), ("OPENCLAW_STATE_DIR", "~/s")],
        );
        assert_eq!(both.paths, vec![home.join("claw/s")]);
        let profile = resolve_env(home, &[("OPENCLAW_PROFILE", "Dev")]);
        assert_eq!(profile.paths, vec![home.join(".openclaw-Dev")]);
        let pinned = resolve_env(
            home,
            &[("OPENCLAW_PROFILE", "dev"), ("OPENCLAW_STATE_DIR", "/s")],
        );
        assert_eq!(pinned.paths, vec![PathBuf::from("/s")]);
        let named_default = resolve_env(home, &[("OPENCLAW_PROFILE", "Default")]);
        assert_eq!(named_default.default_root, Some(home.join(".openclaw")));
        let blank = resolve_env(
            home,
            &[("OPENCLAW_STATE_DIR", " "), ("OPENCLAW_HOME", "undefined")],
        );
        assert_eq!(blank.paths, vec![home.join(".openclaw")]);
        assert_eq!(blank.default_root, Some(home.join(".openclaw")));
        assert_eq!(
            resolve(home, false).paths,
            vec![home.join(".openclaw")],
            "from_home never reads the environment"
        );
    }
}
