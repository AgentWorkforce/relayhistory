//! Goose's process path resolver, frozen at ProviderRoots construction.
use super::*;
use std::ffi::OsString;

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    resolve_with(home, use_env, |key| std::env::var_os(key))
}

pub(super) fn resolve_with(
    home: &Path,
    use_env: bool,
    env: impl Fn(&str) -> Option<OsString>,
) -> ResolvedProviderRoots {
    if use_env {
        if let Some(root) = env("GOOSE_PATH_ROOT").map(PathBuf::from) {
            if root.is_absolute() {
                return vec![root.join("data")].into();
            }
        }
    }
    let data = if cfg!(windows) {
        (if use_env { env("APPDATA") } else { None })
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Roaming"))
            .join("Block/goose/data")
    } else {
        // Goose calls etcetera::choose_app_strategy, which selects Xdg on
        // macOS as well as Linux in etcetera 0.11.
        (if use_env { env("XDG_DATA_HOME") } else { None })
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| home.join(".local/share"))
            .join("goose")
    };
    ResolvedProviderRoots {
        paths: vec![data.clone()],
        default_root: Some(data),
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn absolute_override_joins_data_and_relative_override_is_ignored() {
        let home = Path::new("/isolated/home");
        let absolute = resolve_with(home, true, |key| {
            (key == "GOOSE_PATH_ROOT").then(|| OsString::from("/isolated/goose"))
        });
        assert_eq!(absolute.paths, vec![PathBuf::from("/isolated/goose/data")]);
        assert!(absolute.default_root.is_none());
        let relative = resolve_with(home, true, |key| {
            (key == "GOOSE_PATH_ROOT").then(|| OsString::from("relative"))
        });
        assert_eq!(relative.paths, resolve_with(home, false, |_| None).paths);
        let xdg = resolve_with(home, true, |key| {
            (key == "XDG_DATA_HOME").then(|| OsString::from("/isolated/xdg"))
        });
        if !cfg!(windows) {
            assert_eq!(xdg.paths, vec![PathBuf::from("/isolated/xdg/goose")]);
            let relative_xdg = resolve_with(home, true, |key| {
                (key == "XDG_DATA_HOME").then(|| OsString::from("relative"))
            });
            assert_eq!(
                relative_xdg.paths,
                resolve_with(home, false, |_| None).paths
            );
        }
        let ignored_env = resolve_with(home, false, |_| panic!("explicit home read host env"));
        assert_eq!(ignored_env.paths, resolve_with(home, false, |_| None).paths);
    }
}
