//! Kilo CLI 7.8.15 database selection, frozen at ProviderRoots construction.
use super::*;
use crate::discover::WatchEntries;
use sha2::{Digest, Sha256};
use std::ffi::OsString;

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    resolve_with(home, use_env, |key| std::env::var_os(key))
}

pub(super) fn resolve_with(
    home: &Path,
    use_env: bool,
    env: impl Fn(&str) -> Option<OsString>,
) -> ResolvedProviderRoots {
    let data = if cfg!(windows) {
        (if use_env { env("APPDATA") } else { None })
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Roaming"))
            .join("kilo")
    } else {
        (if use_env { env("XDG_DATA_HOME") } else { None })
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .unwrap_or_else(|| home.join(".local/share"))
            .join("kilo")
    };
    if use_env {
        if let Some(value) = env("KILO_DB") {
            let path = PathBuf::from(value);
            if path == Path::new(":memory:") {
                return ResolvedProviderRoots {
                    paths: vec![],
                    default_root: None,
                    error: Some("KILO_DB=:memory: has no passive history database".into()),
                };
            }
            let path = if path.is_absolute() {
                path
            } else {
                data.join(path)
            };
            return vec![path].into();
        }
    }
    let path = data.join("kilo.db");
    ResolvedProviderRoots {
        paths: vec![path.clone()],
        default_root: Some(path),
        error: None,
    }
}

pub(super) fn watch_roots(roots: &ProviderRoots) -> Vec<WatchRoot> {
    roots
        .for_provider(ID)
        .iter()
        .filter_map(|path| {
            let parent = path.parent()?;
            let filename = path.file_name()?.to_string_lossy().to_string();
            Some(WatchRoot::directory_of(
                parent.to_path_buf(),
                WatchEntries::OpencodeStores {
                    primary: filename.into(),
                    channels: false,
                },
            ))
        })
        .collect()
}

pub(super) fn identity(namespace: &str, native: &str) -> String {
    let encoded: String = native
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("{namespace}:{encoded}")
}

pub(super) fn namespace(roots: &ProviderRoots, path: &Path) -> String {
    if roots.default_for_provider(ID).is_some_and(|p| p == path) {
        "default".into()
    } else {
        format!(
            "root:{:x}",
            Sha256::digest(path.as_os_str().as_encoded_bytes())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn database_override_and_xdg_precedence() {
        let home = tempfile::tempdir().unwrap();
        let default = resolve_with(home.path(), false, |_| panic!("read host env"));
        assert_eq!(
            default.paths[0],
            home.path().join(".local/share/kilo/kilo.db")
        );
        let custom = home.path().join("other/history.sqlite");
        let absolute = resolve_with(home.path(), true, |key| {
            (key == "KILO_DB").then(|| custom.as_os_str().to_os_string())
        });
        assert_eq!(absolute.paths, vec![custom]);
        assert!(absolute.default_root.is_none());
        let xdg = home.path().join("xdg");
        let relative = resolve_with(home.path(), true, |key| match key {
            "XDG_DATA_HOME" => Some(xdg.as_os_str().to_os_string()),
            "KILO_DB" => Some(OsString::from("custom.db")),
            _ => None,
        });
        if !cfg!(windows) {
            assert_eq!(relative.paths, vec![xdg.join("kilo/custom.db")]);
        }
        let memory = resolve_with(home.path(), true, |key| {
            (key == "KILO_DB").then(|| OsString::from(":memory:"))
        });
        assert!(memory.paths.is_empty());
        assert!(memory.error.is_some());
    }
}
