use super::*;
use sha2::{Digest, Sha256};

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let vibe_home = use_env
        .then(|| std::env::var_os("VIBE_HOME"))
        .flatten()
        .filter(|value| !value.is_empty());
    let save_dir = use_env
        .then(|| std::env::var_os("VIBE_SESSION_LOGGING__SAVE_DIR"))
        .flatten()
        .filter(|value| !value.is_empty());
    let configured = vibe_home.is_some() || save_dir.is_some();
    let base = vibe_home
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".vibe"));
    let candidate = save_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| base.join("logs/session"));
    let expanded = match absolute(home, candidate) {
        Ok(path) => path,
        Err(error) => {
            return ResolvedProviderRoots {
                error: Some(format!("VIBE_INVALID_ROOT: {error}")),
                ..Default::default()
            };
        }
    };
    let root = expanded.join("unified");
    ResolvedProviderRoots {
        paths: vec![root.clone()],
        default_root: (!configured).then_some(root),
        error: None,
    }
}

fn absolute(home: &Path, path: PathBuf) -> Result<PathBuf> {
    let path = if path == Path::new("~") {
        home.to_path_buf()
    } else if let Ok(rest) = path.strip_prefix("~/") {
        home.join(rest)
    } else {
        path
    };
    if path.is_absolute() {
        return Ok(path);
    }
    Ok(std::env::current_dir()?.join(path))
}

pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<(String, String)> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|root| path.starts_with(root))
        .max_by_key(|root| root.components().count())
        .context("VIBE_INVALID_PATH: outside Unified root")?;
    let relative = path.strip_prefix(root)?;
    let parts = relative.components().collect::<Vec<_>>();
    anyhow::ensure!(
        parts.len() == 2
            && parts
                .iter()
                .all(|part| matches!(part, std::path::Component::Normal(_)))
            && parts[1].as_os_str() == "CURRENT",
        "VIBE_INVALID_PATH: expected <sessionId>/CURRENT"
    );
    let native = parts[0]
        .as_os_str()
        .to_str()
        .context("VIBE_INVALID_ID: non-UTF8 session ID")?;
    anyhow::ensure!(
        !native.is_empty()
            && native.len() <= 128
            && native
                .bytes()
                .enumerate()
                .all(|(i, b)| b.is_ascii_alphanumeric() || (i > 0 && (b == b'-' || b == b'_'))),
        "VIBE_INVALID_ID: unsupported session ID"
    );
    let sid = if roots.default_for_provider(ID) == Some(root.as_path()) {
        native.into()
    } else {
        format!(
            "root:{:x}:{native}",
            Sha256::digest(root.to_string_lossy().as_bytes())
        )
    };
    Ok((sid, native.into()))
}
