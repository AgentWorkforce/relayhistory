use super::*;
use sha2::{Digest, Sha256};

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let workspaces = if use_env {
        match std::env::var_os("RELAYHISTORY_VTCODE_WORKSPACES") {
            Some(raw) if !raw.is_empty() => std::env::split_paths(&raw).collect::<Vec<_>>(),
            _ => Vec::new(),
        }
    } else {
        vec![home.to_path_buf()]
    };
    resolve_workspaces(workspaces)
}

fn resolve_workspaces(workspaces: Vec<PathBuf>) -> ResolvedProviderRoots {
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(error) => {
            return ResolvedProviderRoots {
                error: Some(format!(
                    "VTCODE_INVALID_ROOT: cannot resolve workspace path: {error}"
                )),
                ..Default::default()
            }
        }
    };
    let mut roots = Vec::new();
    for workspace in workspaces {
        if workspace.as_os_str().is_empty() {
            continue;
        }
        let absolute = if workspace.is_absolute() {
            workspace
        } else {
            cwd.join(workspace)
        };
        let mut normalized = PathBuf::new();
        for part in absolute.components() {
            match part {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                _ => normalized.push(part.as_os_str()),
            }
        }
        roots.push(normalized.join(".vtcode/sessions"));
    }
    roots.sort();
    roots.dedup();
    roots.into()
}

pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<(String, String)> {
    let root = roots
        .for_provider(ID)
        .iter()
        .find(|root| path.starts_with(root))
        .context("VTCODE_INVALID_PATH: outside declared workspace roots")?;
    let relative = path.strip_prefix(root)?;
    let parts = relative.components().collect::<Vec<_>>();
    anyhow::ensure!(
        parts.len() == 2
            && parts
                .iter()
                .all(|p| matches!(p, std::path::Component::Normal(_)))
            && parts[1].as_os_str() == "events.jsonl",
        "VTCODE_INVALID_PATH: expected session/events.jsonl"
    );
    let native = parts[0]
        .as_os_str()
        .to_str()
        .context("VTCODE_INVALID_PATH: non-UTF8 native id")?;
    anyhow::ensure!(!native.is_empty(), "VTCODE_INVALID_PATH: empty native id");
    let namespace = format!("{:x}", Sha256::digest(root.to_string_lossy().as_bytes()));
    Ok((format!("root:{namespace}:{native}"), native.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_roots_are_normalized_deduplicated_and_namespaced() {
        let base = std::env::current_dir().unwrap();
        let resolved = resolve_workspaces(vec![base.join("a/../b"), base.join("b")]);
        assert_eq!(resolved.paths, vec![base.join("b/.vtcode/sessions")]);
        let other = resolve_workspaces(vec![base.join("c")]);
        let mut roots = ProviderRoots::from_home(base.clone(), base.join("opencode.db"));
        roots
            .provider_roots
            .insert(ID.into(), resolved.paths.clone());
        let one = identity(&roots, &resolved.paths[0].join("same/events.jsonl"))
            .unwrap()
            .0;
        roots.provider_roots.insert(ID.into(), other.paths.clone());
        let two = identity(&roots, &other.paths[0].join("same/events.jsonl"))
            .unwrap()
            .0;
        assert_ne!(one, two);
    }
}
