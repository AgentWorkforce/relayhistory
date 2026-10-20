use super::*;
pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let value = |key| {
        use_env
            .then(|| std::env::var(key).ok())
            .flatten()
            .filter(|v| !v.is_empty())
    };
    let base = selected(value("QWEN_RUNTIME_DIR"), value("QWEN_HOME"));
    vec![base
        .map(|v| expand(home, &v))
        .unwrap_or_else(|| home.join(".qwen"))
        .join("projects")]
    .into()
}
fn expand(home: &Path, value: &str) -> PathBuf {
    let path = if value == "~" {
        home.to_owned()
    } else if let Some(s) = value
        .strip_prefix("~/")
        .or_else(|| value.strip_prefix("~\\"))
    {
        home.join(s.split(['/', '\\']).collect::<PathBuf>())
    } else {
        PathBuf::from(value)
    };
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| home.to_owned())
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for part in absolute.components() {
        match part {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}
pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|r| path.starts_with(r))
        .max_by_key(|r| r.components().count())
        .context("QWEN_CODE_INVALID_PATH: outside projects roots")?;
    let relative = path.strip_prefix(root)?;
    let parts: Vec<_> = relative.components().collect();
    anyhow::ensure!(
        parts.len() == 3
            && parts
                .iter()
                .all(|p| matches!(p, std::path::Component::Normal(_)))
            && parts[1].as_os_str() == "chats"
            && path.extension().is_some_and(|v| v == "jsonl"),
        "QWEN_CODE_INVALID_PATH: expected project/chats/session.jsonl"
    );
    let native = path
        .file_stem()
        .and_then(|v| v.to_str())
        .context("QWEN_CODE_INVALID_PATH: invalid session filename")?;
    anyhow::ensure!(
        !native.is_empty(),
        "QWEN_CODE_INVALID_PATH: empty session ID"
    );
    let namespace = root
        .strip_prefix(&roots.home)
        .unwrap_or(root)
        .join(parts[0].as_os_str());
    Ok(format!(
        "root:{:x}:{native}",
        Sha256::digest(
            namespace
                .to_str()
                .context("QWEN_CODE_INVALID_PATH: non-UTF8 namespace")?
                .replace('\\', "/")
                .as_bytes()
        )
    ))
}

fn selected(runtime: Option<String>, home: Option<String>) -> Option<String> {
    runtime
        .filter(|v| !v.is_empty())
        .or_else(|| home.filter(|v| !v.is_empty()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_environment_priority_matches_resolver_oracle() {
        assert_eq!(
            selected(Some("/runtime".into()), Some("/home".into())),
            Some("/runtime".into())
        );
        assert_eq!(
            selected(Some("".into()), Some("/home".into())),
            Some("/home".into())
        );
        assert_eq!(selected(None, None), None);
        let home = std::env::current_dir().unwrap().join("fixture");
        assert_eq!(expand(&home, "~\\sub\\..\\runtime"), home.join("runtime"));
        assert_eq!(expand(&home, "~/runtime"), home.join("runtime"));
    }
}
