use super::*;

pub(super) fn resolve(home: &Path, _: bool) -> ResolvedProviderRoots {
    ["antigravity", "antigravity-cli", "antigravity-ide"]
        .into_iter()
        .map(|variant| home.join(".gemini").join(variant).join("brain"))
        .collect::<Vec<_>>()
        .into()
}

pub(super) fn identity(roots: &ProviderRoots, path: &Path) -> Result<String> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|r| path.starts_with(r))
        .max_by_key(|r| r.components().count())
        .context("ANTIGRAVITY_INVALID_PATH: outside brain roots")?;
    let relative = path.strip_prefix(root)?;
    let parts: Vec<_> = relative.components().collect();
    anyhow::ensure!(
        parts.len() == 4
            && parts
                .iter()
                .all(|p| matches!(p, std::path::Component::Normal(_))),
        "ANTIGRAVITY_INVALID_PATH: expected conversation/.system_generated/logs/transcript JSONL"
    );
    anyhow::ensure!(
        parts[1].as_os_str() == ".system_generated" && parts[2].as_os_str() == "logs",
        "ANTIGRAVITY_INVALID_PATH: invalid transcript directory"
    );
    // A root namespace prevents App, CLI, IDE and explicitly supplied roots
    // from collapsing distinct conversations that happen to reuse an ID.
    let native = parts[0]
        .as_os_str()
        .to_str()
        .context("ANTIGRAVITY_INVALID_PATH: non-UTF8 conversation ID")?;
    anyhow::ensure!(
        !native.is_empty(),
        "ANTIGRAVITY_INVALID_PATH: empty conversation ID"
    );
    Ok(format!(
        "root:{:x}:{native}",
        Sha256::digest(
            root.strip_prefix(&roots.home)
                .unwrap_or(root)
                .to_str()
                .context("ANTIGRAVITY_INVALID_PATH: non-UTF8 root")?
                .replace('\\', "/")
                .as_bytes()
        )
    ))
}
