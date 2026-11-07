use super::*;
pub(super) fn is_transcript(root: &Path, path: &Path) -> bool {
    path.strip_prefix(root).ok().is_some_and(|p| {
        let parts: Vec<_> = p.components().collect();
        // Native project directory spellings differ across operating systems.
        // The session header, not a POSIX-specific leading dash, identifies Droid.
        (parts.len() == 1 || parts.len() == 2) && path.extension().is_some_and(|e| e == "jsonl")
    })
}
fn sidecar(path: &Path) -> PathBuf {
    path.with_extension("settings.json")
}
fn read_settings(path: &Path, context: &AcquisitionContext<'_>) -> Result<Vec<u8>> {
    const SETTINGS_LIMIT: u64 = 1024 * 1024;
    context.validate(path)?;
    let mut raw = Vec::new();
    std::fs::File::open(path)?
        .take(SETTINGS_LIMIT + 1)
        .read_to_end(&mut raw)?;
    anyhow::ensure!(
        raw.len() as u64 <= SETTINGS_LIMIT,
        "DROID_SETTINGS_TOO_LARGE"
    );
    Ok(raw)
}
pub(super) fn stamp(
    path: &Path,
    context: &AcquisitionContext<'_>,
) -> Result<(String, Option<i64>)> {
    context.validate(path)?;
    let (primary, modified) = crate::file_stamp_and_modified(path)?;
    let settings = sidecar(path);
    // Validate even a missing sidecar before probing; symlink escapes are errors.
    let settings_stamp = match context
        .validate(&settings)
        .and_then(|_| crate::file_stamp_and_modified(&settings))
    {
        Ok((stamp, _)) => Some(stamp),
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            None
        }
        Err(e) => return Err(e),
    };
    Ok((serde_json::to_string(&(primary, settings_stamp))?, modified))
}
pub(super) fn snapshot(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let (before, modified) = stamp(path, context)?;
    let bytes = context.read(path)?;
    let entries = format::parse(&bytes, false)?;
    let mut paths = vec![path.to_owned()];
    let mut size = bytes.len();
    let settings = sidecar(path);
    let settings = match read_settings(&settings, context) {
        Ok(raw) => {
            size += raw.len();
            paths.push(settings);
            let value: Value = serde_json::from_slice(&raw)
                .context("DROID_INVALID_SETTINGS: malformed sidecar")?;
            anyhow::ensure!(value.is_object(), "DROID_INVALID_SETTINGS: expected object");
            Some(value)
        }
        Err(e)
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
        {
            None
        }
        Err(e) => return Err(e),
    };
    anyhow::ensure!(
        stamp(path, context)?.0 == before,
        "DROID_CAPTURE_CHANGED: retry after writer settles"
    );
    let tail = bytes.rsplit(|b| *b == b'\n').next().unwrap_or_default();
    let records = entries.len() as i64;
    Ok(AcquiredSession::new(
        serde_json::json!({"entries":entries,"tail":tail,"settings":settings,"modified":modified}),
        size as i64,
        records,
        paths,
    ))
}
