//! Cross-process serialization of remote hydration for one session.
use super::*;
use std::fs::OpenOptions;

pub(super) struct RemoteHydrationLock {
    file: fs::File,
}

impl Drop for RemoteHydrationLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
    }
}

pub(super) fn acquire_remote_hydration_lock(
    db_path: &Path,
    options: &HydrateSessionOptions,
) -> Result<RemoteHydrationLock> {
    super::super::check_capture_cancelled()?;
    let absolute = if db_path.is_absolute() {
        db_path.to_path_buf()
    } else {
        std::env::current_dir()?.join(db_path)
    };
    let canonical_db = fs::canonicalize(&absolute).or_else(|_| {
        let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
        let parent = fs::canonicalize(parent)?;
        Ok::<_, std::io::Error>(
            parent.join(
                absolute
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("history.db")),
            ),
        )
    })?;
    let lock_dir = canonical_db
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".relayhistory-hydration-locks");
    fs::create_dir_all(&lock_dir)?;
    let key = format!("{}\0{}", options.source, options.session_id);
    let lock_path = lock_dir.join(format!("{:x}.lock", Sha256::digest(key.as_bytes())));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening hydration lock {}", lock_path.display()))?;
    wait_for_lock(&file, crate::store::busy_retry_handler)
        .with_context(|| format!("locking hydration target {}", lock_path.display()))?;
    Ok(RemoteHydrationLock { file })
}

// Reuse the bounded SQLite grace window and its cancellation-aware sleeps.
// The callback is injectable so expiry is tested without a 30-second test.
fn wait_for_lock(file: &fs::File, mut retry: impl FnMut(i32) -> bool) -> Result<()> {
    let mut attempts = 0;
    loop {
        super::super::check_capture_cancelled()?;
        match crate::file_lock::try_lock_exclusive(file) {
            Ok(()) => return Ok(()),
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || (cfg!(windows) && matches!(error.raw_os_error(), Some(32 | 33))) => {}
            Err(error) => return Err(error.into()),
        }
        if !retry(attempts) {
            super::super::check_capture_cancelled()?;
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                Some(
                    "remote hydration target is still locked after the bounded retry budget".into(),
                ),
            )
            .into());
        }
        attempts += 1;
    }
}

#[cfg(test)]
mod tests;
