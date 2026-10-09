//! Whether a file's change time alone can prove it unchanged: the
//! filesystems that keep a real one, and the window past which it is trusted.

use super::{SETTLE_REVERIFY_MS, SETTLE_WINDOW_NS};
use std::fs;
use std::path::Path;

#[cfg(test)]
thread_local! {
    static FILESYSTEM_OVERRIDE: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Answer [`filesystem_keeps_change_time`] for every file on this thread, so
/// a test can model a filesystem it is not running on.
#[cfg(test)]
pub(crate) fn set_filesystem_keeps_change_time_for_test(keeps: Option<bool>) {
    FILESYSTEM_OVERRIDE.with(|slot| slot.set(keeps));
}

/// Whether the filesystem holding a file keeps a change time user space
/// cannot set. An allowlist: a filesystem not named here, or one whose type
/// cannot be read, keeps the digest.
pub(crate) fn filesystem_keeps_change_time(path: &Path, metadata: &fs::Metadata) -> bool {
    #[cfg(test)]
    if let Some(keeps) = FILESYSTEM_OVERRIDE.with(|slot| slot.get()) {
        return keeps;
    }
    filesystem_keeps_change_time_cached(path, metadata)
}

#[cfg(unix)]
fn filesystem_keeps_change_time_cached(path: &Path, metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    static BY_DEVICE: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<u64, bool>>> =
        std::sync::OnceLock::new();
    let device = metadata.dev();
    let cache = BY_DEVICE.get_or_init(Default::default);
    if let Some(known) = cache.lock().ok().and_then(|map| map.get(&device).copied()) {
        return known;
    }
    let keeps = statfs_keeps_change_time(path);
    if let Ok(mut map) = cache.lock() {
        map.insert(device, keeps);
    }
    keeps
}

#[cfg(not(unix))]
fn filesystem_keeps_change_time_cached(_path: &Path, _metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn statfs_keeps_change_time(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: `statfs` writes a complete `statfs` on success and reads only
    // the NUL-terminated path; both pointers are to live locals.
    if unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: `statfs` returned 0, so the struct is initialized.
    let stat = unsafe { stat.assume_init() };
    // SAFETY: the kernel NUL-terminates `f_fstypename` within its bounds.
    let name = unsafe { std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr()) };
    matches!(name.to_bytes(), b"apfs" | b"hfs")
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn statfs_keeps_change_time(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    const EXT2_3_4: u64 = 0xEF53;
    const XFS: u64 = 0x5846_5342;
    const BTRFS: u64 = 0x9123_683E;
    const ZFS: u64 = 0x2FC1_2FC1;
    const TMPFS: u64 = 0x0102_1994;
    const F2FS: u64 = 0xF2F5_2010;
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    // SAFETY: as above.
    if unsafe { libc::statfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: `statfs` returned 0, so the struct is initialized.
    let stat = unsafe { stat.assume_init() };
    #[allow(clippy::unnecessary_cast)]
    let kind = (stat.f_type as u64) & 0xFFFF_FFFF;
    matches!(kind, EXT2_3_4 | XFS | BTRFS | ZFS | TMPFS | F2FS)
}

#[cfg(all(
    unix,
    not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "linux",
        target_os = "android"
    ))
))]
fn statfs_keeps_change_time(_path: &Path) -> bool {
    false
}

pub(super) fn now_wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Whether a settle recorded at `settled_at_ms` still vouches for its file.
/// A clock that went backwards past it is treated as expired.
pub(crate) fn settle_is_fresh(settled_at_ms: i64) -> bool {
    let age = now_wall_ms().saturating_sub(settled_at_ms);
    (0..SETTLE_REVERIFY_MS).contains(&age)
}

#[cfg(test)]
thread_local! {
    static SETTLE_WINDOW_OVERRIDE: std::cell::Cell<Option<i64>> = const { std::cell::Cell::new(None) };
}

/// Let a test settle files it has only just written.
#[cfg(test)]
pub(crate) fn set_settle_window_for_test(window_ns: Option<i64>) {
    SETTLE_WINDOW_OVERRIDE.with(|slot| slot.set(window_ns));
}

/// Block until a file written in `dir` gets a change time later than
/// `ctime_ns`, so a test's next write cannot share a tick with a stat it has
/// already recorded, whatever the filesystem's timestamp resolution. Probes
/// rather than sleeping a guessed interval.
#[cfg(test)]
pub(crate) fn wait_for_change_time_after(dir: &Path, ctime_ns: i64) {
    let probe = dir.join(".ctime-probe");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        fs::write(&probe, b"x").unwrap();
        let now = change_time_ns(&fs::metadata(&probe).unwrap()).unwrap_or(i64::MAX);
        let _ = fs::remove_file(&probe);
        if now > ctime_ns {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the filesystem's change time did not advance within 10 s"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn settle_window_ns() -> i64 {
    #[cfg(test)]
    if let Some(window) = SETTLE_WINDOW_OVERRIDE.with(|slot| slot.get()) {
        return window;
    }
    SETTLE_WINDOW_NS
}

/// The file's change time in nanoseconds, where the platform reports one.
#[cfg(unix)]
pub(crate) fn change_time_ns(metadata: &fs::Metadata) -> Option<i64> {
    use std::os::unix::fs::MetadataExt;
    metadata
        .ctime()
        .checked_mul(1_000_000_000)?
        .checked_add(metadata.ctime_nsec())
}

#[cfg(not(unix))]
pub(crate) fn change_time_ns(_metadata: &fs::Metadata) -> Option<i64> {
    None
}

/// `metadata`'s change time, if the file may settle on it: its filesystem
/// keeps a real one, and it is old enough to be past the racy window.
/// `metadata` must have been taken *before* the digest that is about to prove
/// the file, so a write racing the digest leaves a different `ctime` behind.
pub(crate) fn settled_change_time(path: &Path, metadata: &fs::Metadata) -> Option<i64> {
    if !filesystem_keeps_change_time(path, metadata) {
        return None;
    }
    let ctime = change_time_ns(metadata)?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos(),
    )
    .ok()?;
    (now.checked_sub(ctime)? >= settle_window_ns()).then_some(ctime)
}
