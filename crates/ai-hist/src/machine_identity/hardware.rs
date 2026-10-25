//! A hash of this machine's hardware id, for telling a database restored
//! onto another machine from one that never moved.
//!
//! The raw id never leaves this module: only a SHA-256 of it is stored. It is
//! read through `machine-uid` without spawning a process --
//! `/var/lib/dbus/machine-id`, else `/etc/machine-id`, on Linux;
//! `gethostuuid(2)` (the
//! IOPlatformUUID) on macOS, `MachineGuid` from the registry on Windows --
//! and once per process.
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

/// The hash of this machine's hardware id, or `None` where none can be read
/// (a container without a machine id, another OS). `None` turns clone
/// detection off rather than guessing.
pub(crate) fn local_hardware_hash() -> Option<String> {
    static HASH: OnceLock<Option<String>> = OnceLock::new();
    HASH.get_or_init(|| raw_hardware_id().map(|raw| hardware_hash(&raw)))
        .clone()
}

/// The stored form of a hardware id: a domain-separated SHA-256, so the id
/// itself is not in the database.
pub(crate) fn hardware_hash(raw: &str) -> String {
    let digest = Sha256::digest(format!(
        "relayhistory-hardware-id-v1:{}",
        raw.trim().to_ascii_lowercase()
    ));
    format!("{digest:x}")
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn raw_hardware_id() -> Option<String> {
    machine_uid::get()
        .ok()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn raw_hardware_id() -> Option<String> {
    None
}
