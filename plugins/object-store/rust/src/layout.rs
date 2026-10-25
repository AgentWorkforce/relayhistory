//! The canonical raw-session object layout (v1):
//!
//! ```text
//! {prefix}/v1/ws={workspace_id}/m={machine_id}/h={harness}/d={yyyy}/{mm}/{dd}/s={session_id}/
//!     manifest.json
//!     raw/{seq:010}-{start_offset}-{sha256_8}.{jsonl|ndjson|sqlite-rows}.zst
//!     normalized/v{N}/...
//! ```
//!
//! `d=` is the session's start date in UTC, so a session never moves when it
//! spans midnight. Raw chunks are append-only and content-addressed (the
//! first 8 hex characters of the SHA-256 of the *uncompressed* chunk), so
//! re-delivering one is idempotent; `manifest.json` is the only object ever
//! replaced, always by compare-and-swap.

use chrono::{Datelike, NaiveDate};

use crate::{Error, ObjectKey, Result};

pub const LAYOUT_VERSION: &str = "v1";
pub const MANIFEST_FILE: &str = "manifest.json";

/// Encoding of a raw chunk's uncompressed bytes, exactly as the harness wrote
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawFormat {
    Jsonl,
    Ndjson,
    /// Rows read from a harness's own SQLite store, serialized one per line.
    SqliteRows,
}

impl RawFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Ndjson => "ndjson",
            Self::SqliteRows => "sqlite-rows",
        }
    }
}

/// Identifies one session's directory in the bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLocation {
    /// Deployment-wide prefix, e.g. a tenant or environment. May contain `/`.
    pub prefix: Option<String>,
    pub workspace_id: String,
    pub machine_id: String,
    pub harness: String,
    /// Session start date, UTC.
    pub start_date: NaiveDate,
    pub session_id: String,
}

impl SessionLocation {
    /// `{prefix}/v1/ws=../m=../h=../d=yyyy/mm/dd/s=..`
    pub fn session_prefix(&self) -> Result<ObjectKey> {
        for (name, value) in [
            ("workspace_id", &self.workspace_id),
            ("machine_id", &self.machine_id),
            ("harness", &self.harness),
            ("session_id", &self.session_id),
        ] {
            check_component(name, value)?;
        }
        let date = self.start_date;
        let tail = format!(
            "{LAYOUT_VERSION}/ws={}/m={}/h={}/d={:04}/{:02}/{:02}/s={}",
            self.workspace_id,
            self.machine_id,
            self.harness,
            date.year(),
            date.month(),
            date.day(),
            self.session_id,
        );
        match self.prefix.as_deref().map(|p| p.trim_matches('/')) {
            Some(prefix) if !prefix.is_empty() => ObjectKey::new(format!("{prefix}/{tail}")),
            _ => ObjectKey::new(tail),
        }
    }

    /// The workspace+machine prefix a vended credential should be scoped to.
    pub fn machine_prefix(&self) -> Result<ObjectKey> {
        check_component("workspace_id", &self.workspace_id)?;
        check_component("machine_id", &self.machine_id)?;
        let tail = format!(
            "{LAYOUT_VERSION}/ws={}/m={}",
            self.workspace_id, self.machine_id
        );
        match self.prefix.as_deref().map(|p| p.trim_matches('/')) {
            Some(prefix) if !prefix.is_empty() => ObjectKey::new(format!("{prefix}/{tail}")),
            _ => ObjectKey::new(tail),
        }
    }

    pub fn manifest(&self) -> Result<ObjectKey> {
        self.session_prefix()?.join(MANIFEST_FILE)
    }

    /// `raw/{seq:010}-{start_offset}-{sha256_8}.{ext}.zst`. `sha256_hex` is
    /// the digest of the uncompressed chunk; its first 8 characters are used.
    pub fn raw_chunk(
        &self,
        seq: u64,
        start_offset: u64,
        sha256_hex: &str,
        format: RawFormat,
    ) -> Result<ObjectKey> {
        if sha256_hex.len() < 8 || !sha256_hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::InvalidArgument(format!(
                "chunk digest {sha256_hex:?} is not hex"
            )));
        }
        let short = sha256_hex[..8].to_ascii_lowercase();
        self.session_prefix()?.join(&format!(
            "raw/{seq:010}-{start_offset}-{short}.{}.zst",
            format.extension()
        ))
    }

    pub fn raw_prefix(&self) -> Result<ObjectKey> {
        self.session_prefix()?.join("raw")
    }

    /// `normalized/v{version}/{name}`: derived output of transform version N.
    pub fn normalized(&self, version: u32, name: &str) -> Result<ObjectKey> {
        self.session_prefix()?
            .join(&format!("normalized/v{version}/{name}"))
    }
}

/// Parsed fields of a raw chunk file name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawChunkName {
    pub seq: u64,
    pub start_offset: u64,
    pub sha256_8: String,
    pub format: RawFormat,
}

impl RawChunkName {
    /// Parse `{seq:010}-{start_offset}-{sha256_8}.{ext}.zst`.
    pub fn parse(file_name: &str) -> Option<Self> {
        let stem = file_name.strip_suffix(".zst")?;
        let (stem, format) = [RawFormat::Jsonl, RawFormat::Ndjson, RawFormat::SqliteRows]
            .into_iter()
            .find_map(|f| {
                stem.strip_suffix(f.extension())?
                    .strip_suffix('.')
                    .map(|s| (s, f))
            })?;
        let mut parts = stem.splitn(3, '-');
        let seq = parts.next()?;
        let start_offset = parts.next()?.parse().ok()?;
        let sha256_8 = parts.next()?;
        if seq.len() != 10 || sha256_8.len() != 8 {
            return None;
        }
        Some(Self {
            seq: seq.parse().ok()?,
            start_offset,
            sha256_8: sha256_8.to_string(),
            format,
        })
    }
}

/// Path components are interpolated after `key=`, so they must not contain
/// `/` or `=` and must be safe on every backend.
fn check_component(name: &str, value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= 128
        && !value.starts_with('.')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidArgument(format!(
            "{name} {value:?} must be 1-128 chars of [A-Za-z0-9._-] not starting with `.`"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location() -> SessionLocation {
        SessionLocation {
            prefix: Some("tenant-a/".into()),
            workspace_id: "ws_1".into(),
            machine_id: "mach-9".into(),
            harness: "claude".into(),
            start_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            session_id: "0192f0c4-7f00-7000-8000-000000000001".into(),
        }
    }

    #[test]
    fn builds_canonical_keys() {
        let loc = location();
        assert_eq!(
            loc.manifest().unwrap().as_str(),
            "tenant-a/v1/ws=ws_1/m=mach-9/h=claude/d=2026/01/05/s=0192f0c4-7f00-7000-8000-000000000001/manifest.json"
        );
        let sha = crate::sha256_hex(b"chunk");
        let chunk = loc.raw_chunk(42, 1024, &sha, RawFormat::Jsonl).unwrap();
        assert!(chunk
            .as_str()
            .ends_with(&format!("/raw/0000000042-1024-{}.jsonl.zst", &sha[..8])));
        assert_eq!(
            loc.machine_prefix().unwrap().as_str(),
            "tenant-a/v1/ws=ws_1/m=mach-9"
        );
        assert!(loc
            .normalized(2, "events.jsonl.zst")
            .unwrap()
            .as_str()
            .ends_with("/normalized/v2/events.jsonl.zst"));
    }

    #[test]
    fn chunk_names_round_trip_and_sort_by_seq() {
        let loc = SessionLocation {
            prefix: None,
            ..location()
        };
        let sha = crate::sha256_hex(b"x");
        let a = loc.raw_chunk(9, 0, &sha, RawFormat::SqliteRows).unwrap();
        let b = loc.raw_chunk(10, 77, &sha, RawFormat::SqliteRows).unwrap();
        assert!(a < b, "zero-padded seq sorts lexicographically");
        let parsed = RawChunkName::parse(b.file_name()).unwrap();
        assert_eq!(parsed.seq, 10);
        assert_eq!(parsed.start_offset, 77);
        assert_eq!(parsed.format, RawFormat::SqliteRows);
        assert_eq!(parsed.sha256_8, &sha[..8]);
        assert!(RawChunkName::parse("manifest.json").is_none());
    }

    #[test]
    fn rejects_unsafe_components() {
        for bad in ["", "a/b", "a=b", "..", ".hidden", "a b"] {
            let loc = SessionLocation {
                session_id: bad.into(),
                ..location()
            };
            assert!(loc.manifest().is_err(), "{bad:?}");
        }
    }
}
