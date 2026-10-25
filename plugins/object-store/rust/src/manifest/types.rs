//! Serde types for `manifest.json` schema v1 (`docs/manifest-v1.md`).
//!
//! Every struct keeps fields it does not know in `extra`, so a writer that
//! round-trips a section preserves fields added by newer writers. Timestamps
//! stay RFC 3339 strings, exactly as written.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::layout::RawFormat;

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Unknown fields, preserved on round trip.
pub type Extra = Map<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub schema_v: u32,
    pub layout: String,
    pub trace_id: String,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub session: Session,
    pub writer: Writer,
    pub raw: Raw,
    /// Server-owned section; absent or `null` until the server first writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<Server>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Value>>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub workspace_id: String,
    pub machine_id: String,
    pub harness: String,
    pub session_id: String,
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Writer {
    pub delivery_instance_id: String,
    pub plugin_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_version: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RawState {
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Raw {
    pub owner: String,
    pub state: RawState,
    pub updated_at: String,
    #[serde(with = "raw_format")]
    pub format: RawFormat,
    pub compression: Compression,
    pub chunk_count: u64,
    pub raw_bytes: u64,
    pub stored_bytes: u64,
    pub content_sha256: String,
    pub epoch: u32,
    pub epochs: Vec<EpochRecord>,
    pub next_offset: u64,
    pub chunks: Vec<ChunkEntry>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EpochReason {
    Initial,
    Truncated,
    Rewritten,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EpochRecord {
    pub epoch: u32,
    pub opened_at: String,
    pub reason: EpochReason,
    pub first_seq: u64,
    pub opened_by: String,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub seq: u64,
    pub epoch: u32,
    pub start_offset: u64,
    pub bytes: u64,
    pub stored_bytes: u64,
    /// SHA-256 of the uncompressed bytes.
    pub sha256: String,
    pub sha256_8: String,
    /// SHA-256 of the stored (compressed) object.
    pub stored_sha256: String,
    pub compression: Compression,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_ts: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ts: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle {
    Active,
    Idle,
    Abandoned,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Server {
    pub updated_at: String,
    pub normalized: Option<Normalized>,
    pub lifecycle: Lifecycle,
    pub lifecycle_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Normalized {
    pub version: u32,
    pub source_content_sha256: String,
    pub source_epoch: u32,
    pub built_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_key: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Retention {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legal_hold: Option<bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// `closed` if the client closed the session, otherwise the server's view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EffectiveState {
    Closed,
    Active,
    Idle,
    Abandoned,
    Archived,
}

impl Manifest {
    pub fn lifecycle(&self) -> Lifecycle {
        self.server
            .as_ref()
            .map_or(Lifecycle::Active, |s| s.lifecycle)
    }

    pub fn effective_state(&self) -> EffectiveState {
        if self.raw.state == RawState::Closed {
            return EffectiveState::Closed;
        }
        match self.lifecycle() {
            Lifecycle::Active => EffectiveState::Active,
            Lifecycle::Idle => EffectiveState::Idle,
            Lifecycle::Abandoned => EffectiveState::Abandoned,
            Lifecycle::Archived => EffectiveState::Archived,
        }
    }

    /// The normalized output, if it was built from the current raw content.
    pub fn normalized_is_current(&self) -> bool {
        self.server
            .as_ref()
            .and_then(|s| s.normalized.as_ref())
            .is_some_and(|n| n.source_content_sha256 == self.raw.content_sha256)
    }
}

/// `RawFormat` as its file extension (`jsonl`, `ndjson`, `sqlite-rows`).
mod raw_format {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::layout::RawFormat;

    pub fn serialize<S: Serializer>(f: &RawFormat, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(f.extension())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<RawFormat, D::Error> {
        let s = String::deserialize(d)?;
        [RawFormat::Jsonl, RawFormat::Ndjson, RawFormat::SqliteRows]
            .into_iter()
            .find(|f| f.extension() == s)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown raw format {s:?}")))
    }
}
