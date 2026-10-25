//! Client (desktop delivery coordinator) helpers: building the first
//! manifest, appending chunks, opening epochs, and detecting that this writer
//! is stale. See the spec sections "Epochs" and "Stale writers".

use super::types::{
    ChunkEntry, Compression, EpochReason, EpochRecord, Manifest, Raw, RawState, Session, Writer,
    MANIFEST_SCHEMA_VERSION,
};
use super::update::now_rfc3339;
use super::validate::content_sha256;
use crate::layout::{RawFormat, LAYOUT_VERSION};
use crate::{Error, Result};

/// What the client last saw acknowledged; keep it locally with the checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAck {
    pub revision: u64,
    pub chunk_count: u64,
    pub content_sha256: String,
    pub epoch: u32,
}

impl ClientAck {
    pub fn of(m: &Manifest) -> Self {
        Self {
            revision: m.revision,
            chunk_count: m.raw.chunk_count,
            content_sha256: m.raw.content_sha256.clone(),
            epoch: m.raw.epoch,
        }
    }
}

/// How the remote manifest relates to this client's acknowledged state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteState {
    /// Another client instance owns `raw`: stop writing this session.
    Fenced { owner: String },
    /// The manifest went backwards (restore or corruption): stop and alert,
    /// never overwrite.
    RolledBack { reason: String },
    /// `raw` is what this client acknowledged; only the server (or nobody)
    /// wrote since. Re-apply and continue.
    Unchanged,
    /// `raw` extends the acknowledged state: this client's own write whose
    /// acknowledgment it never saw (e.g. a crash after the PUT). Adopt it.
    Extended { new_chunks: u64 },
}

impl RemoteState {
    /// Whether this client may write `raw` on top of the remote copy.
    pub fn may_write(&self) -> bool {
        matches!(self, Self::Unchanged | Self::Extended { .. })
    }
}

/// Classify `remote` for a client whose fencing token is `owner`. With no
/// `ack` (first contact, e.g. after reinstall) only ownership is checked.
pub fn classify(remote: &Manifest, owner: &str, ack: Option<&ClientAck>) -> RemoteState {
    if remote.raw.owner != owner {
        return RemoteState::Fenced {
            owner: remote.raw.owner.clone(),
        };
    }
    let Some(ack) = ack else {
        return RemoteState::Unchanged;
    };
    if remote.revision < ack.revision {
        return RemoteState::RolledBack {
            reason: format!(
                "revision {} < acknowledged {}",
                remote.revision, ack.revision
            ),
        };
    }
    if remote.raw.chunk_count < ack.chunk_count {
        return RemoteState::RolledBack {
            reason: format!(
                "chunk_count {} < acknowledged {}",
                remote.raw.chunk_count, ack.chunk_count
            ),
        };
    }
    let prefix = content_sha256(remote.raw.chunks.iter().take(ack.chunk_count as usize));
    if prefix != ack.content_sha256 {
        return RemoteState::RolledBack {
            reason: "acknowledged chunks are not a prefix of the remote chunk list".into(),
        };
    }
    match remote.raw.chunk_count - ack.chunk_count {
        0 => RemoteState::Unchanged,
        n => RemoteState::Extended { new_chunks: n },
    }
}

/// A sealed chunk before it has a `seq`.
#[derive(Debug, Clone, PartialEq)]
pub struct NewChunk {
    pub start_offset: u64,
    pub bytes: u64,
    pub stored_bytes: u64,
    pub sha256: String,
    pub stored_sha256: String,
    pub records: Option<u64>,
    pub first_ts: Option<String>,
    pub last_ts: Option<String>,
}

/// Assign `seq` and `epoch` to new chunks of the current epoch, in order,
/// continuing from `raw`. Upload each chunk under the key built from the
/// returned entry (`layout::SessionLocation::raw_chunk`) **before** writing
/// the manifest. Offsets must continue from `raw.next_offset`.
pub fn plan_chunks(raw: &Raw, chunks: Vec<NewChunk>) -> Result<Vec<ChunkEntry>> {
    let mut next_offset = raw.next_offset;
    let mut out = Vec::with_capacity(chunks.len());
    for (i, c) in chunks.into_iter().enumerate() {
        if c.start_offset != next_offset {
            return Err(Error::InvalidArgument(format!(
                "chunk {i} starts at {}, expected {next_offset}",
                c.start_offset
            )));
        }
        if c.bytes == 0 || c.stored_bytes == 0 || c.sha256.len() != 64 {
            return Err(Error::InvalidArgument(format!(
                "chunk {i} is empty or has a bad digest"
            )));
        }
        next_offset += c.bytes;
        out.push(ChunkEntry {
            seq: raw.chunk_count + i as u64,
            epoch: raw.epoch,
            start_offset: c.start_offset,
            bytes: c.bytes,
            stored_bytes: c.stored_bytes,
            sha256_8: c.sha256[..8].to_string(),
            sha256: c.sha256,
            stored_sha256: c.stored_sha256,
            compression: Compression::Zstd,
            records: c.records,
            first_ts: c.first_ts,
            last_ts: c.last_ts,
            extra: Default::default(),
        });
    }
    Ok(out)
}

/// Append planned chunks and recompute every derived `raw` field.
///
/// Fails if `entries` were planned against a different `raw` (their `seq`
/// does not continue `raw.chunk_count`): re-plan, re-upload under the new
/// keys, and leave the old uploads as harmless orphans.
pub fn apply_chunks(raw: &mut Raw, entries: &[ChunkEntry]) -> Result<()> {
    if let Some(first) = entries.first() {
        if first.seq != raw.chunk_count
            || first.epoch != raw.epoch
            || first.start_offset != raw.next_offset
        {
            return Err(Error::InvalidArgument(format!(
                "chunks were planned for seq {} epoch {} offset {}, but raw is at seq {} epoch {} offset {}; re-plan",
                first.seq, first.epoch, first.start_offset, raw.chunk_count, raw.epoch, raw.next_offset
            )));
        }
    }
    raw.chunks.extend_from_slice(entries);
    recompute(raw);
    raw.updated_at = now_rfc3339();
    Ok(())
}

/// Open the next epoch: the number is always `raw.epoch + 1` of the copy being
/// edited, which the compare-and-swap write then pins. Only the raw owner may
/// call this, and only after observing the source truncated or rewritten.
pub fn open_epoch(raw: &mut Raw, reason: EpochReason) -> Result<u32> {
    if reason == EpochReason::Initial {
        return Err(Error::InvalidArgument("only epoch 0 is \"initial\"".into()));
    }
    let epoch = raw.epoch + 1;
    raw.epochs.push(EpochRecord {
        epoch,
        opened_at: now_rfc3339(),
        reason,
        first_seq: raw.chunk_count,
        opened_by: raw.owner.clone(),
        extra: Default::default(),
    });
    raw.epoch = epoch;
    raw.next_offset = 0;
    raw.updated_at = now_rfc3339();
    Ok(epoch)
}

/// Mark the harness session ended (or reopen it).
pub fn set_state(raw: &mut Raw, state: RawState) {
    if raw.state != state {
        raw.state = state;
        raw.updated_at = now_rfc3339();
    }
}

fn recompute(raw: &mut Raw) {
    raw.chunk_count = raw.chunks.len() as u64;
    raw.raw_bytes = raw.chunks.iter().map(|c| c.bytes).sum();
    raw.stored_bytes = raw.chunks.iter().map(|c| c.stored_bytes).sum();
    raw.content_sha256 = content_sha256(&raw.chunks);
    raw.next_offset = raw
        .chunks
        .iter()
        .rev()
        .find(|c| c.epoch == raw.epoch)
        .map_or(0, |c| c.start_offset + c.bytes);
}

/// Everything needed for revision 1.
#[derive(Debug, Clone)]
pub struct NewManifest {
    pub trace_id: String,
    pub session: Session,
    pub writer: Writer,
    pub format: RawFormat,
}

/// The first manifest of a session: epoch 0, no chunks, owned by
/// `writer.delivery_instance_id`. Write it with `Edit::Create`.
pub fn new_manifest(params: NewManifest) -> Manifest {
    let now = now_rfc3339();
    let owner = params.writer.delivery_instance_id.clone();
    let mut raw = Raw {
        owner: owner.clone(),
        state: RawState::Open,
        updated_at: now.clone(),
        format: params.format,
        compression: Compression::Zstd,
        chunk_count: 0,
        raw_bytes: 0,
        stored_bytes: 0,
        content_sha256: String::new(),
        epoch: 0,
        epochs: vec![EpochRecord {
            epoch: 0,
            opened_at: now.clone(),
            reason: EpochReason::Initial,
            first_seq: 0,
            opened_by: owner,
            extra: Default::default(),
        }],
        next_offset: 0,
        chunks: Vec::new(),
        extra: Default::default(),
    };
    recompute(&mut raw);
    Manifest {
        schema_v: MANIFEST_SCHEMA_VERSION,
        layout: LAYOUT_VERSION.to_string(),
        trace_id: params.trace_id,
        revision: 1,
        created_at: now.clone(),
        updated_at: now,
        session: params.session,
        writer: params.writer,
        raw,
        server: None,
        extensions: None,
        extra: Default::default(),
    }
}
