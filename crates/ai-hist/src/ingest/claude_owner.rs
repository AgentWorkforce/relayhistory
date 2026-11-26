//! Which session a Claude transcript belongs to.
//!
//! A primary transcript is named after its session. A resumed or forked
//! conversation starts a new file by copying its origin's records, so the file
//! names two sessions: the one it is named after owns it, and the other's
//! records are inherited history, recorded as a fork of the origin.

use super::{is_claude_sidecar_file, ClaudeMetaFold, ClaudeSessionMeta};
use anyhow::{Context, Result};
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

impl ClaudeMetaFold {
    /// A fold for the transcript at `path`, owned by its file stem unless it
    /// is laid out as a subagent sidecar (which carries its parent's id).
    pub(crate) fn for_path(path: &Path) -> Self {
        Self {
            owner: claude_primary_stem(path),
            ..Self::default()
        }
    }

    /// Fold one record in. Must be called in file order, once per record.
    pub(crate) fn observe(&mut self, value: &Value) {
        if let Some(owner) = self.owner.as_deref() {
            let record_id = value
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty());
            let foreign = record_id.is_some_and(|id| id != owner);
            if foreign && self.owned.is_none() {
                // Until now every identified record was the owner's, so this
                // fold is the owner's too; a fold that began on another
                // session holds none of the owner's records.
                let mut owned = match self.session_id.as_deref() {
                    None => self.clone(),
                    Some(id) if id == owner => self.clone(),
                    Some(_) => Self::default(),
                };
                owned.owner = None;
                self.owned = Some(Box::new(owned));
            }
            if let Some(owned) = self.owned.as_deref_mut() {
                if !foreign {
                    owned.observe_record(value);
                }
            }
        }
        self.observe_record(value);
    }

    /// The fold that describes the file's own session: the owner's records
    /// when the file names its owner beside another session, else every
    /// record.
    pub(crate) fn effective(&self) -> &Self {
        match (&self.owned, self.owner.as_deref()) {
            (Some(owned), Some(owner)) if owned.session_id.as_deref() == Some(owner) => owned,
            _ => self,
        }
    }

    /// Two sessions in one transcript, neither of them the one it is named
    /// after: the file is not what it claims.
    pub(crate) fn has_conflicting_identity(&self) -> bool {
        self.conflicting_session_ids && std::ptr::eq(self.effective(), self)
    }

    /// The session a record that names none belongs to.
    pub(crate) fn file_session_id(&self) -> Option<String> {
        self.effective().session_id.clone()
    }

    /// The continuity evidence of the file's own session. Sessions named
    /// before the owner's first record are the history it inherited, and
    /// each becomes a fork target: the parent's file goes on without it.
    pub(crate) fn finish_continuity(
        &self,
        path: &Path,
    ) -> Option<crate::continuity::ContinuityEvidence> {
        let own = self.effective();
        let mut evidence = crate::continuity::finish_claude_fold(
            own.continuity.clone(),
            own.continuity_any,
            path,
        )?;
        if !std::ptr::eq(own, self) {
            let inherited = self
                .continuity
                .in_log_session_ids
                .iter()
                .take_while(|id| **id != evidence.session_id)
                .cloned()
                .collect::<Vec<_>>();
            for parent in inherited {
                crate::continuity::note_inherited_session(&mut evidence, parent);
            }
        }
        Some(evidence)
    }

    /// The session the transcript at `path` describes, or `None` when no
    /// record named one.
    pub(crate) fn finish(&self, path: &Path) -> Option<ClaudeSessionMeta> {
        self.effective().finish_own(path)
    }
}

/// The file stem of a primary transcript, which names the session it owns.
pub(crate) fn claude_primary_stem(path: &Path) -> Option<String> {
    if is_claude_sidecar_file(path) {
        return None;
    }
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
}

/// The session a record naming none belongs to, given the ids a transcript
/// names in file order: the file stem when the file names it, else the first.
pub(crate) fn claude_sessionless_owner(
    path: &Path,
    ids: impl IntoIterator<Item = String>,
) -> Option<String> {
    let stem = claude_primary_stem(path);
    let mut first = None;
    for id in ids {
        if stem.as_deref() == Some(id.as_str()) {
            return Some(id);
        }
        first.get_or_insert(id);
    }
    first
}

/// The session a transcript's records that name none belong to; see
/// [`claude_sessionless_owner`].
///
/// Read ahead of the record walk, and bounded in the case that matters: the
/// id is normally on the first record and is the file's stem, and this stops
/// there. Only a file opening on another session is read on for its stem.
pub(super) fn claude_file_session_id(path: &Path) -> Result<Option<String>> {
    let file = fs::File::open(path)
        .with_context(|| format!("reading claude transcript {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut raw = Vec::new();
    let stem = claude_primary_stem(path);
    let mut first: Option<String> = None;
    loop {
        raw.clear();
        if reader.read_until(b'\n', &mut raw)? == 0 {
            return Ok(first);
        }
        let Ok(line) = std::str::from_utf8(&raw) else {
            continue;
        };
        if let Some(id) = serde_json::from_str::<Value>(line.trim_end_matches(['\n', '\r']))
            .ok()
            .as_ref()
            .and_then(|value| value.get("sessionId"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            if stem.as_deref().is_none_or(|stem| stem == id) {
                return Ok(Some(id.to_string()));
            }
            first.get_or_insert_with(|| id.to_string());
        }
    }
}
