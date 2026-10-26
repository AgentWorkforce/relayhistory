//! Invariant checks for manifest v1, identical to the TypeScript
//! `validateManifest` in relayhistory-cloud's `@relayhistory/object-store`.

use sha2::{Digest, Sha256};

use super::types::{ChunkEntry, EpochReason, Manifest, Raw, MANIFEST_SCHEMA_VERSION};
use crate::layout::LAYOUT_VERSION;
use crate::{Error, ObjectKey, Result};

/// One violated rule, with the JSON path it applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub path: String,
    pub message: String,
}

impl std::fmt::Display for Issue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

/// `host/path` as `canonicalize_remote_url` produces it: a lowercase host
/// (or bracketed IPv6 literal) with no userinfo or port, then a non-empty
/// path, no scheme, no whitespace, no trailing `.git` or `/`.
pub(crate) fn canonical_remote_shape(url: &str) -> bool {
    let Some((host, path)) = url.split_once('/') else {
        return false;
    };
    let host_ok = if host.starts_with('[') {
        host.len() > 2 && host.ends_with(']') && !host[1..host.len() - 1].contains(['[', ']'])
    } else {
        !host.is_empty() && host == host.to_lowercase() && !host.contains([':', '@'])
    };
    host_ok
        && !path.is_empty()
        && !url.contains("://")
        && !url.chars().any(char::is_whitespace)
        && !path.ends_with('/')
        && !path.ends_with(".git")
}

/// Hash chain over the ordered chunk list (spec section `content_sha256`).
pub fn content_sha256<'a>(chunks: impl IntoIterator<Item = &'a ChunkEntry>) -> String {
    let mut h = [0u8; 32];
    for c in chunks {
        let mut hasher = Sha256::new();
        hasher.update(h);
        hasher.update(c.epoch.to_be_bytes());
        hasher.update(c.start_offset.to_be_bytes());
        hasher.update(c.bytes.to_be_bytes());
        // Validated separately; a malformed digest hashes as zeros.
        hasher.update(hex::decode(&c.sha256).unwrap_or_else(|_| vec![0; 32]));
        h = hasher.finalize().into();
    }
    hex::encode(h)
}

/// Parse and fully validate manifest JSON. `key`, when given, must be the
/// object key the manifest was read from. A newer `schema_v` is refused.
pub fn parse_manifest(body: &[u8], key: Option<&ObjectKey>) -> Result<Manifest> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| Error::InvalidArgument(format!("manifest is not JSON: {e}")))?;
    if let Some(v) = value.get("schema_v").and_then(|v| v.as_u64()) {
        if v > u64::from(MANIFEST_SCHEMA_VERSION) {
            return Err(Error::Unsupported {
                backend: "manifest",
                operation: format!(
                    "schema_v {v} (this reader understands {MANIFEST_SCHEMA_VERSION})"
                ),
            });
        }
    }
    let manifest: Manifest = serde_json::from_value(value)
        .map_err(|e| Error::InvalidArgument(format!("manifest schema: {e}")))?;
    ensure_valid(&manifest, key)?;
    Ok(manifest)
}

/// `Err(InvalidArgument)` listing every issue, or `Ok(())`.
pub fn ensure_valid(manifest: &Manifest, key: Option<&ObjectKey>) -> Result<()> {
    let issues = validate(manifest, key);
    if issues.is_empty() {
        return Ok(());
    }
    let list: Vec<String> = issues.iter().map(Issue::to_string).collect();
    Err(Error::InvalidArgument(format!(
        "invalid manifest: {}",
        list.join("; ")
    )))
}

/// Every problem found, not just the first.
pub fn validate(m: &Manifest, key: Option<&ObjectKey>) -> Vec<Issue> {
    let mut v = Checker::default();
    v.header(m);
    v.session(m);
    v.raw_totals(m);
    v.epochs(m);
    v.chunks(m);
    v.server(m);
    if let Some(key) = key {
        v.key(m, key);
    }
    v.issues
}

#[derive(Default)]
struct Checker {
    issues: Vec<Issue>,
}

impl Checker {
    fn issue(&mut self, path: impl Into<String>, message: impl Into<String>) {
        self.issues.push(Issue {
            path: path.into(),
            message: message.into(),
        });
    }

    fn timestamp(&mut self, path: &str, value: &str) {
        if chrono::DateTime::parse_from_rfc3339(value).is_err() {
            self.issue(path, format!("{value:?} is not RFC 3339"));
        }
    }

    fn hex64(&mut self, path: &str, value: &str) {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            self.issue(path, "must be 64 lowercase hex characters");
        }
    }

    fn header(&mut self, m: &Manifest) {
        if m.schema_v != MANIFEST_SCHEMA_VERSION {
            self.issue(
                "schema_v",
                format!("is {}, expected {MANIFEST_SCHEMA_VERSION}", m.schema_v),
            );
        }
        if m.layout != LAYOUT_VERSION {
            self.issue(
                "layout",
                format!("is {:?}, expected {LAYOUT_VERSION:?}", m.layout),
            );
        }
        if !is_uuid_v7(&m.trace_id) {
            self.issue("trace_id", "must be a UUIDv7");
        }
        if m.revision == 0 {
            self.issue("revision", "must be >= 1");
        }
        self.timestamp("created_at", &m.created_at);
        self.timestamp("updated_at", &m.updated_at);
    }

    fn session(&mut self, m: &Manifest) {
        let s = &m.session;
        for (path, value) in [
            ("session.workspace_id", &s.workspace_id),
            ("session.machine_id", &s.machine_id),
            ("session.harness", &s.harness),
            ("session.session_id", &s.session_id),
        ] {
            let ok = !value.is_empty()
                && value.len() <= 128
                && !value.starts_with('.')
                && value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
            if !ok {
                self.issue(
                    path,
                    "must be 1-128 chars of [A-Za-z0-9._-] not starting with `.`",
                );
            }
        }
        self.timestamp("session.started_at", &s.started_at);
        if let Some(t) = &s.ended_at {
            self.timestamp("session.ended_at", t);
        }
        if let Some(h) = &s.cwd_sha256 {
            self.hex64("session.cwd_sha256", h);
        }
        if let Some(repo) = &s.repo {
            if let Some(url) = &repo.remote_url {
                if !canonical_remote_shape(url) {
                    self.issue(
                        "session.repo.remote_url",
                        "must be a canonical `host/path` remote: no scheme, credentials, port or whitespace",
                    );
                }
            }
            if let Some(root) = &repo.root_commit {
                let ok = (root.len() == 40 || root.len() == 64)
                    && root.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'));
                if !ok {
                    self.issue(
                        "session.repo.root_commit",
                        "must be a lowercase 40- or 64-char hex commit id",
                    );
                }
            }
        }
        if m.raw.owner.is_empty() {
            self.issue("raw.owner", "must not be empty");
        }
        self.timestamp("raw.updated_at", &m.raw.updated_at);
    }

    fn raw_totals(&mut self, m: &Manifest) {
        let raw = &m.raw;
        if raw.chunk_count != raw.chunks.len() as u64 {
            self.issue(
                "raw.chunk_count",
                format!("is {}, chunks has {}", raw.chunk_count, raw.chunks.len()),
            );
        }
        self.total(
            "raw.raw_bytes",
            raw.raw_bytes,
            raw.chunks.iter().map(|c| c.bytes),
        );
        self.total(
            "raw.stored_bytes",
            raw.stored_bytes,
            raw.chunks.iter().map(|c| c.stored_bytes),
        );
        self.hex64("raw.content_sha256", &raw.content_sha256);
        if content_sha256(&raw.chunks) != raw.content_sha256 {
            self.issue(
                "raw.content_sha256",
                "does not match the hash chain over chunks",
            );
        }
    }

    /// `declared` must equal the sum of `values`, computed without overflow.
    fn total(&mut self, path: &str, declared: u64, mut values: impl Iterator<Item = u64>) {
        match values.try_fold(0u64, u64::checked_add) {
            Some(sum) if sum == declared => {}
            Some(_) => self.issue(path, "does not equal the sum over chunks"),
            None => self.issue(path, "the sum over chunks overflows u64"),
        }
    }

    fn epochs(&mut self, m: &Manifest) {
        let raw = &m.raw;
        if raw.epochs.is_empty() {
            self.issue("raw.epochs", "must record at least epoch 0");
        }
        for (i, e) in raw.epochs.iter().enumerate() {
            if e.epoch as usize != i {
                self.issue(
                    format!("raw.epochs[{i}].epoch"),
                    format!("is {}, expected {i}", e.epoch),
                );
            }
            if (i == 0) != (e.reason == EpochReason::Initial) {
                self.issue(
                    format!("raw.epochs[{i}].reason"),
                    "only epoch 0 is \"initial\"",
                );
            }
            self.timestamp(&format!("raw.epochs[{i}].opened_at"), &e.opened_at);
            // Every epoch, including one with no chunks yet, starts where
            // the earlier epochs' chunks end.
            let before = raw.chunks.iter().filter(|c| c.epoch < e.epoch).count() as u64;
            if e.first_seq != before {
                self.issue(
                    format!("raw.epochs[{i}].first_seq"),
                    format!(
                        "is {}, but {before} chunks precede epoch {}",
                        e.first_seq, e.epoch
                    ),
                );
            }
        }
        if raw.epoch as usize + 1 != raw.epochs.len() {
            self.issue(
                "raw.epoch",
                format!(
                    "is {}, raw.epochs has {} records",
                    raw.epoch,
                    raw.epochs.len()
                ),
            );
        }
    }

    fn chunks(&mut self, m: &Manifest) {
        let mut prev: Option<&ChunkEntry> = None;
        for (i, c) in m.raw.chunks.iter().enumerate() {
            self.chunk(&m.raw, i, c, prev);
            prev = Some(c);
        }
        self.next_offset(&m.raw);
    }

    fn chunk(&mut self, raw: &Raw, i: usize, c: &ChunkEntry, prev: Option<&ChunkEntry>) {
        let at = format!("raw.chunks[{i}]");
        if c.seq != i as u64 {
            self.issue(
                format!("{at}.seq"),
                format!("is {}, expected {i} (dense from 0)", c.seq),
            );
        }
        self.hex64(&format!("{at}.sha256"), &c.sha256);
        self.hex64(&format!("{at}.stored_sha256"), &c.stored_sha256);
        if let Some(t) = &c.first_ts {
            self.timestamp(&format!("{at}.first_ts"), t);
        }
        if let Some(t) = &c.last_ts {
            self.timestamp(&format!("{at}.last_ts"), t);
        }
        if c.sha256.get(..8) != Some(c.sha256_8.as_str()) {
            self.issue(format!("{at}.sha256_8"), "is not sha256[0..8]");
        }
        if c.bytes == 0 || c.stored_bytes == 0 {
            self.issue(at.clone(), "bytes and stored_bytes must be >= 1");
        }
        if c.epoch > raw.epoch {
            self.issue(
                format!("{at}.epoch"),
                format!("is {}, beyond raw.epoch", c.epoch),
            );
        }
        if prev.is_some_and(|p| c.epoch < p.epoch) {
            self.issue(format!("{at}.epoch"), "decreases");
        }
        let first_of_epoch = prev.is_none_or(|p| p.epoch != c.epoch);
        self.epoch_start(raw, &at, c, first_of_epoch);
        let expected = match prev {
            Some(p) if !first_of_epoch => p.start_offset.checked_add(p.bytes),
            _ => Some(0),
        };
        let Some(expected) = expected else {
            self.issue(
                format!("{at}.start_offset"),
                "previous chunk end overflows u64",
            );
            return;
        };
        if c.start_offset != expected {
            self.issue(
                format!("{at}.start_offset"),
                format!(
                    "is {}, expected {expected} (contiguous within epoch {})",
                    c.start_offset, c.epoch
                ),
            );
        }
    }

    /// The first chunk of each epoch carries that epoch's recorded `first_seq`.
    fn epoch_start(&mut self, raw: &Raw, at: &str, c: &ChunkEntry, first_of_epoch: bool) {
        let Some(rec) = raw.epochs.get(c.epoch as usize).filter(|_| first_of_epoch) else {
            return;
        };
        if rec.first_seq != c.seq {
            self.issue(
                format!("{at}.seq"),
                format!(
                    "opens epoch {} but raw.epochs[{}].first_seq is {}",
                    c.epoch, c.epoch, rec.first_seq
                ),
            );
        }
    }

    fn next_offset(&mut self, raw: &Raw) {
        let expected = raw
            .chunks
            .iter()
            .rev()
            .find(|c| c.epoch == raw.epoch)
            .map_or(Some(0), |c| c.start_offset.checked_add(c.bytes));
        let Some(expected) = expected else {
            self.issue("raw.next_offset", "end of the current epoch overflows u64");
            return;
        };
        if raw.next_offset != expected {
            self.issue(
                "raw.next_offset",
                format!(
                    "is {}, expected {expected} (end of the current epoch)",
                    raw.next_offset
                ),
            );
        }
    }

    fn server(&mut self, m: &Manifest) {
        let Some(s) = &m.server else { return };
        self.timestamp("server.updated_at", &s.updated_at);
        self.timestamp("server.lifecycle_at", &s.lifecycle_at);
        if let Some(n) = &s.normalized {
            if n.version == 0 {
                self.issue("server.normalized.version", "must be >= 1");
            }
            self.hex64(
                "server.normalized.source_content_sha256",
                &n.source_content_sha256,
            );
            if n.source_epoch > m.raw.epoch {
                self.issue("server.normalized.source_epoch", "is beyond raw.epoch");
            }
            self.timestamp("server.normalized.built_at", &n.built_at);
        }
        if let Some(t) = s.retention.as_ref().and_then(|r| r.delete_after.as_ref()) {
            self.timestamp("server.retention.delete_after", t);
        }
    }

    fn key(&mut self, m: &Manifest, key: &ObjectKey) {
        let s = &m.session;
        let date = chrono::DateTime::parse_from_rfc3339(&s.started_at)
            .map(|d| d.with_timezone(&chrono::Utc).format("%Y/%m/%d").to_string())
            .unwrap_or_default();
        let tail = format!(
            "{LAYOUT_VERSION}/ws={}/m={}/h={}/d={date}/s={}/manifest.json",
            s.workspace_id, s.machine_id, s.harness, s.session_id
        );
        let k = key.as_str();
        if !(k == tail || k.ends_with(&format!("/{tail}"))) {
            self.issue(
                "$key",
                format!("{k} does not match the manifest's session (expected …/{tail})"),
            );
        }
    }
}

fn is_uuid_v7(s: &str) -> bool {
    let b = s.as_bytes();
    s.len() == 36
        && [8, 13, 18, 23].iter().all(|&i| b[i] == b'-')
        && b[14] == b'7'
        && matches!(b[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b')
        && b.iter()
            .enumerate()
            .all(|(i, c)| [8, 13, 18, 23].contains(&i) || c.is_ascii_hexdigit())
}
