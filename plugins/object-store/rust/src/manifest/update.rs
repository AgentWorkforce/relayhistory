//! The compare-and-swap write loop shared by both manifest writers.
//!
//! A writer never touches the other side's fields: its edit returns only its
//! own sections ([`Edit`]), and the loop splices them into the copy it just
//! read. Everything else, including fields this version does not know, is
//! carried through unchanged.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use serde_json::{Map, Value};

use super::types::{Manifest, Raw, Server, Session, Writer};
use super::validate::{ensure_valid, parse_manifest};
use crate::{Error, ObjectKey, ObjectStore, ObjectVersion, PutOpts, Result};

/// What a writer wants to change, computed from the current manifest.
#[derive(Debug, Clone)]
pub enum Edit {
    /// Nothing to write.
    Unchanged,
    /// First write of a session (client only). Fails over to a re-read if a
    /// manifest already exists.
    Create(Box<Manifest>),
    /// Replace the client-owned sections.
    Client {
        session: Box<Session>,
        writer: Box<Writer>,
        raw: Box<Raw>,
    },
    /// Replace the server-owned section.
    Server(Box<Server>),
}

#[derive(Debug, Clone)]
pub struct UpdateOpts {
    /// Total attempts, including the first (spec: 8).
    pub max_attempts: u32,
    /// Exponential backoff with full jitter: `base * 2^n`, capped.
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
}

impl Default for UpdateOpts {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            backoff_base: Duration::from_millis(100),
            backoff_cap: Duration::from_secs(5),
        }
    }
}

/// Result of [`update_manifest`].
#[derive(Debug, Clone)]
pub struct Updated {
    /// The manifest as now stored (or as read, when nothing was written).
    pub manifest: Option<Manifest>,
    /// Its object version, for the next compare-and-swap.
    pub version: Option<ObjectVersion>,
    pub written: bool,
    /// Attempts used (1 when there was no contention).
    pub attempts: u32,
}

/// Read, apply `edit` to the fresh copy, write with compare-and-swap, and on a
/// lost race re-read and re-apply. `edit` may run several times and must be
/// a pure function of the manifest it is given.
///
/// Errors: [`Error::ManifestContended`] after `max_attempts` lost races
/// (retryable: wait for the next tick or re-queue); `InvalidArgument` when the
/// stored manifest or the edited result is invalid; backend errors as-is.
pub async fn update_manifest<F>(
    store: &dyn ObjectStore,
    key: &ObjectKey,
    opts: &UpdateOpts,
    mut edit: F,
) -> Result<Updated>
where
    F: FnMut(Option<&Manifest>) -> Result<Edit>,
{
    let attempts = opts.max_attempts.max(1);
    for attempt in 1..=attempts {
        let current = read(store, key).await?;
        let (doc, typed, version) = match current {
            Some((doc, typed, version)) => (Some(doc), Some(typed), Some(version)),
            None => (None, None, None),
        };
        let (next, create) = match edit(typed.as_ref())? {
            Edit::Unchanged => {
                return Ok(Updated {
                    manifest: typed,
                    version,
                    written: false,
                    attempts: attempt,
                })
            }
            Edit::Create(manifest) => {
                if doc.is_some() {
                    return Err(Error::InvalidArgument(format!(
                        "{key}: Edit::Create but a manifest exists; edit the existing one"
                    )));
                }
                (create_doc(*manifest)?, true)
            }
            Edit::Client {
                session,
                writer,
                raw,
            } => {
                let mut doc = require(doc, key)?;
                put_section(&mut doc, "session", &*session)?;
                put_section(&mut doc, "writer", &*writer)?;
                put_section(&mut doc, "raw", &*raw)?;
                bump(&mut doc);
                (doc, false)
            }
            Edit::Server(server) => {
                let mut doc = require(doc, key)?;
                put_section(&mut doc, "server", &*server)?;
                bump(&mut doc);
                (doc, false)
            }
        };
        let body = serde_json::to_vec(&next).map_err(|e| Error::InvalidArgument(e.to_string()))?;
        // Never write a manifest a reader would reject.
        let written = parse_manifest(&body, Some(key))?;
        let put = PutOpts {
            content_type: Some("application/json".into()),
            ..match (create, version) {
                (true, _) | (false, None) => PutOpts::create(),
                (false, Some(v)) => PutOpts::replace(v),
            }
        }
        .with_trace_id(written.trace_id.clone());
        match store.put(key, Bytes::from(body), put).await {
            Ok(ack) => {
                return Ok(Updated {
                    manifest: Some(written),
                    version: Some(ack.version),
                    written: true,
                    attempts: attempt,
                })
            }
            Err(Error::PreconditionFailed { .. } | Error::AlreadyExists { .. }) => {
                tracing::debug!(%key, attempt, "manifest write lost a race; re-reading");
                if attempt < attempts {
                    tokio::time::sleep(backoff(opts, attempt)).await;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Err(Error::ManifestContended {
        key: key.to_string(),
        attempts,
    })
}

type Doc = Map<String, Value>;

async fn read(
    store: &dyn ObjectStore,
    key: &ObjectKey,
) -> Result<Option<(Doc, Manifest, ObjectVersion)>> {
    let got = match store.get_object(key, None).await {
        Ok(got) => got,
        Err(Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };
    let chunks: Vec<Bytes> = got.body.try_collect().await?;
    let body = chunks.concat();
    let typed = parse_manifest(&body, Some(key))?;
    let doc: Doc = serde_json::from_slice(&body)
        .map_err(|e| Error::InvalidArgument(format!("manifest is not a JSON object: {e}")))?;
    Ok(Some((doc, typed, got.meta.version)))
}

fn require(doc: Option<Doc>, key: &ObjectKey) -> Result<Doc> {
    doc.ok_or_else(|| Error::NotFound {
        key: key.to_string(),
    })
}

fn create_doc(mut manifest: Manifest) -> Result<Doc> {
    manifest.revision = 1;
    ensure_valid(&manifest, None)?;
    match serde_json::to_value(&manifest).map_err(|e| Error::InvalidArgument(e.to_string()))? {
        Value::Object(doc) => Ok(doc),
        _ => Err(Error::InvalidArgument(
            "manifest did not serialize to an object".into(),
        )),
    }
}

/// Fields this version knows in each writer-owned section. A key in the
/// stored section that is *not* listed here was added by a newer writer and
/// is carried over even if the edit was built from a fresh struct (whose
/// `extra` is empty); known keys follow the edit, so clearing an optional
/// field still works. To delete an unknown key it owns, a writer sets it to
/// `Value::Null` in the section's `extra`.
fn known_fields(section: &str) -> &'static [&'static str] {
    match section {
        "session" => &[
            "workspace_id",
            "machine_id",
            "harness",
            "session_id",
            "started_at",
            "ended_at",
            "harness_version",
            "cwd_sha256",
            "parent_session_id",
        ],
        "writer" => &["delivery_instance_id", "plugin_version", "host_version"],
        "raw" => &[
            "owner",
            "state",
            "updated_at",
            "format",
            "compression",
            "chunk_count",
            "raw_bytes",
            "stored_bytes",
            "content_sha256",
            "epoch",
            "epochs",
            "next_offset",
            "chunks",
        ],
        "server" => &[
            "updated_at",
            "normalized",
            "lifecycle",
            "lifecycle_at",
            "lifecycle_reason",
            "retention",
        ],
        _ => &[],
    }
}

fn put_section<T: serde::Serialize>(doc: &mut Doc, name: &str, section: &T) -> Result<()> {
    let mut value =
        serde_json::to_value(section).map_err(|e| Error::InvalidArgument(e.to_string()))?;
    if let Value::Object(next) = &mut value {
        let known = known_fields(name);
        if let Some(Value::Object(stored)) = doc.get(name) {
            for (k, v) in stored {
                if !known.contains(&k.as_str()) && !next.contains_key(k) {
                    next.insert(k.clone(), v.clone());
                }
            }
        }
        // An unknown key set to `null` in `extra` is an explicit deletion:
        // the owner clears it, rather than having it carried over.
        next.retain(|k, v| known.contains(&k.as_str()) || !v.is_null());
    }
    doc.insert(name.to_string(), value);
    Ok(())
}

/// `revision + 1` and `updated_at = now`, whoever writes.
fn bump(doc: &mut Doc) {
    let revision = doc.get("revision").and_then(Value::as_u64).unwrap_or(0) + 1;
    doc.insert("revision".into(), revision.into());
    doc.insert("updated_at".into(), now_rfc3339().into());
}

pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Full jitter: uniform in `[0, min(cap, base * 2^(attempt-1))]`.
fn backoff(opts: &UpdateOpts, attempt: u32) -> Duration {
    let ceiling = opts
        .backoff_base
        .saturating_mul(1u32 << (attempt - 1).min(16))
        .min(opts.backoff_cap);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    // Cheap jitter source; uniformity is not security-relevant here.
    let fraction = f64::from(nanos.wrapping_mul(2_654_435_761) % 1_000_000) / 1_000_000.0;
    ceiling.mul_f64(fraction)
}
