//! The shared conformance suite every [`ObjectStore`] implementation must
//! pass. `tests/conformance.rs` runs it against `FsStore` always and against
//! MinIO, fake-gcs-server and Azurite when their endpoints are configured.
//!
//! Each case returns a [`CaseOutcome`]; a harness may mark a case as a known
//! emulator gap with [`SuiteOptions::skip`], which is reported, never hidden.

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use rand::RngCore;
use url::Url;

use crate::{ObjectKey, ObjectStore};

/// Every case in the suite, in run order.
pub const CASES: &[&str] = &[
    "put_get_head_metadata",
    "not_found",
    "create_is_idempotent_for_same_content",
    "create_conflicts_on_different_content",
    "if_match_compare_and_swap",
    "if_match_concurrent_single_winner",
    "content_sha256_verified_before_upload",
    "range_get",
    "list_pagination",
    "presign_get",
    "multipart",
    "multipart_digest_mismatch_aborts",
];

/// Fetches a presigned URL (HTTP for clouds, `file://` for `FsStore`).
pub type Fetcher = Box<dyn Fn(Url) -> BoxFuture<'static, Result<Bytes, String>> + Send + Sync>;

pub struct SuiteOptions {
    /// Unique per run so concurrent runs never collide.
    pub prefix: ObjectKey,
    /// The page size the store under test was built with.
    pub page_size: usize,
    pub fetch: Fetcher,
    /// `(case, reason)` pairs to skip: documented emulator gaps only.
    pub skip: Vec<(&'static str, &'static str)>,
}

#[derive(Debug)]
pub enum CaseOutcome {
    Passed,
    Skipped(&'static str),
    Failed(String),
}

#[derive(Debug)]
pub struct CaseResult {
    pub name: &'static str,
    pub outcome: CaseOutcome,
}

type CaseResultInner = Result<(), String>;

macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

mod cases;
use cases::*;

/// Run every case and return per-case outcomes.
pub async fn run_suite(store: &dyn ObjectStore, opts: &SuiteOptions) -> Vec<CaseResult> {
    let mut results = Vec::new();
    for &name in CASES {
        if let Some((_, reason)) = opts.skip.iter().find(|(case, _)| *case == name) {
            results.push(CaseResult {
                name,
                outcome: CaseOutcome::Skipped(reason),
            });
            continue;
        }
        let outcome = match run_case(name, store, opts).await {
            Ok(()) => CaseOutcome::Passed,
            Err(msg) => CaseOutcome::Failed(msg),
        };
        results.push(CaseResult { name, outcome });
    }
    results
}

async fn run_case(name: &str, store: &dyn ObjectStore, opts: &SuiteOptions) -> CaseResultInner {
    let base = &key(&opts.prefix, name)?;
    match name {
        "put_get_head_metadata" => put_get_head_metadata(store, base).await,
        "not_found" => not_found(store, base).await,
        "create_is_idempotent_for_same_content" => create_idempotent(store, base).await,
        "create_conflicts_on_different_content" => create_conflict(store, base).await,
        "if_match_compare_and_swap" => compare_and_swap(store, base).await,
        "if_match_concurrent_single_winner" => concurrent_cas(store, base).await,
        "content_sha256_verified_before_upload" => digest_checked(store, base).await,
        "range_get" => range_get(store, base).await,
        "list_pagination" => list_pagination(store, base, opts.page_size).await,
        "presign_get" => presign_get(store, base, &opts.fetch).await,
        "multipart" => multipart(store, base).await,
        "multipart_digest_mismatch_aborts" => multipart_mismatch(store, base).await,
        other => Err(format!("unknown case {other}")),
    }
}

fn key(base: &ObjectKey, rest: &str) -> Result<ObjectKey, String> {
    base.join(rest).map_err(|e| e.to_string())
}

fn random_bytes(len: usize) -> Bytes {
    let mut buf = vec![0u8; len];
    rand::rng().fill_bytes(&mut buf);
    buf.into()
}

async fn read(
    store: &dyn ObjectStore,
    key: &ObjectKey,
    range: Option<std::ops::Range<u64>>,
) -> Result<Bytes, String> {
    let chunks: Vec<Bytes> = store
        .get(key, range)
        .await
        .map_err(|e| format!("get {key}: {e}"))?
        .try_collect()
        .await
        .map_err(|e| format!("read {key}: {e}"))?;
    Ok(chunks.concat().into())
}
