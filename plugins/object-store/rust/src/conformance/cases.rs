//! The suite's cases. Each takes a fresh per-case key prefix.

use std::time::Duration;

use bytes::Bytes;

use super::{key, random_bytes, read, CaseResultInner, Fetcher};
use crate::{
    sha256_hex, Error, MultipartOpts, ObjectKey, ObjectStore, PutOpts, META_CONTENT_SHA256,
    META_TRACE_ID,
};

pub(super) async fn put_get_head_metadata(
    store: &dyn ObjectStore,
    base: &ObjectKey,
) -> CaseResultInner {
    let k = key(base, "chunk.jsonl.zst")?;
    let body = crate::compress_zstd(b"{\"a\":1}\n{\"b\":2}\n").map_err(|e| e.to_string())?;
    let trace = "0192f0c4-7f00-7000-8000-00000000abcd";
    let ack = store
        .put(&k, body.clone(), PutOpts::create().with_trace_id(trace))
        .await
        .map_err(|e| format!("put: {e}"))?;
    ensure!(ack.created, "first put must report created");
    ensure!(ack.size == body.len() as u64, "ack size {}", ack.size);
    ensure!(ack.content_sha256 == sha256_hex(&body), "ack digest");
    let meta = store
        .head(&k)
        .await
        .map_err(|e| format!("head: {e}"))?
        .ok_or("head returned None after put")?;
    ensure!(meta.size == body.len() as u64, "head size {}", meta.size);
    ensure!(
        meta.metadata.get(META_TRACE_ID).map(String::as_str) == Some(trace),
        "trace-id metadata missing: {:?}",
        meta.metadata
    );
    ensure!(
        meta.metadata.get(META_CONTENT_SHA256) == Some(&ack.content_sha256),
        "content-sha256 metadata missing: {:?}",
        meta.metadata
    );
    let got = read(store, &k, None).await?;
    ensure!(got == body, "body round trip");
    let raw = crate::decompress_zstd(&got).map_err(|e| e.to_string())?;
    ensure!(raw == b"{\"a\":1}\n{\"b\":2}\n", "zstd round trip");
    Ok(())
}

pub(super) async fn not_found(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "missing")?;
    ensure!(
        store.head(&k).await.map_err(|e| e.to_string())?.is_none(),
        "head of missing object must be None"
    );
    match store.get(&k, None).await {
        Err(Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(format!("get missing: expected NotFound, got {e}")),
        Ok(_) => Err("get missing: expected NotFound, got a stream".into()),
    }
}

pub(super) async fn create_idempotent(
    store: &dyn ObjectStore,
    base: &ObjectKey,
) -> CaseResultInner {
    let k = key(base, "0000000001-0-abcdef12.jsonl.zst")?;
    let body = random_bytes(4096);
    let first = store
        .put(&k, body.clone(), PutOpts::create())
        .await
        .map_err(|e| format!("first put: {e}"))?;
    let again = store
        .put(&k, body.clone(), PutOpts::create())
        .await
        .map_err(|e| format!("re-put of identical content must succeed: {e}"))?;
    ensure!(
        first.created && !again.created,
        "created flags {first:?} {again:?}"
    );
    ensure!(again.content_sha256 == first.content_sha256, "digest drift");
    Ok(())
}

pub(super) async fn create_conflict(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "object")?;
    store
        .put(&k, random_bytes(64), PutOpts::create())
        .await
        .map_err(|e| format!("first put: {e}"))?;
    // Same length, different bytes: a size-only comparison would miss it.
    match store.put(&k, random_bytes(64), PutOpts::create()).await {
        Err(Error::AlreadyExists { .. }) => Ok(()),
        other => Err(format!("expected AlreadyExists, got {other:?}")),
    }
}

pub(super) async fn compare_and_swap(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "manifest.json")?;
    let v1 = store
        .put(&k, Bytes::from_static(b"{\"v\":1}"), PutOpts::create())
        .await
        .map_err(|e| format!("create manifest: {e}"))?;
    let head = store
        .head(&k)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("manifest vanished")?;
    let v2 = store
        .put(
            &k,
            Bytes::from_static(b"{\"v\":2}"),
            PutOpts::replace(head.version.clone()),
        )
        .await
        .map_err(|e| format!("CAS with current version (from head): {e}"))?;
    match store
        .put(
            &k,
            Bytes::from_static(b"{\"v\":3}"),
            PutOpts::replace(v1.version.clone()),
        )
        .await
    {
        Err(Error::PreconditionFailed { .. }) => {}
        other => {
            return Err(format!(
                "stale CAS: expected PreconditionFailed, got {other:?}"
            ))
        }
    }
    store
        .put(
            &k,
            Bytes::from_static(b"{\"v\":3}"),
            PutOpts::replace(v2.version),
        )
        .await
        .map_err(|e| format!("CAS with version from put ack: {e}"))?;
    ensure!(
        read(store, &k, None).await? == b"{\"v\":3}".as_slice(),
        "final manifest body"
    );
    match store
        .put(
            &key(base, "absent.json")?,
            Bytes::from_static(b"{}"),
            PutOpts::replace(v1.version),
        )
        .await
    {
        Err(Error::PreconditionFailed { .. }) => Ok(()),
        other => Err(format!(
            "CAS on missing object: expected PreconditionFailed, got {other:?}"
        )),
    }
}

/// Racing writers holding the same version: exactly one may win, the rest
/// must see `PreconditionFailed` (the manifest's lost-update guarantee).
pub(super) async fn concurrent_cas(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "manifest.json")?;
    let v1 = store
        .put(&k, Bytes::from_static(b"{\"v\":1}"), PutOpts::create())
        .await
        .map_err(|e| format!("create manifest: {e}"))?;
    let writers = 8;
    let attempts = (0..writers).map(|i| {
        let body = Bytes::from(format!("{{\"writer\":{i}}}"));
        store.put(&k, body, PutOpts::replace(v1.version.clone()))
    });
    let results = futures::future::join_all(attempts).await;
    let winners = results.iter().filter(|r| r.is_ok()).count();
    let unexpected: Vec<_> = results
        .iter()
        .filter_map(|r| match r {
            Ok(_) | Err(Error::PreconditionFailed { .. }) => None,
            Err(e) => Some(e.to_string()),
        })
        .collect();
    ensure!(unexpected.is_empty(), "unexpected errors: {unexpected:?}");
    ensure!(
        winners == 1,
        "{winners} of {writers} racing CAS writes succeeded"
    );
    Ok(())
}

pub(super) async fn digest_checked(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "object")?;
    let opts = PutOpts {
        content_sha256: Some(sha256_hex(b"something else")),
        ..PutOpts::create()
    };
    match store.put(&k, Bytes::from_static(b"payload"), opts).await {
        Err(Error::ContentMismatch { .. }) => {}
        other => return Err(format!("expected ContentMismatch, got {other:?}")),
    }
    ensure!(
        store.head(&k).await.map_err(|e| e.to_string())?.is_none(),
        "mismatched body must not be written"
    );
    let opts = PutOpts {
        content_sha256: Some(sha256_hex(b"payload").to_uppercase()),
        ..PutOpts::create()
    };
    store
        .put(&k, Bytes::from_static(b"payload"), opts)
        .await
        .map_err(|e| format!("matching digest (any case) must be accepted: {e}"))?;
    Ok(())
}

pub(super) async fn range_get(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "object")?;
    let body = random_bytes(10_000);
    store
        .put(&k, body.clone(), PutOpts::create())
        .await
        .map_err(|e| format!("put: {e}"))?;
    for range in [0..1, 10..20, 9_000..10_000, 4_096..8_192] {
        let got = read(store, &k, Some(range.clone())).await?;
        ensure!(
            got == body.slice(range.start as usize..range.end as usize),
            "range {range:?} returned {} bytes",
            got.len()
        );
    }
    match store.get(&k, Some(5..5)).await {
        Err(Error::InvalidArgument(_)) => Ok(()),
        other => Err(format!(
            "empty range: expected InvalidArgument, got {:?}",
            other.err()
        )),
    }
}

pub(super) async fn list_pagination(
    store: &dyn ObjectStore,
    base: &ObjectKey,
    page_size: usize,
) -> CaseResultInner {
    let dir = key(base, "raw")?;
    let count = page_size * 2 + 1;
    let mut expected = Vec::new();
    for seq in 0..count {
        let k = key(&dir, &format!("{seq:010}-0-00000000.jsonl.zst"))?;
        store
            .put(&k, random_bytes(16), PutOpts::create())
            .await
            .map_err(|e| format!("put {k}: {e}"))?;
        expected.push(k);
    }
    // Shares the string prefix `raw` but not the `raw/` segment.
    store
        .put(
            &key(base, "raw-sibling")?,
            random_bytes(1),
            PutOpts::create(),
        )
        .await
        .map_err(|e| format!("put sibling: {e}"))?;

    let mut seen = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let page = store
            .list(&dir, cursor)
            .await
            .map_err(|e| format!("list page {pages}: {e}"))?;
        pages += 1;
        ensure!(
            page.items.len() <= page_size,
            "page of {} > {page_size}",
            page.items.len()
        );
        seen.extend(page.items.into_iter().map(|m| m.key));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
        ensure!(pages <= count + 1, "pagination does not terminate");
    }
    ensure!(seen == expected, "listed {seen:?}\nexpected {expected:?}");
    ensure!(
        pages >= 3,
        "expected ≥3 pages at page size {page_size}, got {pages}"
    );
    Ok(())
}

pub(super) async fn presign_get(
    store: &dyn ObjectStore,
    base: &ObjectKey,
    fetch: &Fetcher,
) -> CaseResultInner {
    let k = key(base, "object.json")?;
    let body = Bytes::from_static(b"{\"presigned\":true}");
    store
        .put(&k, body.clone(), PutOpts::create())
        .await
        .map_err(|e| format!("put: {e}"))?;
    let url = store
        .presign_get(&k, Duration::from_secs(300))
        .await
        .map_err(|e| format!("presign: {e}"))?;
    let got = fetch(url.clone())
        .await
        .map_err(|e| format!("fetch {url}: {e}"))?;
    ensure!(got == body, "presigned body mismatch");
    match store.presign_get(&k, Duration::ZERO).await {
        Err(Error::InvalidArgument(_)) => {}
        other => return Err(format!("zero ttl: expected InvalidArgument, got {other:?}")),
    }
    match store
        .presign_get(&k, crate::MAX_PRESIGN_TTL + Duration::from_secs(1))
        .await
    {
        Err(Error::InvalidArgument(_)) => Ok(()),
        other => Err(format!(
            "ttl > max: expected InvalidArgument, got {other:?}"
        )),
    }
}

const PART: usize = 5 * 1024 * 1024;

pub(super) async fn multipart(store: &dyn ObjectStore, base: &ObjectKey) -> CaseResultInner {
    let k = key(base, "large.sqlite-rows.zst")?;
    let parts = [random_bytes(PART), random_bytes(PART), random_bytes(1234)];
    let whole: Bytes = parts.concat().into();
    let sha = sha256_hex(&whole);
    let mut upload = store
        .multipart(
            &k,
            MultipartOpts {
                content_sha256: Some(sha.clone()),
                metadata: [(META_TRACE_ID.to_string(), "t-1".to_string())].into(),
                content_type: None,
            },
        )
        .await
        .map_err(|e| format!("start: {e}"))?;
    for part in parts {
        upload
            .put_part(part)
            .await
            .map_err(|e| format!("part: {e}"))?;
    }
    let ack = upload
        .complete()
        .await
        .map_err(|e| format!("complete: {e}"))?;
    ensure!(
        ack.size == whole.len() as u64 && ack.content_sha256 == sha,
        "ack {ack:?}"
    );
    let meta = store
        .head(&k)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("multipart object missing")?;
    ensure!(meta.size == whole.len() as u64, "size {}", meta.size);
    ensure!(
        meta.content_sha256() == Some(sha.as_str()),
        "digest metadata {:?}",
        meta.metadata
    );
    ensure!(
        meta.trace_id() == Some("t-1"),
        "trace metadata {:?}",
        meta.metadata
    );
    let tail = read(store, &k, Some(PART as u64 * 2 - 10..PART as u64 * 2 + 10)).await?;
    ensure!(
        tail == whole.slice(PART * 2 - 10..PART * 2 + 10),
        "range across part boundary"
    );
    ensure!(read(store, &k, None).await? == whole, "multipart body");

    let aborted = key(base, "aborted")?;
    let mut upload = store
        .multipart(&aborted, MultipartOpts::default())
        .await
        .map_err(|e| format!("start (abort): {e}"))?;
    upload
        .put_part(random_bytes(PART))
        .await
        .map_err(|e| format!("part: {e}"))?;
    upload.abort().await.map_err(|e| format!("abort: {e}"))?;
    ensure!(
        store
            .head(&aborted)
            .await
            .map_err(|e| e.to_string())?
            .is_none(),
        "aborted upload must not create an object"
    );
    Ok(())
}

pub(super) async fn multipart_mismatch(
    store: &dyn ObjectStore,
    base: &ObjectKey,
) -> CaseResultInner {
    let k = key(base, "object")?;
    let mut upload = store
        .multipart(
            &k,
            MultipartOpts {
                content_sha256: Some(sha256_hex(b"not this")),
                ..MultipartOpts::default()
            },
        )
        .await
        .map_err(|e| format!("start: {e}"))?;
    upload
        .put_part(random_bytes(1024))
        .await
        .map_err(|e| format!("part: {e}"))?;
    match upload.complete().await {
        Err(Error::ContentMismatch { .. }) => {}
        other => return Err(format!("expected ContentMismatch, got {other:?}")),
    }
    ensure!(
        store.head(&k).await.map_err(|e| e.to_string())?.is_none(),
        "mismatched multipart must not be committed"
    );
    Ok(())
}
