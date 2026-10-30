//! Two-writer manifest protocol end to end: a client and a server writing the
//! same manifest.json concurrently through `update_manifest`. Runs on
//! `FsStore` always and on MinIO / Azurite when their variables are set (the
//! same ones the conformance suite uses). fake-gcs-server is skipped: it does
//! not enforce generation preconditions.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::NaiveDate;
use relayhistory_object_store::layout::{RawFormat, SessionLocation};
use relayhistory_object_store::manifest::client::{
    apply_chunks, classify, new_manifest, open_epoch, plan_chunks, resume_epoch, ClientAck,
    NewChunk, NewManifest, RemoteState,
};
use relayhistory_object_store::manifest::server::{
    pointer_fields, with_lifecycle, with_normalized,
};
use relayhistory_object_store::manifest::*;
use relayhistory_object_store::*;

const OWNER: &str = "dest_objstore_01";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn stores() -> Vec<(
    &'static str,
    Arc<dyn ObjectStore>,
    Option<tempfile::TempDir>,
)> {
    let dir = tempfile::tempdir().unwrap();
    let mut out: Vec<(
        &'static str,
        Arc<dyn ObjectStore>,
        Option<tempfile::TempDir>,
    )> = vec![(
        "fs",
        Arc::new(FsStore::new(FsConfig::new(dir.path())).unwrap()),
        Some(dir),
    )];
    if let Some(endpoint) = env("RH_OBJSTORE_S3_ENDPOINT") {
        let config = S3Config {
            endpoint: Some(endpoint),
            allow_http: true,
            credentials: S3Credentials::Static {
                access_key_id: env("RH_OBJSTORE_S3_ACCESS_KEY_ID").unwrap(),
                secret_access_key: env("RH_OBJSTORE_S3_SECRET_ACCESS_KEY").unwrap(),
                session_token: None,
            },
            ..S3Config::new("rh-conformance", "us-east-1")
        };
        out.push(("s3", Arc::new(S3Store::new(config).unwrap()), None));
    }
    if let Some(endpoint) = env("RH_OBJSTORE_AZURITE_ENDPOINT") {
        let config = AzureBlobConfig {
            endpoint: Some(endpoint),
            use_emulator: true,
            credentials: AzureCredentials::AccessKey(
                // Azurite's published development key (not a secret).
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
            ),
            ..AzureBlobConfig::new("devstoreaccount1", "rh-conformance")
        };
        out.push((
            "azure",
            Arc::new(AzureBlobStore::new(config).unwrap()),
            None,
        ));
    }
    out
}

fn location(backend: &str) -> SessionLocation {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    SessionLocation {
        prefix: Some(format!("manifest-tests/{backend}")),
        workspace_id: "ws_1".into(),
        machine_id: "mach-1".into(),
        harness: "claude".into(),
        start_date: NaiveDate::from_ymd_opt(2026, 10, 25).unwrap(),
        session_id: format!("s-{nanos}"),
    }
}

fn first_manifest(loc: &SessionLocation) -> Manifest {
    new_manifest(NewManifest {
        trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18".into(),
        session: Session {
            workspace_id: loc.workspace_id.clone(),
            machine_id: loc.machine_id.clone(),
            harness: loc.harness.clone(),
            session_id: loc.session_id.clone(),
            started_at: "2026-10-25T18:01:58.000Z".into(),
            ended_at: None,
            harness_version: None,
            cwd_sha256: None,
            parent_session_id: None,
            extra: Default::default(),
        },
        writer: Writer {
            delivery_instance_id: OWNER.into(),
            plugin_version: "test".into(),
            host_version: None,
            extra: Default::default(),
        },
        format: RawFormat::Jsonl,
    })
}

fn chunk(n: u64, start: u64, bytes: u64) -> NewChunk {
    NewChunk {
        start_offset: start,
        bytes,
        stored_bytes: bytes / 3 + 1,
        sha256: sha256_hex(format!("chunk-{n}").as_bytes()),
        stored_sha256: sha256_hex(format!("stored-{n}").as_bytes()),
        stored_raw_bytes: Some(bytes),
        raw_sha256: Some(sha256_hex(format!("scrubbed-{n}").as_bytes())),
        records: Some(1),
        first_ts: None,
        last_ts: None,
    }
}

fn fast() -> UpdateOpts {
    UpdateOpts {
        max_attempts: 50,
        backoff_base: Duration::from_millis(2),
        backoff_cap: Duration::from_millis(40),
    }
}

/// Client edit: append one chunk to whatever raw is current.
fn append_one(m: Option<&Manifest>, n: u64) -> Result<Edit> {
    let m = m.expect("manifest exists");
    let mut raw = m.raw.clone();
    let planned = plan_chunks(&raw, vec![chunk(n, raw.next_offset, 100 + n)])?;
    apply_chunks(&mut raw, &planned)?;
    Ok(Edit::client(m.session.clone(), m.writer.clone(), raw))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_and_server_write_concurrently_without_losing_updates() {
    for (backend, store, _dir) in stores() {
        let loc = location(backend);
        let key = loc.manifest().unwrap();
        let created = update_manifest(store.as_ref(), &key, &fast(), |m| {
            assert!(m.is_none());
            Ok(Edit::Create(Box::new(first_manifest(&loc))))
        })
        .await
        .unwrap();
        assert_eq!(created.manifest.as_ref().unwrap().revision, 1);

        const ROUNDS: u64 = 6;
        let client = {
            let (store, key) = (store.clone(), key.clone());
            tokio::spawn(async move {
                for n in 0..ROUNDS {
                    update_manifest(store.as_ref(), &key, &fast(), |m| append_one(m, n))
                        .await
                        .unwrap();
                }
            })
        };
        let server = {
            let (store, key) = (store.clone(), key.clone());
            tokio::spawn(async move {
                for n in 0..ROUNDS {
                    update_manifest(store.as_ref(), &key, &fast(), |m| {
                        let m = m.unwrap();
                        let lifecycle = if n % 2 == 0 {
                            Lifecycle::Idle
                        } else {
                            Lifecycle::Active
                        };
                        let mut s = with_lifecycle(m, lifecycle, Some(format!("round {n}")))
                            .unwrap_or_else(|| server::server_section(m));
                        s.normalized = with_normalized(m, m, 1, None).normalized;
                        s.lifecycle_reason = Some(format!("round {n}"));
                        Ok(Edit::server(s))
                    })
                    .await
                    .unwrap();
                }
            })
        };
        client.await.unwrap();
        server.await.unwrap();

        let body: Vec<bytes::Bytes> =
            futures::TryStreamExt::try_collect(store.get(&key, None).await.unwrap())
                .await
                .unwrap();
        let m = parse_manifest(&body.concat(), Some(&key)).unwrap();
        assert_eq!(
            m.raw.chunk_count, ROUNDS,
            "[{backend}] every client append survived"
        );
        assert_eq!(
            m.server.as_ref().unwrap().lifecycle_reason.as_deref(),
            Some(format!("round {}", ROUNDS - 1).as_str()),
            "[{backend}] the server's last write survived"
        );
        assert_eq!(
            m.revision,
            1 + 2 * ROUNDS,
            "[{backend}] one revision per write, none lost"
        );
        let p = pointer_fields(&m);
        assert_eq!(p.chunk_count, ROUNDS);
        eprintln!(
            "[{backend}] ok: revision {}, normalized_stale {}",
            m.revision, p.normalized_stale
        );
    }
}

#[tokio::test]
async fn unknown_fields_and_the_other_section_are_preserved() {
    for (backend, store, _dir) in stores() {
        let loc = location(backend);
        let key = loc.manifest().unwrap();
        // A newer writer's manifest: an unknown top-level field and unknown
        // fields inside the server section.
        let mut doc = serde_json::to_value(first_manifest(&loc)).unwrap();
        doc["future_top_level"] = serde_json::json!({"x": 1});
        doc["server"] = serde_json::json!({
            "updated_at": "2026-10-25T19:00:00.000Z",
            "normalized": null,
            "lifecycle": "idle",
            "lifecycle_at": "2026-10-25T19:00:00.000Z",
            "future_server_field": [1, 2, 3]
        });
        store
            .put(
                &key,
                bytes::Bytes::from(serde_json::to_vec(&doc).unwrap()),
                PutOpts::create(),
            )
            .await
            .unwrap();

        update_manifest(store.as_ref(), &key, &fast(), |m| append_one(m, 0))
            .await
            .unwrap();

        let body: Vec<bytes::Bytes> =
            futures::TryStreamExt::try_collect(store.get(&key, None).await.unwrap())
                .await
                .unwrap();
        let after: serde_json::Value = serde_json::from_slice(&body.concat()).unwrap();
        assert_eq!(
            after["future_top_level"],
            serde_json::json!({"x": 1}),
            "[{backend}]"
        );
        assert_eq!(
            after["server"], doc["server"],
            "[{backend}] client never touches server"
        );
        assert_eq!(after["revision"], 2);
        assert_eq!(after["raw"]["chunk_count"], 1);
    }
}

#[test]
fn stale_writers_are_detected() {
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10), chunk(1, 10, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    m.revision = 3;
    let ack = ClientAck::of(&m);

    assert_eq!(classify(&m, OWNER, Some(&ack)), RemoteState::Unchanged);

    let mut extended = m.clone();
    let more = plan_chunks(&extended.raw, vec![chunk(2, 20, 5)]).unwrap();
    apply_chunks(&mut extended.raw, &more).unwrap();
    extended.revision = 4;
    assert_eq!(
        classify(&extended, OWNER, Some(&ack)),
        RemoteState::Extended {
            new_chunks: 1,
            new_epochs: 0
        }
    );

    assert!(matches!(
        classify(&m, "someone-else", Some(&ack)),
        RemoteState::Fenced { .. }
    ));

    let mut rolled = m.clone();
    rolled.revision = 2;
    assert!(matches!(
        classify(&rolled, OWNER, Some(&ack)),
        RemoteState::RolledBack { .. }
    ));

    let mut diverged = m.clone();
    diverged.raw.chunks[0].sha256 = sha256_hex(b"other");
    assert!(matches!(
        classify(&diverged, OWNER, Some(&ack)),
        RemoteState::RolledBack { .. }
    ));
}

#[test]
fn epochs_number_from_the_copy_and_seq_continues() {
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10), chunk(1, 10, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();

    // Chunks planned before the epoch opened no longer fit: re-plan.
    let stale = plan_chunks(&m.raw, vec![chunk(9, 20, 5)]).unwrap();
    assert_eq!(open_epoch(&mut m.raw, EpochReason::Rewritten).unwrap(), 1);
    assert!(apply_chunks(&mut m.raw, &stale).is_err());

    let fresh = plan_chunks(&m.raw, vec![chunk(2, 0, 7)]).unwrap();
    assert_eq!(
        (fresh[0].seq, fresh[0].epoch, fresh[0].start_offset),
        (2, 1, 0)
    );
    apply_chunks(&mut m.raw, &fresh).unwrap();
    assert_eq!(m.raw.epochs[1].first_seq, 2);
    assert_eq!(m.raw.next_offset, 7);
    ensure_valid(&m, Some(&loc.manifest().unwrap())).unwrap();
    assert!(open_epoch(&mut m.raw, EpochReason::Initial).is_err());
}

type Corruption = Box<dyn Fn(&mut Manifest)>;

#[test]
fn validator_rejects_corruption() {
    let loc = location("unit");
    let key = loc.manifest().unwrap();
    let mut good = first_manifest(&loc);
    let planned = plan_chunks(&good.raw, vec![chunk(0, 0, 10), chunk(1, 10, 10)]).unwrap();
    apply_chunks(&mut good.raw, &planned).unwrap();
    ensure_valid(&good, Some(&key)).unwrap();

    let cases: Vec<(&str, Corruption)> = vec![
        ("raw.chunk_count", Box::new(|m| m.raw.chunk_count = 5)),
        ("raw.raw_bytes", Box::new(|m| m.raw.raw_bytes += 1)),
        (
            "start_offset",
            Box::new(|m| m.raw.chunks[1].start_offset += 1),
        ),
        ("seq", Box::new(|m| m.raw.chunks[1].seq = 7)),
        (
            "sha256_8",
            Box::new(|m| m.raw.chunks[0].sha256_8 = "00000000".into()),
        ),
        (
            "raw.content_sha256",
            Box::new(|m| m.raw.content_sha256 = "f".repeat(64)),
        ),
        ("raw.next_offset", Box::new(|m| m.raw.next_offset = 1)),
        ("raw.epoch", Box::new(|m| m.raw.epoch = 1)),
        ("trace_id", Box::new(|m| m.trace_id = "nope".into())),
        (
            "first_ts",
            Box::new(|m| m.raw.chunks[0].first_ts = Some("not-a-date".into())),
        ),
        (
            "last_ts",
            Box::new(|m| m.raw.chunks[1].last_ts = Some("2026-13-45".into())),
        ),
        ("$key", Box::new(|m| m.session.harness = "codex".into())),
    ];
    for (path, corrupt) in cases {
        let mut m = good.clone();
        corrupt(&mut m);
        let issues = validate(&m, Some(&key));
        assert!(
            issues.iter().any(|i| i.path.contains(path)),
            "{path}: expected an issue, got {issues:?}"
        );
    }

    let mut newer = serde_json::to_value(&good).unwrap();
    newer["schema_v"] = 2.into();
    let err = parse_manifest(&serde_json::to_vec(&newer).unwrap(), None).unwrap_err();
    assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
}

/// Deterministic race: the server commits between the client's read and its
/// conditional write. The client's write must fail, re-read, re-apply only
/// its own change, and succeed, keeping the server's change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_race_reapplies_only_the_writers_own_change() {
    for (backend, store, _dir) in stores() {
        let loc = location(backend);
        let key = loc.manifest().unwrap();
        update_manifest(store.as_ref(), &key, &fast(), |_| {
            Ok(Edit::Create(Box::new(first_manifest(&loc))))
        })
        .await
        .unwrap();

        let mut calls = 0;
        let updated = update_manifest(store.as_ref(), &key, &fast(), |m| {
            calls += 1;
            if calls == 1 {
                // The server writes while the client holds a stale copy.
                let (store, key) = (store.clone(), key.clone());
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async move {
                        update_manifest(store.as_ref(), &key, &fast(), |m| {
                            let s =
                                with_lifecycle(m.unwrap(), Lifecycle::Idle, Some("race".into()));
                            Ok(Edit::server(s.unwrap()))
                        })
                        .await
                        .unwrap();
                    })
                });
            }
            append_one(m, 0)
        })
        .await
        .unwrap();

        let m = updated.manifest.unwrap();
        assert_eq!(
            updated.attempts, 2,
            "[{backend}] first write must lose the race"
        );
        assert_eq!(calls, 2);
        assert_eq!(m.revision, 3, "[{backend}] create, server, client");
        assert_eq!(m.raw.chunk_count, 1, "[{backend}] client change applied");
        assert_eq!(
            m.lifecycle(),
            Lifecycle::Idle,
            "[{backend}] server change kept"
        );
        assert_eq!(m.server.unwrap().lifecycle_reason.as_deref(), Some("race"));
    }
}

#[test]
fn an_unacknowledged_empty_epoch_is_detected_and_invalidates_normalized_output() {
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    relayhistory_object_store::manifest::client::record_scrub(&mut m.raw, "relayhistory-scrub", 1)
        .unwrap();
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    m.server = Some(with_normalized(&m, &m, 1, None));
    assert!(m.normalized_is_current());
    let ack = ClientAck::of(&m);

    // The client opened an epoch (source emptied) and crashed before seeing
    // the acknowledgment: no new chunk, same content digest.
    let mut remote = m.clone();
    open_epoch(&mut remote.raw, EpochReason::Truncated).unwrap();
    remote.revision += 1;
    assert_eq!(
        classify(&remote, OWNER, Some(&ack)),
        RemoteState::Extended {
            new_chunks: 0,
            new_epochs: 1
        }
    );
    assert!(
        !remote.normalized_is_current(),
        "output is for the previous epoch"
    );
    assert!(server::needs_transform(&remote));
    assert!(pointer_fields(&remote).normalized_stale);

    let mut older = m.clone();
    older.raw.epoch = 0;
    let mut ahead = ClientAck::of(&remote);
    ahead.revision = older.revision;
    assert!(matches!(
        classify(&older, OWNER, Some(&ahead)),
        RemoteState::RolledBack { .. }
    ));
}

#[test]
fn closed_sessions_are_never_abandoned() {
    use relayhistory_object_store::manifest::client::set_state;
    use relayhistory_object_store::manifest::server::{lifecycle_for, LifecyclePolicy};
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    m.raw.updated_at = "2026-01-01T00:00:00.000Z".into();
    let later = chrono::DateTime::parse_from_rfc3339("2026-03-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let policy = LifecyclePolicy::default();
    assert_eq!(lifecycle_for(&m, later, &policy), Lifecycle::Abandoned);
    set_state(&mut m.raw, RawState::Closed);
    m.raw.updated_at = "2026-01-01T00:00:00.000Z".into();
    assert_eq!(lifecycle_for(&m, later, &policy), Lifecycle::Idle);
}

#[test]
fn overflowing_chunk_totals_are_rejected_not_wrapped() {
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10), chunk(1, 10, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    m.raw.chunks[0].bytes = u64::MAX;
    m.raw.chunks[1].start_offset = 0;
    let issues = validate(&m, None);
    assert!(
        issues.iter().any(|i| i.message.contains("overflows")),
        "{issues:?}"
    );
}

#[tokio::test]
async fn a_fresh_section_struct_keeps_a_newer_writers_fields() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::new(FsConfig::new(dir.path())).unwrap();
    let loc = location("fs-fresh");
    let key = loc.manifest().unwrap();
    let mut doc = serde_json::to_value(first_manifest(&loc)).unwrap();
    doc["server"] = serde_json::json!({
        "updated_at": "2026-10-25T19:00:00.000Z",
        "normalized": null,
        "lifecycle": "idle",
        "lifecycle_at": "2026-10-25T19:00:00.000Z",
        "added_by_a_newer_server": {"keep": true}
    });
    store
        .put(
            &key,
            bytes::Bytes::from(serde_json::to_vec(&doc).unwrap()),
            PutOpts::create(),
        )
        .await
        .unwrap();
    // An older server builds its section from scratch (empty `extra`).
    update_manifest(&store, &key, &fast(), |_| {
        Ok(Edit::server(Server {
            updated_at: "2026-10-26T00:00:00.000Z".into(),
            normalized: None,
            lifecycle: Lifecycle::Active,
            lifecycle_at: "2026-10-26T00:00:00.000Z".into(),
            lifecycle_reason: None,
            retention: None,
            extra: Default::default(),
        }))
    })
    .await
    .unwrap();
    let body: Vec<bytes::Bytes> =
        futures::TryStreamExt::try_collect(store.get(&key, None).await.unwrap())
            .await
            .unwrap();
    let after: serde_json::Value = serde_json::from_slice(&body.concat()).unwrap();
    assert_eq!(
        after["server"]["added_by_a_newer_server"],
        serde_json::json!({"keep": true})
    );
    assert_eq!(
        after["server"]["lifecycle"], "active",
        "known fields follow the edit"
    );
}

#[tokio::test]
async fn unknown_fields_are_deleted_only_when_listed_and_nulls_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::new(FsConfig::new(dir.path())).unwrap();
    let loc = location("fs-delete");
    let key = loc.manifest().unwrap();
    let mut doc = serde_json::to_value(first_manifest(&loc)).unwrap();
    doc["server"] = serde_json::json!({
        "updated_at": "2026-10-25T19:00:00.000Z",
        "normalized": null,
        "lifecycle": "idle",
        "lifecycle_at": "2026-10-25T19:00:00.000Z",
        "server.review_state": "pending",
        "server.future_flag": null
    });
    store
        .put(
            &key,
            bytes::Bytes::from(serde_json::to_vec(&doc).unwrap()),
            PutOpts::create(),
        )
        .await
        .unwrap();

    // An unrelated edit keeps both, including the stored null.
    update_manifest(&store, &key, &fast(), |m| {
        let s = with_lifecycle(m.unwrap(), Lifecycle::Active, None).unwrap();
        Ok(Edit::server(s))
    })
    .await
    .unwrap();
    let read = |store: FsStore, key: ObjectKey| async move {
        let body: Vec<bytes::Bytes> =
            futures::TryStreamExt::try_collect(store.get(&key, None).await.unwrap())
                .await
                .unwrap();
        (
            store,
            serde_json::from_slice::<serde_json::Value>(&body.concat()).unwrap(),
        )
    };
    let (store, after) = read(store, key.clone()).await;
    assert_eq!(after["server"]["server.review_state"], "pending");
    assert!(after["server"]
        .as_object()
        .unwrap()
        .contains_key("server.future_flag"));

    // An explicit removal deletes exactly the listed key.
    update_manifest(&store, &key, &fast(), |m| {
        Ok(Edit::Server {
            server: Box::new(m.unwrap().server.clone().unwrap()),
            remove: vec!["server.review_state".into()],
        })
    })
    .await
    .unwrap();
    let (_store, after) = read(store, key).await;
    assert!(
        after["server"].get("server.review_state").is_none(),
        "{after}"
    );
    assert!(after["server"]
        .as_object()
        .unwrap()
        .contains_key("server.future_flag"));
}

#[test]
fn server_helpers_keep_reasons_and_unknown_normalized_fields() {
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    let s = with_lifecycle(&m, Lifecycle::Idle, Some("quiet 24h".into())).unwrap();
    let at = s.lifecycle_at.clone();
    m.server = Some(s);
    assert!(with_lifecycle(&m, Lifecycle::Idle, Some("quiet 24h".into())).is_none());
    let reason_only = with_lifecycle(&m, Lifecycle::Idle, Some("quiet 30h".into())).unwrap();
    assert_eq!(reason_only.lifecycle_reason.as_deref(), Some("quiet 30h"));
    assert_eq!(
        reason_only.lifecycle_at, at,
        "lifecycle_at moves only with the lifecycle"
    );

    let mut n = with_normalized(&m, &m, 1, None);
    n.normalized
        .as_mut()
        .unwrap()
        .extra
        .insert("future".into(), serde_json::json!("kept"));
    m.server = Some(n);
    let rebuilt = with_normalized(&m, &m, 2, None);
    assert_eq!(rebuilt.normalized.unwrap().extra["future"], "kept");
}

#[test]
fn plan_chunks_rejects_bad_digests_before_upload() {
    let loc = location("unit");
    let m = first_manifest(&loc);
    for bad in [
        NewChunk {
            sha256: "A".repeat(64),
            ..chunk(0, 0, 10)
        },
        NewChunk {
            stored_sha256: "not hex".into(),
            ..chunk(0, 0, 10)
        },
        NewChunk {
            sha256: "é".repeat(32),
            ..chunk(0, 0, 10)
        },
    ] {
        assert!(plan_chunks(&m.raw, vec![bad]).is_err());
    }
}

#[tokio::test]
async fn client_removals_match_exact_keys_per_section() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::new(FsConfig::new(dir.path())).unwrap();
    let loc = location("fs-client-remove");
    let key = loc.manifest().unwrap();
    let mut doc = serde_json::to_value(first_manifest(&loc)).unwrap();
    doc["session"]["session.future_flag"] = true.into();
    doc["session"]["future_flag"] = 7.into();
    doc["raw"]["future_flag"] = "raw-owned".into();
    store
        .put(
            &key,
            bytes::Bytes::from(serde_json::to_vec(&doc).unwrap()),
            PutOpts::create(),
        )
        .await
        .unwrap();
    update_manifest(&store, &key, &fast(), |m| {
        let m = m.unwrap();
        Ok(Edit::Client {
            session: Box::new(m.session.clone()),
            writer: Box::new(m.writer.clone()),
            raw: Box::new(m.raw.clone()),
            remove: ClientRemovals {
                session: vec!["session.future_flag".into()],
                ..Default::default()
            },
        })
    })
    .await
    .unwrap();
    let body: Vec<bytes::Bytes> =
        futures::TryStreamExt::try_collect(store.get(&key, None).await.unwrap())
            .await
            .unwrap();
    let after: serde_json::Value = serde_json::from_slice(&body.concat()).unwrap();
    assert!(
        after["session"].get("session.future_flag").is_none(),
        "exact key removed"
    );
    assert_eq!(after["session"]["future_flag"], 7, "similar key kept");
    assert_eq!(
        after["raw"]["future_flag"], "raw-owned",
        "other sections untouched"
    );
}

#[test]
fn every_epochs_first_seq_is_checked_even_without_chunks() {
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10), chunk(1, 10, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    open_epoch(&mut m.raw, EpochReason::Truncated).unwrap();
    ensure_valid(&m, None).unwrap();
    m.raw.epochs[1].first_seq = 99;
    let issues = validate(&m, None);
    assert!(
        issues.iter().any(|i| i.path == "raw.epochs[1].first_seq"),
        "{issues:?}"
    );

    let mut m2 = first_manifest(&loc);
    m2.server = Some(server::server_section(&m2));
    m2.server.as_mut().unwrap().retention = Some(Retention {
        delete_after: Some("someday".into()),
        ..Default::default()
    });
    assert!(validate(&m2, None)
        .iter()
        .any(|i| i.path == "server.retention.delete_after"));
}

#[test]
fn scrub_marker_is_validated_and_keeps_the_weakest_version() {
    use relayhistory_object_store::manifest::client::record_scrub;
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    assert!(!m.is_scrubbed());
    record_scrub(&mut m.raw, "relayhistory-scrub", 2).unwrap();
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    let before = m.raw.updated_at.clone();
    std::thread::sleep(Duration::from_millis(2));
    record_scrub(&mut m.raw, "relayhistory-scrub", 1).unwrap();
    assert_ne!(m.raw.updated_at, before, "a marker change is a raw change");
    record_scrub(&mut m.raw, "relayhistory-scrub", 3).unwrap();
    assert_eq!(
        m.raw.scrub.as_ref().unwrap().version,
        1,
        "lowest version wins"
    );
    assert!(record_scrub(&mut m.raw, "other-rules", 1).is_err());
    ensure_valid(&m, None).unwrap();

    // With raw.scrub set, every chunk must say what was stored.
    let mut missing = m.clone();
    missing.raw.chunks[0].raw_sha256 = None;
    assert!(validate(&missing, None)
        .iter()
        .any(|i| i.path == "raw.chunks[0].raw_sha256"));
    let mut zero = chunk(1, 10, 5);
    zero.stored_raw_bytes = Some(0);
    assert!(plan_chunks(&m.raw, vec![zero]).is_err());
    let mut empty = m.clone();
    empty.raw.chunks[0].stored_raw_bytes = Some(0);
    assert!(validate(&empty, None)
        .iter()
        .any(|i| i.path == "raw.chunks[0].stored_raw_bytes"));
    let mut bad = m.clone();
    bad.raw.scrub.as_mut().unwrap().version = 0;
    assert!(validate(&bad, None)
        .iter()
        .any(|i| i.path == "raw.scrub.version"));
}

#[test]
fn unscrubbed_manifests_are_quarantined() {
    use relayhistory_object_store::manifest::client::record_scrub;
    use relayhistory_object_store::manifest::server::{needs_transform, quarantined};
    let loc = location("unit");
    let mut m = first_manifest(&loc);
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    assert!(quarantined(&m));
    assert!(!needs_transform(&m), "never transform unscrubbed chunks");
    assert!(!pointer_fields(&m).scrubbed);
    // A marker added later would vouch for the unscrubbed chunk.
    assert!(record_scrub(&mut m.raw, "relayhistory-scrub", 1).is_err());
    assert!(quarantined(&m));

    let mut fresh = first_manifest(&loc);
    record_scrub(&mut fresh.raw, "relayhistory-scrub", 1).unwrap();
    let planned = plan_chunks(&fresh.raw, vec![chunk(0, 0, 10)]).unwrap();
    apply_chunks(&mut fresh.raw, &planned).unwrap();
    assert!(!quarantined(&fresh));
    assert!(needs_transform(&fresh));
    assert!(pointer_fields(&fresh).scrubbed);
}

const EXPORTER: &str = "migration:mgr_export";

fn ndjson_manifest(loc: &SessionLocation) -> Manifest {
    let mut m = first_manifest(loc);
    m.raw.format = RawFormat::Ndjson;
    m
}

/// What the exporter's write does to `raw`: one sealed epoch after everything
/// already listed, with `raw.owner`, `raw.state` and `raw.updated_at` untouched.
fn with_backfill(raw: &Raw) -> Raw {
    let mut next = raw.clone();
    let epoch = raw.epoch + 1;
    next.epochs.push(EpochRecord {
        epoch,
        opened_at: "2026-10-30T00:00:00.000Z".into(),
        reason: EpochReason::Backfill,
        first_seq: raw.chunk_count,
        opened_by: EXPORTER.into(),
        resumes_epoch: None,
        extra: Default::default(),
    });
    next.epoch = epoch;
    let planned = chunk(900, 0, 500);
    next.chunks.push(ChunkEntry {
        seq: raw.chunk_count,
        epoch,
        start_offset: 0,
        bytes: planned.bytes,
        stored_bytes: planned.stored_bytes,
        sha256_8: planned.sha256[..8].to_string(),
        sha256: planned.sha256,
        stored_sha256: planned.stored_sha256,
        stored_raw_bytes: planned.stored_raw_bytes,
        raw_sha256: planned.raw_sha256,
        compression: Compression::Zstd,
        records: planned.records,
        first_ts: None,
        last_ts: None,
        extra: Default::default(),
    });
    next.chunk_count += 1;
    next.raw_bytes += 500;
    next.stored_bytes += next.chunks.last().unwrap().stored_bytes;
    next.content_sha256 = content_sha256(&next.chunks);
    next.next_offset = 500;
    next
}

fn live_with_backfill() -> Manifest {
    let mut m = ndjson_manifest(&location("backfill"));
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 100), chunk(1, 100, 50)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    m.raw = with_backfill(&m.raw);
    m
}

#[test]
fn a_live_client_resumes_its_offsets_after_a_backfill_epoch() {
    let mut m = live_with_backfill();
    ensure_valid(&m, None).unwrap();
    assert_eq!(m.raw.owner, OWNER, "the exporter never takes the owner");
    assert_eq!(m.raw.next_offset, 500, "end of the exporter's stream");
    assert_eq!(m.raw.live_epoch(), 0);
    assert_eq!(m.raw.live_next_offset(), 150, "the client's own position");

    let err = plan_chunks(&m.raw, vec![chunk(2, 150, 10)]).unwrap_err();
    assert!(err.to_string().contains("resume_epoch"), "{err}");

    let epoch = resume_epoch(&mut m.raw).unwrap();
    assert_eq!(epoch, 2);
    assert_eq!(m.raw.epochs[2].resumes_epoch, Some(0));
    assert_eq!(m.raw.next_offset, 150);
    ensure_valid(&m, None).unwrap();

    let planned = plan_chunks(&m.raw, vec![chunk(2, 150, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    assert_eq!(m.raw.next_offset, 160);
    assert_eq!(m.raw.live_next_offset(), 160);
    ensure_valid(&m, None).unwrap();
}

#[test]
fn classify_never_mistakes_a_backfill_for_the_clients_own_write() {
    let mut m = ndjson_manifest(&location("classify"));
    let planned = plan_chunks(&m.raw, vec![chunk(0, 0, 100)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    m.revision = 2;
    let ack = ClientAck::of(&m);

    let mut remote = m.clone();
    remote.raw = with_backfill(&m.raw);
    remote.revision = 3;
    let state = classify(&remote, OWNER, Some(&ack));
    assert_eq!(
        state,
        RemoteState::Backfilled {
            new_chunks: 1,
            new_epochs: 1
        }
    );
    assert!(state.may_write());

    // Once adopted, later server writes are ordinary.
    let adopted = ClientAck::of(&remote);
    assert_eq!(
        classify(&remote, OWNER, Some(&adopted)),
        RemoteState::Unchanged
    );
}

#[test]
fn a_truncation_after_a_backfill_starts_over_and_keeps_the_backfill() {
    let mut m = live_with_backfill();
    open_epoch(&mut m.raw, EpochReason::Truncated).unwrap();
    assert_eq!(m.raw.next_offset, 0);
    let planned = plan_chunks(&m.raw, vec![chunk(2, 0, 10)]).unwrap();
    apply_chunks(&mut m.raw, &planned).unwrap();
    ensure_valid(&m, None).unwrap();
    assert_eq!(m.raw.live_epoch(), 2);
    assert!(m.raw.chunks.iter().any(|c| c.epoch == 1));
}

#[test]
fn only_the_exporter_opens_backfill_and_only_resume_epoch_opens_resumed() {
    let mut m = live_with_backfill();
    for reason in [EpochReason::Backfill, EpochReason::Resumed] {
        assert!(open_epoch(&mut m.raw, reason).is_err());
    }
    let mut plain = ndjson_manifest(&location("plain"));
    assert!(resume_epoch(&mut plain.raw).is_err());
}

#[test]
fn validator_rejects_malformed_backfill_and_resumed_epochs() {
    type Corrupt = Box<dyn Fn(&mut Manifest)>;
    let cases: Vec<(&str, Corrupt)> = vec![
        (
            "raw.epochs[1].opened_by",
            Box::new(|m| m.raw.epochs[1].opened_by = OWNER.into()),
        ),
        (
            "raw.epochs[1].opened_by",
            Box::new(|m| m.raw.epochs[1].opened_by = "migration:".into()),
        ),
        (
            "raw.epochs[1].opened_by",
            Box::new(|m| m.raw.epochs[1].opened_by = "migration:bad id".into()),
        ),
        ("raw.format", Box::new(|m| m.raw.format = RawFormat::Jsonl)),
        (
            "raw.epochs",
            Box::new(|m| {
                m.raw = with_backfill(&m.raw);
            }),
        ),
        (
            "raw.epochs[1].resumes_epoch",
            Box::new(|m| m.raw.epochs[1].resumes_epoch = Some(0)),
        ),
        (
            "raw.epochs[0].resumes_epoch",
            Box::new(|m| m.raw.epochs[0].resumes_epoch = Some(0)),
        ),
        (
            "raw.epochs[2].resumes_epoch",
            Box::new(|m| {
                resume_epoch(&mut m.raw).unwrap();
                m.raw.epochs[2].resumes_epoch = Some(1);
            }),
        ),
        (
            "raw.epochs[3].reason",
            Box::new(|m| {
                open_epoch(&mut m.raw, EpochReason::Rewritten).unwrap();
                open_epoch(&mut m.raw, EpochReason::Rewritten).unwrap();
                m.raw.epochs[3].reason = EpochReason::Resumed;
                m.raw.epochs[3].resumes_epoch = Some(2);
            }),
        ),
        (
            "raw.chunks[3].start_offset",
            Box::new(|m| {
                resume_epoch(&mut m.raw).unwrap();
                let mut c = plan_chunks(&m.raw, vec![chunk(9, 150, 10)]).unwrap();
                c[0].start_offset = 0;
                m.raw.chunks.extend(c);
                m.raw.chunk_count += 1;
                m.raw.raw_bytes += 10;
                m.raw.stored_bytes += m.raw.chunks[3].stored_bytes;
                m.raw.content_sha256 = content_sha256(&m.raw.chunks);
                m.raw.next_offset = 10;
            }),
        ),
    ];
    for (path, corrupt) in cases {
        let mut m = live_with_backfill();
        corrupt(&mut m);
        let issues = validate(&m, None);
        assert!(issues.iter().any(|i| i.path == path), "{path}: {issues:?}");
    }

    // Empty: a backfill epoch lists at least one chunk.
    let mut empty = ndjson_manifest(&location("empty"));
    let planned = plan_chunks(&empty.raw, vec![chunk(0, 0, 10)]).unwrap();
    apply_chunks(&mut empty.raw, &planned).unwrap();
    let live = empty.raw.chunks.clone();
    empty.raw = with_backfill(&empty.raw);
    empty.raw.chunks = live;
    assert!(validate(&empty, None)
        .iter()
        .any(|i| i.path == "raw.epochs[1]"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backfill_commit_and_live_appends_race_without_loss() {
    for (backend, store, _dir) in stores() {
        let loc = location(backend);
        let key = loc.manifest().unwrap();
        let m = ndjson_manifest(&loc);
        update_manifest(&*store, &key, &fast(), |_| {
            Ok(Edit::Create(Box::new(m.clone())))
        })
        .await
        .unwrap();

        let live_store = store.clone();
        let live_key = key.clone();
        let live = tokio::spawn(async move {
            for n in 0..6u64 {
                update_manifest(&*live_store, &live_key, &fast(), |m| {
                    let m = m.expect("manifest exists");
                    let mut raw = m.raw.clone();
                    if raw.at_backfill_tail() {
                        resume_epoch(&mut raw)?;
                    }
                    let at = raw.next_offset;
                    let planned = plan_chunks(&raw, vec![chunk(n, at, 10)])?;
                    apply_chunks(&mut raw, &planned)?;
                    Ok(Edit::client(m.session.clone(), m.writer.clone(), raw))
                })
                .await
                .unwrap();
            }
        });
        let export_store = store.clone();
        let export_key = key.clone();
        let export = tokio::spawn(async move {
            update_manifest(&*export_store, &export_key, &fast(), |m| {
                let m = m.expect("manifest exists");
                Ok(Edit::client(
                    m.session.clone(),
                    m.writer.clone(),
                    with_backfill(&m.raw),
                ))
            })
            .await
            .unwrap();
        });
        live.await.unwrap();
        export.await.unwrap();

        let done = update_manifest(&*store, &key, &fast(), |_| Ok(Edit::Unchanged))
            .await
            .unwrap()
            .manifest
            .unwrap();
        let backfill = done.raw.chunks.iter().filter(|c| c.bytes == 500).count();
        assert_eq!(backfill, 1, "{backend}");
        assert_eq!(done.raw.chunk_count, 7, "{backend}: no live chunk lost");
        assert_eq!(done.raw.live_next_offset(), 60, "{backend}");
        assert_eq!(done.raw.owner, OWNER, "{backend}");
    }
}
