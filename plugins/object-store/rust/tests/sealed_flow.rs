//! The delivery worker's raw-chunk path end to end: scrub, seal, plan,
//! upload, apply, validate. Source fields must describe the unscrubbed range
//! and stored fields the upload, or offsets, epochs and rewrite detection
//! break.

use chrono::NaiveDate;
use relayhistory_object_store::layout::{RawFormat, SessionLocation};
use relayhistory_object_store::manifest::client::{
    apply_chunks, new_manifest, plan_chunks, NewManifest,
};
use relayhistory_object_store::manifest::*;
use relayhistory_object_store::scrub::scrub_chunk;
use relayhistory_object_store::scrub::seal::{put_raw_chunk, record_sealed_scrub, SealedChunk};
use relayhistory_object_store::*;

fn location() -> SessionLocation {
    SessionLocation {
        prefix: None,
        workspace_id: "ws_1".into(),
        machine_id: "mach-1".into(),
        harness: "claude".into(),
        start_date: NaiveDate::from_ymd_opt(2026, 10, 26).unwrap(),
        session_id: "s-sealed-flow".into(),
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
            started_at: "2026-10-26T08:00:00.000Z".into(),
            ended_at: None,
            harness_version: None,
            cwd_sha256: None,
            parent_session_id: None,
            extra: Default::default(),
        },
        writer: Writer {
            delivery_instance_id: "dest_objstore_01".into(),
            plugin_version: "test".into(),
            host_version: None,
            extra: Default::default(),
        },
        format: RawFormat::Jsonl,
    })
}

#[tokio::test]
async fn sealed_chunks_produce_a_valid_scrubbed_manifest() {
    // Two source chunks, each ending on a record boundary; scrubbing changes
    // both their length and their bytes.
    let parts: [&[u8]; 2] = [
        b"{\"m\":\"key sk-abcdefghijklmnopqrstuvwxyz0123\"}\n",
        b"{\"m\":\"mail alice.smith@example.org\"}\n{\"n\":1}\n",
    ];
    let dir = tempfile::tempdir().unwrap();
    let store = FsStore::new(FsConfig::new(dir.path())).unwrap();
    let loc = location();
    let mut m = first_manifest(&loc);

    // The marker goes on before the first chunk it covers.
    record_sealed_scrub(&mut m.raw).unwrap();
    let mut offset = 0;
    for part in parts {
        let sealed = SealedChunk::seal(&scrub_chunk(part)).unwrap();
        let planned = plan_chunks(&m.raw, vec![sealed.new_chunk(offset)]).unwrap();
        let entry = &planned[0];
        // Source fields: the unscrubbed range.
        assert_eq!(entry.start_offset, offset);
        assert_eq!(entry.bytes, part.len() as u64);
        assert_eq!(entry.sha256, sha256_hex(part));
        // Stored fields: what was uploaded.
        assert_ne!(entry.raw_sha256.as_deref(), Some(entry.sha256.as_str()));
        assert_eq!(entry.raw_sha256.as_deref(), Some(sealed.raw_sha256()));
        assert_eq!(entry.stored_sha256, sealed.stored_sha256());

        // Upload under the planned entry's key before writing the manifest.
        let key = loc
            .raw_chunk(
                entry.seq,
                entry.start_offset,
                &entry.sha256,
                RawFormat::Jsonl,
            )
            .unwrap();
        put_raw_chunk(&store, &key, &sealed, Metadata::new())
            .await
            .unwrap();
        let mut body = Vec::new();
        let mut stream = store.get(&key, None).await.unwrap();
        while let Some(bytes) = futures::StreamExt::next(&mut stream).await {
            body.extend_from_slice(&bytes.unwrap());
        }
        let raw = decompress_zstd(&body).unwrap();
        assert_eq!(sha256_hex(&raw), sealed.raw_sha256());
        let text = String::from_utf8(raw).unwrap();
        assert!(
            !text.contains("sk-abc") && !text.contains("alice.smith"),
            "{text}"
        );

        apply_chunks(&mut m.raw, &planned).unwrap();
        offset += part.len() as u64;
    }

    assert!(m.is_scrubbed());
    assert_eq!(m.raw.next_offset, offset);
    ensure_valid(&m, Some(&loc.manifest().unwrap())).unwrap();
}
