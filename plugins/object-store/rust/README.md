# relayhistory-object-store

Object-storage destination adapters for RelayHistory raw session delivery
(plan 009's ObjectStorage destination). One `ObjectStore` contract, four
implementations:

| Type             | Backend                                      | Feature |
| ---------------- | -------------------------------------------- | ------- |
| `S3Store`        | Amazon S3, Cloudflare R2, MinIO (endpoint override) | `s3` |
| `GcsStore`       | Google Cloud Storage                         | `gcs`   |
| `AzureBlobStore` | Azure Blob Storage                           | `azure` |
| `FsStore`        | Local directory (tests, air-gapped hosts)    | always  |

Transports come from the Apache [`object_store`](https://crates.io/crates/object_store)
crate; this crate adds the guarantees the delivery coordinator depends on.
See the [decision record](../../../docs/decisions/2026-10-25-object-storage-adapters.md)
for why.

**Not part of the local hot path.** This crate is not a root-workspace member,
and `ai-hist`, `ai-hist-cli` and `ai-hist-napi` must never depend on it. CI's
`object-store-plugin` job fails if any of them links `object_store`, and the
`verify` job builds the core with `plugins/` deleted.

## Contract

```rust
#[async_trait]
pub trait ObjectStore: Send + Sync + Debug {
    fn backend(&self) -> &'static str;
    async fn put(&self, key: &ObjectKey, body: Bytes, opts: PutOpts) -> Result<PutAck>;
    async fn head(&self, key: &ObjectKey) -> Result<Option<ObjectMeta>>;
    async fn get(&self, key: &ObjectKey, range: Option<Range<u64>>) -> Result<ByteStream>;
    async fn list(&self, prefix: &ObjectKey, cursor: Option<String>) -> Result<Page<ObjectMeta>>;
    async fn presign_get(&self, key: &ObjectKey, ttl: Duration) -> Result<Url>;
    async fn multipart(&self, key: &ObjectKey, opts: MultipartOpts) -> Result<Box<dyn MultipartUpload>>;
}
```

| Guarantee | How |
| --------- | --- |
| Raw chunks are idempotent | `PutOpts::create()` (`If-None-Match: *` / `ifGenerationMatch=0` / hard-link). Same SHA-256 already stored → `Ok(PutAck { created: false })`; different bytes → `Error::AlreadyExists`. |
| Manifest updates never lose a write | `PutOpts::replace(version)` (`If-Match` / `ifGenerationMatch` / `flock` + compare). Lost race or missing object → `Error::PreconditionFailed`. |
| Integrity | SHA-256 of every body stored as `content-sha256` metadata; a caller digest (`content_sha256`) is checked before upload, and multipart uploads abort rather than commit a mismatch. |
| Tracing | `trace-id` metadata (`x-amz-meta-trace-id`, `x-goog-meta-trace-id`, `x-ms-meta-trace_id`); `deliver.put` and `fetch` tracing spans. |
| Retry classification | `Error::is_retryable()` is true only for transport errors and 408/429/5xx after `object_store`'s own retries. |
| Credentials | `Ambient` (instance/workload identity, env), `Vended` (a `CredentialVendor` minting scoped STS / downscoped / SAS creds, cached and refreshed 5 min before expiry), or in-memory static keys for customer config and emulators. No config type serializes a secret; `Debug` redacts. |

`layout::SessionLocation` builds the canonical v1 keys:

```text
{prefix}/v1/ws={workspace_id}/m={machine_id}/h={harness}/d={yyyy}/{mm}/{dd}/s={session_id}/
    manifest.json
    raw/{seq:010}-{start_offset}-{sha256_8}.{jsonl|ndjson|sqlite-rows}.zst
    normalized/v{N}/...
```

`compress_zstd` / `decompress_zstd` wrap raw chunk bodies (level 3). Objects
are stored as `application/zstd` without `Content-Encoding`, so a presigned
download returns the exact `.zst` object.

## Conformance suite

`conformance::run_suite` (feature `conformance`) is the contract every adapter
must pass: put/get/head + metadata, not-found, idempotent create, create
conflict, compare-and-swap, concurrent CAS (exactly one of 8 racers wins),
digest verification, range get, list pagination with prefix boundaries,
presign + fetch, multipart (2×5 MiB + tail, range across a part boundary,
abort), multipart digest mismatch.

```sh
eval "$(plugins/object-store/rust/scripts/emulators.sh up)"   # MinIO, fake-gcs-server, Azurite
cargo test --manifest-path plugins/object-store/rust/Cargo.toml --all-features -- --nocapture
plugins/object-store/rust/scripts/emulators.sh down
```

`FsStore` always runs; each emulator runs when its variable is set
(`RH_OBJSTORE_REQUIRE_EMULATORS=1` makes a missing one fail). Results today:

| Backend | Emulator | Cases |
| ------- | -------- | ----- |
| fs      | —        | 12/12 |
| s3      | MinIO    | 12/12 |
| azure   | Azurite  | 12/12 |
| gcs     | fake-gcs-server (`tustvold/` fork, as `object_store` uses) | 6/12 — fake-gcs-server ignores generation preconditions and has no XML multipart, so the CAS, idempotent-create and multipart cases are reported as skipped |

The GCS gaps are emulator gaps, not adapter gaps; `gcs_store_conforms_on_real_gcs`
runs all twelve against a real bucket with ambient credentials when
`RH_OBJSTORE_REAL_GCS_BUCKET` is set (likewise `RH_OBJSTORE_REAL_S3_BUCKET` +
`_REGION` and `RH_OBJSTORE_REAL_AZURE_ACCOUNT` + `_CONTAINER`).

## Known limits

- `FsStore`'s compare-and-swap lock is a host-local `flock`; do not share one
  directory between hosts over NFS.
- `FsStore::presign_get` returns a `file://` URL and cannot enforce the TTL.
- GCS presigning needs a service-account key or ambient credentials with
  `iam.serviceAccounts.signBlob`; a vended bearer token alone returns
  `Error::Unsupported`. Azure presigning needs an account key or an Entra ID
  token (user delegation SAS); a vended SAS alone cannot mint another.
- Multipart uploads are unconditional (no backend offers a portable
  precondition on completion); use them only for content-addressed keys.
- Bucket/container provisioning, lifecycle rules (including expiring
  incomplete multipart uploads) and object-landed notifications are the
  operator's/server's job, not this crate's.
