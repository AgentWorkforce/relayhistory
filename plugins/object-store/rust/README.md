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
The `manifest.json` schema is specified in [docs/manifest-v1.md](docs/manifest-v1.md). See the [decision record](../../../docs/decisions/2026-10-25-object-storage-adapters.md)
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

`manifest` implements the [v1 manifest](docs/manifest-v1.md) for both writers:

- **Types:** serde types that preserve unknown fields.
- **Validation:** `parse_manifest` / `validate` check every invariant, matching
  the TypeScript validator in relayhistory-cloud.
- **Writes:** `update_manifest` reads with `get_object`, applies one writer's
  `Edit`, and writes with compare-and-swap. It makes up to 8 attempts in total,
  with jittered backoff between them. An edit can only replace its own writer's sections, so the
  other side's fields are carried through untouched.
- **Client helpers (`manifest::client`):** `new_manifest`, `plan_chunks` /
  `apply_chunks`, `open_epoch`, and `classify` for stale-writer detection.
- **Server helpers (`manifest::server`):** lifecycle, normalized output and
  `pointer_fields`.

`compress_zstd` / `decompress_zstd` wrap raw chunk bodies (level 3). Objects
are stored as `application/zstd` without `Content-Encoding`, so a presigned
download returns the exact `.zst` object.

## Scrubbing raw chunks

`scrub` (feature `scrub`, on by default) removes PII and secrets from raw
chunks **before** they are compressed and uploaded:

- **Rules:** a byte-for-byte port of relayhistory-cloud's `scrubText`. It
  catches private keys (PEM and PGP), home-directory usernames, bearer tokens,
  provider and cloud keys, JWTs, OAuth query values, secret assignments, URL
  credentials, email addresses, payment card numbers (issuer prefix, length
  and Luhn) and phone numbers (`+` E.164, or North American with separators).
  Matches become `[REDACTED]`; a card number held as a JSON integer becomes the
  string `[REDACTED]`. Strings over 256 KiB (in UTF-16 units) are truncated
  with an in-band marker, cut only after whitespace and never between digit
  groups, so a secret is kept whole (and scrubbed) or dropped whole.
- **Limit:** each string is scrubbed on its own. A secret split across two
  strings, JSON values or records (a token streamed as separate deltas) is not
  recognized, because neither part matches a rule.
- **Spec:** the golden vectors in `tests/fixtures/scrub-golden-v2.json`. They
  are generated from the TypeScript reference by relayhistory-cloud's
  `test/scrub-golden.test.ts`, and both CIs check them. Refresh the fixture by
  copying it from there; never edit it here. Bump `RULESET_VERSION` when it
  changes.
- **`scrub_chunk`:** works record by record. Each JSON record is parsed, every
  string value and key is scrubbed, and the record is re-serialized with key
  order and numbers preserved. A record that can't be parsed, or whose keys
  collide after scrubbing, is replaced by a typed placeholder and is never
  stored unscrubbed.
- **Uploading:** raw chunks go through `scrub::seal`, the only sanctioned
  path. `scrub_chunk` returns an opaque `ScrubbedChunk`;
  `SealedChunk::seal` compresses and hashes it (`stored_bytes`,
  `stored_sha256`, `stored_raw_bytes`, `raw_sha256`, `records`, and the
  `raw.scrub` pair for the manifest); `put_raw_chunk` uploads it create-only
  with `scrub-ruleset`, `scrub-version`, `raw-sha256` and `raw-bytes`
  metadata. Neither type can be built from unscrubbed bytes, so a raw chunk
  uploaded this way has always been scrubbed. The generic `ObjectStore::put`
  still uploads anything; the delivery worker must not use it for raw chunks.
  Scrubbing and sealing belong in that background worker, never on the local
  ingest or read path.

| Benchmark (release, `cargo bench --bench scrub`) | Result |
| --- | --- |
| 4 MiB realistic JSONL chunk | ~126 ms (~32 MiB/s, one core) |
| 128 KiB `AAAA…` (the reference's past O(n²) case) | ~0.08 ms |

Lookaround-free rules run on the linear-time `regex` engine. The four rules
that need lookarounds use `fancy-regex` with a bounded backtrack budget; if a
string exceeds it, the whole string is withheld (fail closed).

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
