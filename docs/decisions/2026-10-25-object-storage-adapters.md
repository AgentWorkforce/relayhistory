# Object-storage adapters: one contract over the `object_store` crate, outside the hot path

- **Status:** Proposed
- **Date:** 2026-10-25
- **Plan:** [009 — local history / cloud boundary](../../plans/009-local-history-cloud-boundary.md)
  (the ObjectStorage destination plugin)
- **Code:** [`plugins/object-store/rust`](../../plugins/object-store/rust)

## Context

The sessions-pipeline redesign makes raw harness bytes, stored unchanged in
object storage, the canonical copy of every session. The durable delivery
coordinator (plan 009, stage 5) owns queue state and checkpoints; a
destination plugin turns a batch into durable objects and an acknowledgment.
Enterprise and self-hosted deployments need S3 (and S3-compatible R2/MinIO),
Google Cloud Storage and Azure Blob Storage; tests and air-gapped hosts need a
local directory.

The coordinator relies on four storage behaviours, and each cloud spells them
differently:

| Need | S3 / R2 / MinIO | GCS | Azure | Directory |
| ---- | --------------- | --- | ----- | --------- |
| Create-only chunk write | `If-None-Match: *` (412) | `ifGenerationMatch=0` | `If-None-Match: *` (409) | hard link |
| Compare-and-swap manifest | `If-Match: etag` | `ifGenerationMatch=generation` | `If-Match: etag` | none |
| User metadata | `x-amz-meta-*` | `x-goog-meta-*` | `x-ms-meta-*`, C#-identifier names only | none |
| Presigned GET | SigV4 query | V4 signed URL (key or `signBlob`) | SAS (account or user-delegation key) | — |

The local read/write path must stay offline and fast (a new session is visible
in the desktop app in under 5 ms; local reads never wait on the network), so no
cloud client may be linked into `ai-hist`, the napi addon or the CLI.

## Decision

1. **One trait, four adapters.** `ObjectStore` (`put`, `head`, ranged `get`,
   cursor `list`, `presign_get`, `multipart`) with `S3Store`, `GcsStore`,
   `AzureBlobStore` and `FsStore`, each behind a cargo feature
   (`s3`/`gcs`/`azure`; `fs` always on).

2. **Transport is the Apache `object_store` crate (0.14), not the three vendor
   SDKs.** See the comparison below. This crate adds the semantics
   `object_store` leaves to callers:
   - *Idempotent create.* A create that finds an existing object compares
     SHA-256 (from metadata, or by hashing the stored body when metadata is
     absent) and returns `created: false` for identical bytes,
     `AlreadyExists` otherwise. S3's 412 and Azure's 409 both reach this path.
   - *CAS.* `ObjectVersion { e_tag, version }` is opaque to callers; S3/Azure
     compare `e_tag`, GCS compares the generation. A lost race, and a CAS
     against a missing object (S3 reports 404), are both `PreconditionFailed`.
   - *Integrity.* Every object carries `content-sha256` metadata; caller
     digests are verified before upload; a multipart upload whose assembled
     digest mismatches is aborted, never completed.
   - *Error classification.* `is_retryable()` only for transport errors and
     408/429/5xx (after `object_store`'s own retries); other 4xx are permanent
     so the coordinator dead-letters instead of retrying forever.
   - *Metadata portability.* Keys are `[a-z][a-z0-9-]*`; the Azure adapter
     stores `-` as `_` (Azure requires C# identifiers) and maps back on read,
     so callers always see `trace-id`.
   - *`FsStore`* adds what `LocalFileSystem` lacks: metadata sidecars under
     `{root}/.rh/meta/`, CAS via an exclusive `flock` held across compare and
     atomic rename, and sorted listing. `fsync` is on by default because an
     acknowledgment advances a checkpoint.

3. **The plugin lives at `plugins/object-store/rust`, outside the root
   workspace**, like `plugins/provider-sources`. CI job `object-store-plugin`
   fails if `ai-hist`, `ai-hist-cli` or `ai-hist-napi` ever resolves
   `object_store`, and `verify` builds the core with `plugins/` deleted.

4. **Credentials are never persisted by this layer.** Three sources:
   `Ambient` (instance profile / IRSA / workload identity / managed identity /
   environment), `Vended` (a host-supplied `CredentialVendor` mints
   bucket-and-prefix-scoped STS credentials, downscoped GCS tokens or Azure
   SAS/Entra tokens, cached in memory and refreshed 5 minutes before expiry),
   or in-memory static keys for customer config and emulators. No config type
   derives `Serialize`; every credential `Debug` impl redacts.
   `layout::SessionLocation::machine_prefix()` is the scope a vendor should
   mint for (`{prefix}/v1/ws=…/m=…`).

5. **The canonical v1 key layout is built in one place**, `layout.rs`:
   `{prefix}/v1/ws=/m=/h=/d=yyyy/mm/dd/s=/{manifest.json | raw/{seq:010}-{start_offset}-{sha256_8}.{ext}.zst | normalized/v{N}/…}`.
   Path components are restricted to `[A-Za-z0-9._-]`; keys are restricted to
   printable ASCII without characters any backend encodes differently.

6. **Bodies are zstd (level 3) stored as `application/zstd` without
   `Content-Encoding`**, so presigned downloads return the exact object and
   HTTP clients never transparently decode the canonical bytes.

7. **A shared conformance suite is the contract.** `conformance::run_suite`
   (12 cases) runs against every adapter in CI on MinIO, fake-gcs-server and
   Azurite. Emulator gaps are declared per case and printed as `SKIP`, never
   silently passed.

## `object_store` vs. vendor SDKs

Measured 2026-10-25 with rustc 1.98.1, x86_64-linux, release profile
`strip = true, lto = "thin", codegen-units = 1, opt-level = 3`. Each binary
builds a client and calls head/ranged get/presign so no code is dead-code
eliminated. A tokio-only baseline is 0.54 MiB.

| Option | Stripped binary | Δ vs baseline | Crates | Clean build | TLS / crypto |
| ------ | --------------- | ------------- | ------ | ----------- | ------------ |
| `object_store` 0.14.2 `aws` | 6.58 MiB | +6.04 MiB | 166 | — | rustls 0.23 + aws-lc-rs |
| **`object_store` 0.14.2 `aws,gcp,azure,fs`** | **8.86 MiB** | **+8.32 MiB** | **169** | **52–63 s** | rustls 0.23 + aws-lc-rs |
| aws-sdk-s3 1.155 + aws-config 1.13 | 14.08 MiB | +13.54 MiB | 269 | — | rustls 0.23 + aws-lc-rs **and** rustls 0.21 + ring |
| google-cloud-storage 1.21 (+ auth 1.18) | 14.35 MiB | +13.81 MiB | 247 | — | rustls 0.23 + aws-lc-rs |
| azure_storage_blob 1.1 + azure_identity 1.0 | 6.50 MiB¹ | +5.97 MiB | 181 | — | rustls 0.23 + aws-lc-rs |
| all three SDKs | 23.85 MiB | +23.31 MiB | 404 | 166–190 s | both stacks |

¹ The Azure SDK probe only calls get-properties (no ranged read or SAS), so
it understates that SDK. No option links openssl. Build times are two clean
builds each on a loaded 24-core host; treat them as relative.

All three clouds through `object_store` are 2.7× smaller than the three SDKs
(15 MiB less, 235 fewer crates, about 3× faster to build), and adding GCS,
Azure and fs on top of S3 costs only 2.3 MiB because they share one HTTP, TLS
and crypto stack.

Beyond size: `object_store` gives one retry/backoff policy, one error type and
one credential-provider trait across all three clouds, rustls everywhere, and
is maintained under the Apache Arrow project with integration tests against
LocalStack/MinIO, fake-gcs-server and Azurite. The vendor SDKs would give
three error models, three async credential chains and three retry
configurations to normalise. What `object_store` lacks
(idempotent create, metadata sidecars on disk, retry classification) is small
and lives in this crate.

## Alternatives rejected

- **Vendor SDKs per cloud** — larger binaries and dependency trees (above),
  three behaviours to normalise, and no shared emulator story.
- **OpenDAL** — a much larger abstraction (dozens of services) than the three
  we need, and it would still need the same idempotent-create and
  error-classification layer on top.
- **S3 API only (GCS interop / Azure via a gateway)** — GCS's S3 interop
  authenticates with HMAC keys, which are long-lived, and Azure has no
  first-party S3 API; the requirement is native S3, GCS and Azure.
- **Putting the adapters in `crates/`** — makes linking them into the hot path
  one dependency line away; the plugin boundary plus the CI check makes it
  impossible to do accidentally.

## Consequences

- The delivery coordinator can treat every chunk upload as safe to repeat and
  every manifest update as a CAS with a typed lost-race error.
- GCS conditional writes and multipart cannot be verified on fake-gcs-server
  (it ignores generation preconditions and has no XML multipart); they need a
  real bucket (`RH_OBJSTORE_REAL_GCS_BUCKET`), which this change has not been
  run against. A CI job with a scoped test bucket (OIDC, no stored keys) would
  close that gap; until then the GCS paths rest on `object_store`'s own
  real-GCS tests.
- `FsStore` CAS is host-local; a directory shared across hosts needs a real
  object store.
- Bucket provisioning, lifecycle rules (expire incomplete multipart uploads)
  and object-landed notifications stay with the operator and server side.
