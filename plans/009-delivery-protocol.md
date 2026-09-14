# RelayHistory delivery protocol v1

Approved companion to plan 009 on 2026-09-13. Server baseline: `8f4b8af`.
Client wire types: `sdk-ts/src/delivery-contracts.ts`, native contract 13.
Implementation workers must preserve this agreement or report a needed change.

## Transport and authority

`POST /v1/delivery/batches` uses existing RelayHistory service authentication and
requires `rth:sync`. The JSON request is `{ "protocolVersion": 1, "batch": BATCH }`.
BATCH is the immutable `HistoryExportBatch`: `schema_version`, `origin_id`,
`batch_id`, `job_id`, `generation`, `destination_id`, `instance_id`, `account_id`,
`mapping_version`, and `records`. Each record contains `schema_version`,
`origin_id`, `record_id`, `revision_id`, `revision`, `kind`, `source`,
`session_id`, `operation`, and `payload`.

The authenticated org/workspace establishes tenancy. For this service,
`account_id` must equal `relayhistory:` followed by the lowercase SHA-256 hex
digest of UTF-8 JSON `[orgId, workspaceId ?? ""]`. The server compares this
immutable expected-account assertion against authenticated identity before any
write and returns403 on mismatch. It never uses the body to grant access or
select another tenant. The plugin must also check the configured job identity
when resolving/refreshing credentials. It must preserve the same mapping version and body on retry.
Secrets remain in transport headers, never job configuration or prepared bodies.

Support schema version 1 and the local core's declared evidence kinds. Include
source observations once the observation schema is integrated. Records must have
unique revision IDs within a batch, consistent origin IDs, positive safe integer
revisions, and `upsert` object payloads or `delete` null payloads. Reject unknown
schemas/kinds and invalid requests explicitly. Bound bytes, records, identifiers,
filter values, and cursor decoding. Initial endpoint limits must accommodate the
core's default 100-record/1MiB batch; document exact service limits rather than
silently truncate.

## Atomic persistence and acknowledgment

The record identity is authenticated tenant + stable origin + record ID.
Revision ordering is numeric within that origin. Upsert only a greater revision;
a lower revision never changes the current value. Equal current revisions with
identical canonical record digests are idempotent; conflicting reuse is HTTP409.
Tombstones retain their revision fence so delayed upserts cannot resurrect them.
Digests cover the submitted semantic record before service transformations.

A receipt is identified by authenticated tenant + origin + batch ID and includes
a canonical request digest and exact revision outcomes. The same batch ID with
different content fails409. A valid duplicate returns its original receipt.
Persist record changes and receipt atomically using the database's actual
transactional facilities. Never implement read-then-write ordering in JavaScript
without a database guard. Concurrent overlapping batches must serialize safely.
A failed batch must not leave an acknowledgment or partial unreported changes.

Success returns:

```json
{
  "protocolVersion": 1,
  "batchId": "same batch_id",
  "acceptedRevisionIds": ["exact submitted revision_id"],
  "unsupportedRevisionIds": [],
  "acceptanceLevel": "durable"
}
```

Every accepted revision is either applied, an identical duplicate, or superseded
by a durably stored newer revision of the same record. No success is implied by
an HTTP200 lacking a contract-valid receipt. Do not promise search indexing;
`indexed` may be introduced only with a tested completed indexing guarantee.
Source content remains subject to the existing default-tier scrub/minimization
policy, documented explicitly. Durable acceptance refers to that service
representation, not a claim of byte-identical raw secret retention.

The plugin maps camelCase receipt fields into the core acknowledgment contract.
It blocks unsupported protocol/schema/permission/mapping conditions and retries
transient transport/rate-limit errors using the core scheduler. A server404 must
not trigger fallback to the legacy last-write-wins endpoints.

## Readback and compatibility

`GET /v1/delivery/records` requires `rth:read` and an
`X-RelayHistory-Expected-Account` header containing the same account hash, checked
against authentication. It returns the current retained
records with deterministic opaque keyset pagination, optionally filtered by
kind/source/session identity. Include tombstones when explicitly requested for
synchronizing a destination; ordinary history views exclude them. Bind cursors
to tenant and filters or validate the equivalent scope. Define whether reads
are a live keyset listing or a snapshot; do not call a live listing consistent
snapshot export. Source connectors need a watermark/change cursor if incremental
readback is used, so updates to earlier record IDs cannot be skipped.

Keep old `/v1/ingest`, turns, auth, recall, and replay compatibility. New delivered
records must be available through the new read API; plugin readback can compose
legacy and new history without changing canonical source/session identity.
Keep existing cloud cursors intact. A new reliable job must use an explicit
migration/backfill generation, never reinterpret an old cursor as a generic
journal position or silently claim old data was acknowledged by the new endpoint.

## Verification and release gate

Use real local database tests for atomic rollback, duplicate/lost responses,
stale/concurrent revisions, conflicting keys, tombstones, tenant separation,
bounds, receipt replay, and pagination. Share a wire fixture with the plugin.
Exercise the actual plugin against the server route contract, including account
mismatch and old-server rejection. Retain all auth/legacy compatibility tests.
Server support must be available before enabling reliable plugin jobs in a
release. These changes are PRs only; no deployment or production migration is
part of this task.
