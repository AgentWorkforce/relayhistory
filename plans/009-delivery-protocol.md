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

`409 delivery_conflict` is recovery authority only in this exact error family.
As with `cursor_not_found`, the client must require both HTTP 409 and
`error.code`; another status, code, malformed body, or detail that does not
match the submitted batch is not permission to skip data. A record-revision
conflict returns a bounded deterministic conflict set:

```json
{
  "error": {
    "code": "delivery_conflict",
    "message": "Delivery record revision was reused with different content",
    "conflict": {
      "type": "record_revision",
      "originId": "submitted origin_id",
      "recordId": "submitted record_id",
      "submittedRevisionId": "submitted revision_id",
      "submittedRevision": 42,
      "submittedDigest": "64 lowercase SHA-256 hex characters",
      "currentRevisionId": "comparison revision_id",
      "currentRevision": 42,
      "currentDigest": "64 lowercase SHA-256 hex characters"
    },
    "conflicts": [
      {
        "type": "record_revision",
        "originId": "submitted origin_id",
        "recordId": "submitted record_id",
        "submittedRevisionId": "submitted revision_id",
        "submittedRevision": 42,
        "submittedDigest": "64 lowercase SHA-256 hex characters",
        "currentRevisionId": "comparison revision_id",
        "currentRevision": 42,
        "currentDigest": "64 lowercase SHA-256 hex characters"
      }
    ],
    "conflictCount": 1
  },
  "correlationId": "receiver diagnostic id"
}
```

The authenticated tenant is deliberately absent from the body: it scopes the
lookup but is never learned from an error. `submittedDigest` is the canonical
digest the receiver computed for the normalized submitted record. The current
fields describe the equal-revision identity used for comparison: either its
durable fence or an earlier submitted identity at that revision. The latter
preserves rejection of two records inside one batch that reuse a revision with
different content. The revisions must be equal and the digests different.

`conflicts` contains the first 100 conflicts in deterministic
`(record_id, revision, revision_id)` order, `conflictCount` is the total, and
`conflict` repeats its first entry for a stable discriminator. Clients must not
independently validate that order with a language-level string comparator:
database collation and client string ordering can differ. They instead validate
the list bound, unique submitted revision identities, and exact equality between
`conflict` and the first list entry. A client may quarantine only the returned
records whose origin, record ID, revision ID, revision, and recomputed submitted
digest all match. If the count exceeds the bounded list, the retry can receive
the next set. It retries the remaining
records under a deterministic child batch ID and durably records all reported
quarantines with its queue transition. This
lets records behind the poison record drain without weakening record-level
idempotency. A singleton conflict completes locally as one quarantined record;
it is not retried forever.

A receipt is identified by authenticated tenant + origin + batch ID and includes
a canonical request digest and exact revision outcomes. The same batch ID with
different content fails409. A valid duplicate returns its original receipt.
Batch-ID reuse is distinct and identifies no offending record:

```json
{
  "error": {
    "code": "delivery_conflict",
    "message": "Delivery batch identity was reused with different content",
    "conflict": {
      "type": "batch_id",
      "originId": "submitted origin_id",
      "batchId": "submitted batch_id",
      "submittedDigest": "64 lowercase SHA-256 hex characters",
      "currentDigest": "64 lowercase SHA-256 hex characters"
    }
  },
  "correlationId": "receiver diagnostic id"
}
```

After validating the submitted batch digest, a client retries the same immutable
records under a deterministic recovery batch ID derived from a domain tag, the
origin, parent batch ID, submitted batch digest, and ordered revision IDs. It
does not quarantine a record for a batch-ID conflict. Record-level revision
guards make the re-keyed retry safe after a lost response.

Conflict recovery must never synthesize a higher revision at delivery time.
When re-derivation changes a semantic record, the producer first commits that
change to the local store and the change feed issues its higher revision. A
mapping-only change uses a new mapping version/generation. In particular, the
change feed fingerprints the exact stored-column sets it exports; an additive
or subtractive schema migration rotates the feed epoch atomically and clears
revision-only named cursors before the new shape is replayed. External
watermarks from the prior epoch are refused and must resync from start. This is
the repair for schema additions such as `location`, which change canonical
record digests without writing each existing row.

Repository ownership is split deliberately. RelayHistory owns the change-feed
origin/revision semantics and the TypeScript wire/recovery helpers in this
repository. The hosted producer of this error is
`AgentWorkforce/relayhistory-cloud` (`packages/relayhistory/src/lib/delivery.ts`
and its transactional delivery migration); it must return the detail above
from the same serialized decision that detects the conflict. The durable queue
consumer is `AgentWorkforce/relay-desktop` (`probe/src/destination.rs` and
`probe/src/delivery/`); it must persist quarantine/re-key progress, expose an
accurate conflict state, and own the end-to-end no-head-of-line-stall test.
Pure SDK tests here prove classification, validation, and deterministic plans;
they are not a substitute for that queue integration test.

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
