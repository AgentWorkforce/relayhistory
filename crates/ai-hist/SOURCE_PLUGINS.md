# Source plugins and the local history boundary

The engine contains storage, provider file ingestion, catalog discovery, and
normalization. It contains no commercial auth store or HTTP transport. Optional
Rust adapters implement `ShallowSessionProvider` and are explicitly registered in
`SourceRegistry`. JavaScript plugins acquire externally and call the fixed native
source intake methods, so installing another adapter does not require rebuilding
the native library.

The standalone Rust CLI is local. Use the SDK CLI or an SDK host with explicitly
installed source plugins for remote acquisition. A login never selects a source.
Cached local, remote, and combined catalog reads retain their scope semantics.

## Intake contract

`applySourceObservations` accepts one connector/instance/location batch. Each row
contains the shallow catalog fields, optional `raw_locator` (the opaque handle
used to acquire evidence), and separate `raw_path` (display or storage provenance).
If `raw_locator` is absent, the intake uses `raw_path`, then the session ID. A
plugin must supply only non-secret locators suitable for export.

Before fetching a session, call `getSourceObservation` and retain its opaque
revision. Fetch every page of the complete response before calling
`applySourceEvidence` with that `expected_revision`. A changed observation rejects
the stale result with `SOURCE_REVISION_CONFLICT`; retry by reading the observation
again. The revision is persisted and monotonic, including delete/recreate cycles.

Evidence is a complete snapshot for explicitly listed `covered_kinds`: history,
session events, tool calls, file edits, relationships, and optional commit links.
An empty covered kind removes this observation's owned records of that kind;
unspecified kinds retain their previous snapshot. Invalid identities, duplicate
canonical keys, and mismatched sessions fail validation before opening the DB.
Foreign SQLite IDs never become local row IDs. Upstream record/revision IDs stay
in the independent observation evidence. Record changes, checkpoint updates, and
revision fencing commit together or roll back together, including delivery
capture failures. Each evidence record is exported separately, with the same
configured per-record delivery limits as other evidence kinds.

## Canonical projection and upgrades

Connector identity and instance do not change the canonical provider session ID.
For known owned records, reconciliation is deterministic: local observation
snapshots take precedence, then connector ID and instance. A refresh can remove
its own records without removing another connector's evidence.

Older databases only retained one overwritten presence per session/location.
Migration labels it `legacy-unknown` and does not assign it to a guessed connector
or trust its old hydration checkpoint. Canonical rows that predate reliable
ownership remain intact, even if a later remote response omits or changes them.
The new response is retained in its own observation snapshot; the aggregate may
therefore retain stale legacy content. Reacquisition cannot recover erased
provenance and is not permission to delete that content. Likewise, direct local
parser writes revoke remote ownership when their values change; a local
observation without a normalized snapshot conservatively protects existing
canonical rows, including byte-identical content.

Provider wire formats are normalized in an isolated parser database before using
this same reconciliation path. Complete Claude evidence covers prompts, events,
tools, edits and relationships. Codex's diff interface covers file edits only and
is reported as partial. Legacy bare Codex diff record keys remain stable so an
upgrade does not materialize the same diff twice under a new connector prefix.
