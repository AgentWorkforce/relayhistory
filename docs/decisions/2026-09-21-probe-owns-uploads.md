# The probe owns uploads; core owns evidence

Status: accepted and implemented
Date: 2026-09-21

## Context

Session upload scheduling had become part of ai-hist's local database API,
native addon and SDK. The probe delegated its defining responsibility to that
engine. This coupled local history to upload jobs, consent, leases and receivers,
while selected-session performance required changes on both sides.

## Decision

The existing RelayHistory probe package owns sharing membership, jobs, durable
queues and prepared bodies, scheduling, fencing, leases, retries,
acknowledgments, credentials and transport. Upload execution is an integral
part of the probe. There is no new delivery crate and no copy of the engine.

The ai-hist crate owns acquisition, parsing, evidence writes, metadata queries,
targeted hydration and consistent export/change capture. Its `export::capture`
API provides transaction-scoped subscriptions, snapshot bounds/preimages and
indexed session reads. Probe queue progress and subscription progress commit
atomically. The storage schema contains no account, receiver or retry semantics.
Local NDJSON export remains independent of the probe.

Native contract 20 separates `historyExport` from obsolete upload entry points.
Core SDK/CLI/MCP upload calls return `HISTORY_DELIVERY_MOVED` without loading
credentials, receivers or creating a database. Control moves to the existing
`@relayhistory/capture` helper API and probe commands. Generic JavaScript
receiver orchestration is removed; destination contracts remain for readback
and compatibility. Rust callers of `ai_hist::delivery` must migrate explicitly.
The legacy Cargo feature `delivery` aliases `export`, not an upload engine.

## Migration and recovery

Retain the existing disk table names and all persisted upload state. On first
export-enabled core open, import active, paused and blocked legacy jobs/members
as storage subscriptions and refresh capture triggers in the schema transaction.
A marker makes this repeatable. Core-only writes then preserve future revisions
and unread preimages even before the probe restarts. No upload generation,
prepared bytes, lease, acknowledgment, retry deadline or membership cutoff is
reset. The probe initializes and thereafter owns only its upload tables.

Destructive marker migrations in a build without export support refuse unread
snapshots that they cannot preserve. New native/SDK versions must match contract
20. Upgrade the core and probe together; running an older Rust writer after
migration is not a supported downgrade. Restore a pre-upgrade database backup
and matching binaries if a downgrade is required.

## Alternatives rejected

- Keep a generic uploader in core: leaves upload lifecycle coupled to local
  history and misstates the probe's responsibility.
- Extract a reusable delivery crate: creates another architectural layer without
  another independent consumer that needs it.
- Move parsers or duplicate evidence SQL into the probe: violates the single
  evidence-owner contract.
- Recreate jobs during migration: loses exact prepared bytes, unread preimages,
  retry state and receipt identity.
- Rename every disk table during extraction: adds migration risk without
  improving code ownership.

## Consequences

Core, native addon and SDK build without the plugin tree. Fresh local databases
have no upload tables. The probe uses the same tested upload state machine for
foreground helper drains and background collection. Selected membership and
snapshot lookup remain proportional to selected records.

An enabled probe still shares SQLite and a retention budget with core evidence.
Paused/offline subscriptions can retain records; exhausting the cap rolls back
capture visibly. This change does not provide storage or process isolation.
The separate decision to move the Agent Relay probe to relay-desktop is deferred.
