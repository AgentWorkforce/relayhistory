# Incremental explicit-session delivery

Status: accepted and implemented
Date: 2026-09-21

## Context

The probe's selected mode encoded consent as all sources minus one exclusion
per unselected catalog identity. A checkbox change replayed every exclusion,
cancelled the destination generation and bootstrapped all evidence. The old
snapshot query also collected all remaining rowids before choosing one.
Capture ran before delivery, so an unrelated slow provider delayed records
already available to send.

## Decision

The probe-owned delivery engine owns normalized session membership (see the
[ownership ADR](2026-09-21-probe-owns-uploads.md)). One destination
job retains its immutable configuration and account/instance generation. Each
member has a distinct snapshot identity, cutoff revision, historical bounds,
bootstrap position and indexed journal cursor. Inclusion adds that snapshot;
removal drops that membership and fences dispatch. Re-inclusion creates a new
cutoff and never reuses a cursor that may have skipped private records.

Core's existing capture preimage machinery also serves these snapshots. Current
rows and retained preimages are sought by source, session and rowid before any
payload is read. Ownership-changing updates preserve OLD evidence even when the
NEW identity belongs to another member. An indexed readiness queue is woken
transactionally by capture; idle members do not scan unrelated journal entries.
A persisted round-robin position advances between ready members so a continuously
busy session cannot starve the others, including across worker restarts.
Compaction respects the lowest cursor among members with unread work. Normal
probe job transport, immutable batches, claims, acknowledgments, retry states and
leases remain shared. Consent is checked on prepare, claim and immediately
before dispatch. A member's fresh cutoff excludes stale queued work from a
previous inclusion; an already transmitted request cannot be recalled.

Relationship evidence requires both parent and child membership. Adding a
previously private child journals only its newly eligible incoming edges;
it does not replay the parent's completed history. Persistent global exclusions
remain authoritative. The probe can atomically clear a legacy exclusion for its
selected job by taking a fresh member snapshot and journaling newly eligible
relationships. Other affected jobs still require a new generation; any failed
guard or snapshot rolls back consent, membership and fencing together.

The probe persists a durable change intent under its existing control locks.
Ordinary selected-mode mutations call the probe membership API, retain the
same job, and update the compatibility manifest. All/new modes and explicit mode
transitions retain their generation/exclusion semantics. Selected setup skips
full-history capture. Selected delivery drains committed evidence first and
uses cancellable, targeted core hydration only after its backlog is caught up.
A separate worker performs periodic shallow inventory, leaving newly discovered
sessions private. Targeted hydration reconciles project identity only for the
selected session and its delegation descendants using the existing ancestor
walk and precedence rules.

## Upgrade and recovery

Schema upgrade creates additive tables/indexes and refreshes capture triggers.
Index construction is a one-time cost proportional to existing table sizes.
Read-only commands recognize an older schema without trying to mutate it.

The first writable use adopts an older selected job atomically in place. Its
explicitly selected members inherit the old immutable cutoff, bootstrap bounds,
unread preimages and cursor; pending/prepared batches, destination generation,
receipts, paused state and failures survive. Adoption can need temporary
retention headroom while copying shared preimages; failure rolls back entirely.
Replaying an adopted job is a no-op. Replay of a partially applied checkbox
intent safely completes each changed member before restarting delivery.

## Alternatives rejected

- Replay fewer exclusion setters: still scans unrelated bootstrap payloads and
  restarts completed work.
- Put all session IDs in configuration JSON: violates the 64 KiB bound and makes
  configuration/cursor mutation unsafe.
- Create one destination job per session: violates the 32-job bound and
  duplicates scheduling/transport state.
- Clear an exclusion without a new baseline: permanently loses records skipped
  by an earlier cursor.
- Recreate the old job during upgrade: can discard unacknowledged tombstones or
  deleted-row evidence no longer available in live tables.
- Duplicate provider parsing or evidence traversal in the probe: breaks the
  repository's single sourcing boundary. Upload SQL belongs to the probe.

## Consequences and limits

An ordinary selected mutation performs no work proportional to unselected
history. The compatibility `selected.json` manifest still costs O(selected
members) to read/write on an actual change; repeated controls use indexed
membership checks. Membership rows and snapshot coordination do not grow the
configuration JSON or consume additional destination-job slots.

A schema upgrade and first legacy adoption are measured separately from steady
state. Inventory provider enumeration can still be slow, and SQLite contention
is still possible between independent core callers; inventory is not awaited
by the selected delivery scheduler. All/new mode selection replacement is
unchanged. Historical generic capture-error logs cannot identify their original
failure; new diagnostics classify errors without persisting raw provider text.
