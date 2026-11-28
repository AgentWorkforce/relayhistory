# Durable source event order

Status: Accepted
Date: 2026-10-29

## Context

Native transcripts commonly record several events at the same timestamp.
Identity-keyed source intake previously inserted those events in identity order,
losing native message/tool order and causing later local hydration conflicts.
Destination SQLite row IDs cannot carry order between machines.

## Decision

Store optional `session_events.source_order`, a nonnegative integer ordinal
within `(source, session_id, ts_ms)`. Native acquisition captures its processing
sequence; unchanged events retain ranks, gaps are allowed, and complete native
reorders update ranks in place. Public blocks, generic change-feed rows and
export/intake payloads carry the same fact. IDs and recorded times stay stable.

Readers sort `(ts_ms, COALESCE(source_order,id), id)`. The fallback supplies a
deterministic local display for unknown rows, not claimed source order. Legacy
rows migrate to null; only reacquisition establishes missing source evidence.
Known/unknown mixtures retain that distinction. All event, message, block and
user-turn header/neighbor reads use this shared order.

Snapshot records may arrive shuffled. Covered event snapshots replace prior
event records; kind-partial submissions retain untouched event evidence. Across
observations, a known event rank must agree and distinct events cannot share a
known rank at one timestamp. Legacy null does not override known order.
Conflicts with another observation or protected canonical order return
`SOURCE_ORDER_CONFLICT`, rolling back the acquisition transaction, including
its snapshots and checkpoints. Existing local/remote evidence protection stays
in force; this decision does not authorize overwriting protected payloads.

Event cursors keep stable `{ts_ms,id}` identities. A resumed read resolves rank
from that row and rejects mismatched source/session/ordered time. Historical
composite user-turn anchors remain positional only for wholly unknown-order
legacy buckets in the same source/session. A deleted cursor in
an ordered bucket is stale, not a guessed position. Pagination reads current
state; an order-changing revision requires restarting a cross-revision walk.

## Rejected alternatives

- Encoding positions in event IDs changes identity on reorder.
- Inventing milliseconds misstates source time.
- Sorting initial imports only loses order on rewrite and replay.
- Deleting/reinserting ordered canonical events changes row IDs, unnamed-turn
  keys and cursor positions, and emits avoidable tombstones.
- Reconstructing old order from destination row IDs would certify information
  absent from legacy acquisitions.

## Consequences

Session evidence contract becomes 5. Older strict intake readers reject the
new column rather than silently misorder it; newer readers accept absent order
as unknown. The coordinated export schema upgrade is recorded with the prompt
identity transport change. A later source reread can populate legacy nulls;
opening a database alone cannot certify old order. Consumers preserve the field
and use shared readers rather than guessing their own order.
