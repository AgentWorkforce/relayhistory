# Prompt history retains observed event identity

- Status: accepted
- Date: 2026-10-29

## Context

History's `(source, timestamp_ms, prompt)` uniqueness collapses distinct native
user messages with equal text and time. DeepAgents checkpoints can record a
single available time for several messages. Session events already retain their
native identities; adding ordinal milliseconds would invent source evidence.
Legacy feed keys, exported record IDs and persisted observation ownership
snapshots must remain usable after upgrading.

## Decision

Add nullable `event_uid` to shared history and expose it on `HistoryEntry` and
facade `Prompt`. When present, the observed reference is session-scoped identity:
`(source, session_id, event_uid)`. A rewrite updates the same row even if its
text/time changes. An anonymous row retains the exact legacy identity. Partial
unique indexes implement the disjoint identities; legacy lookups explicitly
require a NULL event UID. Source intake uses the same rule for validation,
upserts, ownership and withdrawal.

Native feed/export keys are `["history", source, "event", session_id, event_uid]`.
Export schema version 3 advertises the new identity namespace and source-order
evidence. New intake accepts prior snapshots; older strict intake rejects
unsupported new fields rather than silently collapsing them. Legacy keys are unchanged, so existing tombstones, record IDs and ownership
snapshots remain valid. The migration atomically rebuilds the original inline
UNIQUE table while preserving every stored column, row ID, revision, index,
non-feed trigger and the autoincrement high-watermark. Feed triggers regenerate
with the native key rule; NULL appended columns do not restamp legacy rows.

DeepAgents writes its existing user text event UID and invalidates prior parser
checkpoints. An authoritative reread retires its unowned anonymous projection;
independently retained remote-owned anonymous records remain separate evidence.
The migration itself never guesses which native event an anonymous row meant.
Other adapters can opt into observed identity separately.

## Rejected alternatives

- Fabricated timestamps: misstate provider time and alter usage correlation.
- Mutable ordinals or new provider IDs: change stable native identity.
- Append event UID to every legacy key: invalidate all old exported IDs and
  persisted ownership snapshots, including records with no new evidence.
- A uniqueness-only change: intake and retirement could still collapse or erase
  rows unless feed, typed reads and transport use the same identity contract.

## Consequences

Repeated identified prompts survive replay, edits, deletion, paged export and
remote intake. Old anonymous snapshots remain supported without an epoch reset.
`HistoryEntry` struct literals must supply the new optional field, a pre-1.0
minor Rust change. This decision does not establish equal-time event order or
change the local/remote retirement ownership policy.
