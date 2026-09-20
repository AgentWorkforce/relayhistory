# Sourcing contract — what the Rust SDK must expose

Skeleton. This document lists the record types `ai-hist` must expose to a Rust
consumer, why each one exists, and where it stands today. It is the companion to
[`docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md`](decisions/2026-09-19-relayhistory-owns-session-sourcing.md),
which decides _who owns sourcing_; this file decides _what the surface has to
carry_ before burn can stop parsing logs.

The shape is `SessionEvidence` from
[#178](https://github.com/AgentWorkforce/relayhistory/issues/178). The demand
side is burn's `DerivedRecords` trait
(`crates/relayburn-sdk/src/ingest/ingest.rs`) and its record types
(`crates/relayburn-sdk/src/reader/types.rs`). Sections marked **TODO** are
filled in by the group-1 and group-2 issues named against them; nothing here
should be treated as shipped until the issue is closed and the ADR's capture
matrix says `✓`.

## The consumer's demand, in one place

burn asks a parser result for exactly seven things per session:

| `DerivedRecords` method | burn record type            | `SessionEvidence` field |
| ----------------------- | --------------------------- | ----------------------- |
| `turns()`               | `TurnRecord`                | `messages` + `requests` |
| `content()`             | `ContentRecord`             | `messages[].blocks`     |
| `events()`              | `CompactionEvent`           | `markers`               |
| `relationships()`       | `SessionRelationshipRecord` | `relationships`         |
| `tool_result_events()`  | `ToolResultEventRecord`     | `tool_results`          |
| `user_turns()`          | `UserTurnRecord`            | `user_turns`            |
| `request_id_lookup()`   | `RequestIdLookup`           | `messages[].request_id` |

Plus one derived type burn builds itself but whose _inputs_ must come from here:
`Inference` (`reader/inference.rs`), grouped by `request_id` where the harness
emits one and by `message_id` where it does not.

## 1. Messages and turns — `TurnRecord`

The per-model-request row. burn's fields, and what the SDK owes each one:

| burn field                           | Source of truth                           | Status                                                                                                                                        |
| ------------------------------------ | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `source`, `session_id`, `message_id` | `session_events`                          | present                                                                                                                                       |
| `turn_index`, `ts`                   | `session_events.ts_ms` + event ordering   | present; ordering key must be stable across re-parses                                                                                         |
| `model`                              | `session_events.model`                    | present for claude/codex; **TODO** cursor, grok, opencode                                                                                     |
| `project`                            | `session_events.project` (a cwd string)   | present                                                                                                                                       |
| `project_key`                        | —                                         | **TODO** [#175](https://github.com/AgentWorkforce/relayhistory/issues/175)                                                                    |
| `usage`                              | `session_events.token_json`               | **TODO** [#172](https://github.com/AgentWorkforce/relayhistory/issues/172) — must arrive normalized, once per request, not once per block     |
| `tool_calls`                         | `tool_calls`                              | present for claude/codex                                                                                                                      |
| `files_touched`                      | `file_edits`                              | present for claude/codex                                                                                                                      |
| `subagent`                           | `session_relationships` + sidechain flags | **TODO** [#164](https://github.com/AgentWorkforce/relayhistory/issues/164), [#170](https://github.com/AgentWorkforce/relayhistory/issues/170) |
| `stop_reason`                        | —                                         | **TODO** [#164](https://github.com/AgentWorkforce/relayhistory/issues/164)                                                                    |
| `fidelity` / `coverage`              | `Source::capabilities()`                  | **TODO** [#169](https://github.com/AgentWorkforce/relayhistory/issues/169) — today `capability` is hard-coded `"full"`                        |
| `activity`, `retries`, `has_edits`   | burn-derived                              | not ours — burn keeps classification                                                                                                          |

`Usage` must carry `input`, `output`, `reasoning`, `cache_read`,
`cache_create_5m` and `cache_create_1h` as distinct counters. The current
`token_json` blob is the provider's own shape, differs per harness, and for
Claude is duplicated across every content block of a message.

## 2. Content blocks — `ContentRecord`

`role` × `kind` (`text`, `thinking`, `tool_use`, `tool_result`) plus the block
payload. `session_events` already stores exactly this shape. Two obligations:

- `SessionQuery.include_text` must be honoured, mapping onto burn's
  `ContentStoreMode::{full, hash-only, off}`. A hash-only consumer must not pay
  to move transcript text.
- Non-text block types must stop being dropped by the parser's `_ => {}` arms
  ([#165](https://github.com/AgentWorkforce/relayhistory/issues/165)).

## 3. Markers — `CompactionEvent` and control rows

burn's `CompactionEvent` carries `ts`, `preceding_message_id` and
`tokens_before_compact`. Nothing in the ledger records a compaction today; there
is no `session_markers` table.

**TODO** [#165](https://github.com/AgentWorkforce/relayhistory/issues/165)
(markers table: compaction/summary, system rows, non-text blocks, Codex
lifecycle) and [#180](https://github.com/AgentWorkforce/relayhistory/issues/180)
(control events as typed evidence: slash-command triads, task notifications,
hooks, system reminders, Codex wrappers).

A compaction boundary is not cosmetic for a consumer: it is where a session's
token baseline resets, so cost attribution across it is wrong without it.

## 4. Relationships — `SessionRelationshipRecord`

burn's `RelationshipType` spans delegation _and_ continuity. The ledger writes
two values today: `delegated` and `materialized_local`.

| burn field                                                       | Status                                                                         |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `relationship_type` = delegation                                 | present (`session_relationships.relationship = 'delegated'`)                   |
| `relationship_type` = fork / resume / continuation               | **TODO** [#170](https://github.com/AgentWorkforce/relayhistory/issues/170)     |
| `related_session_id`                                             | present, nullable — `identity_status` says `observed` or `unlinked`            |
| `parent_tool_use_id`, `agent_id`, `subagent_type`, `description` | partially present via `child_agent_type` / `child_agent_name` / `evidence_ref` |
| `source_version`                                                 | **TODO** [#164](https://github.com/AgentWorkforce/relayhistory/issues/164)     |

burn's `ClaudeRelationshipEvidence` also needs the raw continuity signals a
transcript carries — `first_parent_uuid`, `has_resume_marker`,
`resume_target_session_id`, explicit fork and continuation target ids. Those are
inputs to [#170](https://github.com/AgentWorkforce/relayhistory/issues/170)'s
cross-file reconciliation, and the SDK should expose the reconciled
relationship, not ask the consumer to redo the reconciliation.

## 5. Tool results — `ToolResultEventRecord`

The largest single gap, and larger than it looks: the tool-result **event**
carries no tool identity at all. `session_events` has `message_id`, `ts_ms`,
`role`, `kind` and `text` — and no `tool_use_id` column
(`crates/ai-hist/src/store.rs`). Claude ingestion reads the result block's
`tool_use_id` only to call `set_tool_call_error` and
`update_file_edit_from_tool_result`, then drops it; Codex ingestion does the
same with the output record's `call_id` (`crates/ai-hist/src/ingest.rs`). The
`is_error` that survives is a per-_call_ boolean on `tool_calls`, not a
per-result one, and with several calls or several results in one turn there is
no key to join a stored result event back to the call it belongs to.

burn needs:

| burn field                                                           | Status                                                                                                         |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `message_id`, `ts`                                                   | present                                                                                                        |
| `tool_use_id`                                                        | present for Claude and Codex                                                                                   |
| `is_error` (result-level)                                            | present as `result_status` / `error_signal`; user-turn blocks expose the three-state projection                |
| `status`, `event_source`                                             | present for Claude and Codex                                                                                   |
| `call_index`, `event_index`                                          | present for Claude and Codex; stable across re-parses                                                          |
| `content_length`, `output_bytes`, `output_truncated`, `content_hash` | present as `payload_bytes`, `payload_truncated`, and `payload_hash` for Claude and Codex                       |
| `usage`, `usage_attribution`                                         | **TODO** [#172](https://github.com/AgentWorkforce/relayhistory/issues/172)                                     |
| `subagent_session_id`, `agent_id`                                    | **TODO** [#170](https://github.com/AgentWorkforce/relayhistory/issues/170)                                     |
| `replaced_tools`, `collapsed_calls`                                  | **TODO** [#171](https://github.com/AgentWorkforce/relayhistory/issues/171)                                     |

`output_bytes` and `output_truncated` must be measured at parse time. They
cannot be recovered afterwards from a truncated stored string, and a measurement
that can be destroyed by the truncation it describes is not a measurement.

## 6. User turns — `UserTurnRecord`

Blocks of a human turn with `byte_len`, `approx_tokens`, `tool_use_id` and
`is_error` per block, plus `preceding_message_id` / `following_message_id`. The
ledger stores the prompt text and the surrounding events but no per-block
accounting.

Shipped for Claude and Codex as `SessionStore::session_user_turns_page` and
the corresponding Node/TypeScript page and pagination helpers. Blocks carry
`byte_len`, `tool_use_id`, and three-state `is_error`; `approx_tokens` remains
consumer-derived rather than presenting a heuristic as measured evidence.

## 7. Request identity — `RequestIdLookup`

`BTreeMap<TurnKey, String>` keyed on `(source, session_id, message_id)`. burn
falls back to `message_id` and then to a row uuid, and records which fallback it
used in `Inference::request_id_source` — so a real upstream request id is always
distinguishable from a synthesized key.

Claude emits `requestId`; the crate never reads it. Codex and OpenCode have no
equivalent and legitimately fall back.

**TODO** [#164](https://github.com/AgentWorkforce/relayhistory/issues/164).

One caveat the facade must not paper over: for Codex the ledger's `message_id`
is _synthesized_ by the parser (`{line_index}:agent_message`), not a provider
identifier. It is stable for a given file content and re-parse, but it is not
the provider's id, and a consumer keying long-lived state on it should be told
so — `Source::capabilities()` is the place to say it.

## 8. Per-source capability declaration

`Source::capabilities() -> SourceCapabilities` must declare, statically and
honestly: which `EvidenceKind`s the source can produce, its relationship
capability (`always` / `sometimes` / `never` stable child identity — already
modelled in `relationship_capabilities()`), its usage accounting mode
(per-request, per-session, cumulative-delta, none), whether its message ids are
provider-issued or synthesized, and its watch roots.

This is the contract that lets a consumer distinguish "this session has no tool
calls" from "this source cannot report tool calls". Today the hydration result
claims `capability: "full"` for every source including prompts-only ones
([#169](https://github.com/AgentWorkforce/relayhistory/issues/169)), which is
precisely the well-formed-answer-computed-over-nothing failure mode: a
prompts-only grok session returns the same shape of success as a fully hydrated
Claude one.

## 9. Change feed

`changes_since(Watermark)` with named consumer cursors
([#179](https://github.com/AgentWorkforce/relayhistory/issues/179)). burn
replaces its per-file cursors and fs-event watching with this. Paging must be
keyset on a revision stamp, never an offset, and the tiebreak must be total —
two events in one session routinely share a timestamp.

## Out of scope for this contract

Pricing, cost, token estimation, activity classification, inference grouping,
span trees, and similarity-based session linking. Those are burn's, and this
document must never grow a section for them.
