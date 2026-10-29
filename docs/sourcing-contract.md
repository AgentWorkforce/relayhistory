# Sourcing contract — what the Rust SDK must expose

This document maps the evidence `ai-hist` exposes to a Rust consumer onto
burn's sourcing needs and records the limits of that evidence. It is the companion to
[`docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md`](decisions/2026-09-19-relayhistory-owns-session-sourcing.md),
which decides _who owns sourcing_; this file describes _what the surface
carries_. Per-provider availability is documented in the ADR capture matrix
and [`session-catalog.md`](session-catalog.md).

The shape is `SessionEvidence` from
[#178](https://github.com/AgentWorkforce/relayhistory/issues/178), served by
`SessionStore::session` and documented in
[`docs/sourcing-sdk.md`](sourcing-sdk.md). The demand
side is burn's `DerivedRecords` trait
(`crates/relayburn-sdk/src/ingest/ingest.rs`) and its record types
(`crates/relayburn-sdk/src/reader/types.rs`). The descriptions below reflect
the implemented facade. A field on the shared record does not imply that
every provider records a value: absent upstream facts remain optional.

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

| burn field | Source of truth | Status |
| --- | --- | --- |
| `source`, `session_id`, `message_id` | `session_events` | present |
| `turn_index`, `ts` | `session_events.ts_ms` + event ordering | native transcript ordering is retained; transported equal-time ordering remains limited (see below) |
| `model` | `session_events.model` | present when recorded, including Claude, Codex, Cursor, Grok and OpenCode; Cursor corpora can omit it |
| `project` | `session_events.project` | present; cwd string |
| `project_key` | `session_events.project_key` | present on `Message`; resolved repository identity, optional |
| `usage` | `session_events.token_json` + request rollup | normalized `Message::usage` and `SessionEvidence::requests`; sum request usage rather than block copies |
| `tool_calls` | `tool_calls` | present where supported; see provider capture matrix |
| `files_touched` | `file_edits` | present where supported; see provider capture matrix |
| `subagent` | `session_relationships` + envelope facts | recorded delegation and continuity edges, optional `is_sidechain`, and tool-result child identity |
| `stop_reason` | `session_events.stop_reason` | present on `Message` where recorded, including Claude and OpenCode |
| `fidelity` / `coverage` | `Source::capabilities()` + per-read coverage | hydration reports `full`, `partial`, or `shallow_only` from effective coverage |
| `activity`, `retries`, `has_edits` | burn-derived | consumer classification |

Shared history currently deduplicates by `(source, timestamp_ms, prompt)`.
Distinct native user events with equal text and time remain separate in
conversation evidence but can share one prompt-history row
([#539](https://github.com/AgentWorkforce/relayhistory/issues/539)). Native
equal-time event order also does not yet have a durable export/intake order
contract ([#532](https://github.com/AgentWorkforce/relayhistory/issues/532)).
Neither limitation is repaired by inventing timestamps or changing native IDs.

`NormalizedUsage` separates ordinary input, output, optional reasoning,
cache reads, total cache writes and optional five-minute/one-hour cache-write
buckets. It preserves provider totals and provider-reported costs without
pricing or estimating missing counters. `accounting` and `coverage` describe
how to interpret the counters; context occupancy is not billed request usage.
The raw `token_json` remains provider-shaped and can be repeated per block.
`Message::usage` deduplicates a message's copies; `SessionEvidence::requests`
settles request identity and usage, with `usage_error` and diagnostics for
conflicting or invalid evidence. Consumers sum eligible request rollups.

## 2. Content blocks — `ContentRecord`

`role` × `kind` (`text`, `thinking`, `tool_use`, `tool_result`) plus the block
payload. `session_events` already stores exactly this shape. A Claude
`thinking` block carrying a `signature` but no text stores no event while a
later record of its message follows it: its `unsupported_block` /
`thinking_signature` marker carries the record's `ts_ms`, `message_id` (the
record uuid) and, in its payload, the record's `request_id` and
`provider_message_id`, so a consumer places the record that opens a streamed
response in its request and dates the request by it. A response no record of
which stores rows stores the opening record's thinking event (empty text), so
every response is a request in `SessionEvidence::requests` with its usage.

`SessionQuery.include_text: false` omits transcript text from the facade's
reads while retaining identities, byte counts and recorded evidence hashes.
Consumers choose their own content-storage policy. Recorded non-text and
unsupported blocks are represented by bounded markers where the provider
adapter supports them; consult the capture matrix for provider coverage.

Codex `input_image` blocks are represented by a separate `kind = text` event
with `raw_kind = input_image` and self-contained Markdown in `text`:
`![Attached image](data:image/png;base64,...)`. The bytes come only from the
recorded response item, never a path or remote URL. PNG, JPEG, GIF and WebP
copies are bounded to 1 MiB decoded; unsupported or larger blocks retain an
explicit unavailable-image notice. Image events do not add prompt-history rows.
Consumers render the image block separately from prose. Existing transcript
checkpoints are invalidated once so older recordings gain their image copies.

## 3. Markers — `CompactionEvent` and control rows

burn's `CompactionEvent` carries `ts`, `preceding_message_id` and
`tokens_before_compact`. `session_markers`
([#165](https://github.com/AgentWorkforce/relayhistory/issues/165)) records
compaction and summary boundaries, system rows, non-text blocks and Codex
lifecycle events, read back through `session_markers_page`.

A compaction boundary is not cosmetic for a consumer: it is where a session's
token baseline resets, so cost attribution across it is wrong without it.

**Control rows**
([#180](https://github.com/AgentWorkforce/relayhistory/issues/180)). A row a
harness writes into the user role that is not a human prompt keeps
`role = "user"`, `kind = "text"` and its text verbatim, and carries
`session_events.control_kind`, one of:

| `control_kind`             | Source | Record                                                                                     |
| -------------------------- | ------ | ------------------------------------------------------------------------------------------ |
| `slash_command_caveat`     | claude | the "Caveat: The messages below were generated by the user while running local commands…" |
| `slash_command_invocation` | claude | `<command-name>` / `<command-message>` / `<command-args>`                                  |
| `slash_command_output`     | claude | `<local-command-stdout>` / `<local-command-stderr>`                                        |
| `task_notification`        | claude | `<task-notification>`, or `origin.kind` / `attachment.commandMode` = `task-notification`   |
| `hook_output`              | claude | `<user-prompt-submit-hook>` and any other `<…-hook>` wrapper                               |
| `bash_passthrough_input`   | claude | `<bash-input>`                                                                             |
| `bash_passthrough_output`  | claude | `<bash-stdout>` / `<bash-stderr>`                                                          |
| `system_reminder`          | claude | one `<system-reminder>…</system-reminder>` block, stored as its own row                    |
| `codex_context_wrapper`    | codex  | `<environment_context>`, `<permissions instructions>`, `# AGENTS.md`, …                    |
| `meta`                     | claude | `isMeta: true` and nothing above applies                                                   |
| `resume_marker`            | claude | a bare `/resume <id>` or `/continue <id>`                                                  |
| `synthetic`                | opencode | a user `text` part flagged `synthetic: true` (harness-written context, e.g. an `@file` read) |
| `synthetic`                | openclaw | a user message OpenClaw wrote itself: a heartbeat or wake prompt (`[OpenClaw heartbeat poll]`, the default heartbeat prompts), `provenance.kind` `inter_session` or `internal_system`, `excludeFromContext`, or `__openclaw.contextFreeCommand` |

A `control_kind` is only ever set on a `role = "user"`, `kind = "text"` row,
and only to one of the spellings above; the source-evidence validation a
connector's rows pass through rejects any other spelling and any control kind
on an assistant, thinking, tool-use or tool-result row, because every reader
treats a non-null value as authoritative.

`null` is a genuine prompt, or model output. `history`, `sessions.first_prompt`,
`session_user_turns_page` and `attribute_usage_to_prompts` all derive from the
same classification (`crates/ai-hist/src/ingest/control.rs`), so a control row
is never a prompt, never a first prompt, never a user turn (or a block of one)
and never a prompt root. For Claude the full-transcript metadata fold settles
`first_prompt`, null included, on every sync or hydration; the shallow catalog
upsert still coalesces so a partial or remote read cannot blank a title. A `<system-reminder>` inside a
prompt is split out into a `system_reminder` row sharing the prompt's
`message_id` (`event_uid` = `<prompt uid>:reminder:<n>`), and the prompt row
and `history` carry only the human's text.

A slash-command triad — caveat → invocation → output, chained by `parentUuid` —
is additionally grouped into one `session_markers` row of
`kind = "slash_command"`, `message_id` = the invocation's uuid, `parent_id` =
its parent (the caveat when chained), whose `payload_json` carries
`command_name`, `command_message`, `command_args`, `command_mode`
(`attachment.commandMode`), `origin_kind` (`origin.kind`), `attachment_type`,
`caveat_event_uid`, `invocation_event_uid`, `output_event_uid`, `stdout_bytes`
(bytes inside the `<local-command-stdout>` wrapper), and `stderr_event_uid` /
`stderr_bytes` when a stderr row followed. Absent fields are omitted, never
null. burn's `slash_triads` classifier reads that payload, not the transcript.

## 4. Relationships — `SessionRelationshipRecord`

burn's `RelationshipType` spans delegation _and_ continuity. The ledger writes
`delegated`, `materialized_local`, `continuation`, `fork` and `resume`,
when deterministic provider evidence supports them.

| burn field                                                       | Status                                                                         |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `relationship_type` = delegation                                 | present (`session_relationships.relationship = 'delegated'`)                   |
| `relationship_type` = fork / resume / continuation | present through continuity reconciliation |
| `related_session_id`                                             | present, nullable — `identity_status` says `observed` or `unlinked`            |
| `parent_tool_use_id`, `agent_id`, `subagent_type`, `description` | partially present via `child_agent_type` / `child_agent_name` / `evidence_ref` |
| `source_version` | retained in internal continuity evidence when recorded; not a field on the public `SessionRelationship` |

burn's `ClaudeRelationshipEvidence` also needs the raw continuity signals a
transcript carries — `first_parent_uuid`, `has_resume_marker`,
`resume_target_session_id`, explicit fork and continuation target ids. Those are
inputs to the crate's cross-file continuity reconciliation. Consumers read
the reconciled relationships, including `observed` / `unlinked` identity
status and the captured evidence reference; they do not reparse transcripts.
See [continuity](architecture.md#continuity) for the resolution policy.

## 5. Tool results — `ToolResultEventRecord`

Tool-result events retain `tool_use_id` alongside the message, timestamp and
text. `SessionEvidence::tool_results` exposes result-level status, provenance,
error signal, stable call/event indexes and payload measurements. Tool-call
`is_error` is a separate projection; it does not replace result-level evidence.
Fields remain optional when the source did not record them.

burn needs:

| burn field                                                           | Status                                                                                                         |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `message_id`, `ts`                                                   | present                                                                                                        |
| `tool_use_id`                                                        | present for Claude and Codex                                                                                   |
| `is_error` (result-level)                                            | present as `result_status` / `error_signal`; user-turn blocks expose the three-state projection                |
| `status`, `event_source`                                             | present for Claude and Codex                                                                                   |
| `call_index`, `event_index`                                          | present for Claude and Codex; stable across re-parses                                                          |
| `content_length`, `output_bytes`, `output_truncated`, `content_hash` | present as `payload_bytes`, `payload_truncated`, and `payload_hash` for Claude and Codex                       |
| `usage`, `usage_attribution` | normalized request usage and prompt attribution are separate reads; `ToolResult` does not carry per-result usage |
| `subagent_session_id`, `agent_id` | present when the result records a delegated child |
| `replaced_tools`, `collapsed_calls` | Claude `_meta.replaces` / `_meta.collapsedCalls` are retained in `tool_replacement` marker payloads as `replaces` / `collapsedCalls` |

`output_bytes` and `output_truncated` must be measured at parse time. They
cannot be recovered afterwards from a truncated stored string, and a measurement
that can be destroyed by the truncation it describes is not a measurement.

## 6. User turns — `UserTurnRecord`

Blocks of a human turn with `byte_len`, `approx_tokens`, `tool_use_id` and
`is_error` per block, plus `preceding_message_id` / `following_message_id`. The facade exposes recorded per-block byte counts and tool-result status;
it does not estimate token counts.

Shipped for Claude and Codex as `SessionEvidence::user_turns` (read through
`SessionStore::session`) and the corresponding Node/TypeScript page and
pagination helpers. Blocks carry
`byte_len`, `tool_use_id`, and three-state `is_error`, and each turn carries
`preceding_message_id` / `following_message_id` — the nearest messages recorded
either side of it, null only where the session recorded no named message on
that side; an unnamed event is passed over rather than nulling the field.
`approx_tokens` remains consumer-derived rather than presenting a heuristic as
measured evidence.

## 7. Request identity — `RequestIdLookup`

`BTreeMap<TurnKey, String>` keyed on `(source, session_id, message_id)`. burn
falls back to `message_id` and then to a row uuid, and records which fallback it
used in `Inference::request_id_source` — so a real upstream request id is always
distinguishable from a synthesized key.

Claude's recorded `requestId` is stored as `session_events.request_id` and
exposed on `Message::request_id`; `provider_message_id` preserves the upstream
message identifier separately. `SessionEvidence::requests` carries
`request_key` and `request_key_source`, distinguishing provider request IDs,
provider message IDs, recorded request spans and record-ID fallbacks.
Codex request spans group the records of a request without inventing a
provider request ID. The request rollup is the shared grouping contract.

One caveat the facade must not paper over: Codex JSONL fallback `message_id` values
can be _synthesized_ by the parser (`{line_index}:agent_message`), rather than
provider identifiers. It is stable for a given file content and re-parse, but it is not
the provider's id, and a consumer keying long-lived state on it should be told
so — `Source::capabilities()` is the place to say it.

## 8. Per-source capability declaration

`Source::capabilities() -> SourceCapabilities` must declare, statically and
honestly: which `EvidenceKind`s the source can produce, its relationship
capability (`always` / `sometimes` / `never` stable child identity — already
modelled in `relationship_capabilities()`), its usage accounting mode
(per-request, per-session, cumulative-delta, none), whether its message ids are
provider-issued or synthesized, and its watch roots.

Shipped as `Source::capabilities()` — `evidence_kinds`, `relationships`,
`usage_accounting`, `message_ids` (`Provider` / `Synthesized` / `Mixed` /
`None`), `hydrates_by_path` and `watch_roots(&roots)`; `SessionEvidence::coverage`
repeats the kinds per read.

This lets a consumer distinguish an absent tool call from an unsupported
evidence kind. Hydration computes effective coverage for the selected source
and acquired evidence: all full-session kinds gives `full`, no kinds gives
`shallow_only`, and any other set gives `partial`. Capability is separate from
catalog `discovery_state`, which records whether indexing reached the source
stamp. A successfully indexed session can still have partial evidence coverage.

## 9. Change feed

Shipped as `SessionStore::changes_since(from, ChangeQuery)` with named
consumer cursors
([#179](https://github.com/AgentWorkforce/relayhistory/issues/179)). burn
replaces its per-file cursors and fs-event watching with this. Paging is keyset
on a per-row `revision` stamp, never an offset; the order is
`(revision, kind, record_key)` and revisions are unique per write, so the order
is total whatever the timestamps say. Each `Upsert` carries the typed row, so
a consumer needs no second read. Two obligations on the consumer, both
documented in [`architecture.md`](architecture.md#change-feed): a re-seen
`record_key` is a replace, and a watermark the store did not issue — another
database epoch, or a revision ahead of `head_revision` — means the store was
reset or replaced and the consumer must resync from `Watermark::START`. A
column-name or declared-type change restamps only rows of the affected kind
above the current head, so consumers never reuse an equal revision with a new
semantic payload or replay unrelated kinds. An appended column restamps only
the rows holding a value in it; a row holding NULL there keeps its revision and,
re-read, carries the column as `null`, so a consumer reads a column a row was
delivered without as NULL. A newly fed kind is backfilled
above the old head and does not reset established streams. Retiring a kind is
the exceptional epoch reset because no live rows remain to communicate its
removal; it resets only consumers of every current kind, and one whose kinds
leave out a current kind keeps its position. A named
cursor is bound to the kind set it was committed for; a consumer that filters
keeps one name per filter. Requests
(section 7) are not fed as rows of their own — `session_requests` is a view —
the events that compose one are.

## Out of scope for this contract

Pricing, cost, token estimation, activity classification, inference grouping,
span trees, and similarity-based session linking. Those are burn's, and this
document must never grow a section for them.
