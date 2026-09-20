# relayhistory owns session sourcing; burn consumes evidence through a Rust SDK

- **Status:** Accepted
- **Date:** 2026-09-19
- **Issue:** [#161](https://github.com/AgentWorkforce/relayhistory/issues/161)
- **Epic:** [#160](https://github.com/AgentWorkforce/relayhistory/issues/160)
  (burn side: [AgentWorkforce/burn#553](https://github.com/AgentWorkforce/burn/issues/553))
- **Supersedes:** nothing. First ADR in this repository.

## Context

Two repositories parse the same harness logs today, with no shared code.

| Harness     | relayhistory (`crates/ai-hist`)                 | burn (`crates/relayburn-sdk/src/reader/`)                                                     |
| ----------- | ----------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Claude Code | `ingest_claude_transcript_as` (`src/ingest.rs`) | `reader/claude.rs` + `claude/{incremental,parent_chain,relationships,tool_results,subagents}` |
| Codex       | `ingest_codex_rollout` (`src/ingest.rs`)        | `reader/codex.rs` + `codex/incremental.rs`                                                    |
| OpenCode    | `sync_opencode_session` (`src/store.rs`)        | `reader/opencode.rs`                                                                          |
| Cursor      | `ingest_cursor_line` (`src/ingest.rs`)          | not supported                                                                                 |
| Grok        | `ingest_grok_session` (`src/ingest.rs`)         | not supported                                                                                 |

burn has no dependency on relayhistory. relayhistory already defers cost to
burn by name (`plugins/relayhistory/rust/src/convergence.rs` — "Input excludes
cache reads; cost is owned by burn", and a reserved `lens: "burn"`). The result
is that every harness change is paid for twice and the two parsers disagree:
burn merges Claude's per-block `usage` copies at parse time, while relayhistory
copies `message.usage` onto every block (`src/ingest.rs`) and de-duplicates them
later in plugin-private code (`plugins/relayhistory/rust/src/outbox.rs`, "Claude
copies message.usage onto every content block").

## Decision

1. **relayhistory is the single owner of acquiring, parsing and storing session
   evidence** for every harness — Claude Code, Codex, Cursor, Grok, OpenCode,
   Agent Relay, and any future collector. "Evidence" is everything the log
   contains that any downstream consumer needs: messages, content blocks, usage,
   model, stop reason, request ids, tool calls, tool results (with sizes, errors
   and truncation), file edits, compaction and summary markers, control and meta
   rows, relationships (delegation, fork, resume, continuation), and session
   metadata (cwd, git, versions).

2. **burn is a consumer.** burn keeps pricing (`analyze/pricing.rs`,
   `models.dev.json`), cost, activity classification, inference grouping, span
   trees, hotspot/overhead/compare analytics, its ledger and fingerprints, and
   its enrichment/pending stamps. burn drops harness readers, session-root
   walking, per-file cursors, fs-event watching, and gap detection over raw logs.

3. **The SDK is one published Rust crate, `ai-hist`.**
   [#162](https://github.com/AgentWorkforce/relayhistory/issues/162) merged
   `ai-hist-core` into `crates/ai-hist` (shipped in
   [PR #185](https://github.com/AgentWorkforce/relayhistory/pull/185)), so the
   current layout is one crate: parsers in `src/ingest.rs` and `src/ingest/`,
   shallow discovery in `src/discover.rs`, schema and queries in `src/store.rs`,
   and the public entry point in `src/session_store.rs`. `SessionStore` is the
   primary store-operation entry point
   ([#178](https://github.com/AgentWorkforce/relayhistory/issues/178)); the
   evidence and record types an embedder reads back — `EvidenceKind`,
   `EvidenceRecord`, `HistoryEntry`, `SessionEvent`, `SessionToolCall`,
   `SessionFileEdit`, `SessionScope`, `SessionLocation` — are re-exported
   alongside it from `src/lib.rs`. Everything that takes a raw
   `rusqlite::Connection` is behind the `unstable-internal` feature.
   **Cargo semver is the Rust contract** — there is no separate Rust
   contract-version constant, and a `cargo public-api` snapshot in CI guards the
   surface. The TypeScript `ai-hist` SDK remains for JS consumers; burn is
   Rust-first and its Node package is napi over Rust.

4. **Store shape:** burn reads relayhistory's SQLite database (`ai-history.db`)
   _through_ `SessionStore` — read plus change feed — and keeps its own durable
   state in its own ledger. burn never writes `ai-history.db` itself. Because
   `ai-hist` is an in-process crate, a burn process that calls `sync`, `hydrate`
   or `watch` **is** a SQLite-writing process; what the design guarantees is one
   writer _implementation_ (one schema, one lock discipline, one busy handler),
   not one writer process. See
   [Store shape](#store-shape-one-writer-implementation-not-one-writer-process).

5. **"Complete" is defined by the capture matrix below**, which is the
   acceptance checklist for the group-1 parity issues and is kept in sync with
   [`docs/session-catalog.md`](../session-catalog.md) and
   [`docs/sourcing-contract.md`](../sourcing-contract.md).

## Alternatives rejected

**Library-only — burn calls relayhistory's parsers but keeps its own ingest loop
and store.** Two stores, two cursors, two watchers, and every incremental-read
bug fixed twice. Rejected as the end state. Acceptable only as a transitional
step if the group-2 facade cannot land in one release.

**burn consumes the TypeScript SDK or an NDJSON export.** Forces a Node
dependency into a Rust hot path and loses the in-process incremental API.
Rejected.

**A shared parser crate extracted from burn into a third repository.** burn's
parser drops record types relayhistory needs (prompts, thinking text, file
edits), and relayhistory already owns the storage, identity, relationship and
delivery layers that a parser-only crate would still need a home for. Rejected.

**Two published crates (`ai-hist-core` + `ai-hist-engine`).** Every consumer
already depended on both — `crates/ai-hist-cli`, `crates/ai-hist-napi`,
`plugins/provider-sources/rust`, `plugins/relayhistory/rust` — and the layering
was not real: "core" contained `parse_claude`, `parse_codex`,
`parse_cursor_text` and the OpenCode sync, while the main Claude and Codex
parsers lived in "engine". It was not storage versus parsing; it was two halves
of one library. Rejected in favour of one crate with feature gates
(`delivery`, `opencode-backup`, `git-hooks`, `unstable-internal`), shipped in
[#162](https://github.com/AgentWorkforce/relayhistory/issues/162).

## Capture matrix

Derived from the code on the date of this ADR, not from the issue text. Each
cell was checked against the parser (`crates/ai-hist/src/ingest.rs`,
`src/ingest/hydrate.rs`, `src/discover.rs`) and the schema
(`crates/ai-hist/src/store.rs`).

Legend:

- **✓** — captured from the provider, stored in the ledger, and readable back.
- **◐** — partial: captured but lossy, or stored but not exposed as typed
  evidence, or available only from the shallow catalog read.
- **✗** — not captured. The provider record is read and discarded, or never
  read.
- **—** — not applicable, and therefore not a backlog item: the provider emits
  nothing to capture, or the record type does not apply to that source at all.

`trajectory` is not a harness. It is a derived record type
(`DISCOVERY_EXEMPTIONS` in `src/discover.rs`, "derived trajectory records, not
provider sessions") that lands in the `trajectories` table and is filtered out
of `sessions list`. It is listed for completeness; every session-evidence row is
`—` for it by construction.

| Record type                                                        | claude | codex | cursor | grok | opencode | relay | trajectory | Owner / closes                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------ | ------ | ----- | ------ | ---- | -------- | ----- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Prompt / `history` row                                             | ✓      | ✓     | ✓      | ◐    | ✓        | ✓     | —          | grok timestamps: [#167](https://github.com/AgentWorkforce/relayhistory/issues/167)                                                                                                                                                                                            |
| `session_events` — `text`                                          | ✓      | ✓     | ✓      | ✗    | ✗        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168) / [#177](https://github.com/AgentWorkforce/relayhistory/issues/177) |
| `session_events` — `thinking`                                      | ✓      | ✓     | —      | ✗    | ✗        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168)                                                                     |
| `session_events` — `tool_use`                                      | ✓      | ✓     | ✓      | ✗    | ✗        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168)                                                                     |
| `session_events` — `tool_result`                                   | ✓      | ✓     | —      | ✗    | ✗        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168)                                                                     |
| Model, per event                                                   | ✓      | ✓     | —      | ◐    | ◐        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168)                                                                     |
| Token usage                                                        | ◐      | ◐     | —      | ✗    | ✗        | ✗     | —          | [#172](https://github.com/AgentWorkforce/relayhistory/issues/172) (supersedes #99)                                                                                                                                                                                            |
| `request_id`                                                       | ✗      | —     | —      | —    | —        | —     | —          | [#164](https://github.com/AgentWorkforce/relayhistory/issues/164)                                                                                                                                                                                                             |
| `stop_reason`                                                      | ✗      | —     | —      | —    | ✗        | —     | —          | [#164](https://github.com/AgentWorkforce/relayhistory/issues/164)                                                                                                                                                                                                             |
| Codex `turn_id`                                                    | —      | ✗     | —      | —    | —        | —     | —          | [#164](https://github.com/AgentWorkforce/relayhistory/issues/164)                                                                                                                                                                                                             |
| Sidechain / meta flags                                             | ✗      | ✗     | —      | —    | —        | —     | —          | [#164](https://github.com/AgentWorkforce/relayhistory/issues/164)                                                                                                                                                                                                             |
| Tool calls (`tool_calls`)                                          | ✓      | ✓     | ✓      | ✗    | ✗        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168)                                                                     |
| Tool-result fidelity (identity, bytes, truncation, hash, ordering) | ✗      | ✗     | ✗      | ✗    | ✗        | ✗     | —          | [#171](https://github.com/AgentWorkforce/relayhistory/issues/171)                                                                                                                                                                                                             |
| File edits (`file_edits`)                                          | ✓      | ✓     | ✓      | ✗    | ✗        | ✗     | —          | [#166](https://github.com/AgentWorkforce/relayhistory/issues/166) / [#167](https://github.com/AgentWorkforce/relayhistory/issues/167) / [#168](https://github.com/AgentWorkforce/relayhistory/issues/168)                                                                     |
| Compaction / summary markers                                       | ✗      | ✗     | ✗      | ✗    | ✗        | ✗     | —          | [#165](https://github.com/AgentWorkforce/relayhistory/issues/165)                                                                                                                                                                                                             |
| Control / lifecycle rows                                           | ✗      | ✗     | ✗      | ✗    | ✗        | ✗     | —          | [#165](https://github.com/AgentWorkforce/relayhistory/issues/165), [#180](https://github.com/AgentWorkforce/relayhistory/issues/180)                                                                                                                                          |
| Relationship — delegated                                           | ◐      | ✓     | —      | —    | —        | —     | —          | [#170](https://github.com/AgentWorkforce/relayhistory/issues/170)                                                                                                                                                                                                             |
| Relationship — fork / resume / continuation                        | ✗      | ✗     | ✗      | ✗    | ✗        | ✗     | —          | [#170](https://github.com/AgentWorkforce/relayhistory/issues/170)                                                                                                                                                                                                             |
| Session metadata (cwd, branch, versions)                           | ◐      | ✓     | ◐      | ◐    | ◐        | ✗     | —          | [#164](https://github.com/AgentWorkforce/relayhistory/issues/164), [#177](https://github.com/AgentWorkforce/relayhistory/issues/177)                                                                                                                                          |
| Canonical `project_key`                                            | ✗      | ✗     | ✗      | ✗    | ✗        | ✗     | —          | [#175](https://github.com/AgentWorkforce/relayhistory/issues/175)                                                                                                                                                                                                             |
| Declared hydration `capability`                                    | ◐      | ◐     | ◐      | ◐    | ◐        | ✗     | —          | [#169](https://github.com/AgentWorkforce/relayhistory/issues/169)                                                                                                                                                                                                             |

### Why each non-`✓` cell reads the way it does

**Prompt / `history` row.** Every source populates `history`. Grok is `◐`
because `ingest_grok_session` writes `timestamp_ms: session.first_ts + idx` —
the ordinal index, not a provider clock, so prompt timestamps inside a grok
session are synthesized. Relay has no local transcript at all: its rows arrive
from a remote connector, and `RelayProvider` in `src/discover.rs` derives its
catalog row from `history` rows a previous sync already stored.

**Event rows, model, tool calls, file edits — grok, opencode, relay.**
`ingest_grok_session` inserts `history` rows and nothing else.
`sync_opencode_session_from_connection` selects only
`role = 'user' AND type = 'text'` parts. Relay never reaches an event-level
parser: `source_snapshot` in `src/ingest/hydrate.rs` returns
`HYDRATION_UNSUPPORTED` — "Relay catalog evidence has no configured
full-evidence connector". These three sources are prompts-only. Model is `◐` for
grok and opencode because the shallow catalog read records a `models` list
(from grok's `summary.json`, and from OpenCode's indexed part read when the
provider index exists) while no per-event model is ever stored.

**Cursor, since [#166](https://github.com/AgentWorkforce/relayhistory/issues/166).**
`ingest_cursor_transcript` replaced the prompt-only `ingest_cursor_line`, so
`text`, `tool_use`, `tool_calls` and `file_edits` are `✓`. The four cells that
read `—` are the provider's own silence, not a backlog item: no reported Cursor
build writes a `tool_result` block ("not one line of tool output is
persisted"), a `thinking` block, a `message.model` or a `message.usage`. The
parser reads all four when a record carries them, and the fixture that proves
it is named `extended-unverified.jsonl` precisely because no source shows
Cursor writing them. `docs/session-catalog.md` cites the evidence per field and
carries the `jq` checklist a maintainer with Cursor installed should run; a cell
here moves if that checklist comes back different. Delegation stays `—` for a
different reason — a `Task` block is captured as an ordinary `tool_calls` row,
so the spawn is visible, but it names no child transcript, so there is nothing
to write a relationship to.

**Token usage.** Claude is `◐`: `ingest_claude_transcript_as` serializes
`message.usage` once and passes the same `token_json` to every block of the
message, so one model request is written N times, once per content block.
Nothing in the crate corrects it — de-duplication lives in plugin-private code
(`plugins/relayhistory/rust/src/outbox.rs`). Codex is `◐` for a different
reason: `token_count` events carry _cumulative_ totals, and the parser derives a
delta against the previous snapshot and attaches it to one assistant event,
holding a `pending_delta` when no event is available yet. Both are usable, both
are source-specific, and neither is a normalized per-request usage record.

**`request_id`, `stop_reason`, `turn_id`, sidechain and meta flags.** Grep the
crate: `requestId`, `stop_reason`, `stopReason` and `turn_id` appear only in
test fixtures. Claude transcripts carry `requestId` on assistant rows and
`stop_reason` on the message; neither is read. OpenCode records a step-finish
reason on its parts, which is also not read. Codex reports no stop reason at
all — burn's own Codex reader hard-codes `stop_reason: None` — so that cell is
`—` rather than a gap. Codex payloads do carry `turn_id`, and it is not read. `isSidechain` and `isMeta` _are_ read, but only as filters — a
sidechain row decides attribution and a meta row is excluded from `history`;
neither flag is stored, so a consumer cannot tell a meta turn from a human one
after the fact. There is no column for any of them in `session_events`.

**Tool-result fidelity.** `session_events` stores the materialized result text
and nothing else about the result. There is no `payload_bytes`, no truncation
flag, no content hash, and no call/event index — and, less obviously, **no
`tool_use_id` either**: `session_events` has no such column, so the identity is
parsed and then dropped once it has been used to update the separate
`tool_calls` and `file_edits` rows. `tool_calls.is_error` is a per-_call_
boolean, not a per-result one. With more than one call or result in a turn there
is no key that joins a stored result event to its call, so even the fields that
look present are not recoverable on the record burn asks for.
`ToolResultEventRecord` needs all of it.

**Compaction, summary and control rows.** The Claude block loop ends in
`_ => {}`, so any block type that is not `text`, `thinking`, `tool_use` or
`tool_result` is dropped. Claude `type: "summary"` records carry no `sessionId`
and are skipped before the block loop is reached. The Codex arms end in
`_ => {}` twice, so `task_started`, `turn_context` (beyond the model) and the
rest of the lifecycle vanish. `is_claude_control_prompt` removes slash-command
wrappers from `history` and leaves no record that the command was issued. There
is no `session_markers` table.

**Relationships.** Only two relationship values are ever written:
`delegated` and `materialized_local` (the latter is the remote↔local identity
correlation, claude only). Claude's `delegated` is `◐` because a subagent
sidecar carries the _parent's_ `sessionId` on every record — the child is only
independently addressable when the provider emits a per-child `agentId`;
otherwise `relationship_capture` records it as `identity_status = 'unlinked'`
with a null child id. `relationship_capabilities()` declares codex `always`,
claude `sometimes`, and cursor/grok/opencode/relay `never`. Fork, resume and
continuation are not written by any path.

**Session metadata.** The `sessions` table has `originator`, `agent_version`,
`repo_url`, `initial_commit`, `workspace_roots_json` and `models_json`.
`upsert_session` — the parser-side write — sets none of them; they are written
by the shallow discovery upsert. **That is not a second call the consumer has
to make**: `sync_basic` ends by running
`discover::discover_sessions_with_providers` over every shallow provider
(`crates/ai-hist/src/ingest.rs`), so one `sync` populates them, which is what
`docs/architecture.md` means by "sync ends by running shallow discovery". A
single `SessionStore::sync` against a fresh database leaves a Codex row with
`cwd`, `git_branch`, `agent_version`, `repo_url`, `initial_commit` and
`workspace_roots_json` all set and `discovery_state = 'full'` — verified
empirically, not inferred.

The cells therefore track what the _provider_ exposes, not which internal pass
writes it: codex is the one complete row (`session_meta` gives originator,
`cli_version`, git remote, initial commit and workspace roots); claude gets the
record `version` and cwd/branch; cursor gets a cwd decoded from the directory
name; grok and opencode get cwd and (for grok) branch; relay has none. See the
per-provider matrix in
[`docs/session-catalog.md`](../session-catalog.md#per-provider-capability-matrix),
which this table must stay consistent with.

**Canonical `project_key`.** Nothing computes one. `project` is a cwd string —
and for cursor it is a string _decoded from a directory name_
(`decode_cursor_project` turns `-` back into `/`), which is not the same value
another source would report for the same repository.

**Declared hydration `capability`.** `hydrate.rs` returns
`capability: "full".to_string()` unconditionally on the success path, for every
source. A prompts-only source therefore reports full evidence coverage. That is
a bug, not a policy; it is `◐` for every file-backed source and `✗` for relay,
which cannot hydrate at all.

## Store shape: one writer implementation, not one writer process

burn reads `ai-history.db` through `SessionStore` and keeps its own durable
state — the ledger, its fingerprints and its enrichment stamps — in its own
files. **burn never writes a row of `ai-history.db` itself, and never issues
SQL against it.**

That is a statement about the _implementation_, not about the process table.
`ai-hist` is an in-process Rust crate: when burn calls `SessionStore::sync`,
`hydrate` or `watch`, the write happens in burn's own OS process, against a
read-write connection burn's handle owns. The facade makes this explicit rather
than hiding it — `SessionStore::open` calls `open_db` (read-write, creating the
schema) only when `StoreOptions.read_only` is false, and `sync` on a
`read_only` handle returns an error instead of silently upgrading
(`crates/ai-hist/src/session_store.rs`). A consumer that wants fresh evidence
in-process therefore _is_ a second SQLite-writing process alongside the
`ai-hist` CLI and the napi addon. A consumer that only reads can and should open
with `read_only: true`; burn's steady state is reads, and it should sync only
when it is the component responsible for freshness.

What the design does guarantee is a single writer **implementation**. Every
mutation — whoever's process it runs in — goes through the crate's own code
paths and therefore through:

- the `SyncRunLock` advisory file lock, taken for the whole run
  (`try_acquire_sync_lock` / `sync_exclusive` in `crates/ai-hist/src/ingest.rs`,
  over `crates/ai-hist/src/file_lock.rs`);
- the per-session hydration locks
  (`acquire_remote_hydration_lock` in `crates/ai-hist/src/ingest/hydrate.rs`);
- the WAL busy handler installed on every connection the crate opens
  (`configure_busy_retry` in `crates/ai-hist/src/store.rs`);
- one schema, one set of migrations, and the `.sync-state.json` cursor file with
  its merge and crash-recovery rules.

No consumer defines its own schema, its own cursor, or its own lock discipline.
That is the property the ADR is buying, and it is worth stating plainly because
it is weaker than "one writer process".

### Relationship to #47

[#47](https://github.com/AgentWorkforce/relayhistory/issues/47) proposes routing
every SQLite writer through append-only spools, because SQLite's WAL permits one
writer and the write lock belongs to a _process_: a producer that is suspended
or wedged mid-transaction blocks every other writer indefinitely, and no timeout
or backoff survives that.

Adopting burn as a consumer that can call `sync` **adds a process to that set**.
It does not make #47 unnecessary. The honest position is:

- The risk is **mitigated**, not eliminated, by the crate-owned machinery above:
  one lock discipline, one busy handler with bounded jittered retry, and a sync
  lock that is released on `Drop`.
- The mitigation is weakest exactly where #47 says it is — against a _stopped_
  process, which never runs its `Drop` and never answers a busy handler.
- **#47's spool architecture remains the escalation path** if contention is
  observed once burn is a caller. The trigger to escalate is operational, not
  theoretical: sync runs failing on `SQLITE_BUSY` after the retry budget, or a
  `SyncRunLock` held by a process in state `T`.
- A consumer that never calls `sync` (`read_only: true`) genuinely adds no
  writer, and is the preferred integration where freshness is somebody else's
  job.

This ADR neither depends on #47 landing first nor resolves it; the MCP server
and `agent-relay` broker writers it names are out of scope here.

## Consequences

- Any new harness is added in this repository and nowhere else. See
  [`docs/session-catalog.md` → Adding a provider](../session-catalog.md#adding-a-provider).
- The seventeen group-1 issues have a single acceptance artefact: a `✗` or `◐`
  cell above must become `✓` and the matrix must be edited in the same PR that
  closes the issue. A parity PR that does not touch this table is incomplete.
  A `—` cell is **not** a backlog item — it means the provider emits nothing to
  capture, or the row does not apply to that record type at all (the whole
  `trajectory` column). Turning a `—` into a `✓` is out of scope by
  construction; if a provider starts emitting the field, change the cell to `✗`
  first and open an issue for it.
- burn's cutover
  ([#183](https://github.com/AgentWorkforce/relayhistory/issues/183)) is gated on
  the matrix having no `✗` in the record types burn's `DerivedRecords` trait
  consumes — see [`docs/sourcing-contract.md`](../sourcing-contract.md).
- relayhistory will never own pricing, cost, token estimation, activity
  classification, or similarity-based session linking. Those stay in burn.

## References

- [`docs/sourcing-contract.md`](../sourcing-contract.md) — the record types the
  SDK must expose, mapped onto burn's reader types.
- [`docs/session-catalog.md`](../session-catalog.md) — shallow discovery and the
  per-provider capability matrix this table must agree with.
- [`docs/architecture.md`](../architecture.md) — the production call graph.
- burn's consumer contract: `DerivedRecords` in
  `crates/relayburn-sdk/src/ingest/ingest.rs`; record types in
  `crates/relayburn-sdk/src/reader/types.rs`.
