# Changelog

User-facing release notes for RelayHistory. Every public package — the `ai-hist` npm package and CLI, `ai-hist-native` and its platform packages, `ai-hist-mcp`, the optional history plugins, and the `ai-hist` crate on crates.io — is released in lockstep at one version, tagged `sdk-ts-v<version>`.

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0, a breaking change is a minor release.

## [Unreleased - Minor]

### Added

- Text-bearing OpenCode `reasoning` parts are thinking blocks and an encrypted-only one is an `encrypted_reasoning` marker; synthetic user text (harness-written context such as an `@file` read) is captured as a user text block with `control_kind = "synthetic"`, never as a prompt.

### Changed

- `Message::raw_usage` returns the usage blob of the message's own record, so each streamed copy of one Claude response keeps its own snapshot (and a copy that carried none has none); `Message::usage` and `SessionEvidence::requests` still read the request's settled usage. The first `sync` after upgrading fills it through a one-time raw-facts re-read of Claude and Codex transcripts.
- Session events carry `recordTokenJson` natively and `recordTokenUsage` in the TypeScript SDK; the session evidence contract is version 4.

### Fixed

- A Claude transcript made only of `isSidechain` rows (inline Task traffic from older Claude Code) is now catalogued as its own session, with its sidechain turns' usage in `SessionEvidence::requests`; only an `agent-*.jsonl` file is read as a subagent sidecar, and the first `sync` after upgrading catalogs transcripts an earlier build dropped.
- Every OpenCode assistant message is a `Message` and a request in `SessionStore::session`, with its `tokens` verbatim as `raw_usage`, model, provider and stop reason, including messages made only of `step-start` / `step-finish` / `reasoning` parts; their usage was previously missing from the store in both the `opencode.db` and legacy `storage/` layouts.
- An OpenCode message's stop reason is its final `step-finish` reason, else the message's own `finish`; a reasonless final step no longer reports an earlier step's reason.
- `sync` no longer rewrites the full-text index entry of a row it re-reads unchanged: a sweep that re-reads the OpenCode store, or re-parses a transcript, re-indexes only rows whose text, role or project changed, so walking syncs write less and stop fragmenting the full-text index searches read. Fragmentation an existing store already has stays until FTS5's own merges or `ai-hist compact` (which runs `optimize`) consolidate it.
- Writing a marker reuses its prepared statement instead of compiling it, and the change-feed triggers it fires, once per marker: cold sync and hydration of marker-heavy transcripts are ~10% faster.
- Claude: the `thinking_signature` marker of a signed `thinking` block with empty text carries the record's `request_id` and `provider_message_id` in its payload, so the record that opens a streamed response — which stores no event — can be placed in its request, and the marker's `ts_ms` is when the request started.
- Claude: `continuation` and `fork` edges from `continuedFromSessionId` / `forkSessionId` carry the timestamp of the record that named them in `spawned_at_ms`, not the transcript's first record.
- Existing stores gain both on the next `sync`, which re-reads only the Claude transcripts whose signature markers do not yet name their request and re-captures continuity only for transcripts naming an explicit target.
- Re-reading a Claude subagent transcript is linear in its own records again: retiring a sidechain record's rows under the parent session is an indexed lookup instead of a scan of the parent's events, tool calls, edits and markers. On a store with a 50 MB session and ~1,100 sidecar subagents the one-time re-read above takes about as long as a cold sync instead of ~19x it.

### Rust API

- Added `ControlKind::Synthetic` (`"synthetic"`); `SESSION_EVIDENCE_CONTRACT_VERSION` is 4 in Rust and TypeScript, so an SDK without the value rejects an addon that can return it.
- Added `SessionEvent::record_token_json`; code constructing `SessionEvent` with a struct literal must set it. `SESSION_EVIDENCE_CONTRACT_VERSION` is 4.

## [0.36.0] - 2026-10-07

### Breaking Changes

- A serialized `CatalogQuery` now carries `exclude_delegated`; a value saved without it no longer deserializes.

### Added

- `SessionStore::delegated_descendants` lists the subagents and child threads a set of sessions delegated work to, through every generation and whether or not the catalog holds them, so an embedder sharing a session can share its delegated work with it.
- `CatalogQuery::exclude_delegated` and `IdentityQuery::exclude_delegated()` list only sessions of their own, leaving out subagents and child threads that are part of another session's work; `SessionStore::delegated_by` names a delegated session's parents.

### Rust API

- Added `SessionStore::delegated_descendants(&[SessionIdentity]) -> Result<Vec<SessionIdentity>, Error>`.
- Added `SessionStore::delegated_by(&SessionIdentity) -> Result<Vec<SessionIdentity>, Error>`, `CatalogQuery::exclude_delegated` and `IdentityQuery::exclude_delegated` (field and builder).

## [0.35.0] - 2026-10-07

### Breaking Changes

- A serialized `WatchedPath` now carries its entry filter as `entries`, and a value saved without it no longer deserializes; regenerate saved watch roots from `SourceCapabilities::watch_roots`.

### Added

- `WatchedPath::admits(name)` publishes the entry filter the store's own watcher applies to each advertised watch path (OpenCode's database directory admits only its SQLite stores and sidecars), so an embedder's watcher cannot drift from it; `WatchedPath` serializes the filter as `entries`.
- An embedder can drop the evidence of sessions it no longer keeps while they stay in the catalog and re-hydratable, and compact the database afterwards; a 4.1 GB store keeping 10 of 46,502 sessions shrinks to about 0.7 GB. Sessions whose evidence hydration could not read back (deleted or forked transcripts, sessions gone from their store, remote or plugin evidence) are kept unless the caller opts in.

### Changed

- `discover` and sync sweeps re-run project-key inheritance only for sessions written since the previous refresh and the delegation trees below the ones that can lend a key, instead of walking every delegated session (unresolved keys are still re-resolved in full): a scoped discover on a 48k-session store takes ~30 ms instead of ~2.9 s after the first pass in a process.

### Fixed

- Hydrating an archived Codex session also reads the child threads it delegated to that are still under their dated `sessions/` directory.
- Hydrating a Claude transcript from its start keeps the sessionless records ahead of its first session-naming line (`file-history-snapshot`, summaries) as markers, as a sweep does.

### Rust API

- Added `WatchedPath::admits(&OsStr) -> bool`.
- `SessionStore::forget_evidence(ForgetScope, ForgetOptions) -> ForgetReport` removes the named sessions' evidence (`ForgetScope::Sessions`) or every other session's (`ForgetScope::AllExcept`), together with their catalog-less delegated children, leaving them catalogued as `Shallow`; `ForgetOptions::include_unrecoverable` and `ForgetReport::skipped_unrecoverable` govern sessions a local hydration cannot restore; the change feed reports catalog rows, not tombstones.
- `SessionStore::compact(CompactOptions) -> CompactReport` merges the full-text indexes and rewrites the database with `VACUUM` under the `SyncRunLock`; `Error::InsufficientSpace` (`INSUFFICIENT_SPACE`) is its refusal when the volume cannot hold the rewrite.

## [0.34.3] - 2026-10-06

### Changed

- GetDiskFreeSpaceExW for 64-bit, quota-aware Windows free space
- Read Windows free space with GetDiskFreeSpaceW instead of spawning df

### Fixed

- Follow a database symlink whose target does not exist yet
- Measure the sync-state directory that receives checkpoint writes
- Check free space on both the database and sync-state volumes
- Measure a symlinked database on its target's filesystem
- Measure free space for a bare database filename; lock windows-sys in provider-sources

## [0.34.2] - 2026-10-05

### Fixed

- The first open after upgrading a large store no longer hangs on "Upgrading the ai-hist database": the export-schema restamp is linear in table size (770k `session_events` rows in ~14 s instead of ~27 h).

## [0.34.1] - 2026-10-03

### Fixed

- Codex Desktop assistant replies stored only as response items now appear in session history; mirrored CLI/desktop encodings are stored once, and existing captures are re-read on upgrade to recover missing replies.

## [0.34.0] - 2026-10-03

### Added

- Devin CLI is a first-class source, `devin`, read read-only from `$XDG_DATA_HOME/devin/cli` (default `~/.local/share/devin/cli`) by discovery, `sync`, hydration and live capture, with prompts, replies, thinking, tool calls, file edits, models and markers; hidden sessions are skipped.
- `ai-hist watch --no-leading-edge` restores the old trailing-only debounce; by default a quiet watch loop now sweeps a filesystem event after a 10 ms settle, cutting write-to-report latency (p50 259 -> 65 ms on a 300-session store).
- The SDK adds the typed `DeliveryConflictResponse` contract with canonical digest, strict HTTP 409 parsing and deterministic recovery-plan helpers.

### Changed

- `search` (CLI, SDK, MCP) returns a common term's newest matches without reading every match (~240 ms -> ~3 ms on a 500k-event store); results are unchanged.
- `resume` and `pack` use the same prompt search, and the SDK's `search`, `searchPage`, `recent`, `recentPage` and `getSession` read each session's locations once per result set.
- Sync reads only the appended turns of a growing Codex rollout instead of re-parsing the whole file.
- Forced sweep ticks are substantially cheaper on large stores: incremental `.sync-state.json` checkpoints (Unix), change-feed-scoped destination checks and project-identity refresh, a single Grok session-tree walk, cached discovery and probe queries, and `statfs`/`statvfs` instead of spawning `df`.
- The Node CLI no longer recognises the leftover `--once` and `--interval` flags; they fail as unknown options.

### Fixed

- `ai-hist export --out` refuses the history database's `-wal`, `-shm` and `-journal` sidecars as well as the database itself (also for `file:` URI `--db`), which previously could corrupt a live store.
- `ai-hist resume <query>` resumes the newest match that names a session instead of reporting "No session found" when the newest match has no session id.
- Live capture sweeps writes made while another process holds the sync lock within about a second of its release, instead of up to 30-60 s later.
- Live capture runs one forced sweep per write again instead of a redundant second sweep for writes reported in several callbacks.
- Live capture no longer forces sweeps for unrelated files beside the OpenCode database; only the configured and channel databases and their SQLite sidecars count.
- Change-feed schema reconciliation fingerprints each evidence kind's columns and restamps only affected kinds, fixing durable receivers stuck on `409 delivery_conflict` after the `location` migration; stale explicit watermarks are refused.
- An abandoned export snapshot is released when its TTL elapses, so long-lived SDK hosts no longer hold WAL checkpoints back; the timer thread never keeps Node alive.
- Export pages stream rows and stop when full instead of decoding and discarding rows past the page boundary (3,000 one-record pages: 10.8 s -> 0.08 s).
- `SessionStore::session` reads a session's requests and usage summary in one pass (50k-event session ~1.9 s -> ~0.23 s), and `session_requests_page` is faster to walk.
- A `SessionStore` handle reuses up to four idle read connections while the path names the same file, making small reads such as `head_revision` and `changes_since` microseconds instead of milliseconds.

### Rust API

- `WatchOptions::leading_edge` (default `true`), `WatchLoop::leading_edge`/`with_leading_edge` and `watch::LEADING_EDGE_SETTLE_MS`; older serialized `WatchOptions` load with it on.
- `WatchOptions::stop: Option<StopToken>` lets `SessionStore::watch` cancel the sweep in flight; `WatchStop::stop` or dropping the handle cancels it, and the tick reports the new `TickReport::cancelled`.
- `TickReport::elapsed_ms` and `TickReport::first_event_age_ms` report sweep wall time and event-to-report latency.
- `WatchHandle::next` / `next_timeout` block on the report channel instead of polling every 50 ms, so idle watch consumers barely wake.
- `Source::Devin` and `ProviderRoots::devin` (`from_env` honours `XDG_DATA_HOME`).
- The `unstable-internal` `storage` module and its raw-SQL readers are removed.

## [0.33.0] - 2026-10-02

### Changed

- `sync` and `watch` write each Claude transcript and Codex rollout in a transaction, so a failed transcript leaves no partial rows and a cold sync is much faster (37.3 s -> 13.4 s on a 100 MB store).
- The change feed stamps a new `revision` only when an update actually changes a row, so re-reads, hydration and repeated observations no longer re-report identical rows to `changes_since`; `session_relationships.updated_ms` and `session_observations.updated_ms` now mean the time the row last changed.

### Fixed

- A sweep no longer stalls other writers for up to ~30 s while a read is active: it checkpoints the WAL `PASSIVE` and escalates to `TRUNCATE` only past 4 MiB with a short busy budget (`compact` still truncates).
- Reading a session's user turns no longer scans the session once per turn (`session_user_turns_page` and `SessionStore::session` on a 50,000-event session: ~100 s -> ~35 ms).
- Continuity reconciliation no longer scans every event of a source per pending transcript on each sync; the next writable open builds the partial index `idx_session_events_claude_uid_unmatched`.
- `session_events` and `SessionStore::session` read events in index order instead of sorting them in a temporary b-tree.
- A `watch` tick or `SessionStore::sync` that wrote nothing skips digesting every catalog row to compute `changed`.

## [0.32.3] - 2026-09-30

### Breaking Changes

- SDK `search()` and MCP `search_history` now run the same search as `ai-hist search`: they match session events (assistant text, tool calls and results) as well as prompts and return `SearchMatch` rows, a `HistoryEntry` plus `matchSource` (`history` | `session_event`), `role` and `kind`.
- `search` takes `role` (`all` by default, `user`, `assistant`, or `prompt` for the old prompts-only result) on the SDK, MCP and both CLIs' `--role`; `resume` and `pack` search with `prompt`.
- A prompt that a hydrated session also recorded as the same user turn matches once, as its `history` row, and a non-raw query no longer matches an event's indexed `role`, so searching for `assistant` or `user` does not return every event of that role.
- `search` and `recent` take inclusive `sinceMs`/`untilMs` and an `after` cursor `{ timestampMs, id, matchSource? }` on every surface (SDK, MCP `since_ms`/`until_ms`/`after`, TS CLI `--since-ms`/`--until-ms`/`--after`, native CLI `--after-ms`/`--after-id`/`--after-match-source`).
- Newest-first reads order by `(timestamp, id)`, so rows sharing a timestamp page without skips or repeats; `sinceMs > untilMs` or an unknown cursor `matchSource` is `INVALID_ARGUMENT`.
- Native contract 22 -> 23 -> 24 -> 25 (search matches and `role`, keyset pagination, change-feed ops `changes`/`commit_changes`); an SDK paired with an addon of another contract fails at load with `NATIVE_CONTRACT_MISMATCH`.
- `trajectory_fts` and the legacy `trajectories_ai/au/ad` triggers are dropped on writable open, fixing a change-feed migration that failed with `SQLITE_CORRUPT_VTAB` on older databases whose index had drifted.

### Added

- SDK `searchPage()` and `recentPage()` return one page plus a `nextCursor` (`null` when nothing further exists).
- Muse Code (`muse`) is a first-class source: sessions under `$XDG_DATA_HOME/muse/sessions` are discovered, synced, hydrated and live-captured with prompts, prose, thinking, tool calls, file edits, token usage, models and lifecycle markers, subagents link to their parent as `delegated` children, and `ai-hist resume` prints `muse resume <id>`.
- Grok per-inference usage is read from `<GROK_HOME>/logs/unified.jsonl`, falling back to a turn's own usage where the log does not cover it, with `GROK_UNIFIED_LOG_UNREADABLE`, `GROK_USAGE_MIXED_SOURCES` and `GROK_USAGE_PARTIAL` caveats.
- Grok sessions recover their model from `_meta.modelId`, `modelUsage`, `summary.json` `current_model_id`/`model_id`, or the head of `events.jsonl` when `summary.json` is missing.
- The SDK reads the revision-stamped change feed with `getChangesPage()`, `changesSince()` and `commitChanges(consumer, position)`, filtered by `kinds` and session, and exports `CHANGE_KINDS` and the `ChangeKind`, `Watermark`, `FeedChange`, `ChangesPage` and `CommittedCursor` types.
- Evidence rows (`session_events`, `tool_calls`, `file_edits`, `session_markers`) record `location` (`local` / `remote` / `both`), and a local re-read no longer deletes remote-supplied evidence for the same session.
- A `HistorySource` plugin can declare `location: 'local'` with absolute `roots` to run beside the built-in parsers for `local` and `all` scope; `docs/remote-connectors.md` is now `docs/source-plugins.md`.
- MCP `list_relay_agents`, `relay_status`, `join_relay` and `leave_relay` list live Agent Relay participants and let the calling session join or leave over the local desktop socket, without any Relaycast token in ai-hist.
- The CLI reports a schema migration on stderr while it runs, `onStoreMigration()` delivers the same events to SDK callers, and `migrateStore()` runs the migration explicitly; requires native contract 26.

### Deprecated

- `beforeMs` / `before_ms` / `--before-ms` keep their exclusive semantics but are deprecated in favor of `untilMs` and `after`, since they skip rows tied on the boundary timestamp.

### Fixed

- Forked Codex rollouts no longer re-index the parent's replayed prompts, events and token totals under the child; the first `sync` repairs existing forks once.
- Grok messages that reuse an ACP `eventId` no longer overwrite each other; every Grok session is re-read once by `sync` and hydration.
- Syncs over unchanged files skip re-reading and re-querying them, cutting a post-append sync on a 100 MB store from 1.77 s to 0.39 s; the first writable open after upgrading builds the new `idx_session_events_project` index.

### Rust API

- Added `Source::Muse` and `ProviderRoots::muse` (`from_env` honours `XDG_DATA_HOME`).

## [0.31.0] - 2026-09-29

### Added

- OpenCode discovery, `sync`, `sync-opencode`, hydration and live capture read every release-channel database (`opencode.db`, `opencode-stable.db`, `opencode-nightly.db`, ...), not only `opencode.db`; `OPENCODE_DB` and `sync-opencode --opencode-db` still name exactly one store.
- Grok per-turn usage (`turn_completed.usage`) is stored and normalized as `per-request` usage, `source_accounting("grok")` is `per-request`, and `session_requests` reports one request per Grok turn; indexed Grok sessions re-read once.
- Cursor, Grok and OpenCode `tool_result` events carry the same fidelity fields as Claude and Codex (`payload_bytes`, `payload_hash`, `result_status`, `error_signal`, ...), and `error_signal` gains `tool_status`; indexed Cursor and Grok sessions re-parse once.
- New `ai-hist compact [--json]` reclaims unused space in the history database without deleting rows, refusing while a sync runs or when the volume lacks room for the rewrite.
- `ai-hist doctor` reports `reclaimable` bytes (`reclaimable_bytes` under `--json`) and suggests `compact` when enough of the file is free pages.

### Fixed

- Codex forks (`forked_from_id`) and spawned subagents (`parent_thread_id`) are recorded as `fork` edges, and `guardian_review` subagent rollouts are hidden from the root catalog; indexed rollouts re-read once on the next `sync`.
- `ai-hist export` refuses to write over the database it is reading (including via `--db`, relative paths, symlinks, hard links and `-wal`/`-shm`/`-journal` sidecars) and exits non-zero before touching anything.
- `ai-hist export` writes to a temporary file and renames it into place, so a failed export leaves an existing destination untouched.
- Claude discovery and `sync` no longer index the subagent workflow `journal.jsonl` as a transcript, and evidence earlier builds derived from it is retracted on the next sync.
- Claude requests written as streamed snapshots report their final usage instead of being refused as `ambiguous-usage-copies`; existing databases are settled on the next writable open.
- Claude `<synthetic>` assistant records (local API-error and login notices) are stored as `local_notice` markers and no longer count as requests, models or `last_assistant_text`; existing rows are repaired on the next writable open.
- A sweep no longer re-runs the Codex project/branch backfill over every indexed Codex session, so unrelated source changes no longer re-stamp every Codex row in the change feed.
- Claude `sync` no longer scans every Claude event per transcript without its own session row, making incremental and cold syncs substantially faster.

### Rust API

- `ProviderRoots` gains `opencode_db_pinned`, set by `from_env` when `OPENCODE_DB` is set, to read `opencode_db` alone instead of every channel database beside it.

## [0.30.0] - 2026-09-28

### Breaking Changes

- Native contract 21 -> 22: an SDK/addon mismatch fails at load with `NATIVE_CONTRACT_MISMATCH`, and `historyExport` accepts only `create_export`, `export_page`, `close_export` and `expire_exports` (the `EXPORT_RETENTION_LIMIT` and `DELIVERY_RETENTION_LIMIT` error codes are gone).
- Uploads are no longer part of `ai-hist` (team uploads come from the Agent Relay desktop app); `@relayhistory/capture`, its platform helper packages, the `relayhistory-plugin` crate and the `agent-relay-probe` assets are no longer published.
- The SDK drops `deliveryRequest`, `createHistoryDelivery`, `runHistoryDelivery` and the other `*HistoryDelivery*` functions and types, along with the `HISTORY_DELIVERY_MOVED` error; the native addon drops `historyDelivery` and `historyDeliveryDrain`.
- The CLI drops `ai-hist delivery ...`, `ai-hist plugin COMMAND` and the `--job`, `--poll-ms`, `--timeout-ms`, `--base-url`, `--label`, `--max-content` and `--token` flags.
- The MCP server drops `delivery_status`, `delivery_pause`, `delivery_resume` and `delivery_retry`, and `AI_HIST_PLUGIN_CONFIG` loads source connectors only.
- `HistoryPlugin` loses `commands` and `tools` and `HistoryPluginRegistry` loses `command()` and `registeredTools()`, so plugins contribute only `sources` and `destinations`; the history config file loses `job`.
- The store keeps no upload capture journal or retention budget, so evidence writes are never refused for retention; the first writable open drops the old capture triggers and indexes, and uploaders read the change feed instead.
- An export snapshot is one read transaction that sees the store as it stood when opened, `beginHistoryExport` cursors resume only within the process that opened them, and records are `schema_version` 2 with `record_id` and `revision` matching the change feed.

### Added

- `createHandoff()` / `resumeHandoff()` and MCP `create_handoff` / `resume_handoff` hand a session to another agent in the same workspace via a pointer plus a self-describing intent (at most 4,000 characters), with no receiver skill required.

### Rust API

- The `export` feature is local export only: `ExportSnapshot` replaces `create_export`, `export_page`, `close_export` and `expire_exports`, `EXPORT_SCHEMA_VERSION` is 2, the `export::capture` module, retention and compaction APIs and the `delivery` feature alias are removed, and the default-feature surface is unchanged.

## [0.29.0] - 2026-09-25

### Rust API

- `SessionStore::has_session(&SessionIdentity)` reports whether the store holds anything under one identity, by the same tables and rules as `session_identities`.
- `session_identities` skips an empty source as well as an empty session id, so every listed session can be selected and drained.

## [0.28.0] - 2026-09-25

### Rust API

- `ChangeQuery::session(source, session_id)` restricts a change-feed drain to one session, tombstones included, through per-table indexes; combining it with a named consumer is `Error::InvalidArgument`.
- `SessionStore::session_identities(IdentityQuery { after, limit })` pages every `(source_name, session_id)` the store holds evidence under, catalogued or not, including sources this build does not know.
- `storage::session_identities_after` covers every evidence table rather than only the catalog, prompts and events, and reads each page on one snapshot.
- The first writable open adds `idx_sessions_identity`; until then a read-only store answers `session_identities` with `StaleSchema`.

## [0.27.1] - 2026-09-25

### Rust API

- The change feed covers every evidence table: `ChangeKind` gains `History`, `Presence`, `CommitLink`, `Trajectory`, `SourceObservation` and `ObservationEvidence` (`ChangeKind::ALL` lists twelve), and existing databases stamp the new rows once on open.
- A named cursor stored as `*` now spans all twelve kinds; a consumer that passed the original six kinds as an explicit list now fails with `ConsumerKindsMismatch` and must drain under a new consumer name or resync from `Watermark::START`.
- `Change` gains `columns: Option<StoredRow>` (every stored column but `revision`, as SQLite holds it) and `key`, the record's identity, on upserts and tombstones alike; `EvidenceRow::History(HistoryEntry)` types prompt rows and `EvidenceRow::Untyped` marks kinds without a typed row.
- `Change::source` is now `Option<Source>` with `Change::source_name` holding the stored name, so a row from a source this build does not know no longer fails the drain.

## [0.27.0] - 2026-09-24

### Breaking Changes

- Native contract 19 -> 21: the `ai-hist-native` addon adds `historyExport(requestJson, dbPath)` and the `sessionStoreCall(op, argsJson)` dispatcher, and pairing the new SDK with an older platform package fails at load with `NATIVE_CONTRACT_MISMATCH`.

### Added

- `getSessionMarkersPage(source, sessionId, {limit, after})`, `sessionMarkers()`, `getSessionMarkers()`, MCP `get_session_markers` and `ai-hist sessions markers SOURCE SESSION_ID` on both CLIs read a session's markers (kind, subkind, text and parsed payload), undated markers last.
- `getSourceCapabilities(source)` and MCP `get_source_capabilities` report which evidence kinds a source's hydration covers, whether it can ever report `full`, and what it establishes about delegation, before any sync.
- `ai-hist sessions usage SOURCE SESSION_ID [--json]` on both CLIs prints the provider-reported usage rollup; usage is never estimated and cost appears only when the source data carried one.
- Session events carry `control_kind` (TypeScript `controlKind`), typing harness-written user-role rows that are not prompts: slash-command records, task notifications, hook output, bash passthrough, system reminders, Codex context wrappers, `meta` and resume markers.
- A Claude slash command's caveat, invocation and output records are grouped into one `session_markers` row of `kind = "slash_command"` carrying the command name, args, mode and output size.

### Changed

- SDK `getSessionRequestsPage`, `getSessionUsage` and `getSessionUserTurnsPage` read through `sessionStoreCall`: a missing database answers an empty page without being created, and failures carry the store's typed codes (`QUERY_FAILED` as `DATABASE_QUERY_FAILED`); the old typed native exports remain until a later major.
- `history`, `sessions.first_prompt`, user turns and prompt usage attribution exclude control rows: a task notification or bare `/resume <id>` is no longer a history row or session title, `<system-reminder>` blocks become their own rows, and a slash command's answer is charged to the human prompt before it.
- The relayhistory plugin no longer publishes control rows as conversation turns, and rewrites sessions an earlier release published with them in place.
- `SESSION_EVIDENCE_CONTRACT_VERSION` 2 -> 3 on the Rust and TypeScript sides; `HYDRATION_PARSER_VERSION` 10 -> 11 and `SHALLOW_SCANNER_VERSION` 5 -> 6 re-read existing sessions once on the next hydration or `sync`, retiring rows the previous parser stored differently.

### Fixed

- A `/resume` preceded by a system reminder is now detected for session continuity.

### Rust API

- `SessionStore` is now the whole default surface of the `ai-hist` crate, with no raw connection or contract constant, documented in `docs/sourcing-sdk.md` with a standalone `examples/rust-consumer` against the published crate.
- `StoreOptions` gains `roots: Option<ProviderRoots>`, resolved once at `open` and used by `sync`, `hydrate`, `watch` and `Source::capabilities().watch_roots(&roots)`.
- `SessionStore::discover(DiscoveryOptions)` sweeps every local provider's sessions from metadata as `Shallow` rows without hydrating or taking the `SyncRunLock`.
- `sync` takes `SyncOptions { force, lock_timeout_ms }` plus a `progress` `ProgressObserver`, reports `swept` and the changed `SessionRef`s, and returns `Error::SyncLocked` after the timeout instead of silently skipping a held lock.
- `discover`, `sync` and `hydrate` take an optional `StopToken`; a stopped call returns `Error::Cancelled` (`CANCELLED`) at the next provider, file or record boundary.
- New `hydrate(&SessionRef, HydrateOptions)` (by id or transcript path), `watch(WatchOptions)` returning a `WatchHandle` iterator of `TickReport`s, and `sessions(CatalogQuery)` walking the catalog.
- `session(&SessionRef, SessionQuery)` returns one session's `SessionEvidence` (prompts, messages, tool calls and results, file edits, markers, relationships, requests, usage, user turns, coverage) on one SQLite snapshot, with JSON columns parsed and `SessionQuery { include_text, kinds }` limiting what is read.
- `Block::control: Option<ControlKind>` and `SessionEvent::control_kind` classify user-role rows that are not human prompts.
- `Source::capabilities()` declares a source's evidence kinds, relationship capabilities, usage accounting mode, message-id provenance, path hydration and watch roots; `Source::ALL` lists every source.
- `Error` is an enum whose `code()` mirrors the TypeScript native error codes plus `SyncLocked`, `SourceMismatch`, `StaleSchema`, `WatermarkAheadOfStore` and `ConsumerKindsMismatch`; `Error::is_stale_schema()` marks the one failure fixed by reopening writable.
- A read-only `open` also checks the marker, relationship and per-request usage schema, refusing up front with `Error::StaleSchema` instead of failing inside a read.
- Facade value types are `#[non_exhaustive]`, `Clone`, `Serialize`, `Deserialize` and `PartialEq`; `CatalogIter` and `WatchHandle` are not value types.
- Removed `SessionStore::session_user_turns_page`, `session_markers_page`, `session_requests_page` and `session_usage`; use the matching `SessionEvidence` fields.
- `EvidenceRecord::{exists, matches_canonical, remove, write}` are crate-private, leaving no `rusqlite` types in the default public API.
- `SessionStore::changes_since(from, ChangeQuery)` is a revision-stamped change feed over sessions, events, tool calls, file edits, markers and relationships, yielding typed `Upsert` rows and `Delete` tombstones in revision order.
- Named consumers resume with `Watermark::CONSUMER` and advance only via `Changes::commit()`; cursors never move backward and are bound to the kind set they were committed for (`ConsumerKindsMismatch` otherwise).
- A `Watermark` carries its database's `epoch`, so one from a replaced database or beyond the head fails with `ErrorKind::WatermarkAheadOfStore` via `Error::kind()`; `SessionStore::head_revision()` and `SyncReport::head_revision` report the head.
- Existing databases are stamped once on first writable open so a replay from `Watermark::START` reports everything; a re-parse re-stamps rows, so consumers must treat a re-seen `record_key` as a replace, and incomplete messages are withheld until they finish.
- `ShallowSession` and `SessionRelationship` are re-exported on the default feature set.

## [0.24.0] - 2026-09-21

### Added

- `ai-hist watch` wakes on filesystem events across every provider session root, the flat `~/.claude/history.jsonl` / `~/.codex/history.jsonl` logs and `.trajectories` directories, with a 200 ms debounce and a 30 s backstop poll; new `--no-fsevents` and `--debounce-ms` flags sit beside `--interval`.
- The watcher backend is behind the optional `fs-events` crate feature, which the CLI enables; a `--no-default-features` build polls.
- `ai-hist ingest --hook claude [--quiet] [--json]` reads a Claude Code lifecycle-hook payload from stdin and hydrates only the transcript it names, reporting `mismatched` and ingesting nothing when the payload's session id and file disagree; it always exits 0 (see `docs/agent-integration.md` for wiring).

### Changed

- `ai-hist watch` reports at startup which roots are still pending, and picks up a provider root created after it started (or deleted and recreated) within seconds, without a restart.
- `sync` skips unchanged sources using a stat-only fingerprint stored in `.sync-state.json`, so a tick over unchanged sources opens no files; filesystem-event ticks always sweep.
- The sync fingerprint is invalidated by parser upgrades and by lost evidence, so a session that loses rows (a partial backup restore, a truncated write) is re-ingested instead of being skipped indefinitely.

### Fixed

- `ai-hist watch` no longer drops a change that arrives during a manual `tick()`, while another process holds the sync lock, or when a sweep fails; the sweep is retried promptly instead of waiting for the backstop.
- `ai-hist watch` matches events for roots given as relative paths or reached through symlinks, and clamps every interval to seven days so an absurd `--debounce-ms` cannot stall capture.
- `watch --remote` no longer watches local roots, so local writes cannot trigger remote connector traffic.
- A file `sync` could not read leaves the fingerprint stale so the next tick retries it instead of caching the failure.

## [0.23.0] - 2026-09-21

### Rust API

- New `session_markers` table records previously dropped transcript records (compaction and summary boundaries, provider `system` rows, image/document/redacted-thinking blocks, Codex lifecycle events), with unknown provider types stored as `kind = "unknown"` and payloads size-bounded; read with `session_markers_page` or `SessionStore::session_markers_page`.
- `SessionEvent` gains `raw_kind`, the provider-native record or block type each event came from.
- Grok markers move onto the shared marker model (`detail_json` becomes `payload_json`), migrated forward by `session_markers_v2`.
- `HYDRATION_PARSER_VERSION` 7 and new sync generations make an existing install re-read each Claude and Codex transcript once.

## [0.22.1] - 2026-09-21

### Breaking Changes

- Native contract 17 -> 19 for the usage surface and request-identity fields; an older addon is rejected at load.

### Added

- Per-request usage records and a session rollup: `getSessionRequestsPage` / `getSessionUsage` and MCP `get_session_requests` / `get_session_usage` return one row per model request (Claude's per-content-block copies collapsed), and a session with no usage evidence reports `null` rather than zeros.
- `session_events` capture the provider's request identity as `request_id` and `provider_message_id`, carried through the evidence contract; sessions backfill on re-parse, and until then report an `unresolved-request-identity` diagnostic and no usage totals.

### Rust API

- `ai_hist::normalize_usage` turns stored `token_json` into a `NormalizedUsage` (input excludes cache reads, Anthropic 5m/1h cache-creation split preserved, provider totals never recomputed), rejecting invalid counters with a coded `UsageError` and recording coverage so a reported zero is distinguishable from an absent counter.
- `ai_hist::attribute_usage_to_prompts` now hosts prompt usage attribution formerly in the commercial plugin, with unchanged semantics.
- `session_requests_page` and `session_usage_summary` expose per-request usage and the session rollup.

## [0.21.0] - 2026-09-21

### Added

- Session relationships record `continuation`, `fork` and `resume` edges with an `origin_session_id`, derived only from explicit provider evidence (never similarity); unresolved evidence reports `RELATIONSHIP_CONTINUITY_UNRESOLVED` until the missing transcript is hydrated.
- `getSessionTree` and `getSessionChildrenPage` accept `relationshipKinds` (default delegation only, so existing output is unchanged), and `getSessionRelationships` returns continuity on a separate `continuity` array.
- `SessionEvent` gains per-tool-result fidelity fields (`toolUseId`, `payloadBytes`, `payloadTruncated`, `payloadHash`, `callIndex`, `eventIndex`, `resultStatus`, `eventSource`, `errorSignal`, `subagentSessionId`, `agentId`), null when the provider did not record them.
- New `getSessionUserTurnsPage(source, sessionId, options?)`, plus `getSessionUserTurns()` and the `sessionUserTurns()` iterator in the SDK, return user turns with their ordered tool-result blocks and the neighbouring message ids.
- Native contract 16 -> 17; `SESSION_RELATIONSHIP_CONTRACT_VERSION` 1 -> 2. Plain `sync` re-reads existing transcripts once to backfill continuity and fidelity data.

### Rust API

- `session_events` store per-tool-result fidelity columns, with payload size and hash measured over the raw provider payload so they compare equal to relayburn's `content_hash`; Claude subagent notifications are indexed as tool results, and Codex results settle from turn signals at `task_complete`.
- Source adapters' submitted fidelity fields are validated against the documented vocabularies and non-negative ranges.
- New `session_user_turns_page(conn, source, session_id, limit, after)` returns a keyset page of user turns with their tool-result blocks, read from one snapshot.
- A read-only `SessionStore::open` on a database older than the current event shape fails with an error naming the remedy instead of failing later on `no such column`.

## [0.20.0] - 2026-09-20

### Breaking Changes

- Hydration contract 3: local hydration reports `capability` from the evidence kinds a provider's parser actually produces, adding `coverage` and a `HYDRATION_PARTIAL_COVERAGE` diagnostic; Cursor, Grok and OpenCode now return `partial` with `coverage: ["history"]`, and Claude/Codex return `partial` when related evidence is not requested or not fully acquired.
- Session evidence contract 1 -> 2 for the per-message raw provider facts below; existing databases are migrated on first open and re-parsed once by `sessions hydrate` and plain `sync`.

### Added

- `SessionEvent` carries per-message provider facts as `requestId`, `stopReason` (verbatim, null while a turn is in flight), `agentVersion`, `isSidechain`, `isMeta` and `turnId`, captured from Claude envelopes and Codex `turn_context`.

### Fixed

- Delivery capture triggers that predate a column on their table are rebuilt rather than kept by `CREATE TRIGGER IF NOT EXISTS` and silently emitting the old column list.
- `sync` no longer leaves backfilled columns permanently null when a transcript was unreadable or a mount was partially available; such files are re-read on a later sync.

## [0.19.0] - 2026-09-20

### Added

- Every session and event carries a canonical project identity, exposed as `projectKey` / `projectKeyMethod` on `CatalogSession` and `projectKey` on `SessionEvent`: the `origin` remote as `host/owner/repo` (resolved without running `git`, honouring global `insteadOf` rewrites and includes), else the working directory, with delegated children inheriting their parent's key.
- Checkouts, worktrees and subdirectories of one repository share one key, matching `burn --group-by project`.
- `ai-hist sessions list --project <key>` and `listSessionCatalogPage({ projectKey })` filter by project, and `ai-hist stats` groups `top_projects` by project key (reporting `grouped_by`), with `--by-cwd` restoring per-directory grouping.
- Native contract 15 -> 16; catalog contract 4. Existing databases gain the columns without a backfill, and keys fill in on the next sync or hydration.

### Fixed

- The delivery worker's lease keepalive no longer lets a live claim lapse under load (letting a second worker dispatch the same batch); renewals track elapsed time against the lease deadline and retry `SQLITE_BUSY`.

### Rust API

- New public `ai_hist::project_identity` module holds the project-key canonicalization rules.

## [0.18.8] - 2026-09-19

### Rust API

- The Rust engine publishes as one `ai-hist` crate (replacing `ai-hist-core` and `ai-hist-engine`), versioned with the npm release line; default features expose `SessionStore`, evidence structs, `Source`, and `Error`, with optional `delivery`, `opencode-backup`, and `git-hooks` features.

## [0.18.1] - 2026-09-18

### Added

- New `ai-hist/relay-cli` export: `createRelayCliSurface()` returns a `RelayCliSurface` (contract v1, id `relayhistory`) that hosts such as `agent-relay sessions` mount, built from the same command table as the `ai-hist` bin.
- `createRelayCliSurface({ cloud })` accepts a `@relayhistory/cloud-client` and adds `cloud list|events|search|thread|turns|digest|coverage`; without a client those commands are hidden and running one points to `agent-relay login`.

### Fixed

- The CLI accepts `--acquisition-timeout-ms` for `sessions discover`, `sessions hydrate`, and `sync` instead of failing with `unknown option`.
- The npm package no longer ships compiled test files.

## [0.16.0] - 2026-09-13

### Breaking Changes

- Native contract 10 -> 11. Cloud auth results include expiry, org, and workspace metadata, and discovery counters include `providerQueries` and `recordsInspected`.

### Changed

- Cloud credentials resolve only through the Rust stage store, so session thread reads use the same eligibility checks and locked, atomic token rotation as native cloud operations.
- Stage selection honours `RELAYHISTORY_BASE_URL` before `AI_HIST_BASE_URL` and rejects malformed values.

## [0.15.2] - 2026-09-09

### Added

- `getSessionThread()` and MCP `get_session_thread` return a session's cloud-linked lifecycle (shipped commits, pull requests, reviews, incidents, tickets, Slack threads, hotfixes, and follow-up sessions), filterable by `kinds`, `since`, `cursor`, and `limit`.

## [0.14.3] - 2026-09-07

### Added

- The npm CLI adds `ai-hist resume <query>`, which prints the native resume command for the best-matching session, and `ai-hist pack <query>`, which builds a token-budgeted context block for handing a session to another agent.

## [0.14.1] - 2026-09-04

### Changed

- Cloud sync envelopes always carry a `projectId` (repo slug from the history project or the session cwd's git remote, else `unknown`), and sessions publish `filesTouched` from their file edits, republishing it when edits grow.
- Cloud sync pushes `session_outcome` envelopes from commit links and re-pushes revised trajectories; upgraded clients re-upsert previously synced history once.

## [0.14.0] - 2026-09-02

### Added

- The SDK exports `SOURCES` and `CATALOG_SOURCES` registries with `isSource()` and `isCatalogSource()` guards for validating source input.

## [0.13.0] - 2026-09-02

### Breaking Changes

- Native contract 5 -> 7 and session-catalog contract 2 -> 3.
- Discovery summaries report OpenCode work as `provider_queries` and `records_inspected`, and `bytes_read` no longer substitutes the OpenCode database file size.
- Claude subagent transcripts whose records carry an `agentId` are indexed under the child session instead of the parent; the next `sessions hydrate` moves existing events, tool calls, and file edits to the child.

### Added

- Delegation topology: `getSessionRelationships()`, `getSessionTree()`, `getSessionChildrenPage()`, the `sessionDescendants()` and `sessionEventsIncludingDescendants()` iterators, `ai-hist sessions relationships` / `ai-hist sessions tree`, and MCP `get_session_relationships` / `get_session_tree`.
- Relationships record identity status (`observed` or `unlinked`), child agent type, name, model, spawn depth, provider evidence, and spawn time, and trees are deterministic, cycle-safe, bounded by `max_depth` / `max_nodes`, and always include the root.
- A full `sync` records Codex delegation (including previously ingested rollouts) and treats Claude subagent sidecars as delegated evidence rather than separate sessions, so topology is queryable without hydration.
- Targeted hydration works for Claude Code web sessions and, partially, for Codex cloud tasks via `codex cloud diff`; hydration contract v2 reports `full`, `partial`, or `shallow_only` capability, file-edit counts, and stable connector/auth/missing/partial error codes.
- A remote Claude session is related to its local continuation only when the local record contains the exact `remoteSessionId`.

### Changed

- OpenCode shallow discovery reads the live database in one read-only transaction instead of backing it up, fetches at most `limit` candidates, and omits fields whose optional schema or indexes are missing rather than scanning.

## [0.12.0] - 2026-09-01

### Added

- Paginated tool-call and file-edit access per session: `getSessionToolCallsPage()` / `getSessionFileEditsPage()`, the `sessionToolCalls` / `sessionFileEdits` iterators, `getSessionToolCalls` / `getSessionFileEdits`, `ai-hist sessions tools` / `ai-hist sessions edits`, and MCP `get_session_tool_calls` / `get_session_file_edits` (session evidence contract 1).
- These operations require both a source and a session ID so providers sharing a session ID never mix, and an unknown source raises `InvalidArgumentError` instead of returning an empty page.
- Pages sort undated records last with a `{ tsMs: number | null; id }` cursor, and parsed `args` / `structuredPatch` fall back to `null` while `argsJson` / `structuredPatchJson` keep the raw string; `parseStoredJson(raw)` is exported.
- File-edit records, including `ai-hist events --json`, carry `message_id`, `structured_patch_json`, `git_branch`, and `cwd`.

### Changed

- Native contract 4 -> 5.

## [0.11.0] - 2026-09-01

### Added

- Remote connectors run behind `--remote` / `--all` and `scope: 'remote'`: `claude-web` lists claude.ai/code sessions using the Claude Code CLI sign-in, and `codex-cloud` lists Codex cloud tasks via `codex cloud list --json`.
- Remote rows land in the shared catalog with a `remote` presence and dedupe against local presences of the same session.
- `RELAYHISTORY_CLAUDE_CREDENTIALS` overrides the Claude credentials path and `RELAYHISTORY_CLAUDE_API_BASE_URL` (https or loopback only) overrides the endpoint.
- Discovery results report `locations_run` / `locationsRun`, the connector locations that actually executed.

### Changed

- With no connector configured, remote requests still fail with `no remote provider connectors are configured` (`UNSUPPORTED_OPERATION` in the SDK), now naming each connector's reason.
- MCP `discover_sessions` and `sync` are declared open-world.

## [0.10.0] - 2026-09-01

### Added

- Targeted session hydration via `hydrateSession()`, `ai-hist sessions hydrate`, and MCP `hydrate_session` reports indexed-through state, evidence counts, related sessions, and diagnostics without returning a transcript (hydration contract 1).
- OpenCode hydration queries the live database by session ID instead of copying or scanning it.
- Existing databases gain hydration checkpoint and session relationship tables on their next writable open.

### Changed

- Native contract 3 -> 4.

## [0.9.2] - 2026-08-31

### Changed

- Shallow discovery writes the catalog faster through cached statements, single-step upserts, and relaxed durability for rebuildable catalog rows only.
- Opening a database with a current schema takes no write lock, and four unused `sessions` indexes are dropped to speed up catalog writes.

## [0.9.1] - 2026-08-31

### Breaking Changes

- Native contract 2 -> 3 and session-catalog contract 1 -> 2.
- Human-readable history and catalog rows show observed locations, statistics print the selected scope, and a remote-only `resume` match no longer prints a local command.

### Added

- `--local`, `--remote`, and `--all` (SDK `SessionScope`, MCP `scope`) select session location for discovery, listing, search, recent history, statistics, packs, resume, and sync, defaulting to local; `all` deduplicates sessions present in both.
- Catalog rows report their `local` and/or `remote` `locations`, and catalog pages, discovery, statistics, and sync results echo the applied scope.

### Fixed

- Current Codex Desktop `response_item/message` user turns are recognized in discovery and ingestion, and existing Codex indexes are repaired automatically.

### Rust API

- Catalog and discovery option, page, summary, and row structs carry scope and location data; `list_sessions_local*` and `discover_sessions_local*` reject non-local options, so use the `*_scoped*` variants for `remote` or `all`.

## [0.8.2] - 2026-08-30

### Breaking Changes

- npm now distributes the TypeScript SDK, Node CLI, MCP server, and a mandatory `ai-hist-native` Node-API engine (native contract 2); standalone Rust CLI release assets and the curl/source installer are retired.
- The synchronous `openAiHist()` / `AiHist` snapshot API is replaced by top-level async native-backed functions.
- `sql.js`, the JSONL/trajectory fallback scanners, CLI subprocess bridges, and `AI_HIST_RUST_BIN` are removed.
- Shallow discovery and full sync are explicit operations; cache-only reads never trigger them.

### Added

- Native-backed discovery, catalog pages, session history, paged session events, search, recent history, statistics, and sync, with event pages ordered by `(ts_ms, id)`.
- Stable errors for unsupported or missing platforms, native load or version mismatches, and database failures.
- The public `ai-hist` CLI ships from the npm package, with native packages for macOS, glibc and musl Linux, and Windows x64.
- `ai-hist --version` reports the package version and can notify interactive users of a newer release; disable with `--no-warning` or `RELAYHISTORY_NO_UPDATE_CHECK=1`.

## [0.6.0] - 2026-08-30

### Breaking Changes

- The legacy Python CLI and the `ai-hist-python` / `ai-hist-rust` launchers are removed, and `AI_HIST_CLI` is no longer supported.
- Installation is Rust-only; upgrades remove recognized legacy launchers and report any unrecognized files left in place.

### Added

- `ai-hist sessions discover` builds a shallow session catalog across providers, reading only bounded slices of the most recent sessions, and `ai-hist sessions list` serves it with no provider I/O (session-catalog contract 1).
- `listSessionCatalog()` and `listSessionCatalogPage()` return catalog rows (`CatalogSession`) newest first, with a `CatalogCursor` that walks the whole catalog without skipping or repeating tied timestamps.
- `discoverSessions()` runs discovery and streams rows through `onSession`, throwing `DiscoveryError` with diagnostics, summary, stderr, and exit code when the run fails or reports an unsupported contract version.
- MCP `list_sessions` returns catalog rows with `nextCursor`, and answers an empty catalog without scanning provider files when no database exists.
- The napi binding exposes `listSessions` and `discoverSessions` for in-process use.
- The `sessions` table gains `first_prompt`, `models_json`, `originator`, `agent_version`, `repo_url`, `initial_commit`, `workspace_roots_json`, `source_stamp`, and `discovery_state`; existing databases migrate on open.

### Changed

- Catalog cursors missing `source` or `sessionId` throw a `TypeError`, and a negative `limit` throws a `RangeError`.

### Fixed

- Exceptions thrown by `onSession` or `onDiagnostic` abort discovery and reject the promise instead of escaping as unhandled errors.

## [0.5.0] - 2026-08-20

### Added

- `getSessionEvents(sessionId, { source? })` returns a session's normalized transcript (text, thinking, tool calls, and results) with per-event `tokenUsage`.
- `getToolCalls(sessionId, { source? })` returns a session's tool calls with typed `isError`.

## [0.4.1] - 2026-07-07

### Added

- `pushToCloud` (`ai-hist/cloud`) runs `ai-hist push --json` in-process for hosts, and `resolveAiHistBinary` is exported.

## [0.3.7] - 2026-06-27

### Added

- Cloud client module with `loginCloud` and `loadStoredRelayhistoryAuth`.

## [0.3.5] - 2026-06-24

### Added

- Grok history source.

## [0.3.4] - 2026-06-20

### Changed

- The public `ai-hist` command defaults to the Rust CLI, including sync, show/context/session, stats, pack, resume, export/import, and tagging.
- A one-command installer installs the `ai-hist`, `ai-hist-rust`, and `ai-hist-python` launchers without manual Cargo steps.
- The legacy Python CLI remains available via `AI_HIST_CLI=python` or `ai-hist-python`.

### Fixed

- The Rust default database path honours `XDG_DATA_HOME`.
- Rust database initialization creates the legacy session metadata schema and enables WAL mode.
- The legacy Python fallback imports on Python 3.9.

## [0.3.2] - 2026-06-12

### Added

- MCP project scope argument.

## [0.3.1] - 2026-06-06

### Added

- The `ai-hist-mcp` package provides a TypeScript MCP server with Smithery config.
- MCP can read ai-hist trajectories as a source.

## [0.2.3] - 2026-05-22

### Changed

- `listSessions` is about 68x faster.

## [0.2.1] - 2026-05-22

### Added

- The SDK reads provider JSONL natively and works without the Python CLI.

[Unreleased]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.36.0...HEAD
[0.36.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.35.0...sdk-ts-v0.36.0
[0.35.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.34.3...sdk-ts-v0.35.0
[0.34.3]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.34.2...sdk-ts-v0.34.3
[0.34.2]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.34.1...sdk-ts-v0.34.2
[0.34.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.34.0...sdk-ts-v0.34.1
[0.34.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.33.0...sdk-ts-v0.34.0
[0.33.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.32.3...sdk-ts-v0.33.0
[0.32.3]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.31.0...sdk-ts-v0.32.3
[0.31.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.30.0...sdk-ts-v0.31.0
[0.30.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.29.0...sdk-ts-v0.30.0
[0.29.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.28.0...sdk-ts-v0.29.0
[0.28.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.27.1...sdk-ts-v0.28.0
[0.27.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.27.0...sdk-ts-v0.27.1
[0.27.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.24.0...sdk-ts-v0.27.0
[0.24.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.23.0...sdk-ts-v0.24.0
[0.23.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.22.1...sdk-ts-v0.23.0
[0.22.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.21.0...sdk-ts-v0.22.1
[0.21.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.20.0...sdk-ts-v0.21.0
[0.20.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.19.0...sdk-ts-v0.20.0
[0.19.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.18.8...sdk-ts-v0.19.0
[0.18.8]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.18.1...sdk-ts-v0.18.8
[0.18.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.16.0...sdk-ts-v0.18.1
[0.16.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.15.2...sdk-ts-v0.16.0
[0.15.2]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.14.3...sdk-ts-v0.15.2
[0.14.3]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.14.1...sdk-ts-v0.14.3
[0.14.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.14.0...sdk-ts-v0.14.1
[0.14.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.13.0...sdk-ts-v0.14.0
[0.13.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.12.0...sdk-ts-v0.13.0
[0.12.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.11.0...sdk-ts-v0.12.0
[0.11.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.10.0...sdk-ts-v0.11.0
[0.10.0]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.9.2...sdk-ts-v0.10.0
[0.9.2]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.9.1...sdk-ts-v0.9.2
[0.9.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.8.2...sdk-ts-v0.9.1
[0.8.2]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.6.0...sdk-ts-v0.8.2
[0.6.0]: https://github.com/AgentWorkforce/relayhistory/releases/tag/sdk-ts-v0.6.0
[0.4.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.3.7...sdk-ts-v0.4.1
[0.3.7]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.3.5...sdk-ts-v0.3.7
[0.3.5]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.3.4...sdk-ts-v0.3.5
[0.3.4]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.3.2...sdk-ts-v0.3.4
[0.3.2]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.3.1...sdk-ts-v0.3.2
[0.3.1]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.2.3...sdk-ts-v0.3.1
[0.2.3]: https://github.com/AgentWorkforce/relayhistory/compare/sdk-ts-v0.2.1...sdk-ts-v0.2.3
[0.2.1]: https://github.com/AgentWorkforce/relayhistory/releases/tag/sdk-ts-v0.2.1
