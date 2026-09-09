# Changelog

All notable changes to `ai-hist` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- Advance the native-addon contract to 7 and the session-catalog contract to
  3. Discovery counters add `providerQueries` and `recordsInspected`; OpenCode
  no longer reports its database file size as `bytesRead`. The native contract
  also includes the delegation topology additions below.
- Replace the synchronous `openAiHist()`/`AiHist` snapshot API with top-level
  async native-backed functions.
- Remove `sql.js`, JSONL/trajectory fallback scanners, CLI subprocess bridges,
  `AI_HIST_RUST_BIN`, binary resolution, and fallback selection.
- Make shallow discovery and full sync explicit; cache-only reads never invoke
  either operation.
- Make session-event pagination the primitive API and ship the public
  `ai-hist` Node CLI from this package.
- Add Claude subagent identity behavior to the native contract. Subagent
  transcripts whose records carry an `agentId` are indexed under that child id,
  so their events — and the tool calls and file edits derived from them — move
  off the parent on the next `hydrateSession`.

### Added

- Add the `get_session_thread` MCP tool and its `getSessionThread()` SDK
  function: the lifecycle fan-out for one session — the commits it shipped plus
  the pull requests, reviews, incidents, tickets, Slack threads, hotfixes and
  follow-up sessions the cloud has linked to it. It complements
  `get_session_tree`'s subagent fan-out. Cloud-only and never cached, backed by
  `GET /v1/sessions/:sessionId/thread`. Takes `source`, `session_id`, and
  optional `kinds`, `since`, `cursor` and `limit` (1..500); returns the recall
  envelope unchanged. The MCP server now registers 14 tools.
- Resolve cloud credentials from the native `ai-hist login` store
  (`RELAYHISTORY_HOME`, else `~/.agentworkforce/relayhistory/stages/*.auth.json`)
  in addition to this SDK's `~/.config/ai-hist/auth.json`, holding native
  sessions to the same preconditions the `cloud` connector applies to recall.
  An expired session with a refresh token is rotated once and the new pair
  merged over the file it came from, so the CLI and the MCP stay in step.
  Stage selection honours `RELAYHISTORY_BASE_URL` before `AI_HIST_BASE_URL`,
  ignoring a value that names no stage, as the engine does.
  `SessionThreadOptions.resolveSession` overrides that resolution for tests; no
  other credential hook is exposed, and none was released.
- Export immutable `SOURCES` and derived `CATALOG_SOURCES` runtime registries alongside
  `isSource()` and `isCatalogSource()` guards, so consumers can validate source
  input without duplicating the SDK's provider registry.
- Surface bounded OpenCode provider-query and inspected-record counters through the typed SDK,
  CLI JSON, and MCP discovery result.
- Expose hydration contract v2 with `capability`, shallow/partial outcomes,
  file-edit evidence counts, and stable remote connector error classes.

- Add delegation topology APIs: `getSessionRelationships()`,
  `getSessionTree()`, and `getSessionChildrenPage()`, plus the
  `sessionDescendants()` and `sessionEventsIncludingDescendants()` async
  iterators. Every event `sessionEventsIncludingDescendants()` yields keeps the
  `sessionId` of the session that produced it; a child's event is never
  rewritten as the parent's. New exported types: `SessionRelationship`,
  `RelationshipCapabilities`, `RelationshipDiagnostic`, `SessionRelationships`,
  `SessionTree`, `SessionTreeNode`, `SessionChildrenPage`,
  `RelationshipCursor`, `RelationshipType`, `IdentityStatus`,
  `StableChildIdentity`, `GetSessionRelationshipsOptions`,
  `GetSessionTreeOptions`, `GetSessionChildrenPageOptions`,
  `SessionDescendantsOptions`, `DescendantEventsOptions`, and the
  `SESSION_RELATIONSHIP_CONTRACT_VERSION` constant. Results report
  `identityStatus` and `capabilities.stableChildIdentity` instead of
  synthesizing a child identity the provider never recorded. `getSessionTree()`
  always returns the root as `nodes[0]`, including for a session with no
  recorded delegation and for a database that does not exist yet.
- Add the SDK-only `sessions relationships` and `sessions tree` CLI commands
  and the read-only `get_session_relationships` and `get_session_tree` MCP
  tools. Both address a session by identity and take no scope.
- Add structured, paginated access to one session's recorded tool calls and
  file edits: `getSessionToolCallsPage(source, sessionId, options?)` and
  `getSessionFileEditsPage(source, sessionId, options?)`, the
  `sessionToolCalls` / `sessionFileEdits` async iterators, and the
  `getSessionToolCalls` / `getSessionFileEdits` collecting conveniences, plus
  `ai-hist sessions tools` / `ai-hist sessions edits` and MCP
  `get_session_tool_calls` / `get_session_file_edits`. Source and session ID
  are both required so records from two providers that share a session ID
  never mix, and a source outside the published `Source` set raises
  `InvalidArgumentError` rather than reading as an empty session. The cursor is `{ tsMs: number | null; id: number }` because a
  record may be indexed without a timestamp and undated records are ordered
  last. `args` and `structuredPatch` are parsed JSON; an absent or unparseable
  value is `null` and the raw string stays in `argsJson` /
  `structuredPatchJson`; the same parse is exported as `parseStoredJson(raw)`
  for callers holding a raw string of their own. A cursor going back in may
  spell its undated tail either way — `tsMs: null` (what a page hands back) or
  no `tsMs` at all (what a transport that drops nulls delivers) — so a printed
  or serialized cursor always feeds back unedited. Pages carry session
  evidence contract 1.

- Support remote acquisition through the engine's provider connectors:
  `discoverSessions`/`sync` with `scope: 'remote'` (and the remote half of
  `'all'`) run the claude.ai/code and Codex cloud connectors when the provider
  CLI is signed in on the machine, and keep throwing the stable
  `UNSUPPORTED_OPERATION` error when no connector is configured.
  `DiscoverResult` gains `locationsRun` — the connector locations that
  actually executed — the CLI's human discovery summary prints it, and the
  MCP `discover_sessions`/`sync` tools are now declared open-world. This
  advances the native-addon contract to 4.
- Add typed `hydrateSession()` with stable acquisition errors, evidence counts,
  indexed-through state, related session IDs, and performance diagnostics.
- Add SDK-only `sessions hydrate` CLI and `hydrate_session` MCP adapters. The
  native contract advances to 4 and hydration uses contract version 1.

- Add the shared `SessionScope` (`local`, `remote`, or `all`) to session
  discovery, listing, search, recent history, statistics, and sync. The SDK and CLI default
  to local scope; the CLI exposes mutually exclusive `--local`, `--remote`, and
  `--all` flags, and MCP tools expose the equivalent `scope` argument.
- Catalog pages, discovery results, statistics, and sync results echo their applied scope, while each
  `CatalogSession` reports its `local` and/or `remote` locations. This advances
  the session-catalog contract to 2 and the native-addon contract to 3.
- Mandatory `ai-hist-native` contract validation and stable errors for
  unsupported/missing platforms, load/version failures, and database errors.
- Native-backed discovery, catalog pages, session history, event pages,
  search, recent history, statistics, and sync.
- Thin SDK-only CLI and MCP adapters plus macOS, glibc, musl, and Windows x64
  native-package release validation.

## [0.6.0] - 2026-08-30

### Added

- `listSessionCatalog({ sources?, limit?, beforeMs?, after? })` — the materialized
  session catalog, newest first, as one indexed query over the `sessions` table.
  No provider transcript is opened and neither `history` nor `session_events` is
  scanned, so it stays fast on first paint with thousands of sessions. Rows come
  back as `CatalogSession`: cwd, git branch, first/last activity, derived
  `firstPrompt`, observed `models`, originator, agent version, repo identity,
  `sourceStamp`, and `discoveryState` (`'shallow'` for a catalog-only row,
  `'full'` once a sync ingested the transcript; a `NULL` column predates the
  catalog and reads as `'full'`). `source = 'trajectory'` rows are excluded
  defensively — trajectories are derived records, not sessions. Ordering is
  `last_activity_ms DESC` with null timestamps last, then `source` and
  `session_id` — the catalog's total order. Recency alone is not a key: one
  discovery pass can stamp many sessions with the same mtime-derived
  millisecond, so a cursor that carries only a timestamp drops every row tied
  with a page boundary. A configured `projectScope` constrains rows by `cwd`, as
  `getHandoff` does, so sources with no working directory (relay) drop out of a
  scoped listing. A negative `limit` throws a `RangeError` — SQLite reads a
  negative `LIMIT` as "unlimited", so it would otherwise dump the whole catalog.
  Returns `[]` in JSONL fallback mode: the fallback scan builds `history` only,
  and just the native discovery engine writes the catalog.
- `listSessionCatalogPage(options)` → `{ sessions, nextCursor }` — the same
  listing plus the cursor that continues it, mirroring the native
  `list_session_catalog_page` and the CLI's `next_cursor`. `nextCursor` is
  a `CatalogCursor` (`{ lastActivityMs, source, sessionId }`) and is non-null
  only when the page filled its limit, so following it until `null` walks the
  whole catalog with no skipped and no repeated rows — even across a page
  boundary that lands inside a group of tied timestamps, and through the tail of
  rows whose recency is unknown, which stays reachable from a dated cursor.
  `ListCatalogOptions` gains `after` for that cursor; `beforeMs` survives as a
  coarse cutoff and is ignored when `after` is set.
- **In-memory catalog migration** — the catalog columns (`first_prompt`,
  `models_json`, `originator`, `agent_version`, `repo_url`, `initial_commit`,
  `workspace_roots_json`, `source_stamp`, `discovery_state`) landed after 0.5.0,
  so opening an older database file now backfills each one best-effort with
  `ALTER TABLE ... ADD COLUMN` against the in-memory copy sql.js holds — the same
  trick already used for `history.git_branch`. Without it every catalog read on
  a pre-0.6 file would fail with `no such column`; with it the missing values
  simply read as `NULL`. The file on disk is never modified. The catalog
  indexes are created on the copy too — `idx_sessions_source_last`,
  `idx_sessions_raw_path`, and the two composite indexes that carry the whole
  total order (`idx_sessions_recency`, `idx_sessions_source_recency`) — so a
  query here plans the way it does natively. The `discovery_skips` table
  (`source`, `locator`, `stamp`, …), which native discovery uses to remember
  that a file is not a session, is created for shape parity; the SDK never
  reads it.
- `discoverSessions({ sources?, limit?, onSession?, onDiagnostic?, binPath?, env? })`
  — a top-level async function (not an `AiHist` method) that drives
  `ai-hist sessions discover --json` and parses its JSONL stream into
  `{ sessions, diagnostics, summary }`, invoking `onSession` progressively as
  rows arrive. It is top-level because discovery **writes** the on-disk database
  while an `AiHist` is an in-memory snapshot: a method would return a reader that
  cannot see the rows it just wrote, so re-open before listing. The limit is
  global across providers and applied by recency, matching the native engine.
  Binary discovery reuses `resolveAiHistBinary` from the cloud-push path
  (`$AI_HIST_RUST_BIN` → the install.sh location → `ai-hist` on `PATH`).
  Unparseable or unknown JSONL lines are skipped rather than failing the run, so
  a newer binary's extra line types are forward-compatible.
- **`DiscoveryError`, with the run's own evidence attached.** A provider that
  fails still only contributes a diagnostic and the run resolves; the promise
  rejects when the binary cannot be run (the message names `AI_HIST_RUST_BIN`),
  when the run exits non-zero because every provider failed, or when the closing
  summary is missing or announces a contract version this SDK does not
  implement. The native command writes its diagnostics and the summary trailer
  even on an all-provider failure, so the error carries `diagnostics`,
  `summary`, `stderr`, and `exitCode` rather than an opaque exit status.
  Checking `contractVersion` is no longer left to the caller: a mismatch (or an
  absent trailer, which the contract makes mandatory) means the rows cannot be
  interpreted safely, so it is refused instead of half-parsed. `DiscoverResult.summary`
  is therefore non-optional.
- **Callback exceptions no longer escape the stream.** An error thrown by
  `onSession` or `onDiagnostic` used to surface as an unhandled exception inside
  the stdout handler, which can take a host process down. It now aborts the run:
  the child is killed and the promise rejects with the caller's own error.
- `SESSION_CATALOG_CONTRACT_VERSION` (currently `1`), mirroring the native
  constant, plus the `CatalogSession`, `ListCatalogOptions`,
  `DiscoverSessionsOptions`, `DiscoverResult`, `DiscoverySummary`,
  `DiscoveryDiagnostic`, `DiscoveryCounters`, `ProviderDiscoverySummary`, and
  `SourceExemption` types.
- **MCP tool `list_sessions`** (read-only) — `sources?`, `limit` (default 20,
  max 200), `before_ms`, and `after` (the previous reply's `nextCursor` object);
  returns the catalog rows plus `nextCursor` as JSON alongside
  `contractVersion`, `sourceKind`, `dbPath`, and `projectScope`. Its `sources`
  enum omits `trajectory`, which the catalog never contains. With no SQLite
  database present the tool answers `{ sessions: [], nextCursor: null,
  sourceKind: "none", note }` **without** opening the reader: the catalog only
  ever lives in SQLite, so building the JSONL fallback there would walk every
  local provider file — seconds of I/O — to produce a guaranteed-empty catalog.
  Every other tool keeps the fallback behavior it had.

### Changed

- `listSessions` is unchanged and still derives sessions from `history`; its doc
  comment now points at `listSessionCatalog` as the fast and complete path
  (the catalog also holds sessions that only shallow discovery has seen).
- A cursor missing `source` or `sessionId` throws a `TypeError`. A timestamp
  alone cannot separate rows that share a millisecond, and quietly ignoring the
  half-cursor would restart the walk at page one — the same half-cursor the
  native CLI refuses.

## [0.5.0] - 2026-08-20

### Added

- `getSessionEvents(sessionId, { source? })` — the normalized transcript for one
  session from the `session_events` table: user/assistant text, thinking, tool
  calls, and tool results in order, with parsed per-event token usage
  (`tokenUsage`). Returns `[]` on databases that predate the table and in JSONL
  fallback mode. On large databases prefer streaming from the native CLI
  (`ai-hist events <session-id> --json`); this SDK loads the whole file into
  memory.
- `getToolCalls(sessionId, { source? })` — the session's `tool_calls` rows with
  typed `isError`.
- `SessionEvent` and `SessionToolCall` types.

## [0.4.1] - 2026-07-07

### Added

- **In-process cloud push (`ai-hist/cloud` → `pushToCloud`)** — a thin wrapper
  that drives the real `ai-hist push --json` Rust binary and parses its result,
  rather than re-implementing the push pipeline in TypeScript. The binary stays
  the single source of truth for batching/cursor/dedup; the SDK just gives hosts
  (e.g. the Agent Relay runtime) an ergonomic surface to sync in-process without
  shelling out to a CLI by hand. Binary discovery: `$AI_HIST_RUST_BIN` → the
  install.sh location → `ai-hist` on `PATH`. Also exports `resolveAiHistBinary`.

## [0.3.7] - 2026-06-27

### Added

- **Add cloud-client module with loginCloud and loadStoredRelayhistoryAuth**

## [0.3.5] - 2026-06-24

### Added

- **Add grok history source**

### Fixed

- Address grok review feedback

### Documentation

- Pair + cloud-sync guides + Pair client SDK/hook (#25)

## [0.3.4] - 2026-06-20

### Changed

- Make the public `ai-hist` command Rust-default for the user-facing CLI
  surface, including sync, show/context/session, stats, pack, resume,
  export/import, and tagging commands.
- Add a one-command installer that builds and installs deterministic `ai-hist`,
  `ai-hist-rust`, and `ai-hist-python` launchers without requiring users to run
  Cargo commands manually.
- Keep the legacy Python CLI as an explicit compatibility escape hatch via
  `AI_HIST_CLI=python` or `ai-hist-python`.

### Fixed

- Align Rust default database path with `XDG_DATA_HOME`.
- Create legacy session metadata schema from Rust DB initialization.
- Set WAL mode from Rust DB initialization.
- Keep the legacy Python fallback importable on Python 3.9.6 by avoiding PEP
  604-only annotations.

## [0.3.2] - 2026-06-12

### Added

- **Add MCP project scope argument**

### Dependencies

- Apply pr-reviewer fixes for #11 (#11)
- Apply pr-reviewer fixes for #11 (#11)

## [0.3.1] - 2026-06-06

### Added

- **Add ai-hist-mcp wrapper package**
- **Add ai-hist MCP trajectory source**
- **Add TypeScript MCP server and Smithery config for ai-hist-mcp**

### Documentation

- Keep MCP local-first

### Dependencies

- Apply pr-reviewer fixes for #9 (#9)
- Apply pr-reviewer fixes for #9 (#9)

## [0.2.3] - 2026-05-22

### Changed

- Rewrite listSessions to use window functions (~68x faster)

## [0.2.1] - 2026-05-22

### Added

- **Native JSONL fallback — SDK works without the Python CLI**
