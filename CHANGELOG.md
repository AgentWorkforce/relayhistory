# Changelog

Notable changes to the native `ai-hist` CLI are documented here.

## [Unreleased]

### Rust API

- Record per-tool-result fidelity on `session_events`: `tool_use_id`,
  `payload_bytes`, `payload_truncated`, `payload_hash`, `call_index`,
  `event_index`, `result_status`, `event_source`, `error_signal`,
  `subagent_session_id`, `agent_id`. Bytes and hash are measured over the
  provider's **raw** payload before the `text` column is materialized — a
  string as-is, any other JSON stable-stringified with sorted keys — so a
  measurement here and one taken by relayburn's `stable_stringify` /
  `content_hash` compare equal. `payload_truncated` records that the harness
  had already cut the output. Every column is null when the provider does not
  record it, never a stand-in zero. Claude also indexes `type: "system"`
  subagent notifications as tool results carrying the delegated child's
  `subagent_session_id` / `agent_id`; Codex writes results with
  `result_status = 'unknown'` and settles them from the turn's out-of-band
  signals (`exit_code`, `patch_apply`, `mcp_err`) at `task_complete`. End of
  file is not a turn boundary — a live rollout can still report a failure after
  the bytes a sync read — so a partial read records the failures it saw and
  leaves the rest `unknown`. A result with no displayable text (a silent
  command, a structured payload with no text member) is recorded too, with its
  measured zero-byte payload, rather than dropped. Added by the
  `session_events_tool_result_fidelity_v1` marker migration, which also adds
  `session_hydration_checkpoints.last_tool_result_index`; a database written
  before this shape is routed through the writable open rather than read as
  current. Plain `sync` runs one recorded backfill pass per provider, re-reading
  a transcript whose indexed tool results have no `event_index`, so an upgraded
  install backfills them instead of skipping every unchanged file on the stamp
  fast path and reporting a successful sync over permanently null columns. The
  pass is recorded only after a walk that read every file whose recorded stamp
  would otherwise skip it next time — a failed provider read is propagated
  rather than silently read as an empty file, and the walk reports that failure
  after indexing the rest of the tree, so the source is classified as failed
  instead of reporting a cache it does not have — and that walk reached every
  rollout root the database has indexed from. The pass is bounded by a recorded generation rather than by
  "a null row exists",
  because local and remote observations share `(source, session_id)` and an
  adapter may contribute a tool result with no fidelity that re-reading the
  local transcript can never repair.
  `HYDRATION_PARSER_VERSION` 2 -> 3.

- Validate submitted `session_events` fidelity on the source-adapter boundary:
  `payload_bytes`, `call_index` and `event_index` must be non-negative,
  `result_status`, `event_source` and `error_signal` must come from the
  documented vocabularies, and all of them must be null on a row that is not a
  tool result. The TypeScript SDK types these as closed unions and casts
  without re-checking, so an unvalidated synonym would reach consumers looking
  exactly like a value they were told to expect.

- Add `session_user_turns_page(conn, source, session_id, limit, after)`:
  one keyset page of user turns, each with the ordered
  `[{kind, tool_use_id, byte_len, is_error}]` blocks its message carried,
  derived from `session_events` rather than a second table. A turn is what
  arrived on one user message. A block's `is_error` is `true` for a
  `result_status` of `errored` or `cancelled`, `false` for `completed`, and
  `null` only while the outcome is genuinely undecided — a terminal status the
  provider stated is never reported as unknown. Membership is asserted through `event_source`
  rather than inferred from `role`: only `tool_result` means "a block inside a
  message", so a Claude subagent notification and a Codex
  `function_call_output` — both stored with `role = 'tool_result'`, both
  carrying their own `message_id` — are excluded instead of each becoming a
  turn of its own. Codex has no in-message grouping, so a Codex turn is the
  prompt alone; its tool results are read through the event APIs. The turn headers and the per-turn block
  reads share one deferred read transaction, so a concurrent sync cannot
  produce a page whose headers and blocks come from different snapshots. `approx_tokens` is
  deliberately not computed — every estimate available here is a
  bytes-per-token heuristic, and one served beside measured values is
  indistinguishable from a measurement at the call site.
  `SESSION_EVIDENCE_CONTRACT_VERSION` 1 -> 2.

- Publish `ai-hist` as one crate (the former `ai-hist-core` and
  `ai-hist-engine` packages). Default features expose `SessionStore`, evidence
  structs, `Source`, and `Error`. Optional features: `delivery`,
  `opencode-backup`, `git-hooks`. Workspace crates enable `unstable-internal`
  for connection-level maintenance APIs. Cargo semver is the Rust contract;
  the crate version matches the npm release line.

- Add `ai-hist resume <query>` (prints the native resume command for the
  best-matching session) and `ai-hist pack <query>` (a compact, token-budgeted
  context block for handing a session to a different agent/tool) to the
  published npm CLI, matching the existing native Rust CLI's commands.

- Populate `projectId` on every cloud-sync envelope (repo slug from
  `history.project` or the session cwd's git remote, including relative
  forms such as `./repo`; the explicit string `unknown` when neither is
  known). Emit `filesTouched` from `session_file_edits` /
  `session_file_edits_page`, `session_outcome` envelopes from
  `session_commit_links` (`shippedAt` from evidence `commit_time_ms`;
  event ids include source and match method), and re-push revised
  trajectories through a `(updated_ms, rowid)` keyset. An upgraded client
  re-upserts previously synced history once (`capture_version` on the
  cursor). Sessions whose `file_edits` grow after the last prompt
  envelope republish `filesTouched`.

- Add targeted remote hydration for Claude Code web sessions through the
  provider's bounded teleport-evidence interface, and partial Codex cloud task
  hydration through `codex cloud diff`. Hydration contract v2 reports honest
  `full`, `partial`, and `shallow_only` capabilities, file-edit counts, and
  stable connector/auth/missing/partial failure codes.
- Relate a Claude remote session to its materialized local continuation only
  when the local provider record contains the exact `remoteSessionId`; title or
  repository similarity never creates a canonical relationship.

- Expose per-tool-result fidelity across the Node boundary. `SessionEvent`
  gains `toolUseId`, `payloadBytes`, `payloadTruncated`, `payloadHash`,
  `callIndex`, `eventIndex`, `resultStatus`, `eventSource`, `errorSignal`,
  `subagentSessionId` and `agentId`, and the new
  `getSessionUserTurnsPage(source, sessionId, options?)` —
  with `getSessionUserTurns()` and the `sessionUserTurns()` iterator in the
  TypeScript SDK — returns one keyset page of user turns and their ordered
  blocks. Native contract 16 -> 17 (the project-identity work landed 16 in
  parallel, so a build carrying both answers with 17); session-evidence
  contract 1 -> 2.

### Breaking

- The native-addon contract is now 16 and the session evidence contract is now
  2: `session_events` rows carry the per-message raw provider facts (see
  Added). Hydration parser version 3 re-parses existing databases once on the
  next `sessions hydrate` so rows already indexed gain the facts instead of
  staying null forever, and the `session_events_raw_facts_v1` schema marker is
  required, so the first read of an existing database is routed through a
  writable open that migrates it. Delivery capture triggers that were created
  before a captured table gained a column are now rebuilt rather than left in
  place by `CREATE TRIGGER IF NOT EXISTS`; without that they would go on
  reporting successful delivery while silently emitting the old column list.
  The read-only schema check validates each capture trigger's payload rather
  than only its name, so a database that gained a column under a
  `--no-default-features` build — which migrates the table but compiles the
  rebuild out — is routed through the writable open that rebuilds the trigger
  instead of passing a fast path the names alone satisfy.
  `session_events` also gains `raw_facts_version`, stamped by the local parser
  on every event it writes: plain `sync` runs one recorded backfill pass per
  provider and reads that column to pick the transcripts to re-read, telling a
  row indexed before the facts existed from one whose facts the provider never
  recorded. Without the pass a migrated database skipped every unchanged
  transcript on the stamp fast path and left the six columns null forever while
  reporting a successful sync. The pass is bounded by a recorded generation
  rather than by "an unstamped row exists", because local and remote
  observations share `(source, session_id)` and an adapter contributes rows
  through the evidence path, which does not carry the column — re-reading the
  local transcript can never stamp those. Claude selects sidecar transcripts
  through `session_relationships.evidence_locator` as well as
  `sessions.raw_path`, since a subagent sidecar has no catalog row of its own,
  and the generation is recorded only when this run saw every file the sync
  state already names, since a walk that could not read them has not
  backfilled them. Availability is judged per file rather than per root: a
  partially mounted archive returns some known paths and not others. A known
  path this run did not see also loses its stamp, so a file that comes back is
  read afresh instead of skipped on a stamp nothing watched — which is also
  what keeps a genuinely deleted file cheap, costing one further sync rather
  than leaving the pass pending forever. The checkpoint merge honours that
  removal: it folds a run's keys over the state already on disk and cannot
  express a delete, so the dropped paths are carried as an instruction that the
  merge applies and then discards, rather than living only in the run's own
  copy of the map. A transcript this run enumerated but could not read counts
  as unobserved rather than as an empty file: both parsers read with
  `unwrap_or_default()`, so a permission change, a swapped-out path or an I/O
  error would otherwise be stamped as seen and leave that path's rows null for
  good.

- Hydration contract 3; local hydration no longer claims `full` for
  prompt-only providers. `capability` is computed from the evidence kinds the
  selected provider's parser actually produces, declared per adapter as
  `ShallowSessionProvider::evidence_kinds`, instead of being the literal
  `"full"` for every local source. `HydrateSessionResult` gains
  `coverage` (the covered kinds, in canonical order) and a
  `HYDRATION_PARTIAL_COVERAGE` diagnostic naming what is absent, and
  `discovery_state` is read back off the catalog row rather than asserted.
  Cursor, Grok and OpenCode now return `capability: "partial"` with
  `coverage: ["history"]`; Claude and Codex return `"full"` when related
  evidence is requested and fully acquired, and `"partial"` without
  `relationship` when `includeRelated: false`, which never reads delegation
  evidence. Codex also reports partial relationship coverage when a bounded
  targeted search leaves newer rollout dates unexamined.
  Consumers ranking merges on `capability` (`{full, partial, shallow_only}`)
  will see prompt-only presences drop below full ones, which is the point.
- Add truthful OpenCode SQL work counters to discovery summaries. The catalog
  contract is now 3 and the native-addon contract is now 7; `bytes_read` no
  longer substitutes the OpenCode database file size, and summaries add
  `provider_queries` plus `records_inspected`.
- Retire standalone Rust CLI release assets and the curl/source installer.
  npm now distributes the public TypeScript SDK, Node CLI, MCP server, and
  mandatory Node-API engine.
- Rust engine consumers must recompile for the scoped session API. Public
  catalog/discovery option, page, summary, and row structs now carry scope or
  location data; that scoped-session change advanced the native/catalog
  contract versions to 3 and 2 at the time.
  The legacy-named `list_sessions_local*` and `discover_sessions_local*`
  wrappers reject non-local options instead of silently rewriting them; use
  their `*_scoped*` counterparts for `remote` or `all`.
- Human-readable history and catalog rows now include observed locations, statistics print
  the selected scope, and discovery summaries distinguish the requested scope
  from the connector locations that ran. A remote-only resume match no longer
  prints a local command; JSON reports it as unavailable and readable mode
  exits with an explanation.

- The native-addon contract also includes Claude subagent transcript identity
  behavior: transcripts whose records carry an `agentId` are now indexed under
  that child id instead
  of the parent's. Hydration parser version 2 re-parses and heals existing
  databases in place on the next `sessions hydrate`, moving those events —
  along with the tool calls and file edits derived from them — from the parent
  to the child rather than duplicating them, so a parent stops reporting a
  delegated thread's actions as its own. The `session_relationships_v2` schema
  marker is required, so the first read of an existing database is routed
  through a writable open that migrates it.

### Added

- Add canonical project identity on every session and event, for every source.
  `sessions.project_key` / `sessions.project_key_method` and
  `session_events.project_key` carry the `origin` remote canonicalized to
  `host/owner/repo`, or the working directory when no remote resolves, with the
  method recorded as `remote`, `path`, or `inherited`. Two checkouts,
  worktrees, or subdirectories of one repository now share one key, so a
  rollup no longer splits `/Users/a/proj` from `/home/b/proj`. The rules live
  in the new public `ai_hist::project_identity` module and match burn's
  `crates/relayburn-sdk/src/reader/git.rs` vector for vector, so
  `burn --group-by project` and a RelayHistory rollup agree on the same
  checkout; `.git/config` is read directly (including a linked worktree's
  `gitdir:` pointer) and no `git` subprocess runs. Codex's recorded
  `session_meta.payload.git.repository_url` is preferred over resolving the
  working directory. A delegated child whose own directory resolves to nothing
  canonical inherits its parent's key as a post-pass over
  `session_relationships`, so it does not depend on the order transcripts are
  parsed in. Exposed as `projectKey` / `projectKeyMethod` on `CatalogSession`
  and `projectKey` on `SessionEvent` (catalog contract version 4, native
  contract version 16); filter with `ai-hist sessions list --project <key>` or
  `listSessionCatalogPage({ projectKey })`. `ai-hist stats` now groups
  `top_projects` by the canonical key and reports `grouped_by`; `--by-cwd`
  restores the previous per-directory grouping. The cloud outbox's `projectId`
  derivation reads the remote through the same helper instead of shelling out
  to `git remote get-url`. Git's configuration is read in the scopes and
  precedence git uses — system, then global (`$GIT_CONFIG_GLOBAL`,
  `$XDG_CONFIG_HOME/git/config`, `~/.gitconfig`), then the repository's own —
  with `include.path` and `includeIf` (`gitdir:`, `gitdir/i:`, `onbranch:`)
  expanded at the position of their own line, so `url.<base>.insteadOf`
  rewrites apply wherever they are configured, as that command does. A linked
  worktree reads `config` from the directory `commondir` names and `HEAD` from
  its own, and its `includeIf` conditions are evaluated against its own git
  directory — a worktree is on a different branch from the checkout it shares a
  repository with, which is the point of it. A rewrite is overwhelmingly a global
  setting, and a reader that stopped at `.git/config` saw `gh:Org/Repo.git` as
  an unresolvable remote and fell back to a path key. A remote's URL is read as the
  list git treats it as, so a repository with a mirror configured after its
  origin keys to the origin (matching `git remote get-url`, not
  `git config --get`), and an IPv6 authority keeps its brackets instead of
  being cut at the first colon of its own address. Events of a delegated thread
  the catalog does not hold take the key of their nearest cataloged *ancestor*,
  so a subagent that delegates again still rolls up to the repository the work
  was done for. Existing databases migrate additively and deliberately
  backfill no keys: a column stays `null` until the next sync or hydration
  resolves it for real, rather than being stamped with a path key for a
  checkout that does have a remote. A `path` key is likewise never final —
  every pass reconsiders it, so a session whose checkout has been deleted
  picks up the canonical key as soon as a recorded remote makes one available,
  and a `remote` key is never downgraded. `ai-hist sessions list --project`
  and `ai-hist stats --by-cwd` are available on the Node CLI as well as the
  native one.

- Fix three defects in the delivery worker's lease keepalive that let a live
  claim lapse under load, allowing a second worker to dispatch the same batch:
  the renewal cadence was measured in requested sleep rather than elapsed time
  (so it stretched by exactly the factor the machine was overloaded by), each
  wait was scheduled from the previous renewal instead of the lease's own
  deadline (so a slow renewal compounded rather than corrected), and a
  contended `SQLITE_BUSY` write was treated as a lost lease rather than
  retried while the claim still had time to run.

- Capture the per-message raw facts a provider records on the envelope rather
  than in the message body. `session_events` gains `request_id`, `stop_reason`,
  `agent_version`, `is_sidechain`, `is_meta` and `turn_id`, and every one is
  stored as the provider wrote it -- `stop_reason` in particular is the
  verbatim wire string and stays null while a turn is still in flight, because
  its absence is how an in-progress turn is recognized. Claude supplies
  `requestId`/`request_id`, `message.stop_reason`, `version`/`sourceVersion`,
  `isSidechain` and `isMeta`; Codex stamps `turn_id` from each `turn_context`
  onto every record until the next one names a different turn. The fields are
  exposed on `SessionEvent` in Rust, on `NativeSessionEvent`, and as
  `requestId`, `stopReason`, `agentVersion`, `isSidechain`, `isMeta` and
  `turnId` on the SDK's `SessionEvent`. `message.usage` continues to be stored
  verbatim, so nested `cache_creation.ephemeral_5m_input_tokens` and
  `ephemeral_1h_input_tokens` survive a round trip; there is now a test that
  says so.

- Add first-class delegation topology. `session_relationships` gains an
  identity status (`observed` or `unlinked`), child agent type, name, model and
  spawn depth, the provider evidence that established the link (kind, file
  locator, and native reference such as a Claude `toolUseId` or a Codex
  `parent_thread_id`), the provider's spawn time, and whether the child's
  events are independently addressable. Read it with the new
  `getSessionRelationships`, `getSessionTree`, and `getSessionChildrenPage`
  operations (session-relationship contract version 1), the
  `ai-hist sessions relationships` and `ai-hist sessions tree` commands, or the
  `get_session_relationships` and `get_session_tree` MCP tools. Traversal is
  pre-order, deterministically ordered by `(spawned_at_ms, relationship_uid)`,
  cycle-safe, and bounded by `max_depth` / `max_nodes`; a tree always contains
  its root, and a repeated session reached along a second path is reported as a
  cycle only when the edge points back into its own ancestry. Global `sync` now
  records Codex delegation too — including a backfill for rollouts an earlier
  version already ingested — so topology is queryable without targeted
  hydration, and existing databases migrate automatically through the
  `session_relationships_v2` marker. A full `sync` also treats a Claude
  subagent sidecar as delegated evidence rather than a session: it records the
  same observed row (or, for a sidechain the provider never named, the same
  unlinked evidence) that targeted hydration records, keeps the child's output
  under the child, and leaves the parent's own provider locator alone.
- Add first-class structured access to a hydrated session's recorded tool
  calls and file edits: `session_tool_calls_page` / `session_file_edits_page`
  in the Rust engine, `getSessionToolCallsPage` / `getSessionFileEditsPage`
  (plus the `sessionToolCalls` / `sessionFileEdits` async iterators and the
  `getSessionToolCalls` / `getSessionFileEdits` collecting conveniences) in the
  TypeScript SDK, `ai-hist sessions tools` and `ai-hist sessions edits`, and
  MCP `get_session_tool_calls` / `get_session_file_edits`. Every one of them
  requires both a source and a session ID, because provider session IDs
  collide and evidence from two providers must never merge; a source this
  build has no provider for is rejected rather than answered with an empty
  page. Pages are keyset
  paginated over `(ts_ms IS NULL, ts_ms, id)` — undated rows sort last and the
  cursor's `ts_ms` is nullable — and carry session evidence contract 1.
  File edit rows now also expose `message_id`, `structured_patch_json`,
  `git_branch`, and `cwd`. Stored provider JSON reaches the SDK as the raw
  indexed string and is parsed into `args` / `structuredPatch`; an absent or
  unparseable value becomes `null` while `argsJson` / `structuredPatchJson`
  keep the original, so one bad payload cannot fail a page. New
  `idx_tool_calls_page_v2` and `idx_file_edits_page_v2` indexes back the access
  path, ordering on `(source, session_id, (ts_ms IS NULL), ts_ms, id)` so a
  page is read in order rather than sorted; existing databases add them, and
  drop the superseded `idx_tool_calls_page` / `idx_file_edits_page` and the
  now-redundant `idx_tool_calls_session` / `idx_file_edits_session`, on their
  next writable open.
- `ai-hist events --json` file-edit records additively carry `message_id`,
  `structured_patch_json`, `git_branch`, and `cwd`.
- Add remote provider connectors behind the existing `--remote` / `--all`
  acquisition scopes: `claude-web` lists claude.ai/code web sessions with the
  OAuth sign-in the Claude Code CLI stored (`~/.claude/.credentials.json`,
  overridable with `RELAYHISTORY_CLAUDE_CREDENTIALS`; the endpoint moves only
  via the connector-specific `RELAYHISTORY_CLAUDE_API_BASE_URL`, guarded to
  https-or-loopback, never via the generic `ANTHROPIC_BASE_URL`), and
  `codex-cloud` lists Codex cloud tasks through `codex cloud list --json`,
  paging with `--cursor` inside the CLI's 1–20 `--limit` window
  (`~/.codex/auth.json` marks it configured). Connector rows land in the
  shared ledger as shallow catalog rows with a `remote` presence, participate
  in stamp-guarded rescans, and dedupe against local presences of the same
  session. `sessions discover --remote`, `sync --remote`, and the remote half
  of `--all` now execute configured connectors; a remote-only request on a
  machine with no connector configured keeps failing with the established
  `no remote provider connectors are configured` error, now naming each
  connector's reason. Discovery summaries gain `locations_run`, the connector
  locations that actually executed (the native-addon contract is now 4), and
  the human summary line reports it in place of the hardcoded `local`. See
  `docs/remote-connectors.md`.
- Add transactional targeted session hydration through Rust, N-API, the typed
  `hydrateSession()` SDK API, `ai-hist sessions hydrate`, and MCP
  `hydrate_session`. The result reports indexed-through state, evidence counts,
  related sessions, and bounded-work diagnostics without returning a transcript.
- Add automatic `session_hydration_checkpoints` and `session_relationships`
  migrations. Existing databases upgrade in place on their next writable open.
- Add bounded live OpenCode hydration queries keyed by session ID; targeted
  hydration never copies or scans the complete OpenCode database.
- Add a real-catalog hydration benchmark that selects provider-diverse local
  sessions and reports first-call plus unchanged-checkpoint latency.

- Add a consistent session location scope to collection operations: `--local`,
  `--remote`, and `--all` are mutually exclusive, with local as the default.
  Listing, search, recent history, statistics, packs, and resume selection
  filter one cached session ledger; `all`
  deduplicates sessions that have both local and remote presences. Direct
  session/event lookup remains scope-independent. Remote discovery and sync
  run through the provider connectors introduced above and fail explicitly on
  a machine where none is configured; `all` acquisition runs local adapters
  plus every configured connector. Discovery summary `scope` is the requested
  acquisition scope and `locations_run` names the connector locations that
  executed, while each history/catalog row's `locations` contains observed
  presences.
- Add native search, recent, session, paged events, statistics, discovery,
  catalog listing, and explicit sync operations. The native-addon contract is
  now version 4.
- Add deterministic bounded event pagination using `(ts_ms, id)`.

- Add `ai-hist sessions list` and `ai-hist sessions discover`: a shallow session
  catalog over every provider. `discover` enumerates candidates cheaply, orders
  them globally by recency, and reads only bounded head/tail slices of the
  winners; `list` serves the cached catalog with one indexed query and no
  provider I/O. Both emit a versioned contract (`contract_version: 2`) —
  `list --json` as one object, `discover --json` as JSONL rows, diagnostics, and
  a closing summary with per-provider counts and operation counters. See
  `docs/session-catalog.md`.
- Extend the `sessions` catalog table with `first_prompt`, `models_json`,
  `originator`, `agent_version`, `repo_url`, `initial_commit`,
  `workspace_roots_json`, `source_stamp`, and `discovery_state`, plus the
  `idx_sessions_source_last` and `idx_sessions_raw_path` indexes. Existing
  databases migrate in place on the next open.
- Add `session_presences(source, session_id, location, raw_locator,
  source_stamp, discovery_state)`, backfill existing local evidence, and expose
  each catalog row's aggregated `locations` in catalog contract version 2.
- Expose `listSessions` and `discoverSessions` from the napi binding, so a Node
  host can drive the catalog in-process instead of shelling out.
- The npm-installed `ai-hist --version` reports the SDK package version and can
  notify interactive users when a newer npm release exists. The best-effort
  check has a 3-second timeout and is suppressed with `--no-warning` or
  `RELAYHISTORY_NO_UPDATE_CHECK=1`.

### Breaking

- Remove the legacy Python CLI and the public `ai-hist-python` and
  `ai-hist-rust` compatibility launchers. `AI_HIST_CLI` is no longer supported;
  the source-checkout launcher exits with an explanatory error when it is set.
- Make installation Rust-only. Upgrades remove recognized installer-managed
  legacy launchers and report both removals and unrecognized files left intact.

### Changed

- Replace OpenCode shallow discovery's full-database SQLite backup with a
  coherent transaction on the live read-only database. A limited request
  fetches at most that many candidate sessions and uses only provider-supplied
  session/message/part indexes for selected-session metadata. Missing optional
  schema elements or indexes now omit affected shallow fields instead of
  scanning or mutating provider tables. WAL appends, busy stores, malformed and
  partial rows, database replacement, and query-plan/scaling regressions have
  dedicated coverage.
- Recognize current Codex Desktop `response_item/message` user turns in both
  bounded session discovery and full ingestion. Existing Codex rollout indexes
  are repaired automatically, while adjacent legacy/current mirror records are
  collapsed without removing intentionally repeated prompts.
- Replace Python-based installer and end-to-end verification with shell,
  SQLite, Node.js, and the public Rust CLI interfaces.
- The earlier OpenCode private-snapshot optimization reduced repeated scans,
  but has now been superseded by the bounded live read-only path above.
- Shallow discovery's per-candidate catalog statements (candidate
  classification, skip markers, the discovery upsert) execute through the
  prepared-statement cache, and the upsert hands back the merged catalog row
  via `RETURNING` instead of a second lookup. Discovery's catalog
  transactions commit at WAL's NORMAL durability, scoped to each transaction
  and restored before rows are emitted: discovery writes only catalog rows a
  provider rescan reproduces, while user-created records (tags, commit
  links) — including any an `on_row` callback writes through the same
  connection — keep the database's default FULL durability.
- `init_db` applies the schema in one transaction when the database needs it,
  and takes no write lock at all when the schema is already current. The
  unused `idx_sessions_cwd`, `idx_sessions_branch`, `idx_sessions_last`, and
  `idx_sessions_source_last` indexes are dropped — nothing queries them, and
  each was one more btree per catalog write.
