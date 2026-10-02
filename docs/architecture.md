# Production architecture

The local history packages have this production call graph. burn, a separate
repository, is shown beside them because it consumes the published crate rather
than the Node packages. That edge is the target of burn 5.0.0
([burn #562](https://github.com/AgentWorkforce/burn/issues/562)); until then
burn still reads harness logs with its own readers, as the ADR's context
describes:

```text
provider files / SQLite
        │
        ▼
ai-hist (published Rust crate) ──── SessionStore facade ───▶ burn (cost)
        │ typed Rust functions         (in-process crates.io
        │                               dependency, from burn 5.0.0)
        ▼
ai-hist-native (Node-API, async worker tasks)
        │ typed native objects, plus one JSON dispatcher
        │ (`sessionStoreCall`) over the `SessionStore` facade
        ▼
ai-hist TypeScript SDK
        ├── ai-hist Node CLI
        └── ai-hist MCP server
                └── MCP-only relay presence tools
                        │ local HTTP over a Unix-domain socket
                        ▼
                Agent Relay desktop
```

Rust owns provider discovery/parsing, schema creation and migration, direct
SQLite connections, catalog queries, history/event queries, search,
statistics, and sync. Blocking filesystem and SQLite work is dispatched away
from Node's event loop. TypeScript validates inputs, validates native contract
version 25, catalog contract version 4, hydration contract version 3,
session-relationship contract version 2, and session evidence contract version
3 and session usage contract version 3, normalizes nullable fields, maps native
errors, and supplies pagination
helpers.

The CLI and the MCP server's history tools import only the SDK's public
functions. They do not open SQLite, import `ai-hist-native`, scan providers, or
invoke another CLI. The intentional exception is the MCP-only relay presence
surface (`list_relay_agents`, `relay_status`, `join_relay`, and `leave_relay`):
`sdk-ts/src/relay-agents.ts` sends bounded local HTTP requests directly to the
Agent Relay desktop Unix-domain socket. That module does not enter the history
SDK/native layers and does not load cloud clients, authentication, tokens, or
workspace keys.

The native addon exposes two kinds of entry point. The older operations are
hand-mirrored typed functions with their own option and result objects. Reads
added since the `SessionStore` facade go through one JSON dispatcher instead:
`sessionStoreCall(op, argsJson)` takes `{dbPath?, source, sessionId?, limit?,
after?}` and answers with the same camelCase document the typed function for
that read returns, so the SDK normalizes both with one set of functions. The
ops are `markers`, `requests`, `usage_summary`, `user_turns` and
`capabilities`, plus the store-wide change feed: `changes` (`{dbPath?, from?,
consumer?, kinds?, session?, limit?}`, one bounded page of
`SessionStore::changes_since`) and `commit_changes` (`{dbPath?, consumer,
kinds?, position}`, which moves a named consumer cursor). A feed watermark
crosses as `{epoch, revision}` with the epoch as 16 hex digits, because it is a
random 64-bit store identity a JavaScript number cannot hold. `sdk-ts/src/native.ts` (`SESSION_STORE_OPS`) is the only place
in the SDK that spells them, and the SDK's request, usage and user-turn reads
use the dispatcher. The dispatcher calls only the facade and the crate's pure
capability tables — no connection, no SQL — and a new facade read is one new
arm there rather than another typed native function. The typed
`getSessionRequestsPage` / `getSessionUsage` / `getSessionUserTurnsPage`
exports stay for compatibility until a later major.

## Session sourcing ownership

RelayHistory is the single owner of acquiring, parsing and storing session
evidence for every harness. Downstream consumers — including
[`AgentWorkforce/burn`](https://github.com/AgentWorkforce/burn), which owns
pricing, cost and analytics — read that evidence through the `ai-hist` crate's
`SessionStore` facade rather than parsing harness logs themselves. New harnesses
are added here and nowhere else.

`ai-hist` is an in-process crate, so that buys one writer *implementation*, not
one writer process: a consumer that calls `sync`, `hydrate` or `watch` holds a
read-write connection in its own process, while every mutation still goes
through this crate's schema, migrations, sync lock, hydration locks and WAL busy
handler.

See [ADR: relayhistory owns session
sourcing](decisions/2026-09-19-relayhistory-owns-session-sourcing.md) for the
decision, the rejected alternatives and the per-source capture matrix, and
[`sourcing-contract.md`](sourcing-contract.md) for the record types the Rust SDK
must expose.

Two CI checks will hold the boundary; both are armed by burn's side of the
migration, not by anything here. The `burn-contract-drift` job in `ci.yml`
builds burn (main, or the `BURN_REF` repository variable) with its `ai-hist`
requirement rewritten to a path dependency on the pull request's crate, and
runs burn's relayhistory parity suite, because the effect of a change to
message ids, timestamps or usage dedup lives in burn's ledger fingerprints,
not in this workspace's tests. It runs only for changes under
`crates/ai-hist/` (and the workspace manifest and the check itself), and is a
notice until burn depends on `ai-hist` (burn #557). The weekly
`burn-reader-tripwire.yml` fails if burn's harness-parser symbols reappear, or
its parity suite is missing, after its cutover release tag
(`relayburn-sdk-v5.0.0`). Both are driven by `scripts/burn-guardrails.mjs`.

## Optional services and package boundaries

The local Rust workspace publishes one crate, `ai-hist`, containing storage,
identity, observations, evidence, relationships, local parsing, consistent
export snapshots and transactional change capture. CLI parsing and presentation live in unpublished
`ai-hist-cli`; the N-API addon is unpublished `ai-hist-napi`.
The SDK separates contracts, native loading, normalization, pagination, local
operations and generic plugin orchestration. Core, native, SDK and MCP build
without the `plugins/` tree; CI physically removes it before local checks.

`plugins/provider-sources` owns remote provider credentials/transports. Its
Rust helper depends on public local-history APIs and ships in optional platform
packages. Its JS package shares the installed SDK's public error classes. No
second addon or implicit plugin discovery is involved. Explicit registration is
inert until an operation selects the plugin; normal local operations do not
read its auth.

Uploads are not part of this repository. Team uploads come from the Agent Relay
desktop app, which consumes the published `ai-hist` crate.

## Session ledger and location scope

There is one session ledger. `local` and `remote` are presences recording where
a logical session was observed, not independent session stores. Collection
operations accept one scope: `local` (the default), `remote`, or `all`. The
`all` view is the union of both presences, deduplicated by canonical session
identity, so materializing a remote session locally does not create a second
user-visible session. Each connector/instance has an independent observation with its own opaque
locator, stamp, access state, revision and hydration checkpoint. Location
presences are aggregate views. Per-record evidence ownership prevents one
connector snapshot from deleting evidence retained by another.

Cached SDK catalog, search, recent, and statistics reads preserve the requested
scope without reading commercial credentials or invoking remote transports.
Stored remote evidence remains readable with absent, malformed, expired, or
ambiguous commercial auth. Direct session and event lookup already names one
session and remains scope-independent. The CLI's existing first-use local
bootstrap can be disabled with `--no-bootstrap`.

Acquisition accepts an explicit `HistoryPluginRegistry` and `sourceConnectors`
on discovery, hydration and sync. Omitted selection means the sources explicitly
registered in that registry; `[]` disables them. Without a registry the native
engine performs local acquisition only and rejects unknown remote selectors.
Local scope never probes remote credentials. Source identifies the provider;
connector/instance identifies its acquisition path.

Source plugins return normalized evidence with an explicit covered-kind set.
The native JSON intake validates identities before DB writes and requires the
observation revision acquired before external work. A stale completion fails
with `SOURCE_REVISION_CONFLICT`. Complete snapshots replace only covered kinds
for that connector. JavaScript source and destination plugins use the public SDK/native contracts
and do not open SQLite. Optional Rust compatibility implementations depend on
public core storage operations; they do not move transport or credential
dependencies back into the local engine. See [source plugins](source-plugins.md).

## Evidence retention

Provider files are not the source of truth for what was already observed. A
re-parse of the same provider file never deletes evidence an earlier parse
stored for the same session: local re-ingests upsert by provider-native
identity (Claude records carry their `uuid` into every derived `event_uid`),
so rows the rewritten file no longer contains are left untouched in
`session_events`, `tool_calls`, `file_edits` and `history`. Records without
provider identity derive a content-hash fallback instead of a line index, so
a compaction that drops the prefix or inserts summary rows cannot shift
survivors onto earlier rows' identities. Byte-identical id-less rows share
that identity by design: an ordinal would be positional identity by another
name. Pre-upgrade positional leftovers heal onto a re-attributed record
only on a unique full-record match — event text, timestamp, role, kind,
model and token spend, or a session-unique tool use id — otherwise they
stay preserved. Claude Code
rewrites a transcript in place on resume/compact, and the compacted file is
routinely missing assistant turns the pre-compaction file contained; those
turns stay queryable. The only local deletion path is a targeted heal that
names its exact rows (sidechain re-attribution moving a delegated thread's
records onto the child). Retention/compaction deletion of a large database is
explicit and opt-in, never a side effect of re-parsing. Codex rollout events
are keyed by line position rather than provider identity, so the pinned
guarantee there covers relocation (the `sessions/` to `archived_sessions/`
move re-ingests under the same session id with no loss or duplication);
content-stable identity for prefix-dropping rollout rewrites is future work
for incremental hydration. `crates/ai-hist/tests/claude_rewrite_retention.rs`
pins all of this: in-place compaction through `sync` and through targeted
`hydrateSession`, an mtime-only rewrite at identical size, and the Codex
archive relocation.

## Operation semantics

| Operation | Provider I/O | Database work | Missing database |
|---|---:|---|---|
| `listSessionCatalog*` (`local` / `remote` / `all`) | none | one indexed cache query | empty page |
| `discoverSessions` (`local`, default) | bounded shallow reads | catalog upserts | creates catalog DB |
| `discoverSessions` (`remote`) | explicitly selected source plugins (error when none) | catalog upserts + presences | creates catalog DB |
| `discoverSessions` (`all`) | local adapters + explicitly selected source plugins | catalog upserts + presences | creates catalog DB |
| `hydrateSession` | one selected provider session and linked evidence | transactional evidence + checkpoint upsert | `SESSION_NOT_FOUND` |
| `search`, `recent` (`local` / `remote` / `all`) | none | indexed reads | empty result |
| `stats` (`local` / `remote` / `all`) | none | indexed aggregate reads | empty result |
| `getSession` | none | indexed identity read | empty result |
| `getSessionEventsPage` | none | bounded keyset page | empty page |
| `getSessionUserTurnsPage` | none | bounded keyset page plus ordered block reads on one snapshot | empty page |
| `SessionStore::sessions` | none | keyset-paged catalog reads, one page at a time | not reached: `SessionStore::open` created the database (writable) or already failed (read-only) |
| `SessionStore::session` | none | every evidence table for one session on one read snapshot; `kinds` skips tables, `include_text: false` never moves the text column | `None` for an uncatalogued session |
| `getSessionRelationships` | none | indexed relationship reads | empty result |
| `getSessionTree` | none | indexed relationship reads, one child query per emitted node | root-only tree |
| `getSessionChildrenPage` | none | bounded keyset page | empty page |
| `getSessionToolCallsPage`, `getSessionFileEditsPage` | none | bounded keyset page over one source's session | empty page |
| `getSessionMarkersPage`, `session_markers_page` (`SessionStore::session` carries the same markers untruncated) | none | bounded keyset page over one source's session | empty page; a read-only `SessionStore::open` over a database older than the marker page index is refused, naming the remedy (the native dispatcher then reopens writable and migrates, as the typed reads do) |
| `getSessionRequestsPage`, `getSessionUsage` | none | bounded keyset page / streamed rollup over the derived request view | empty page / summary with no requests |
| `getSourceCapabilities` | none | none: answered from the provider capability tables | the same answer |
| `getChangesPage`, `changesSince`, `SessionStore::changes_since` | none | one bounded page per call: indexed revision-range reads per kind, plus tombstones; a session filter seeks that session's index | SDK: empty, finished feed; the file is not created. `SessionStore`: not reached, since `SessionStore::open` created the database (writable) or already failed (read-only); a read-only store over a database older than the change-feed schema is refused, naming the remedy |
| `commitChanges`, `Changes::commit` | named consumer cursor in `consumer_cursors` (forward-only, bound to its kind set) | none | `WATERMARK_AHEAD_OF_STORE`: the position names no store |
| `sync` (`local`, default) | full explicit scan | migrations + ingestion | creates DB |
| `sync` (`remote`) | explicitly selected source plugins (error when none) | observations, normalized evidence, checkpoints | creates DB |
| `sync` (`all`) | full local scan + explicitly selected source plugins | migrations + ingestion | creates DB |
| `SessionStore::sync`, `SessionStore::watch` | full local scan (fingerprint-gated; forced on fs-event ticks) | the `local` sync under `SyncRunLock`; a held lock is `SyncLocked` after the caller's timeout, never a silent skip | not reached: created at `open` |
| `SessionStore::hydrate` | one session by id, or one transcript by path (hook fast path, Claude only) | as `hydrateSession`; a missing transcript is a status, not an error | `SESSION_NOT_FOUND` for an id; `Missing` for a path |

A writable `SessionStore::open` migrates the database it opens; a read-only
one cannot, so it refuses a database older than the shape this version reads
and names the remedy, rather than handing back a store whose first read fails
inside a query. The facade's full surface, its error codes and lock semantics
are in [`docs/sourcing-sdk.md`](sourcing-sdk.md).

No read operation invokes discovery or sync. A common cold start is:

```ts
await discoverSessions({ limit: 100, scope: 'local' });
const sessions = await listSessionCatalog({ limit: 100, scope: 'all' });
await hydrateSession({ source: sessions[0].source, sessionId: sessions[0].sessionId });
```

Global sync owns enumeration while targeted hydration resolves one persisted
connector observation. Optional provider helpers use the public Rust normalizer;
other source plugins can supply already-normalized records.
TypeScript never parses a provider source or opens SQLite. Per-observation checkpoints retain acquisition progress independently. Local
provider stamps avoid reparsing unchanged files; remote plugins may need a full
readback traversal to establish whether their snapshot changed. Remote
hydration stays provider-bounded: Claude uses the CLI's private
teleport-evidence contract, while Codex indexes the supported cloud diff and
reports partial capability because no transcript export exists.

## Session topology

`session_relationships` records one row per observed relationship, keyed by
`(source, parent_session_id, relationship_uid)`. Each row carries what
established the link — `evidence_kind`, the provider file in
`evidence_locator`, and the provider-native reference in `evidence_ref` (a
Claude `toolUseId`, a Codex `parent_thread_id`, the field name or the record
uuid that established a continuity edge) — plus whatever the provider recorded
about the child: agent type, agent name, model, spawn depth, and the
provider's own spawn time.

There are two kinds of relationship, and they answer different questions.
**Delegation** (`delegated`, `materialized_local`) is one session starting a
different thread of work. **Continuity** (`continuation`, `fork`, `resume`) is
one conversation carrying on as another: a `/resume`d session, a branch taken
from a shared origin, a transcript that opens by answering a record it does not
contain. Continuity rows also carry `origin_session_id`: the conversation a
fork or continuation came from, when the provider named one distinct from the
parent.

`identity_status` separates two honestly different things. An `observed` row
names the child: `child_session_id` is the provider's own identity for it, and
`child_has_events` says whether that child is independently addressable
through `getSessionEventsPage`. An `unlinked` row means the provider recorded
a delegation but no stable child identity, so `child_session_id` is null and
the child's output stays attributed to the parent. A child id is never
synthesized, never derived from a file name, and never inferred.

What a provider can record is a property of the provider, not of a database,
so every result reports it:

| Source | Stable child identity | Agent type | Spawn time | Evidence locator |
|---|---|---|---|---|
| `codex` | always | yes | yes | yes |
| `claude` | sometimes (only versions that emit a per-child `agentId`) | yes | yes | yes |
| `grok` | sometimes (only `subagents/` entries carrying a session id) | yes | yes | yes |
| `opencode` | always | no | yes | yes |
| `muse` | always | yes | yes | yes |
| `cursor`, `devin`, `relay` | never | no | no | no |

A linked child's events are stored under the child's own session id and are
never flattened into the parent. The delegated instruction that started a
child is not a human prompt: it never becomes a `history` row, and a delegated
thread never becomes a top-level catalog session.

Children are ordered by `(spawned_at_ms, relationship_uid)` with null spawn
times at the tail; `relationship_uid` is unique per parent, so that is a total
order shared by `getSessionChildrenPage`, `getSessionTree`, and the SDK's
`sessionDescendants` walker. Traversal is pre-order over an explicit stack,
never recursion, and `nodes[0]` is always the root — including for a session
with no recorded delegation and for a database that does not exist yet.

A session appears exactly once, at the position pre-order first reaches it. An
edge back into the current branch's own ancestry is a cycle: it is not expanded
again and emits a `RELATIONSHIP_CYCLE` diagnostic. An edge to a session already
emitted on another branch is a diamond, not a loop; it is simply not expanded a
second time, and is neither diagnosed nor counted as truncation.

`maxDepth` (default 32, maximum 64) and `maxNodes` (default 1000, maximum
10000) bound the work to one indexed child query per emitted node and surface
`RELATIONSHIP_TREE_DEPTH_LIMIT` and `RELATIONSHIP_TREE_TRUNCATED` diagnostics
instead of silently short results. Tree-level `truncated` means a budget
stopped the walk short of the recorded evidence, so a cycle or diamond never
sets it; node-level `truncated` marks every node whose children were left
unexpanded, including all parents still pending when the node budget ran out.
Unlinked rows are reported in `unlinked` with a `RELATIONSHIP_UNLINKED_CHILD`
diagnostic rather than traversed — at every depth, the boundary included, so a
node whose only children are unlinked evidence is complete rather than
truncated. Only a session the tree has not already emitted is charged against
`maxNodes`, so a diamond is never reported as a budget truncation. Deterministic
Claude remote-to-local materialization is also recorded as a
`materialized_local` relationship; title or prompt similarity is never used to
infer one. The SDK's
`sessionDescendants` walker applies the same `childCount` and `truncated`
rules to the nodes it yields.

### Continuity

`getSessionTree` and `getSessionChildrenPage` take `relationshipKinds`. Omitted
means delegation only — defined as *every kind that is not a continuity kind*,
not as a whitelist of `delegated`, so `materialized_local` keeps traversing as
it always has and a delegation-only database answers byte-identically to before
continuity existed. Naming the continuity kinds instead expands from an origin
to its resumed, continued, or forked descendants over the same bounded walk.
`getSessionRelationships` reports continuity on its own `continuity` array, in
both directions, leaving `asParent` and `asChild` delegation-only.

Continuity is reconstructed from four signals, in that order of authority: the
explicit fields a provider writes (`continuedFromSessionId`, `forkSessionId`,
`sourceSessionId`), a `/resume <id>` or `/continue <id>` the human typed, a
transcript's first `parentUuid` resolved against the session that actually
holds that record, and two or more transcripts carrying one in-log session id.
`evidence_ref` names which one produced the row.

A `/resume` is read in both forms Claude writes: the bare `/resume <id>` a
human types, and the control wrapper Claude Code actually stores —
`<command-name>/resume</command-name>` with the target in `<command-args>`.
Both are control rows (`session_events.control_kind` = `resume_marker` and
`slash_command_invocation`; see `src/ingest/control.rs`), so neither is a
prompt. Matching only the bare form matched the one shape a real transcript
never contains.

Unlike delegation, continuity is not observable inside a single transcript, so
each transcript's evidence is banked in `session_continuity_evidence`, keyed by
the transcript. Reconciliation then runs over the stored rows rather than over
a set of files held in memory, which is what makes it work during targeted
hydration of one session. Evidence that cannot resolve yet — a parent record no
session has indexed, a lone branch with no sibling, a resume marker naming no
session — keeps a `pending_reason` and is reported as a
`RELATIONSHIP_CONTINUITY_UNRESOLVED` diagnostic on both hydration and
`getSessionRelationships`. Hydrating the file that supplies the missing piece
resolves it without re-reading the first file.

A re-read replaces what a transcript says, including retracting it. Every edge
carries the `evidence_locator` that established it, and a reconciliation pass
removes that locator's edges which the current read no longer produces — so a
rewritten `/resume`, a changed explicit field, or a file that stops being a
session at all cannot leave a stale edge queryable. Retraction is keyed on the
whole row identity, not the uid alone: a `/resume` retyped against a different
session keeps its uid and changes only the parent.

The stamp maps decide whether a file is opened at all, so an install upgrading
into continuity would otherwise skip exactly the files whose evidence has never
been banked, and report a successful sync over an empty table. Both providers
re-read once when a locator has no evidence row: Claude falls through to a full
re-read, Codex reads only the `session_meta` line its continuity lives on. Every
readable file writes a row — including one carrying no continuity at all — so
the condition always clears and nothing is re-read forever. This is deliberately
narrower than bumping a stamp-map generation, which would re-read the whole
archive and discard the selective-repair state those maps carry.

A fork branch is given a child identity only when the provider gave it one. Two
transcripts carrying the same in-log `sessionId` are branches with no identity
of their own, so each is recorded as unlinked evidence keyed on its transcript;
a branch's identity is never taken from its file name, here or anywhere else.

Codex does record forks, under field names of its own on `session_meta`: a
human "fork conversation" carries `forked_from_id` (with `thread_source:
"user"`), and a spawned subagent names the thread it started from in
`source.subagent.thread_spawn.parent_thread_id`. Each becomes a `fork` edge
whose `evidence_ref` is the field that named it; a subagent keeps its
`delegated` row and gains the `fork` edge beside it, and a human fork stays a
top-level catalog session. Evidence banked before these fields were read is
re-read once, from the `session_meta` line alone. What is still unobservable is
a plain `codex resume`: it opens with a fresh `payload.id` and leaves behind
only a carried-over token baseline, which is a number and not a session, so no
`resume` row is recorded for it.

A forked rollout also **replays its parent's history** before its own turns:
Codex copies the parent's `session_meta`, its turns' `task_started` /
`turn_context` / message records and their cumulative `token_count` snapshots
into the child's file. The rollout walk gates that copy (`ForkReplaySpan` in
`src/ingest.rs`), and only on explicit evidence:

- The span **opens** at a `session_meta` after the file's first line whose
  `payload.id` is the parent the opening `session_meta` named in
  `forked_from_id` or `thread_spawn.parent_thread_id`. A rollout that names no
  such parent, or a fork that opens on something else (a guardian's
  `compaction` item), is never gated.
- It **closes** at the first `task_started` or `turn_context` whose turn is the
  child's: a UUIDv7 `turn_id` at or after the child thread's own UUIDv7
  timestamp (else its `session_meta` timestamp), or, for a legacy turn id,
  `started_at` after the fork's second. A turn nothing orders against the
  fork -- including a legacy `started_at` in the fork's own second, which
  second resolution cannot order -- also closes it: undecided is indexed
  rather than dropped. A `turn_context` with no `turn_id`, or repeating the
  `turn_id` of a replayed `task_started` before it, takes that verdict, since
  it describes the turn that record opened. The presence of
  `task_started` is not used: Codex 0.155 replays the parent's `task_started`
  records too.

Two limits follow from gating on explicit evidence only. A replayed legacy
turn with no UUIDv7 id and no `started_at` (or one in the fork's own second)
closes the span early, and the rest
of that replay is indexed under the child as before. And a record the child
writes before its first `task_started` / `turn_context` falls inside the span:
nothing in it tells it from the parent's copy (codex-rs appends a
`thread_settings_applied` after the copied prefix, the same shape as the
parent's own). Every observed build opens a turn with `task_started` before any
prompt or model output, so what that drops is settings state the child's own
`turn_context` restates.

Lines inside the span write nothing under the child. One
`fork_replay_boundary` marker, keyed by the replayed `session_meta`'s line,
accounts for them (`first_line`, `last_line`, `replayed_lines`, the closing
turn and the rule that closed it). The last readable `token_count` inside the
span becomes the child's inherited baseline, so the child's first request is
charged only what it spent beyond the parent's total. codex-rs seeds a fork's
usage from the copied history (`record_initial_history` on
`InitialHistory::Forked` calls `last_token_info_from_rollout`), and each request
then grows `total_token_usage` by exactly `last_token_usage`; the child's first
readable snapshot is checked against that: `total == last` means its counter
restarted and the baseline is dropped, `total == inherited + last` confirms it,
and without `last_token_usage` only a total below the inherited one drops it.
The marker records the outcome in `inherited_baseline` (`pending`, `applied`,
`dropped`) and `inherited_baseline_basis`, and a pending decision rides on the
cursor. The cursor never commits inside a span, so a live fork read before its
first own turn re-reads the replay on the next pass; once the child's first
turn completes it commits past it.

Rows an earlier parser indexed under the child for the replayed lines are
retired when the span is read, and sync re-reads every unchanged fork rollout
once (`codex_fork_replay_gate` in the sync state) so an existing install loses
its duplicates too; that walk also rewrites the fork's `first_prompt`, clearing
a replayed parent prompt when the fork has none of its own, because the
shallow writer only fills a missing value. The cleanup is one pass, run by
`sync` only: an older build still writing to the same database can put the
duplicates back.

Events use `(ts_ms, id)` keyset pagination. Tool calls and file edits use the
same keyset shape over `(ts_ms IS NULL, ts_ms, id)`: both tables allow a null
timestamp, so undated rows sort last and the cursor carries a nullable
`ts_ms`. Those two pages require a source as well as a session id, because a
session id alone can name one session per provider. Catalog ordering is
`(last_activity_ms DESC, source ASC, session_id ASC)`, with null timestamps at
the tail. Relationship ordering is `(spawned_at_ms, relationship_uid)`, also
with null timestamps at the tail. These total orders prevent duplicate or
omitted rows at timestamp ties.

## Change feed

A downstream consumer that keeps its own materialised view — burn's watch
loop — reads "what changed since my last tick" through
`SessionStore::changes_since` rather than rescanning the catalog or watching
provider files itself.

Every row of `sessions`, `session_events`, `tool_calls`, `file_edits`,
`session_markers` and `session_relationships` carries a `revision`: one value
per row write, drawn from the database-wide `observation_clock`, stamped by
`change_feed_<table>_{insert,update,delete}` triggers. Stamping lives in
triggers rather than in each writer because the ledger has well over a hundred
write sites, and a write site that forgot the stamp would be a row the feed
silently never reports. A deleted row — a sidechain heal moving records onto
the child, a session leaving the catalog and the cascade under it — leaves a
tombstone in `evidence_tombstones(kind, source, session_id, record_key,
revision)`; a later insert of the same key clears it. Every fed table has a
`(revision)` index and the tombstone table a `(kind, revision)` one, so each
page of the feed is an indexed range read with no scan and no sort. A catalog
row's `locations` is derived from `session_presences`, so a presence arriving
or leaving re-stamps its `sessions` row too: a consumer sees the row replaced
even though nothing wrote `sessions` itself.

Each page is read in two passes: a covering read of each stream's revision
index finds the page's cut (the `batch`-th smallest revision across streams),
and only the rows below it are then fetched, so at most one page of typed rows
is resident however many kinds are fed. Both passes read one snapshot, so a
writer re-stamping or deleting the page's rows between them cannot empty the
window the cut describes; and an empty window steps the position forward rather
than declaring the head, so exhaustion is only ever what the key pass proved.
The drain's start and its head are
resolved from one read snapshot, so a sibling drain committing the cursor while
this one opens can never make a valid cursor look ahead of the head. A
read-only handle over a database the feed has not migrated reports
`Watermark::START` as its head; its pre-feed `observation_clock` is not a feed
position.

`changes_since(from, ChangeQuery { kinds, consumer, batch })` yields
`Change { kind, source, session_id, record_key, revision, op }` in
`(revision, kind, record_key)` order, where `op` is `Upsert(EvidenceRow)` —
the typed row, `ShallowSession`, `SessionEvent`, `SessionToolCall`,
`SessionFileEdit`, `SessionMarker` or `SessionRelationship` — or `Delete`.
`record_key` is the record's provider-native identity within its session and
kind: `event_uid`, `tool_use_id`, `marker_uid`, `relationship_uid`, or the
session id for a catalog row. The drain is bounded to the head revision at
open and pages in `batch`-sized reads, at most 10,000. `ChangeKind` is not
`EvidenceKind`: the catalog row is fed and is not adapter evidence.

Three rules a consumer must hold:

- **A re-seen key is a replace.** Every upsert re-stamps, so a parser-version
  re-parse re-reports every row of that session at a new revision. Applying
  the feed in order onto a keyed map reconstructs the tables (modulo rows
  whose tombstones it also applied); `crates/ai-hist/tests/change_feed.rs`
  pins that over the fixture corpus after each of a sequence of syncs.
- **The cursor moves only on commit, and only forward.** With
  `ChangeQuery::consumer` set, `Watermark::CONSUMER` resumes from that
  consumer's last committed position (`consumer_cursors`, inside the store, so
  it survives the consumer's own ledger reset), and `Changes::commit()`
  persists `position()` and returns the cursor as stored. A drain that fails
  partway re-reads from the previous commit rather than skipping what it had
  reached. A stale commit — an older drain committing after a newer one, or a
  replay from an explicit watermark under a name that has moved past it —
  leaves the cursor where it is; a consumer that wants to reprocess drains from
  an explicit `from` and does not commit. Two consumers advance independently.
- **A consumer name is scoped to one kind set.** A cursor is a position in a
  stream, and a stream is defined by its kinds: a drain over events alone that
  reaches the head and commits has accounted for no relationship, marker or
  catalog row on the way. So `consumer_cursors` records the normalized kind
  set a cursor was committed for, and a `Watermark::CONSUMER` drain or a
  `commit()` under a different kind set fails with
  `ErrorKind::ConsumerKindsMismatch` rather than silently skipping the other
  kinds. Use another consumer name for another filter.
- **A watermark this store never issued is a reset.** Every database counts
  revisions from zero, so a revision alone cannot tell a replacement database
  from the one it replaced. A `Watermark` also carries the issuing database's
  `epoch`, an identity drawn when its feed schema is created
  (`change_feed_store`). The store also fingerprints the exact stored-column
  sets each upsert exports. A migration that changes one rotates the epoch and
  clears in-database named cursors atomically: otherwise an unchanged row
  would keep its revision while acquiring different semantic JSON. This is a
  stream reset, so every consumer replays the new shape.
  `SessionStore::head_revision` reports the head with it; a stored watermark
  with another epoch, or beyond the head, fails with
  `ErrorKind::WatermarkAheadOfStore`, and the recovery is a full resync from
  `Watermark::START`, which names no store. `Changes::commit()` checks the
  same two things against the database it writes into, so a drain whose
  path was replaced under it cannot plant its position as the replacement's
  cursor. A copy of a database keeps its current epoch, so a restore from
  backup is caught by the revision check alone, while the restored store is still
  behind the watermark. A named cursor
  past the head names no revision of this store, so the resync's commit
  replaces it: the one commit that moves a cursor back.

An in-progress message is never in the feed. Incremental hydration holds a
Claude message whose `stop_reason` is still `null` and writes nothing for it;
when it completes, its blocks arrive together, each exactly once, with the
usage they ended with. `session_requests` is a view over `session_events`, so
a request is not fed as a row of its own: the events that make it up are, and
`session_requests_page` reads the grouped result.

The feed is a pull cursor for an in-process consumer.

## Native errors

The SDK distinguishes unsupported platform, supported platform package
missing, addon load failure, native/SDK contract mismatch, database-open
failure, invalid argument, query failure, discovery failure, and sync failure.
There is no alternate runtime after any native-load error.

## Snapshot export and upload state

`ai-hist::export` (the `export` feature) is local export: NDJSON a user
writes to a file or a pipe through the SDK's `exportHistory`, in bounded
pages. An `ExportSnapshot` owns a connection holding one read transaction, so
every page reads the store as it stood when the snapshot opened, whatever is
written meanwhile; in WAL mode the transaction never blocks the writer. Each
record carries the change feed's key and revision for its row. Nothing is
stored: an open snapshot lives in the addon process until it is closed or
expires.

The crate keeps no upload state. An uploader reads the change feed
(`SessionStore::changes_since`), which carries every row in full, and keeps
its own cursor and consent. A write to the store is never refused on an
uploader's behalf.

A store an earlier release armed for upload capture has capture triggers on
every evidence table, retention triggers that abort a write once the capture
budget is spent, and `delivery_identity_*` indexes. The first writable open
drops them (marker `export_capture_retired_v1`), and any later open that finds
one again drops it again. It leaves every table of that era —
`delivery_state`, `delivery_journal`, `delivery_shadow`,
`delivery_bootstrap_bounds`, `delivery_exclusions`, `history_subscriptions`,
`history_compaction` — exactly as it is. Their owner is the upload daemon that
created them, and it rebuilds an old install from two of their facts:
`delivery_state.origin_id`, and its revision floor from `sqlite_sequence` where
`name = 'delivery_journal'`. SQLite deletes a table's `sqlite_sequence` row
when the table is dropped, so this crate never drops or alters them.

See [export](export.md) for the NDJSON snapshot surface.
