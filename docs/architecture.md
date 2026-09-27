# Production architecture

The local history packages have this production call graph:

```text
provider files / SQLite
        │
        ▼
ai-hist (published Rust crate)
        │ typed Rust functions
        ▼
ai-hist-native (Node-API, async worker tasks)
        │ typed native objects, plus one JSON dispatcher
        │ (`sessionStoreCall`) over the `SessionStore` facade
        ▼
ai-hist TypeScript SDK
        ├── ai-hist Node CLI
        └── ai-hist MCP server
```

Rust owns provider discovery/parsing, schema creation and migration, direct
SQLite connections, catalog queries, history/event queries, search,
statistics, and sync. Blocking filesystem and SQLite work is dispatched away
from Node's event loop. TypeScript validates inputs, validates native contract
version 20, catalog contract version 4, hydration contract version 3,
session-relationship contract version 2, and session evidence contract version
3 and session usage contract version 3, normalizes nullable fields, maps native
errors, and supplies pagination
helpers.

The CLI and MCP server import only the SDK's public functions. They do not
open SQLite, import `ai-hist-native`, scan providers, or invoke another CLI.

The native addon exposes two kinds of entry point. The older operations are
hand-mirrored typed functions with their own option and result objects. Reads
added since the `SessionStore` facade go through one JSON dispatcher instead:
`sessionStoreCall(op, argsJson)` takes `{dbPath?, source, sessionId?, limit?,
after?}` and answers with the same camelCase document the typed function for
that read returns, so the SDK normalizes both with one set of functions. The
ops are `markers`, `requests`, `usage_summary`, `user_turns` and
`capabilities`; `sdk-ts/src/native.ts` (`SESSION_STORE_OPS`) is the only place
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
dependencies back into the local engine. See [source plugins](remote-connectors.md).

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
| `SessionStore::changes_since` (no SDK/MCP surface yet) | none | one indexed revision-range read per kind per page, plus one for tombstones; `commit` writes one cursor row | not reached: `SessionStore::open` created the database (writable) or already failed (read-only); a read-only store over a database older than the change-feed schema is refused, naming the remedy |
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
| `cursor`, `grok`, `opencode`, `relay` | never | no | no | no |

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

Codex records continuity only when a producer writes those explicit fields on
`session_meta`. A plain `codex resume` opens with a fresh `payload.id` and
leaves behind only a carried-over token baseline, which is a number and not a
session, so no `resume` row is recorded for it.

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
  `epoch`, a random identity drawn once when its feed schema is created
  (`change_feed_store`). `SessionStore::head_revision` reports the head with
  it; a stored watermark with another epoch, or beyond the head, fails with
  `ErrorKind::WatermarkAheadOfStore`, and the recovery is a full resync from
  `Watermark::START`, which names no store. `Changes::commit()` checks the
  same two things against the database it writes into, so a drain whose
  path was replaced under it cannot plant its position as the replacement's
  cursor. A copy of a database keeps its epoch, so a restore from backup is
  caught by the revision check alone, while the restored store is still
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

## Durable delivery and snapshot export

`ai-hist::export` owns evidence records, bounded snapshots, preimages, tombstones
and durable change subscriptions. These storage primitives know no destination,
account, upload acknowledgment or retry state. File/NDJSON exports remain in the
local SDK through native contract 21's `historyExport` bridge. Ordinary core
opens create no upload job, batch or membership tables.

Upload state machines live outside this repository. Indexed session
snapshot/change APIs keep provider traversal in core. See [export](export.md)
for the NDJSON snapshot surface.

Storage and uploads still share a retention budget in an enabled database.
An unread durable subscription can therefore hold evidence and exhaust capacity;
capture checks the budget before each source pass and each session's write,
compacts consumed changes above 90% of the cap, and stops the pass with a typed
`retention_limit` failure instead of losing records or attempting every
remaining session. Moving code ownership does not provide resource isolation.
