# Embedding relayhistory from Rust — the sourcing SDK

The `ai-hist` crate is the supported way for another Rust program to read
coding-agent session evidence without parsing harness logs itself. This guide is
for that program's author. It describes the surface the crate exposes on its
default features today, the rules that surface is governed by, and — in
sections marked **target contract** — the parts of
[`docs/sourcing-contract.md`](sourcing-contract.md) that are decided but not on
crates.io yet. Nothing in a target-contract section compiles against the
published crate; the version it lands in is named where it is known.

The companion example is [`examples/rust-consumer`](../examples/rust-consumer):
a standalone Cargo project outside this workspace that depends on the published
crate, syncs a corpus into a throwaway `HOME` and prints usage totals. It is
built on every pull request and, nightly, against the crate crates.io serves.

```sh
cargo add ai-hist
```

```rust,no_run
use ai_hist::{SessionStore, Source, StoreOptions, SyncOptions};

fn main() -> Result<(), ai_hist::Error> {
    let store = SessionStore::open(StoreOptions::default())?;
    store.sync(SyncOptions::default())?;
    if let Some(usage) = store.session_usage(Source::Claude, "<session id>")? {
        println!("{} requests, models {:?}", usage.request_count, usage.models);
    }
    Ok(())
}
```

## What `SessionStore` is for

RelayHistory is the single owner of acquiring, parsing and storing session
evidence for every harness ([ADR](decisions/2026-09-19-relayhistory-owns-session-sourcing.md)).
`SessionStore` is the one public entry point to that store from Rust: it opens
`ai-history.db`, runs the local sweep that fills it, and reads evidence back as
typed rows. Everything else the crate can do — search, statistics, catalog
listing, delivery, remote connectors, the watch loop — is workspace surface
behind the `unstable-internal` feature and is not part of the contract.

The store is an **in-process** library, not a daemon. A handle you open
read-write is a writer in your process; the crate makes every writer go through
its own schema, migrations, sync lock and WAL busy handler, but it cannot make
your process not be one. Open with `read_only: true` when you only read (see
[Store shape](decisions/2026-09-19-relayhistory-owns-session-sourcing.md#store-shape-one-writer-implementation-not-one-writer-process)).

What the facade owes you, and what it asks in return:

- **Typed rows, never a connection.** No method takes or returns a `rusqlite`
  type; the public-API snapshot (below) fails CI if one appears. The schema is
  explicitly not a contract, and a query you write against it can break on any
  release.
- **Bounded reads.** Every evidence read is a page with a keyset cursor. There
  is no "give me the whole session" call, and a page never materializes the
  rest of a large transcript.
- **Grouped requests.** One API request is several stored rows for every
  provider, by a different rule for each. The facade groups them; counting rows
  yourself over-reports a session by its block count.
- **Facts, not estimates.** A counter the provider did not write is `None`, a
  measurement the provider destroyed is a diagnostic, and no number is ever
  clamped, estimated or priced.

## `StoreOptions`

```rust
pub struct StoreOptions {           // #[non_exhaustive]
    pub db_path: Option<PathBuf>,
    pub home: Option<PathBuf>,
    pub read_only: bool,
}
```

`StoreOptions` is `#[non_exhaustive]`: build it with `StoreOptions::default()`
and assign the fields you set, not with a struct literal.

| Field | Meaning |
| --- | --- |
| `db_path: Some(path)` | Open exactly this file. Created (with its parent directory) unless `read_only`. |
| `db_path: None`, `home: Some(dir)` | `<dir>/.local/share/ai-hist/ai-history.db`. |
| `db_path: None`, `home: None` | The CLI's database: `$AI_HIST_DB` if set, else `$XDG_DATA_HOME/ai-hist/ai-history.db` if `XDG_DATA_HOME` is set, else `~/.local/share/ai-hist/ai-history.db` (`HOME`, or `USERPROFILE` on Windows). |
| `home` | Also the directory `sync` scans for providers: `<home>/.claude`, `<home>/.codex`, `<home>/.grok`, `<home>/.local/share/opencode/`. `None` means the process `HOME`. |
| `read_only` | Open without creating or migrating. `sync` on such a store is an `Error`. |

**Provider-root overrides.** Whatever `home` says, the sweep honours the same
environment variables the CLI does: `CLAUDE_CONFIG_DIR`, `CODEX_HOME`,
`GROK_HOME`, `OPENCODE_DB` and `OPENCODE_STORAGE_DIR` each replace one
provider's root when set to a non-empty value. An embedder that stages its own
`HOME` (a test, the example) and runs under a shell that exports `CODEX_HOME`
would sync the user's real rollouts into its store; clear those variables first
if that is not what you want.

**Schema and read-only opens.** A writable open migrates the database to the
shape this crate version reads. A read-only open cannot, so it checks and
refuses at `open` — the error names the remedy ("open it writable once, or run a
sync") — rather than handing back a handle whose first read fails on a missing
column. The marker page makes the same check at read time, because its index
arrived after the store shipped and a caller that never reads markers should
still be able to open an older database. Migrations are forward-only: a
database written by a newer crate is not guaranteed readable by an older one.

## Lifecycle: `sync`, `hydrate`, `watch`

### `sync` (shipped)

```rust
pub fn sync(&self, opts: SyncOptions) -> Result<SyncReport, Error>
```

A full local sweep of every provider root under the store's `home`: enumerate
transcripts, skip the ones whose recorded stat fingerprint is unchanged, parse
the rest, upsert their evidence. `SyncOptions` is an empty `#[non_exhaustive]`
struct today; pass `SyncOptions::default()`. On a `read_only` store it is an
`Error`.

**Locking.** Every sync path in the crate — this one, `ai-hist sync`, the CLI's
first-run bootstrap, the watch loop, the napi addon's `sync()` — takes one
exclusive advisory lock, `<db>.sync.lock` beside the database, resolved through
the database's canonical path so two spellings of one file share one lock. The
lock is **try-acquired**: a sync that finds it held returns immediately without
scanning. The holder may have already walked past the provider that just wrote,
so a skipped sync is not "someone else did my work"; it is "retry later".
SQLite-level contention below that (a delivery worker, a hook writing a prompt
row) is absorbed by a busy handler with bounded exponential backoff, not
surfaced.

What a concurrent `ai-hist sync` means for you, on the published crate: your
`sync` returns `Ok(SyncReport { changed: [] })` whether it swept or was turned
away. The report does not yet distinguish the two, and `changed` is always
empty; a consumer that needs to know must sync again once the other process is
done. The **target contract** ([#178](https://github.com/AgentWorkforce/relayhistory/issues/178))
is `Err(Error::SyncLocked)` for the turned-away case and a populated `changed`
list for the swept one.

`Error` is an opaque `#[non_exhaustive]` struct: `Display` carries the full
context chain, there are no variants to match yet, and it implements
`std::error::Error`. Matching on the message is not supported.

### `hydrate` — target contract

Targeted hydration of one session (re-parse one file, resolve continuity
across files, backfill what a sweep skipped) exists in the crate and is used by
the CLI and the napi layer, but it takes a raw connection and is not on the
facade. #178 adds `SessionStore::hydrate(SessionRef)`; until then a full `sync`
is the only supported way to bring one session up to date. Hydration is
guarded by locks of its own rather than the sweep lock, so it does not contend
with a running sweep the way two sweeps do.

### `watch` — target contract

The live-capture loop (filesystem events with a debounce, a polling backstop,
per-root recovery) is `ai_hist::watch` behind `unstable-internal` and is what
`ai-hist watch` runs. It is not on the facade; #178 names
`SessionStore::watch`. A consumer today runs `sync` on its own cadence, and the
change feed (below) is how it learns what moved.

## Reads on the facade

A `SessionStore` holds only the resolved database path; every read opens its
own short-lived read-only connection, so the handle is `Send + Sync`, cheap, and
safe to share across threads. Each read takes a `Source` and a session id and
returns one page.

| Method | Returns | Cursor |
| --- | --- | --- |
| `session_user_turns_page(source, id, limit, after)` | `SessionUserTurnPage` | `SessionEventCursor { ts_ms, id }` |
| `session_markers_page(source, id, limit, after)` | `SessionMarkerPage` | `SessionEvidenceCursor { ts_ms: Option, id }` |
| `session_requests_page(source, id, limit, after)` | `SessionRequestPage` | `SessionRequestCursor { ts_ms, id }` |
| `session_usage(source, id)` | `Option<SessionUsageSummary>` | — |

Paging is keyset, oldest first, and the tiebreak is total: timestamps repeat
freely inside a session (two events in one message routinely share one), so
every cursor carries a row id and no page is ever keyed on time alone. A
`next_cursor` of `None` means the page was the last one *at the moment it was
read*; a session that is still being written grows past it.

`Source` names the harness: `Claude`, `Codex`, `Cursor`, `Grok`, `OpenCode`,
`Relay`, `Trajectory`. It is `#[non_exhaustive]` — a `match` needs a wildcard
arm — and `as_str()` is the lowercase ledger spelling.

**Enumerating sessions is not on the facade yet.** A consumer today must know
the id it asks for (from its own configuration, from a transcript it can see,
or from the CLI's `ai-hist sessions list`). #178's `sessions()` closes
this; the example carries a `TODO(#178)` where it will go.

## The evidence model

One section per struct the facade returns. Field lists are the published ones;
`#[non_exhaustive]` structs can gain fields in a minor release, so destructure
them by name, never positionally.

### `SessionUserTurn` and `SessionUserTurnBlock`

One human-side message and the ordered blocks it carried.

| Field | Meaning |
| --- | --- |
| `id` | Row id of the turn's first event; the cursor tiebreaker |
| `source`, `session_id` | Which session |
| `message_id` | Provider message id the blocks share, when there is one |
| `preceding_message_id`, `following_message_id` | The nearest *named* message on either side. `None` only when the session recorded no named message on that side; an unnamed event is passed over rather than nulling the field |
| `ts_ms` | Unix milliseconds |
| `blocks` | `SessionUserTurnBlock`s in transcript order |

A block is `kind` (`text` or `tool_result`), `tool_use_id` (the call it answers;
`None` on text and on a result whose provider recorded no id), `byte_len` (the
provider's raw payload bytes when the parser measured them, else the UTF-8
length of the stored text) and `is_error` (`Some(1)` known failed, `Some(0)`
known succeeded, `None` not said). There is deliberately no `approx_tokens`: a
bytes-per-token heuristic served next to measured values reads as a measurement.
Bring a tokenizer and apply it to `byte_len`.

### `SessionMarker`

A provider record the normalized event model cannot carry, kept rather than
dropped: compaction and summary boundaries, provider `system` rows, non-text
content blocks (`image`, `document`, `redacted_thinking`, thinking signatures),
tool-replacement metadata, Codex lifecycle events.

| Field | Meaning |
| --- | --- |
| `id`, `marker_uid` | Row id and the stable per-session marker identity |
| `ts_ms` | `Option` — some markers are undated; undated rows sort last |
| `message_id`, `parent_id`, `turn_id` | Where in the conversation it sits, as far as the provider said |
| `kind` | The parser's classified vocabulary — `compaction_boundary`, `system`, `synthetic_turn`, `encrypted_reasoning`, `unknown`, … — stable per source once written |
| `subkind` | The provider-native type, verbatim — so a record no classifier knows still lands with its real name |
| `text` | The provider's own readable text, when it wrote one |
| `payload_json` | An allowlisted, bounded projection: strings cut at 128 characters, containers at 32 entries, recursively. Never the bytes of an image |

A compaction marker is where a session's token baseline resets. Cost attribution
across one without it is wrong, which is why the table exists.

### `SessionRequest`

One model request, as grouped by the facade from the provider's several rows.

| Field | Meaning |
| --- | --- |
| `id`, `request_key`, `request_key_source` | The group's row id, its grouping key, and where the key came from — `RequestKeySource::RequestId` (Claude's `requestId`), `ProviderMessageId` (Claude's `message.id`, no request id), `RequestSpan` (Codex: the rows between two cumulative `token_count` snapshots, which is one API call, not one turn), `RecordId` (the stored message id, a *record* identity that may be finer than a request). `is_request_identity()` says whether the key is a real upstream identity or a fallback |
| `message_ids` | Every message id folded into this request |
| `model`, `provider` | As the harness recorded them; `None` when it did not |
| `first_ts_ms`, `last_ts_ms` | Span of the request's rows |
| `usage` | `Option<NormalizedUsage>` — `None` when the request's usage could not be established |
| `usage_error` | The stable normalization error code when it could not |
| `tool_use_ids`, `has_thinking`, `event_count` | What the request contained |
| `diagnostics` | `UsageDiagnostic`s explaining any gap |

### `SessionUsageSummary`

The whole-session rollup. `session_usage` returns `None` when the session has
**no requests at all**; a session whose requests exist but whose usage could
not be established returns `Some` with `usage: None` and diagnostics. The two
are different answers and are kept apart.

| Field | Meaning |
| --- | --- |
| `usage` | `Option<NormalizedUsage>`: the checked sum over measured requests |
| `request_count`, `total_request_count` | Measured requests, and all requests recorded |
| `accounting` | Every `UsageAccounting` mode present. More than one means the totals mix units — read them per mode |
| `models` | Distinct models seen |
| `first_ts_ms`, `last_ts_ms` | Session span |
| `diagnostics` | Per-session `UsageDiagnostic`s |
| `overflowed` | The sum exceeded `u64` and was refused rather than clamped |

### `NormalizedUsage`

The one shape every provider's token payload is normalized into, and the
contract [`docs/usage-accounting.md`](usage-accounting.md) spells out: `input_tokens`
(always excluding cache reads), `output_tokens`, `reasoning_tokens: Option`,
`cache_read_tokens`, `cache_write_tokens` and the Anthropic `5m`/`1h` split as
`Option`s, `provider_total_tokens` (as reported, never recomputed),
`reported_cost_usd` (only when the source carried one), `accounting` and a
`coverage` block saying which counters the provider actually wrote.
`checked_add` sums two records and returns `None` on overflow; the example uses
it to fold requests by model.

### Structs exported without a facade read

`HistoryEntry`, `SessionEvent`, `SessionToolCall`, `SessionFileEdit`,
`SessionLocation` and `SessionScope` are re-exported on the default features so
their shapes are public and stable, but no `SessionStore` method returns them
yet. The **target contract** (#178) adds `session_events_page`,
`session_tool_calls_page` and `session_file_edits_page` on the same cursor
rules as above. Until then they are reachable only through `unstable-internal`.

### What each source populates

Generated from what each provider adapter declares
(`ai_hist::declared_evidence_kinds`) and the crate's accounting table
(`ai_hist::source_accounting`); `crates/ai-hist/tests/sourcing_sdk_doc.rs`
fails when this table and the code disagree. A `—` means the source *cannot*
report that kind — different from a session that happens to have none. Markers
are parser-derived and are written for every source the crate's own parsers
handle.

<!-- sourcing-sdk-population-table:start -->
| Source | history | session_event | tool_call | file_edit | relationship | usage accounting |
| --- | --- | --- | --- | --- | --- | --- |
| claude | ✓ | ✓ | ✓ | ✓ | ✓ | per-message |
| codex | ✓ | ✓ | ✓ | ✓ | ✓ | cumulative-delta |
| cursor | ✓ | ✓ | ✓ | ✓ | — | none |
| grok | ✓ | ✓ | ✓ | ✓ | ✓ | none |
| opencode | ✓ | ✓ | ✓ | ✓ | ✓ | none |
| relay | — | — | — | — | — | none |
| trajectory | — | — | — | — | — | none |
<!-- sourcing-sdk-population-table:end -->

The ADR's [capture matrix](decisions/2026-09-19-relayhistory-owns-session-sourcing.md#capture-matrix)
is the finer-grained, per-column view of the same question, and the place a
parser change has to move its cell.

## Accounting semantics

Read [`docs/usage-accounting.md`](usage-accounting.md) before summing anything.
The short version: `accounting` on a record says what one record *is*, which is
what decides whether adding records together is meaningful.

| Mode | Sources | Sum it? |
| --- | --- | --- |
| `per-request` | none yet | Yes, exactly |
| `per-message` | claude | Yes — the facade has already deduplicated the copies Claude writes onto every block |
| `cumulative-delta` | codex | Yes — deltas sum back to the provider's final cumulative total |
| `context-proxy` | none yet | **Never**; it is occupancy, not spend |

Cost is never estimated. `reported_cost_usd` is populated only when the source
data carried a cost. A counter that is negative, fractional or too large is a
diagnostic with a stable code, not a zero.

## Change feed — target contract

Decided in [#179](https://github.com/AgentWorkforce/relayhistory/issues/179),
not on crates.io. The example prints a placeholder where its drain will go.

- `changes_since(watermark, consumer)` returns a page of change records keyed on
  a monotonic **revision stamp** the store assigns at write time, with a total
  tiebreak. Never an offset, never a timestamp.
- A **named consumer cursor** is stored in the database. `commit(consumer,
  watermark)` advances it; a consumer that crashes between a read and its commit
  re-reads the same page. Two consumers with different names never interfere.
- **Tombstones**: a session or record the sweep retracts (a rewritten
  transcript that no longer establishes an edge, a re-parsed file with fewer
  rows) is delivered as a deletion, not silently missing.
- **Re-hydration replaces**: a re-parse of a session delivers the session as
  replaced, not as a diff against what the consumer holds. The consumer drops
  and reloads that session.
- `Error::WatermarkAheadOfStore`: the consumer's watermark names a revision the
  store does not have (the database was recreated, or restored from an older
  backup). The only correct response is a **full resync** from a fresh
  watermark; there is no partial recovery.

A new database does not rewind an existing consumer's cursor for it — that is
the case `WatermarkAheadOfStore` exists to make loud.

## Versioning: Cargo semver is the contract

There is no Rust contract-version constant. The crate's version on crates.io is
the contract, and it shares the npm version line (`sdk-ts-v0.24.0` is
`ai-hist@0.24.0`); see [`docs/releasing.md`](releasing.md).

**Pre-1.0**, a minor bump (`0.24 → 0.25`) may: add a field to a
`#[non_exhaustive]` struct or a variant to a `#[non_exhaustive]` enum, add a
method, change the semantics of a field a consumer observes, or remove an item.
A patch bump (`0.24.0 → 0.24.1`) changes no public item and no observable
behaviour of one. In Cargo's semver, `0.24.x` requirements never resolve to
`0.25.0`, so a consumer opts into every minor by bumping.

**The snapshot.** `crates/ai-hist/public-api.txt` is the crate's default-feature
public API as `cargo public-api` lists it (blanket, auto-trait and derived impls
omitted). CI regenerates it and fails on any difference, and fails separately if
any line names a `rusqlite` type. A pull request that changes the surface:

1. runs `node scripts/check-public-api.mjs --update` (needs
   `cargo install cargo-public-api --locked` and a nightly toolchain, which
   rustdoc's JSON output requires) and commits the snapshot;
2. adds a `### Rust API` entry to `CHANGELOG.md` under the unreleased section;
3. expects a minor bump at the next release.

The diff in review is the API change; the changelog entry is the sentence an
embedder reads before bumping.

## Feature flags

| Feature | Default | What it adds | For |
| --- | --- | --- | --- |
| *(none)* | ✓ | `SessionStore`, `Source`, `Error`, the evidence structs above, `NormalizedUsage` and the usage normalizers, `project_identity`, `declared_evidence_kinds` | Embedders |
| `fs-events` | — | The `notify` backend for the watch loop; without it `watch` polls | The CLI |
| `delivery` | — | Durable delivery of captured evidence to a destination | The CLI, napi, the relayhistory plugin |
| `opencode-backup` | — | Snapshot a live OpenCode SQLite store through `rusqlite`'s backup API before reading it | The CLI, napi |
| `git-hooks` | — | Git helpers and hook installation (`url`) | The CLI, napi |
| `unstable-internal` | — | Every workspace module, public: raw-connection APIs, the parsers, discovery, search, statistics, remote connectors, the watch loop. **Not covered by semver.** | This workspace and its plugins only |

An embedder enables nothing. If a feature other than the default is needed for
something a consumer legitimately does, that is a facade gap — file it.

## What relayhistory will never do

- **No cost and no pricing.** No price table, no model lookup, no per-token
  arithmetic; `reported_cost_usd` is only ever copied from the source.
- **No token estimation.** No `approx_tokens`, no bytes-per-token heuristic. A
  counter the provider did not write is `None`.
- **No activity classification.** What a turn *was* (coding, review, retry) is
  the consumer's.
- **No similarity-based session linking.** Relationships come from explicit
  provider fields, resume markers, parent-record chains and shared provider ids;
  never from content similarity.
- **No inference grouping, span trees or analytics.** The facade groups rows
  into requests because the provider's row shape demands it; everything above
  that is burn's.

These are ownership decisions, not roadmap gaps; a pull request adding one is
declined.

## Appendix: who still needs `unstable-internal`, and why

Every in-tree consumer of the crate enables `unstable-internal`. Each entry
below is a facade gap or a deliberate non-goal; the target-contract items above
close the first kind.

| Consumer | What it reaches for | Why the default surface does not cover it |
| --- | --- | --- |
| `crates/ai-hist-cli` | `open_db`, `open_db_readonly`, `init_db` | Every subcommand holds a raw connection; the CLI predates the facade |
| | `search`, `search_all`, `history_search::*`, `recent`, `sessions`, `session`, `session_events`, `session_tool_calls`, `session_file_edits`, `QueryFilter`, `ProjectGrouping`, statistics | Search, catalog and statistics are product surface, not sourcing surface; unbounded reads are not offered to embedders |
| | `insert_history`, `prompt_hash`, `HistoryEntry`, `import_json` | The hook fast path and `ai-hist import` *write* prompt rows; the facade is read-side plus `sync` |
| | `sync_local_at_cancellable`, `prepare_local_sync_snapshot`, `watch::*` | Cancellation, progress output and the watch loop (`hydrate`/`watch` target contract) |
| | `discover`, `diagnostics::doctor_report`, `paths::*`, `git_helpers::*`, tags, `resume_command` | Operator tooling: doctor, discovery diagnostics, resume commands, tagging |
| `crates/ai-hist-napi` | `open_db*`, `schema_is_*_read_current`, `default_db_path` | Node holds one long-lived connection per addon and answers a schema mismatch by reopening writable |
| | connection-taking `session_*_page`, `session_relationships`, `session_tree`, `session_children_page`, `stats_scoped_by`, `search`, `recent`, `session_locations` | The TypeScript SDK exposes relationships, trees, statistics and search that the Rust facade does not (relationships are a #178 target) |
| | `SESSION_*_CONTRACT_VERSION` constants, `delivery` | The native contract is versioned separately from Cargo semver; delivery is the CLI's worker |
| `plugins/relayhistory/rust` | `discover::{DiscoveryEnv, ShallowSessionProvider, …}`, `list_session_catalog`, `CatalogListOptions`, `SOURCE_CHOICES`, `init_db`, `prompt_hash`, `HistoryEntry` | It *is* a provider adapter (Agent Relay) and a catalog reader; adapters are workspace surface by design |
| `plugins/provider-sources/rust` | `discover::DiscoveryEnv`, `ShallowSessionProvider`, `sources::NormalizedSourceEvidence`, `observations::SessionObservation`, `EvidenceKind`, `SOURCE_CHOICES` | Remote connectors supply normalized evidence into the store; the intake contract is internal |

The rule that follows: a new in-tree call site that reaches past the facade
must be able to say which row of this table it belongs to, or it is a facade
gap and the facade grows instead.
