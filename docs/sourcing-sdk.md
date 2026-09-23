# Embedding relayhistory from Rust — the sourcing SDK

The `ai-hist` crate is the supported way for another Rust program to read
coding-agent session evidence without parsing harness logs itself. This guide
is for that program's author: the surface the crate exposes on its **default
features**, and the rules that surface is governed by.

Everything a consumer needs is one type, `ai_hist::SessionStore`, nine
operations, and the typed structs they return. Nothing on this surface names a
`rusqlite` type, and no JSON column reaches a consumer as a string. This is the
surface [`docs/sourcing-contract.md`](sourcing-contract.md) is delivered
through and the ADR
[relayhistory owns session sourcing](decisions/2026-09-19-relayhistory-owns-session-sourcing.md)
decided on.

The companion example is [`examples/rust-consumer`](../examples/rust-consumer):
a standalone Cargo project outside this workspace that depends on the published
crate, syncs a corpus into a throwaway `HOME`, walks the catalog, prints usage
totals by model and drains the change feed. It is built on every pull request
against this checkout and, nightly, against the crate crates.io serves.

```toml
[dependencies]
ai-hist = "0.24"
```

```rust,no_run
use ai_hist::{CatalogQuery, SessionQuery, SessionStore, StoreOptions, SyncOptions};

fn main() -> Result<(), ai_hist::Error> {
    let store = SessionStore::open(StoreOptions::default())?;
    let report = store.sync(SyncOptions::default())?;
    println!("swept={} changed={}", report.swept, report.changed.len());
    for row in store.sessions(CatalogQuery::default()) {
        let row = row?;
        let Some(evidence) = store.session(&row.session_ref(), SessionQuery::default())? else {
            continue;
        };
        for message in &evidence.messages {
            if let Some(usage) = &message.usage {
                println!("{} {:?} in={} out={}", row.session_id, message.request_id, usage.input_tokens, usage.output_tokens);
            }
        }
    }
    Ok(())
}
```

## What `SessionStore` is for

RelayHistory is the single owner of acquiring, parsing and storing session
evidence for every harness ([ADR](decisions/2026-09-19-relayhistory-owns-session-sourcing.md)).
`SessionStore` is the one public entry point to that store from Rust: it opens
`ai-history.db`, runs the local sweep that fills it, and reads evidence back as
typed rows. Everything else the crate can do — search, statistics, delivery,
remote connectors — is workspace surface behind the `unstable-internal` feature
and is not part of the contract.

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
- **One session, one snapshot.** `session()` reads every table for a session
  inside one deferred read transaction, so the parts cannot disagree with each
  other, and `SessionQuery { include_text, kinds }` is how a consumer bounds
  what that costs. The catalog walk pages internally on a keyset and holds one
  snapshot for the life of the iterator, so drain or drop it promptly.
- **Grouped requests.** One API request is several stored rows for every
  provider, by a different rule for each. The facade groups them into
  `SessionEvidence::requests`; counting messages yourself over-reports a
  session by its block count.
- **Facts, not estimates.** A counter the provider did not write is `None`, a
  measurement the provider destroyed is a diagnostic, and no number is ever
  clamped, estimated or priced.
- **Nothing is a silent no-op.** A sweep turned away by another process's lock
  is `Error::SyncLocked`, a read-only handle asked to write is
  `Error::UnsupportedOperation`, and a watermark the store cannot serve is
  `Error::WatermarkAheadOfStore`.

## The nine operations

| Method                                           | Provider I/O                                | Database work                                                             | Lock                                             |
| ------------------------------------------------ | ------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------ |
| `SessionStore::open(StoreOptions)`               | none                                        | creates and migrates the schema (writable); checks it (read-only)         | none                                             |
| `sync(SyncOptions)`                              | full local sweep, fingerprint-gated         | migrations + ingestion + shallow discovery                                | `SyncRunLock`, whole run                         |
| `hydrate(&SessionRef, HydrateOptions)`           | one session and its bounded related files   | transactional evidence + checkpoint upsert                                | per-session hydration lock                       |
| `watch(WatchOptions) -> WatchHandle`             | fs-event or polling driven sweeps           | the same as `sync`, per tick                                              | `SyncRunLock`, per tick                          |
| `sessions(CatalogQuery) -> CatalogIter`          | none                                        | keyset-paged reads over `sessions`                                        | none (WAL reader)                                |
| `session(&SessionRef, SessionQuery)`             | none                                        | every table for one session, on one snapshot                              | none (one deferred read transaction)             |
| `changes_since(Watermark, ChangeQuery)`          | none                                        | one indexed revision-range read per kind per page, plus tombstones        | none (one read snapshot per page)                |
| `head_revision() -> Watermark`                   | none                                        | one read of the feed head                                                 | none                                             |
| `Source::capabilities() -> SourceCapabilities`   | none                                        | none — static                                                             | none                                             |

### `open`

```rust
pub struct StoreOptions {           // #[non_exhaustive]
    pub db_path: Option<PathBuf>,
    pub home: Option<PathBuf>,
    pub roots: Option<ProviderRoots>,
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
| `home` | Also the directory the providers are rooted at when `roots` is `None`: `<home>/.claude`, `<home>/.codex`, `<home>/.grok`, `<home>/.local/share/opencode/`. `None` means the process `HOME`. Ignored when `roots` is set. |
| `roots: Some(ProviderRoots)` | Exactly where each provider keeps its sessions, resolved by the caller. |
| `read_only` | Open without creating or migrating. `sync`, `hydrate` and `watch` are `Error::UnsupportedOperation`. |

**Provider roots.** `ProviderRoots::from_env(home)` is the CLI's resolution —
`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GROK_HOME`, `OPENCODE_DB`,
`OPENCODE_STORAGE_DIR` and `TRAJECTORY_ROOT`, each replacing one provider's
root when set to a non-empty value, read once at construction and stored — and
is what `roots: None` derives from `home`. `ProviderRoots::from_home(home,
opencode_db)` reads nothing from the environment at all, and is what a test or
an embedder with its own layout passes: an embedder that stages its own `HOME`
and runs under a shell exporting `CODEX_HOME` would otherwise sweep the user's
real rollouts into its store. Whichever way they are resolved, the store
resolves them **once** at `open` (`SessionStore::roots()`), and `sync`,
`hydrate`, `watch` and `SourceCapabilities::watch_roots` all read that one
value, so a session the sweep catalogued is always hydrated from the same tree.
Nothing on those paths reads the environment afterwards.

**Schema and read-only opens.** A **writable** open migrates the database to
the shape this crate version reads. A **read-only** open cannot, so it checks
the catalog, marker, relationship, per-request usage and change-feed schema and
refuses at `open` with `Error::StaleSchema` (`DATABASE_STALE_SCHEMA`), naming
the remedy — open it writable once, or run a sync — rather than handing back a
handle whose first read dies inside a query. `Error::is_stale_schema()` is the
predicate a caller that *may* write uses to decide to reopen writable; it is
the wrong answer to every other failure, which is why it is its own variant. A
read-only handle truly adds no writer to the machine — the integration the ADR
prefers when freshness is somebody else's job. Migrations are forward-only: a
database written by a newer crate is not guaranteed readable by an older one.

### `sync`

The sweep `ai-hist sync` runs: every local provider, the stat-only source
fingerprint fast path unless `SyncOptions::force`, shallow discovery at the end.
It takes the same `SyncRunLock` (an exclusive advisory lock on
`<db>.sync.lock`) the CLI, the napi addon and every plugin take. When another
process holds it, `sync` waits up to `SyncOptions::lock_timeout_ms`, re-trying
every 100 ms, and then returns `Error::SyncLocked { path, waited_ms }`. The
default timeout is `0`: one try; a budget above seven days is treated as seven
days, the same ceiling the watch intervals have. **It is never a silent no-op**; a caller that
asked for a sweep and got none is told.

`SyncReport { swept, changed, head_revision }`: `swept` is false when the fingerprint matched
and nothing was opened. `changed` lists the `SessionRef`s whose catalog row was
created or changed while the call held the lock — swept or not — derived from
a per-row digest of the `sessions` table taken after the lock was acquired and
again before it was released, not from the provider walk. A row another
process wrote while this call was still waiting for the lock is not counted;
another sync cannot land inside the window (it needs the same lock); a
hydration writes the catalog outside it, so one that lands inside the window is
included even when the sweep itself opened nothing. `watch` reports a
hydration between ticks once, on the next tick; per-row attribution to one
writer is what the change feed's `revision` stamp is for. `head_revision` is
the feed head after the sweep, which a consumer compares against its stored
watermark before resuming. Every catalog
column takes part except the two bounded text excerpts (`first_prompt`,
`last_assistant_text`): a new session, new activity, a moved source stamp or
discovery state, a re-resolved or inherited `project_key`, a metadata field the
shallow read filled in. Once every catalog write stamps a revision (the change
feed's `revision`), the digest can become a read of that one column.

### `hydrate`

`SessionRef::Id { source, session_id }` hydrates a catalogued session the way
`ai-hist hydrate` does (a session never discovered is `Error::SessionNotFound`).
`SessionRef::Path { source, path }` is the hook fast path: the transcript is
read by locator before any catalog row exists, and it is accepted only for
sources whose `SourceCapabilities::hydrates_by_path` is true (Claude Code
today); the rest answer `Error::HydrationUnsupported`. `session()` applies the
same rule to a path reference: a path names one session only where the
provider keeps one session per file, and OpenCode's rows all carry the provider
database as their locator, so a lookup by it would answer with an arbitrary
session rather than the one meant. `HydrateOptions {
include_related }` (default `true`) also hydrates the bounded related
transcripts beside the session — Claude subagent sidecars, Codex child rollouts
— and never walks the rest of the provider root.

`HydrateReport` carries the resolved `session`, a `HydrateStatus`
(`Hydrated` for a first ingestion, `Updated` when a changed source was read on
top of an existing checkpoint, `Unchanged`, `CapabilityLimited`, and for the
path form `Missing`, `Unidentified`, `Mismatched`), the engine's `Capability`
classification (`Full`, `Partial`, `ShallowOnly`), the `coverage` kinds — the
source's `SourceCapabilities::evidence_kinds`, the same set `session()` reports,
narrowed by the request (no `Relationship` when `include_related` is off) —
`related` sessions, `bytes_read` and diagnostics. A missing transcript is a status, not an error: a
hook fires inside the harness's tool call, and a file that was cleaned up
before the hook ran is an answer.

### `watch`

The `ai-hist watch` loop on its own thread, as an iterator of `TickReport`s.
Filesystem events over the providers' roots drive it when the crate is built
with the `fs-events` feature and `WatchOptions::use_fs_events` is on; it polls
at `poll_interval_ms` otherwise, with a `slow_poll_ms` backstop either way. A
tick is the same locked `sync`; one that finds the lock held reports
`contended` and is retried by the loop rather than counted as done. A failed
sweep arrives as an `Err` and the loop keeps running; the rolling catalog
baseline survives it, so rows a failed sweep had already committed are
reported by the next tick that succeeds rather than lost. `WatchHandle::stopper()`
hands another thread a `WatchStop`; iteration ends once the loop has stopped
and every reported tick has been read, and dropping the handle stops it.
`next_timeout(timeout)` waits at most `timeout` for a tick, never past a short
deadline, and a `timeout` too large to name an instant simply waits without
one.

### `sessions`

The catalog, newest first, paged internally on
`(last_activity_ms DESC, source ASC, session_id ASC)` so a page boundary inside
one millisecond neither drops nor repeats a row. Every page is read on one
SQLite snapshot, taken at the first row and held until the iterator is dropped
— the order key is `last_activity_ms`, which a concurrent sync moves, and pages
on separate snapshots would skip or repeat a session that moved across the
cursor. A WAL reader blocks no writer but pins the WAL while it lives, so drain
or drop the iterator promptly. `CatalogQuery { scope,
sources, project_key, before_ms, page_size }` — `sources: None` is every
source, `Some(vec![])` an allowlist that admits none and yields no rows. `CatalogSession` is the typed
catalog row — `source: Source`, `project_key`, `discovery_state`, the
provider-observed metadata — and `session_ref()` turns it into the reference
`session` takes.

### `session`

Everything the store holds about one session, on one SQLite snapshot:

```rust
pub struct SessionEvidence {
    pub session: CatalogSession,
    pub prompts: Vec<Prompt>,                 // history rows; every source
    pub messages: Vec<Message>,               // one per message_id, with typed blocks
    pub tool_calls: Vec<ToolCall>,            // args: Option<serde_json::Value>
    pub tool_results: Vec<ToolResult>,        // payload_bytes / hash / status / event_source …
    pub file_edits: Vec<FileEdit>,            // structured_patch: Option<Value>
    pub markers: Vec<Marker>,                 // compaction, summary, system rows, lifecycle
    pub relationships: Vec<Relationship>,     // delegated | materialized_local | fork | resume | continuation
    pub requests: Vec<SessionRequest>,        // one per API request, usage normalized
    pub usage: Option<SessionUsageSummary>,   // whole-session rollup
    pub user_turns: Vec<SessionUserTurn>,     // human turns with per-block byte accounting
    pub coverage: Vec<EvidenceKind>,          // what the source's parser can produce
    pub loaded: Vec<EvidenceKind>,            // coverage ∩ the query's kinds, in coverage order
    pub include_text: bool,
    pub diagnostics: Vec<Diagnostic>,
}
```

`SessionQuery { include_text, kinds }`. `include_text: false` is burn's
hash-only / off content mode: every transcript string is `None`, byte lengths
(`text_bytes`, `payload_bytes`, `prompt_bytes`) and hashes stay, and none of
the text columns — event text, prompt bodies, marker text, the catalog's
`first_prompt` / `last_assistant_text` excerpts — is moved out of SQLite at
all; `Prompt::prompt_hash` is then the ledger's stored hash (`None` only for a
row written without one). `kinds` skips the tables a consumer does not need:
`SessionEvent` loads messages, tool results, user turns, requests and the usage
summary together; `History` the prompts; the other kinds their own table. What
is read is `coverage ∩ kinds`, reported back as `loaded`, so a kind the source
cannot produce is never fetched and never listed. `CommitLink` is not carried
by `session()`.

A `Block` carries `control: Option<ControlKind>`: why a block in the user role
is not a human prompt — a slash-command caveat, invocation or output, a task
notification, hook output, bash pass-through, a `<system-reminder>`, a Codex
context wrapper, a meta record, a bare resume marker. It is `None` on a genuine
prompt and on every model-output block, and the vocabulary is closed and
validated where evidence enters the store, so a stored spelling always parses.
`prompts` and `user_turns` already exclude control rows; `messages` reports
them classified rather than dropping them.

Every value type the facade returns — `SessionEvidence` and its parts,
`CatalogSession`, `SyncReport`, `HydrateReport`, `TickReport`, the option
structs, `SourceCapabilities`, `Error` — is `#[non_exhaustive]`, `Clone`,
`Serialize`, `Deserialize` and `PartialEq`, so a consumer can persist and
round-trip it. `CatalogIter` and `WatchHandle` are deliberately not value
types: one holds a read snapshot, the other a running thread, and neither is
cloned or serialized. JSON columns arrive
parsed (`ToolCall::args`, `FileEdit::structured_patch`, `Marker::payload`,
`Message::usage` as `NormalizedUsage`); the stored string is reachable through
`raw_args()`, `raw_structured_patch()`, `raw_payload()` and `raw_usage()`, and
is never a public field.

Two facts about identity a consumer must not paper over:

- A `Message` is one ledger `message_id`, which is the *record* identity.
  Claude writes one API response as several records with distinct `uuid`s
  sharing `provider_message_id` and `request_id`; those are several `Message`s
  and **one** entry in `requests`. Sum `requests`, not `messages`.
- `SourceCapabilities::message_ids` says whether a source's ids are the
  provider's (`Provider`: claude, opencode), synthesized from record position
  (`Synthesized`: codex, grok), `Mixed` (cursor) or `None` (relay,
  trajectory).

### `changes_since`

The revision-stamped change feed: every row of `sessions`, `session_events`,
`tool_calls`, `file_edits`, `session_markers` and `session_relationships`
carries a `revision` drawn from the database-wide `observation_clock` and
stamped by a trigger on every insert and update, so no write site can forget
one; a deleted row leaves a tombstone at its own revision, which a later insert
of the same key clears. `changes_since(from, ChangeQuery)` drains
`Change { kind, source, session_id, record_key, revision, op }` in
`(revision, kind, record_key)` order, bounded to the head at open, in
`batch`-sized indexed reads of at most `MAX_CHANGE_BATCH`; `op` is
`Upsert(EvidenceRow)` — the typed row, so no second read is needed — or
`Delete`. A re-seen `record_key` is a replace, never a duplicate.

`from` is `Watermark::START` to replay everything, an explicit watermark to
resume from one a consumer stored itself, or `Watermark::CONSUMER` with
`ChangeQuery::consumer` to resume from a named cursor kept in `consumer_cursors`
inside the store. The cursor moves only on `Changes::commit()` and only
forward, so a drain that fails mid-page re-reads rather than skips, and a stale
commit cannot rewind it. A named cursor is bound to the kind set it was first
committed for: draining or committing it under another filter is
`Error::ConsumerKindsMismatch`. A watermark past the head is
`Error::WatermarkAheadOfStore` — the database was reset or replaced, and the
only recovery is a resync from `Watermark::START`; a named cursor past the head
names no revision of this store, so that resync's commit replaces it.
`head_revision()` reports
the head on its own, and `SyncReport::head_revision` reports it after a sweep.
A read-only handle drains the feed but cannot commit a cursor.

### `Source::capabilities()`

Static, per source: `evidence_kinds` (the parser's ceiling — a kind absent here
is one the source never reports; a kind present with no rows means the session
has none; relay and trajectory, which shallow discovery exempts, still declare
`History` because their sweeps write prompt rows), `relationships` (`RelationshipCapabilities`: `always` / `sometimes`
/ `never` stable child identity and which delegation facts are recorded),
`usage_accounting` (`per-request`, `per-message`, `cumulative-delta`,
`context-proxy`, or `None`), `message_ids`, `hydrates_by_path`, and
`watch_roots(&roots)` — every path the watcher covers for that source under a
`ProviderRoots`, as `Vec<WatchedPath { path, scope }>` from the same builder
`watch` registers with: the transcript tree, and for Claude and Codex the flat
`history.jsonl` prompt log as a `File` scope (register its parent, filter to
the one name — never watch the parent as a tree).

## Errors

One `#[non_exhaustive] enum Error`. `Error::code()` is the stable
`SCREAMING_SNAKE_CASE` code, identical to the TypeScript SDK's native error
codes where both sides have the failure; `Display` renders `CODE: message`.

| Variant                    | Code                         | When                                                                   |
| -------------------------- | ---------------------------- | ---------------------------------------------------------------------- |
| `DatabaseOpen`             | `DATABASE_OPEN_FAILED`       | cannot open, create or migrate                                         |
| `StaleSchema`              | `DATABASE_STALE_SCHEMA`      | read-only open of a database older than the shape this version reads   |
| `InvalidArgument`          | `INVALID_ARGUMENT`           | a caller value was rejected before anything was read                   |
| `UnsupportedOperation`     | `UNSUPPORTED_OPERATION`      | `sync` / `hydrate` / `watch` on a read-only handle                     |
| `SessionNotFound`          | `SESSION_NOT_FOUND`          | `hydrate` of a session the catalog does not hold                       |
| `SessionSourceUnavailable` | `SESSION_SOURCE_UNAVAILABLE` | catalogued, but the provider source is gone                            |
| `SourceMismatch`           | `SESSION_SOURCE_MISMATCH`    | the provider data and the claimed session disagree                     |
| `HydrationUnsupported`     | `HYDRATION_UNSUPPORTED`      | no full-evidence path for the source, or `Path` on a hookless source   |
| `HydrationFailed`          | `HYDRATION_FAILED`           | hydration failed with no narrower code                                 |
| `ConnectorNotConfigured`   | `CONNECTOR_NOT_CONFIGURED`   | remote scope with no connector                                         |
| `AuthenticationExpired`    | `AUTHENTICATION_EXPIRED`     | a remote connector's credentials lapsed                                |
| `EvidencePartial`          | `EVIDENCE_PARTIAL`           | a connector returned less than it declared                             |
| `ConnectorFailure`         | `CONNECTOR_FAILURE`          | a remote connector failed outright                                     |
| `Query`                    | `QUERY_FAILED`               | a read failed inside SQLite                                            |
| `Discovery`                | `DISCOVERY_FAILED`           | shallow discovery failed during a sweep                                |
| `SyncFailed`               | `SYNC_FAILED`                | a sweep failed with no narrower code                                   |
| `SyncLocked`               | `SYNC_LOCKED`                | another process holds the `SyncRunLock` past the caller's timeout      |
| `WatermarkAheadOfStore`    | `WATERMARK_AHEAD_OF_STORE`   | a `changes_since` watermark names a revision the store has not reached  |
| `ConsumerKindsMismatch`    | `CONSUMER_KINDS_MISMATCH`    | a named cursor was drained under a different kind set than it holds     |

The four Node-only classes (`UNSUPPORTED_PLATFORM`, `NATIVE_PACKAGE_MISSING`,
`NATIVE_LOAD_FAILED`, `NATIVE_CONTRACT_MISMATCH`) have no Rust counterpart.

## The evidence model

`SessionEvidence` above is the shape of a whole session; this section is one
per struct whose fields are worth spelling out. Field lists are the published
ones; `#[non_exhaustive]` structs can gain fields in a minor release, so
destructure them by name, never positionally.

### `SessionUserTurn` and `SessionUserTurnBlock`

`SessionEvidence::user_turns`: one human-side message and the ordered blocks it
carried. Control rows are not turns — a Codex context wrapper, a task
notification or a split `<system-reminder>` is classified (`Block::control`)
and left out here.

| Field | Meaning |
| --- | --- |
| `id` | Row id of the turn's first event |
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

### `Marker`

`SessionEvidence::markers`: a provider record the normalized event model cannot
carry, kept rather than dropped — compaction and summary boundaries, provider
`system` rows, non-text content blocks (`image`, `document`,
`redacted_thinking`, thinking signatures), tool-replacement metadata, Codex
lifecycle events, and the folded slash-command triad.

| Field | Meaning |
| --- | --- |
| `id`, `marker_uid` | Row id and the stable per-session marker identity |
| `ts_ms` | `Option` — some markers are undated |
| `message_id`, `parent_id`, `turn_id` | Where in the conversation it sits, as far as the provider said |
| `kind` | The parser's classified vocabulary — `compaction_boundary`, `system`, `synthetic_turn`, `encrypted_reasoning`, `slash_command`, `unknown`, … — stable per source once written |
| `subkind` | The provider-native type, verbatim — so a record no classifier knows still lands with its real name |
| `text` | The provider's own readable text, when it wrote one. `None` under `include_text: false` |
| `payload` | `Option<serde_json::Value>`: an allowlisted, bounded projection, parsed — strings cut at 128 characters, containers at 32 entries, recursively. Never the bytes of an image. `raw_payload()` is the stored string |

A compaction marker is where a session's token baseline resets. Cost attribution
across one without it is wrong, which is why the table exists. A slash command's
caveat, invocation and output records are folded into one `slash_command` marker
whose payload carries `command_name`, `command_message`, `command_args`,
`command_mode`, `origin_kind`, the three rows' event uids and `stdout_bytes` —
the whole of what an activity classifier reads, with no raw JSON.

### `SessionRequest`

`SessionEvidence::requests`: one model request, as grouped by the facade from
the provider's several rows.

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

`SessionEvidence::usage`: the whole-session rollup. It is `None` when the
session has **no requests at all**; a session whose requests exist but whose
usage could not be established is `Some` with `usage: None` and diagnostics.
The two are different answers and are kept apart.

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

### Row shapes exported beside the facade

`HistoryEntry`, `SessionEvent`, `SessionToolCall`, `SessionFileEdit`,
`ShallowSession`, `SessionRelationship`, `SessionLocation` and `SessionScope`
are re-exported on the default features because the change feed's
`EvidenceRow` carries them: a `Change` hands back the typed row it is about,
so a consumer needs no second read. `session()` returns the facade's own
structs (`Prompt`, `Message` and its `Block`s, `ToolCall`, `ToolResult`,
`FileEdit`, `Marker`, `Relationship`) — those are the read-side shapes, with
JSON columns parsed. `CommitLink` is carried by neither.

### What each source populates

Generated over `Source::ALL` from what each provider adapter declares
(`ai_hist::declared_evidence_kinds`) and the crate's accounting table
(`ai_hist::source_accounting`); `crates/ai-hist/tests/sourcing_sdk_doc.rs`
fails when this table and the code disagree, and `ALL` is checked
exhaustively against the enum and the ledger's source registry inside the
crate, so a new source cannot be added without this table gaining a row. A `—` means the source *cannot*
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
| *(none)* | ✓ | `SessionStore` and its nine operations, the change feed (`Change`, `ChangeQuery`, `Watermark`, `EvidenceRow`), `Source` and `SourceCapabilities`, `Error`, the evidence structs above, `NormalizedUsage` and the usage normalizers, `project_identity`, `declared_evidence_kinds` | Embedders |
| `fs-events` | — | The `notify` backend behind `watch`; without it `watch` polls at `poll_interval_ms`. `WatchOptions::use_fs_events` selects it when it is compiled in | The CLI, and an embedder that wants event-driven ticks |
| `delivery` | — | Durable delivery of captured evidence to a destination | The CLI, napi, the relayhistory plugin |
| `opencode-backup` | — | Snapshot a live OpenCode SQLite store through `rusqlite`'s backup API before reading it | The CLI, napi |
| `git-hooks` | — | Git helpers and hook installation (`url`) | The CLI, napi |
| `unstable-internal` | — | Every workspace module, public: raw-connection APIs, the parsers, discovery, search, statistics, remote connectors, the connection-level sync and watch entry points. **Not covered by semver.** | This workspace and its plugins only |

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

## Tests that hold this surface

- `crates/ai-hist/tests/sourcing_api.rs` builds on the crate's **default**
  features — no `unstable-internal`, no raw connection — and takes a corpus of
  fixtures through `open` → `sync` → `sessions` → `session`, comparing the
  `SessionEvidence` field for field with the parser characterization snapshot
  `tests/fixture_corpus.rs` commits for the same fixture, then round-trips it
  through `serde_json`. It also holds the lock (`SyncLocked` within the
  documented wait), the watch loop (startup tick, a session appearing between
  polls), hydration by id and by path, and the hash-only read.
- `crates/ai-hist/tests/public_marker_reads.rs`, likewise on default features.
- `crates/ai-hist/tests/sourcing_sdk_doc.rs` regenerates the population table
  above from `declared_evidence_kinds` and `source_accounting` and fails when
  this document and the crate disagree.
- `crates/ai-hist/public-api.txt` and `scripts/check-public-api.mjs` (the
  `public-api` CI job) hold the whole default-feature listing; see
  [Versioning](#versioning-cargo-semver-is-the-contract).
- `examples/rust-consumer` is the same surface exercised from outside the
  workspace, against the published crate nightly.

## Appendix: who still needs `unstable-internal`, and why

The facade is the whole default surface. The workspace's own binaries and the
plugin crates enable `unstable-internal`, which re-exports the
connection-level modules, for the reasons below. Nothing outside this
repository should — each entry is a facade gap or a deliberate non-goal.

| Consumer | What it reaches for | Why the default surface does not cover it |
| --- | --- | --- |
| `crates/ai-hist-cli` | `open_db`, `open_db_readonly`, `init_db` | Every subcommand holds a raw connection; the CLI predates the facade |
| | `search`, `search_all`, `history_search::*`, `recent`, `sessions`, `session`, `session_events`, `session_tool_calls`, `session_file_edits`, `QueryFilter`, `ProjectGrouping`, statistics | Search, catalog grouping and statistics are product surface, not sourcing surface; unbounded reads are not offered to embedders |
| | `insert_history`, `prompt_hash`, `HistoryEntry`, `import_json` | The hook fast path and `ai-hist import` *write* prompt rows; the facade is read-side plus `sync`/`hydrate` |
| | `sync_local_at_cancellable`, `prepare_local_sync_snapshot`, `watch::*` | Cancellation and progress output around the same engine paths `sync` and `watch` wrap |
| | `discover`, `diagnostics::doctor_report`, `paths::*`, `git_helpers::*`, tags, `resume_command` | Operator tooling: doctor, discovery diagnostics, resume commands, tagging |
| `crates/ai-hist-napi` | `open_db*`, `schema_is_*_read_current`, `default_db_path` | Node holds one long-lived connection per addon and answers a schema mismatch by reopening writable |
| | connection-taking `session_*_page`, `session_relationships`, `session_tree`, `session_children_page`, `stats_scoped_by`, `search`, `recent`, `session_locations` | The TypeScript SDK exposes relationships, trees, statistics and search the Rust facade does not; typed facade exposure to TypeScript is [#181](https://github.com/AgentWorkforce/relayhistory/issues/181) |
| | `SESSION_*_CONTRACT_VERSION` constants, `delivery` | The native contract is versioned separately from Cargo semver; delivery is the CLI's worker |
| `plugins/relayhistory/rust` | `discover::{DiscoveryEnv, ShallowSessionProvider, …}`, `list_session_catalog`, `CatalogListOptions`, `SOURCE_CHOICES`, `init_db`, `prompt_hash`, `HistoryEntry` | It *is* a provider adapter (Agent Relay), a delivery worker and a catalog reader; adapters are the producer side of the store |
| `plugins/provider-sources/rust` | `discover::DiscoveryEnv`, `ShallowSessionProvider`, `sources::NormalizedSourceEvidence`, `observations::SessionObservation`, `EvidenceKind`, `SOURCE_CHOICES` | Remote connectors supply normalized evidence into the store; the intake contract is internal |

The rule that follows: a new in-tree call site that reaches past the facade
must be able to say which row of this table it belongs to, or it is a facade
gap and the facade grows instead. A consumer outside this repository that
finds itself reaching for `unstable-internal` has found a gap in this
document; open an issue against the facade rather than depending on the
feature.
