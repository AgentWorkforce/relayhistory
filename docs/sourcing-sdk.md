# Sourcing SDK — `SessionStore`

The Rust entry point for reading coding-agent session evidence out of
RelayHistory. Everything a consumer needs is one type, `ai_hist::SessionStore`,
seven operations, and the typed structs they return. Nothing on this surface
names a `rusqlite` type, and no JSON column reaches a consumer as a string.

This is the surface [`docs/sourcing-contract.md`](sourcing-contract.md) is
delivered through and the ADR
[relayhistory owns session sourcing](decisions/2026-09-19-relayhistory-owns-session-sourcing.md)
decided on. **Cargo semver is the contract**: there is no Rust
contract-version constant, and any change to a public field, variant or
observable behaviour is a minor bump pre-1.0 with a `### Rust API` entry in
`CHANGELOG.md` (see [`docs/releasing.md`](releasing.md)).

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

## The seven operations

| Method                                           | Provider I/O                                | Database work                                                             | Lock                                             |
| ------------------------------------------------ | ------------------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------ |
| `SessionStore::open(StoreOptions)`               | none                                        | creates and migrates the schema (writable); checks it (read-only)         | none                                             |
| `sync(SyncOptions)`                              | full local sweep, fingerprint-gated         | migrations + ingestion + shallow discovery                                | `SyncRunLock`, whole run                         |
| `hydrate(&SessionRef, HydrateOptions)`           | one session and its bounded related files   | transactional evidence + checkpoint upsert                                | per-session hydration lock                       |
| `watch(WatchOptions) -> WatchHandle`             | fs-event or polling driven sweeps           | the same as `sync`, per tick                                              | `SyncRunLock`, per tick                          |
| `sessions(CatalogQuery) -> CatalogIter`          | none                                        | keyset-paged reads over `sessions`                                        | none (WAL reader)                                |
| `session(&SessionRef, SessionQuery)`             | none                                        | every table for one session, on one snapshot                              | none (one deferred read transaction)             |
| `Source::capabilities() -> SourceCapabilities`   | none                                        | none — static                                                             | none                                             |

`changes_since(Watermark)` — the change feed with named consumer cursors — is
[#179](https://github.com/AgentWorkforce/relayhistory/issues/179) and is not on
the surface yet. `Error::WatermarkAheadOfStore` is reserved for it.

### `open`

`StoreOptions { db_path, home, read_only }`. `db_path` defaults to
`$AI_HIST_DB`, then the XDG data path, then `<home>/.local/share/ai-hist/ai-history.db`
when `home` is set. `home` replaces the process `HOME` as the provider root;
`CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GROK_HOME` and `OPENCODE_DB` are still
honoured, exactly as the CLI honours them.

A **writable** open migrates the database. A **read-only** open cannot, so it
checks the schema and returns `Error::DatabaseOpen` naming the remedy (open it
writable once, or run a sync) instead of handing back a handle whose first read
dies inside a query. A read-only handle refuses `sync`, `hydrate` and `watch`
with `Error::UnsupportedOperation`, and truly adds no writer to the machine —
the integration the ADR prefers when freshness is somebody else's job.

### `sync`

The sweep `ai-hist sync` runs: every local provider, the stat-only source
fingerprint fast path unless `SyncOptions::force`, shallow discovery at the end.
It takes the same `SyncRunLock` (an exclusive advisory lock on
`<db>.sync.lock`) the CLI, the napi addon and every plugin take. When another
process holds it, `sync` waits up to `SyncOptions::lock_timeout_ms`, re-trying
every 100 ms, and then returns `Error::SyncLocked { path, waited_ms }`. The
default timeout is `0`: one try. **It is never a silent no-op**; a caller that
asked for a sweep and got none is told.

`SyncReport { swept, changed }`: `swept` is false when the fingerprint matched
and nothing was opened. `changed` lists the `SessionRef`s whose catalog row was
created or changed by the sweep — new sessions, new activity, a moved discovery
state or source stamp — derived from the `sessions` table before and after, not
from the provider walk.

### `hydrate`

`SessionRef::Id { source, session_id }` hydrates a catalogued session the way
`ai-hist hydrate` does (a session never discovered is `Error::SessionNotFound`).
`SessionRef::Path { source, path }` is the hook fast path: the transcript is
read by locator before any catalog row exists, and it is accepted only for
sources whose `SourceCapabilities::hydrates_by_path` is true (Claude Code
today); the rest answer `Error::HydrationUnsupported`. `HydrateOptions {
include_related }` (default `true`) also hydrates the bounded related
transcripts beside the session — Claude subagent sidecars, Codex child rollouts
— and never walks the rest of the provider root.

`HydrateReport` carries the resolved `session`, a `HydrateStatus`
(`Hydrated`, `Unchanged`, `CapabilityLimited`, and for the path form
`Missing`, `Unidentified`, `Mismatched`), the declared `Capability`
(`Full`, `Partial`, `ShallowOnly`), the `coverage` kinds, `related` sessions,
`bytes_read` and diagnostics. A missing transcript is a status, not an error: a
hook fires inside the harness's tool call, and a file that was cleaned up
before the hook ran is an answer.

### `watch`

The `ai-hist watch` loop on its own thread, as an iterator of `TickReport`s.
Filesystem events over the providers' roots drive it when the crate is built
with the `fs-events` feature and `WatchOptions::use_fs_events` is on; it polls
at `poll_interval_ms` otherwise, with a `slow_poll_ms` backstop either way. A
tick is the same locked `sync`; one that finds the lock held reports
`contended` and is retried by the loop rather than counted as done. A failed
sweep arrives as an `Err` and the loop keeps running. `WatchHandle::stopper()`
hands another thread a `WatchStop`; iteration ends once the loop has stopped
and every reported tick has been read, and dropping the handle stops it.

### `sessions`

The catalog, newest first, paged internally on
`(last_activity_ms DESC, source ASC, session_id ASC)` so a page boundary inside
one millisecond neither drops nor repeats a row. `CatalogQuery { scope,
sources, project_key, before_ms, page_size }`. `CatalogSession` is the typed
catalog row — `source: Source`, `project_key`, `discovery_state`, the
provider-observed metadata — and `session_ref()` turns it into the reference
`session` takes.

### `session`

Everything the store holds about one session, on one SQLite snapshot:

```rust
pub struct SessionEvidence {
    pub session: CatalogSession,
    pub prompts: Vec<Prompt>,                 // history rows; every source
    pub messages: Vec<Message>,               // one per message_id, with blocks
    pub tool_calls: Vec<ToolCall>,            // args: Option<serde_json::Value>
    pub tool_results: Vec<ToolResult>,        // payload_bytes / hash / status / event_source …
    pub file_edits: Vec<FileEdit>,            // structured_patch: Option<Value>
    pub markers: Vec<Marker>,                 // compaction, summary, system rows, lifecycle
    pub relationships: Vec<Relationship>,     // delegated | materialized_local | fork | resume | continuation
    pub requests: Vec<SessionRequest>,        // one per API request, usage normalized
    pub usage: Option<SessionUsageSummary>,   // whole-session rollup
    pub user_turns: Vec<SessionUserTurn>,     // human turns with per-block byte accounting
    pub coverage: Vec<EvidenceKind>,          // what the source's parser can produce
    pub loaded: Vec<EvidenceKind>,            // what this read fetched
    pub include_text: bool,
    pub diagnostics: Vec<Diagnostic>,
}
```

`SessionQuery { include_text, kinds }`. `include_text: false` is burn's
hash-only / off content mode: every transcript string is `None`, byte lengths
(`text_bytes`, `payload_bytes`, `prompt_bytes`) and hashes stay, and the event
query does not move the `text` column out of SQLite at all. `kinds` skips the
tables a consumer does not need: `SessionEvent` loads messages, tool results,
user turns, requests and the usage summary together; `History` the prompts; the
other kinds their own table. `CommitLink` is not carried by `session()`.

Every struct is `#[non_exhaustive]`, `Clone`, `Serialize`, `Deserialize` and
`PartialEq`, so a consumer can persist and round-trip it. JSON columns arrive
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

### `Source::capabilities()`

Static, per source: `evidence_kinds` (the parser's ceiling — a kind absent here
is one the source never reports; a kind present with no rows means the session
has none), `relationships` (`RelationshipCapabilities`: `always` / `sometimes`
/ `never` stable child identity and which delegation facts are recorded),
`usage_accounting` (`per-request`, `per-message`, `cumulative-delta`,
`context-proxy`, or `None`), `message_ids`, `hydrates_by_path`, and
`watch_roots(home)` — the paths the watcher registers for that source under a
provider home.

## Errors

One `#[non_exhaustive] enum Error`. `Error::code()` is the stable
`SCREAMING_SNAKE_CASE` code, identical to the TypeScript SDK's native error
codes where both sides have the failure; `Display` renders `CODE: message`.

| Variant                    | Code                         | When                                                                   |
| -------------------------- | ---------------------------- | ---------------------------------------------------------------------- |
| `DatabaseOpen`             | `DATABASE_OPEN_FAILED`       | cannot open, create or migrate; read-only open of an older schema      |
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
| `WatermarkAheadOfStore`    | `WATERMARK_AHEAD_OF_STORE`   | reserved for `changes_since` (#179)                                    |

The four Node-only classes (`UNSUPPORTED_PLATFORM`, `NATIVE_PACKAGE_MISSING`,
`NATIVE_LOAD_FAILED`, `NATIVE_CONTRACT_MISMATCH`) have no Rust counterpart.

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
- The `cargo public-api` snapshot that fails CI on an unreviewed surface change
  is [#182](https://github.com/AgentWorkforce/relayhistory/issues/182).

## Appendix — who still uses `unstable-internal`

The facade is the whole default surface. The workspace's own binaries and the
plugin crates enable the `unstable-internal` feature, which re-exports the
connection-level modules, for the reasons below. Nothing outside this
repository should.

| Crate                             | Why it needs more than the facade                                                                                                                                                                                                                                                                    |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/ai-hist-cli`              | Every maintenance verb: FTS `search`/`recent`/`stats`, tags, `import`/`export`, `doctor`, `pack`/`resume`, git hooks, delivery, the remote-connector scopes, `sync`'s progress output, and the hook-ingest verb. `watch` and `sync` run the same engine paths the facade wraps.                        |
| `crates/ai-hist-napi`             | The Node binding's paged reads (`getSessionEventsPage`, `getSessionUserTurnsPage`, `getSessionRequestsPage`, …) take a raw connection per call; typed facade exposure to TypeScript is [#181](https://github.com/AgentWorkforce/relayhistory/issues/181).                                              |
| `plugins/provider-sources/rust`   | Implements `ShallowSessionProvider` / remote connectors, which are engine extension points, not consumer reads.                                                                                                                                                                                       |
| `plugins/relayhistory/rust`       | Delivery worker, observations, and `DiscoveryEnv`-level discovery — the producer side of the store.                                                                                                                                                                                                  |

A consumer that finds itself reaching for `unstable-internal` has found a gap
in this document; open an issue against the facade rather than depending on
the feature.
