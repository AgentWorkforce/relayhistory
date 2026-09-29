# SQLite writers stay direct; no append-only spool layer

- **Status:** Accepted
- **Date:** 2026-09-28
- **Issue:** [#47](https://github.com/AgentWorkforce/relayhistory/issues/47)
- **Related:** [#44](https://github.com/AgentWorkforce/relayhistory/issues/44),
  [#45](https://github.com/AgentWorkforce/relayhistory/issues/45),
  [#46](https://github.com/AgentWorkforce/relayhistory/issues/46),
  [#48–#52](https://github.com/AgentWorkforce/relayhistory/issues/50),
  [2026-09-19 ADR, store shape](2026-09-19-relayhistory-owns-session-sourcing.md#store-shape-one-writer-implementation-not-one-writer-process),
  [SQLite contention incident verdict](../sqlite-contention-incident-2026-08-06.md)
- **Supersedes:** nothing. Answers the architecture question #47 left open.

## Context

#47 proposed that only `ai-hist sync` open `ai-history.db` read-write, and that
every other producer append to its own JSONL spool for sync to ingest by byte
offset. SQLite WAL permits one writer, the write lock belongs to a process, and
a process stopped mid-transaction (state `T`) holds it until it resumes, so no
timeout or retry survives it.

When #47 was filed, the producers it named wrote SQLite in ways only a spool
could have tamed:

- **Agent Relay brokers** called the napi `syncAndPush()` aggregator from a
  timer in every broker process: a full scan plus a cloud push, many times per
  machine.
- **The MCP server's tag writes** loaded the whole database into a sql.js
  in-memory snapshot and exported the entire file back, which is unsafe beside
  any WAL writer regardless of locking.

Neither exists on `main` any more:

- `syncAndPush`, `sync_and_push` and every cloud upload path were removed
  (d4d97af, 6b25d47); uploads belong to AgentWorkforce/relay-desktop. The napi
  addon now exposes an explicit `sync()` that a host calls, not a timer in
  each broker.
- sql.js is gone from the SDK and is forbidden by
  `sdk-ts/src/architecture.test.ts`. The MCP server's tools are reads plus the
  explicit `sync`, `discover_sessions` and `hydrate_session`, all of which call
  into the Rust crate.

Every write to `ai-history.db` now goes through the `ai-hist` crate, under the
lock discipline recorded in the 2026-09-19 ADR. Grouped by where their input
comes from, today's writers are:

| Writer | Entry points | Input | Replayable if the write fails? |
| --- | --- | --- | --- |
| Local ingest | `ai-hist sync`/`watch`, napi `sync()`, `SessionStore::sync`/`watch` | Provider transcripts and `history.jsonl` files on disk | Yes: the provider file is untouched and `.sync-state.json` only advances after commit |
| Hook ingest | `ai-hist ingest --hook <harness>` | The `transcript_path` the hook names, or a forced local sweep | Yes: same files, and the next hook or sync re-reads them |
| Discovery / hydration | `discover_sessions`, `hydrate_session`, `apply_source_observations`, `apply_source_evidence` | Provider catalogs, remote connectors, host-supplied evidence | Yes: rediscovery or rehydration reproduces it |
| Git links | `link_git_commit`, installed post-commit hook | The commit in the repository | Yes: re-linking the commit reproduces it |
| User-authored rows | `ai-hist tag`, `tag_session` | The command line | No, but the command is interactive and reports the failure |

The first four rows are the "append-only spool with a byte-offset cursor" #47
asked for: the provider's own files are the spool, `.sync-state.json` (and
source fingerprints) is the cursor, and `ai-history.db` is a derived index that
can always be rebuilt from them. A stopped writer delays that index; it does
not lose evidence, because nothing is consumed from the source when a write
fails.

The incident verdict of 2026-08-06 also removed the other half of #47's
motivation. The reported fleet freeze was a Relaycast inventory timestamp, not
a SQLite stop, and the causal direction supported by the evidence is "external
stop → possibly frozen transaction", never the reverse. The stopped brokers of
#50 held a connection because every broker ran `syncAndPush` in-process, which
is exactly the pattern that no longer exists.

## Decision

1. **No spool layer.** RelayHistory does not add per-producer JSONL spools or
   a single-writer ingestion service in front of `ai-history.db`. Writers keep
   opening the database directly, through the crate's code paths only.
2. **The provider file is the spool.** Every new ingestion path must read from
   a durable source it does not consume (a provider file, a provider API, a
   git object) and advance its cursor only after the SQLite commit. A path
   whose evidence exists only in memory at the time of the write is a design
   smell that needs its own decision, not a direct `INSERT`.
3. **Contain writers instead of serializing them.**
   - Hosts write only when they are the component responsible for freshness;
     everything else opens `read_only: true` (2026-09-19 ADR).
   - Transactions stay short and bounded (JSONL ingestion commits in batches of
     at most 2,000 lines), so a stopped process holds the lock only if it is
     stopped inside one.
   - `SQLITE_BUSY` is retried with bounded, jittered backoff
     (`configure_busy_retry`, `crates/ai-hist/src/store.rs`) and then surfaced
     as an error, not swallowed. `SQLITE_LOCKED` is reported, not replayed.
   - The whole-run `SyncRunLock` means concurrent syncs skip rather than queue.
   - A failed source does not stop later sources, and cursors of successful
     sources are kept (#48).
   - `ai-hist doctor` and contention errors probe whether a writer can start
     now and name stopped or zombie holders (#52,
     `crates/ai-hist/src/diagnostics.rs`).
4. **Periodic in-process capture loops are not a supported integration.** A
   long-lived host (a broker, an editor extension, a desktop app) that wants
   fresh history calls `sync` explicitly or runs `ai-hist watch` as its own
   process. It does not run a scan on a timer inside every one of its
   processes; that multiplication of writers is what produced #50.

## Escalation triggers

Revisit this decision, starting from the rejected spool design below, if any of
these is observed on a supported configuration:

- A sync or hook ingest fails with `SQLITE_BUSY` after the retry budget and
  `ai-hist doctor` names a *stopped* holder that is one of our writers.
- A writer is introduced whose input is not replayable (row 5 of the table
  grows beyond interactive commands, for example an unattended producer of
  user-authored annotations).
- A consumer needs more than one long-lived writer process per machine as a
  normal operating mode, rather than as an occasional overlap.

## Alternatives considered

### Per-producer append-only JSONL spools, one read-write `ai-hist sync` (#47)

Rejected for now. It would be the right shape if producers held evidence that
existed only in their memory. They do not: every remaining producer is an
ingester of files or APIs that are already durable and replayable. A spool in
front of them would copy evidence the provider already keeps, and would add:

- a versioned spool record schema for each write kind, including user tags and
  host-supplied `apply_source_evidence` payloads;
- atomic-append rules across platforms (appends above `PIPE_BUF` are not
  atomic between writers without a lock, which reintroduces a lock);
- per-producer cursors, acknowledgement, and quarantine of malformed records;
- an always-running consumer. Without one, `ai-hist ingest --hook` and SDK
  `sync()` would stop making data visible until some other process ran sync,
  regressing "works without a separate service";
- a second place for evidence to be lost if the spool and the database
  disagree.

The one failure it removes — a stopped writer blocking other writers — is
already reduced to a delay with a diagnosable holder, and the configuration
that made it frequent (a capture timer in every broker) is gone.

### A single long-lived writer daemon with an IPC API

Rejected. Same availability problem as the spool consumer, plus a process
supervision surface on three operating systems, and a stopped daemon blocks
every writer just as a stopped sync does.

### Rollback-journal mode, or a lock lease file with forced takeover

Rejected. Rollback journal mode blocks readers during writes, which undoes #46.
SQLite has no supported way to break another process's write lock; a lease that
deletes or rewrites lock state while the owner is stopped risks corrupting the
database when the owner resumes.

## Consequences

- #47 is answered by this record. No spool code is added.
- New writer paths are reviewed against decision 2: durable, replayable input
  and cursor-after-commit. A PR adding a writer states which row of the table
  above it belongs to.
- The 2026-09-19 ADR's "Relationship to #47" section and the incident verdict's
  "Deferred architecture" section point here.
- The residual risk is stated rather than hidden: a writer stopped *inside* a
  write transaction still blocks other writers until it resumes or exits.
  Readers are unaffected, evidence is not lost, and `ai-hist doctor` names the
  stopped holder.
