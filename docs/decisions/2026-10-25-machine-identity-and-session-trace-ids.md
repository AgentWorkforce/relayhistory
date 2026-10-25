# Machine identity and per-session trace ids live in the history database

- **Status:** Proposed. The id strategy (A or B) and the name privacy mode
  wait on Khaliq's decision; the code ships B and the slug as marked interim
  defaults (`DEFAULT_MACHINE_ID_STRATEGY`, `DEFAULT_MACHINE_NAME_PRIVACY`).
- **Date:** 2026-10-25
- **Related:** Sessions Pipeline Redesign, workstream W2;
  [plan 009](../../plans/009-local-history-cloud-boundary.md)

## Context

The sessions pipeline lands raw session evidence in object storage under
`{prefix}/v1/ws={workspace}/m={machine_id}/h={harness}/d=.../s={session}/`
and traces every session end to end (ingest, deliver.put, storage.event,
transform, index.upsert, fetch). Two identities are needed before anything
leaves the machine: which machine a session came from, and one trace id per
session. Both must be available offline, cost nothing on the ingest hot path
(a new session is visible locally in under 5 ms), and survive restarts.

`history.db` already has a store identity, the change-feed epoch
(`delivery_state.origin_id`). That id is per database, not per machine, and
resets when a database is replaced, so it is not the machine id.

## Decision

### Machine identity

`machine_identity` is a single-row table written by the first writable open,
with `machine_aliases` beside it. `machine_name` is the canonical hostname slug:
lowercase `[a-z0-9-]`, `.local` stripped, first DNS label, apostrophes dropped,
63 characters at most. The hostname comes from `uname(2)` (`COMPUTERNAME` on
Windows). No process is spawned and nothing touches the network.

`machine_id` is computed once by `machine_id()`, the single function that
decides it, and is then frozen. The row records the strategy that produced
it, so changing the default never re-keys a machine that has already
uploaded. A rename updates `machine_name` and adds an alias; it never changes
the id. Two strategies are implemented and tested:

| | A: slug is the id (`macbook-pro`) | B: slug + install suffix (`macbook-pro-7f3a9c`) |
|---|---|---|
| Two machines named `macbook-pro` | **same id**: histories merge silently, and an upload can't be separated afterwards | distinct ids |
| Re-image or new disk, same hostname | same id, so the history continues | new id, seen as a new machine (aliases/UI can link them) |
| Database copied to a second machine | same id (also a collision) | same id (clone; needs detection, see below) |
| Readability in paths and UI | best | good: the slug stays readable |
| Rename | id frozen; name and alias updated | id frozen; name and alias updated |

B's suffix is the first 6 hex digits of a random install UUID (24 bits).
Among 1,000 machines that share one hostname, the chance that any two collide
is about 3%. With the 4-digit suffix from the design sketch it would be about
99.9%.

`MachineNamePrivacy::Hashed` replaces the slug with `h` plus 12 hex digits of
a SHA-256 of it. That keeps the hostname out of object keys, but hostnames are
guessable, so the hash is pseudonymity, not anonymity. It stays deterministic
so strategy A keeps its re-image property.

### Trace ids

`sessions.trace_id` is a UUIDv7 minted by an `AFTER INSERT` trigger when a
session row is first written. Every catalog upsert is `ON CONFLICT DO
UPDATE` and never names the column, so it is never rewritten. An explicit
value (an import) is kept. The expression is pure core SQL (`julianday`,
`randomblob`, `printf`). Minting happens inside the insert's own statement
and transaction, so it adds no fsync, no custom function, and no network.
A trigger, not the write sites, because the catalog has dozens of insert
sites and the change feed already relies on triggers for the same reason.

Because it is a stored column, the change feed's `StoredRow` and the export's
session payload carry it with no new wire field. The TS `HistoryExportBatch`
carries export records, so batches carry it too. On upgrade, every existing
row is backfilled once (marker `session_trace_id_v1`). The change feed's
export-schema reconciliation then restamps each session row once, above the
old head.

## Alternatives rejected

- **Generate trace ids in Rust at each write site.** That's dozens of
  sites, and any one that forgot would be a session with no trace.
- **A column `DEFAULT (randomblob ...)`.** SQLite refuses a non-constant
  default in `ALTER TABLE ADD COLUMN`, so existing databases would need a
  rebuild of `sessions`.
- **Fold minting into the change feed's insert trigger.** That would couple
  two features' DDL. The change feed's update guard instead treats a NULL to
  value fill of `sessions.trace_id` as no change (`MINTED_ON_INSERT`), so a
  new session still takes exactly one revision. Measured in release, minting
  adds about 11 µs to a new session's insert (62 to 73 µs) and nothing to
  later upserts or event writes.
- **Use the change-feed epoch as the machine id.** It's per database and
  resets on replacement.
- **Hardware ids (IOPlatformUUID, `/etc/machine-id`) as the id.** They're
  privacy-sensitive, need a process spawn on macOS, and VM images clone them.

## Consequences and open items

- A session row deleted and later re-inserted (a catalog drop followed by
  rediscovery) gets a new trace id.
- The upgrade restamps every session row once, so a feed consumer re-reads
  each catalog row one time.
- Clone detection (a database restored onto a different machine) isn't
  implemented. A cheap check would compare a hash of `/etc/machine-id` or
  IOPlatformUUID at first run and on open, and mint a new install id when it
  moves.
- `machine_id` isn't yet on any export envelope. The delivery coordinator
  reads `SessionStore::machine_identity()` when it builds object keys.
