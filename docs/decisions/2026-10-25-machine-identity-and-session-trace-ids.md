# Machine identity and per-session trace ids live in the history database

- **Status:** Proposed. The machine-id scheme is decided (Khaliq,
  2026-10-25): the canonical hostname plus an install suffix, never the
  hostname alone. The name privacy mode (`DEFAULT_MACHINE_NAME_PRIVACY`,
  slug for now) is still open.
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

`machine_identity` is a single-row table written by the first writable open.
`machine_name` is the canonical hostname (`canonical_hostname`, rule version
1): lowercased, `.local` and the domain stripped, every run of punctuation,
spaces, underscores and apostrophes turned into one `-`, hyphens trimmed, 63
characters at most. Every hostname spelling the machine has reported is kept
in `hostname_aliases` with its canonical form, so all of them resolve to the
one machine. A spelling already recorded is never written again. The
[cloud backfill proposal](../specs/2026-10-25-cloud-hostname-canonicalization-backfill.md)
applies the same rule to existing cloud rows, additively. The hostname comes from `uname(2)` (`COMPUTERNAME` on
Windows). No process is spawned and nothing touches the network.

`machine_id` is the canonical hostname, a `-`, and the first 6 hex digits of
a random install UUID generated at first run (`khaliqs-mbp-7f3a9c`). It is
computed once by `machine_id()`, the single function that decides it, and
then frozen. The row's `id_scheme` (`hostname_install_suffix_v1`) records
which version of that function produced it, so a future change never re-keys
a machine that has already uploaded. A rename or respelling updates
`machine_name` and adds a `hostname_aliases` entry. It never changes the id,
and neither does canonicalization.

| Case | Result |
|---|---|
| Two machines named `macbook-pro` | distinct ids |
| Hostname renamed or respelled | same id; `machine_name` and aliases updated |
| Restart | same id (stored, not recomputed) |
| Re-image or new disk, same hostname | new install, so a new id. The shared canonical name lets the UI link them |
| Database copied to a second machine | same id (clone; needs detection, see below) |

The 6-digit suffix is 24 bits. Among 1,000 machines that share one hostname,
the chance that any two collide is about 3%. With 4 digits it would be about
99.9%.

`MachineNamePrivacy::Hashed` replaces the slug with `h` plus 12 hex digits of
a SHA-256 of it. That keeps the hostname out of object keys, but hostnames are
guessable, so the hash is pseudonymity, not anonymity. It stays deterministic
so a name maps to one pseudonym.

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
- **The canonical hostname alone as the machine id.** Readable, and it
  survives a re-image. But a rename or respelling would change the id, and
  every unrenamed `macbook-pro` would share one id, silently merging those
  machines' histories with no way to separate them after upload. Rejected by
  Khaliq.
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
