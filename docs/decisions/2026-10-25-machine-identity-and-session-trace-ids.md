# Machine identity and per-session trace ids live in the history database

- **Status:** Proposed. Decided by Khaliq on 2026-10-25: the machine id is
  the canonical hostname plus an install suffix, never the hostname alone,
  and it carries the plain slug (`DEFAULT_MACHINE_NAME_PRIVACY = Slug`) so
  ids and object paths stay readable.
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
2). The version is stored beside every name it produced, so a later rule
change recanonicalizes stored names on the next open without touching the
id. The rule:

- Lowercase, and strip `.local` and the domain.
- Drop apostrophes, both straight and curly (decided by Khaliq,
  2026-10-25). macOS's ComputerName `Khaliq's MacBook Pro` and its
  LocalHostName `Khaliqs-MacBook-Pro` are then one name.
- Turn every other run of punctuation, spaces and underscores into one `-`.
- Trim hyphens, and cap the result at 63 characters.

Rule 1 turned apostrophes into hyphens. Every hostname spelling the machine has reported is kept
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

The default is the plain slug, for readability. `MachineNamePrivacy::Hashed`
stays available as an option: it replaces the slug with `h` plus 12 hex digits of
a SHA-256 of it. That keeps the hostname out of object keys, but hostnames are
guessable, so the hash is pseudonymity, not anonymity. It stays deterministic
so a name maps to one pseudonym.

### Trace ids

`sessions.trace_id` is a UUIDv7 minted when a session row is first written,
by the change feed's own insert-stamp `UPDATE` (`SET revision = ...,
trace_id = COALESCE(trace_id, <uuidv7>)`). Every catalog upsert is `ON CONFLICT DO
UPDATE` and never names the column, so it is never rewritten. An explicit
value (an import) is kept. The expression is pure core SQL (`julianday`,
`randomblob`, `printf`). Minting happens inside the insert's own statement
and transaction, so it adds no fsync, no custom function, and no network.
A trigger, not the write sites, because the catalog has dozens of insert
sites and the change feed already relies on triggers for the same reason.
It's the feed's insert trigger rather than one of its own, so the mint and
the stamp are a single write. A new session takes exactly one revision, and
the feed's update guard needs no exception: any later change to `trace_id`,
including a repair of a NULL, takes a revision like any other column.

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
- **A standalone `AFTER INSERT` trigger that fills `trace_id`.** Its second
  write either stamps a new session twice, or needs the update guard to
  ignore NULL to value fills. The guard exception (an earlier revision of this
  change) also hid a later repair of a NULL id from feed consumers, so it was
  replaced by minting in the feed's own stamp.
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
