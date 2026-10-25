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
3). The version is stored beside every name it produced. A later rule change
makes the next writable open recanonicalize stored names and aliases, and the
id is untouched. The rule (all three steps decided by Khaliq on 2026-10-25):

- Lowercase, and trim whitespace and trailing dots.
- Strip network suffixes, repeatedly: `.local`, `.localdomain`, `.lan`,
  `.home`, `.home.arpa` and `.internal`. Each only matches after a dot, so a
  bare `local` stays a name.
- For a cloud `*.internal` name, keep only the first label
  (`ip-10-0-0-1.ec2.internal` becomes `ip-10-0-0-1`); the rest names a zone
  or project.
- Every other dot is a separator, so no label is lost:
  `khaliqs.macbook.pro` becomes `khaliqs-macbook-pro`, and
  `build-01.corp.example.com` becomes `build-01-corp-example-com`.
- Drop apostrophes, both straight and curly. macOS's ComputerName
  `Khaliq's MacBook Pro` and its LocalHostName `Khaliqs-MacBook-Pro` are then
  one name.
- Turn every other run of punctuation, spaces and underscores into one `-`.
- Trim hyphens, and cut the result at 63 characters from the right, so the
  most specific labels stay.

Rule 1 turned apostrophes into hyphens and kept only the first DNS label.
Rule 2 dropped apostrophes. Rule 3 keeps every label except network
suffixes. Keeping labels was chosen over stripping a domain because
`khaliqs.macbook.pro` and an ordinary `host.example.pro` can't be told
apart (`.pro` is a real TLD). A split name is repaired by `hostname_aliases`
locally and shows up in the cloud dry-run report. A false merge can't be
undone. Every hostname spelling the machine has reported is kept
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
| Database restored or copied onto other hardware | new id on the first writable open there; the old id is kept in `machine_identity_history` |

The 6-digit suffix is 24 bits. Among 1,000 machines that share one hostname,
the chance that any two collide is about 3%. With 4 digits it would be about
99.9%.

The default is the plain slug, for readability. `MachineNamePrivacy::Hashed`
stays available as an option: it replaces the slug with `h` plus 12 hex digits of
a SHA-256 of it. That keeps the hostname out of object keys, but hostnames are
guessable, so the hash is pseudonymity, not anonymity. It stays deterministic
so a name maps to one pseudonym.

### Clone detection

The row stores `hardware_id_hash`, a domain-separated SHA-256 of the
machine's hardware id. The raw id is never stored. It's read once per
process, without spawning anything:

| Platform | Source |
|---|---|
| Linux | `/etc/machine-id`, falling back to `/var/lib/dbus/machine-id` |
| macOS | `gethostuuid(2)`, the IOPlatformUUID |
| Windows | `HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid` (64-bit view) |

If the hash differs from the one the id was issued on, the database has moved
to another machine. The writable open then:

- moves the old id, install id, name and hostname aliases to
  `machine_identity_history` (reason `hardware_changed`);
- mints a fresh install id;
- issues a new id with the same canonical slug and a new suffix.

`MachineIdentity::previous` lists the retired ids. A read-only handle won't
hand out an id issued on other hardware; it fails until a writable open has
reissued it. When no hardware id is available (a container without
`/etc/machine-id`), clone detection is off rather than guessed. A row
written before hashes existed records its first hash without being reissued.

Both clone detection and rule recanonicalization run on the writable open,
not only when the identity is requested. A schema can be current while an
identity is still stale, so `needs_migration` also asks
`machine_identity::needs_refresh`. That check costs two indexed reads plus
the process-cached hash.

The reads go through the `machine-uid` crate (MIT, no dependencies beyond
`libc` and the Windows registry crates). Its FFI keeps this crate free of
`unsafe`, which rust-oleum enforces. Version 0.6 calls `gethostuuid(2)`
directly on macOS rather than spawning `ioreg`.

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
  privacy-sensitive and VM images clone them. They're used only as a hash,
  and only to detect a moved database.

## Consequences and open items

- A session row deleted and later re-inserted (a catalog drop followed by
  rediscovery) gets a new trace id. Khaliq accepted this.
- The upgrade restamps every session row once, so a feed consumer re-reads
  each catalog row one time.
- `machine_id` isn't yet on any export envelope. The delivery coordinator
  reads `SessionStore::machine_identity()` when it builds object keys.
