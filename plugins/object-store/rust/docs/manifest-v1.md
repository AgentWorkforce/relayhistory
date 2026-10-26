# Session manifest (`manifest.json`), schema v1

- **Status:** Proposed (draft for review). The ownership model (two writers,
  each owning its own sections) was decided by Khaliq on 2026-10-25. The epoch
  rule and chunk fields are still pending review.
- **Implementations:** Rust `relayhistory_object_store::manifest` (types,
  validation, `update_manifest`, client and server helpers); TypeScript
  `parseManifest` in relayhistory-cloud's `@relayhistory/object-store`.
- **Object:** `{prefix}/v1/ws={workspace_id}/m={machine_id}/h={harness}/d={yyyy}/{mm}/{dd}/s={session_id}/manifest.json`,
  built by `layout::SessionLocation::manifest()`
- **Writers:** two of them, each owning its own sections
  ([Ownership](#ownership)):
  - the **client**: the desktop's local delivery coordinator for that machine
  - the **server**: the storage-event handler, the normalize transform and the
    lifecycle sweeper
- **Readers:** the server-side `storage.event` handler, the normalize transform,
  the Supabase `sessions` pointer upsert, and `fetch`.

The manifest is the only mutable object under a session prefix. Raw chunks are
immutable and content-addressed. The manifest is the authoritative list of
which chunks make up the session, in what order, and with which digests. A chunk
object that no manifest lists does not exist as far as readers are concerned.
Orphan chunks can appear after a crash between the chunk upload and the
manifest write, and they are harmless.

Neither writer ever waits for the other. The server keeps the session moving
(normalize, lifecycle, retention) while the desktop is offline for days. The
client keeps appending raw data whatever the server is doing.

## Ownership

Each writer owns some fields outright and may change only those. Every write
copies the other writer's sections, and **any field the writer does not
recognize**, through byte-for-byte unchanged. A writer never rebuilds the
manifest from scratch; it always edits the copy it just read.

To delete an unrecognized field in a section it owns, a writer must name it
explicitly (in the Rust helper, the edit's `remove` list). A field that is
merely absent from the writer's copy, or has a `null` value, is carried
through unchanged.

| Fields | Owner | Notes |
| ------ | ----- | ----- |
| `session.*` | client | Only the client sees the harness metadata. |
| `raw` (everything in it, including `raw.state`, `raw.epoch`, `raw.epochs`, `raw.owner`, `raw.updated_at`) | client | The server has no raw bytes and never writes this section. |
| `writer.*` | client | Which client delivery instance last wrote. |
| `server` (everything in it: `normalized`, `lifecycle`, `retention`, `updated_at`) | server | The client never writes this section. |
| `schema_v`, `layout`, `trace_id` | first writer | Set once on creation, normally by the client, and never changed. |
| `revision`, `updated_at` | whoever writes | Every write by either side sets `revision = previous + 1` and `updated_at = now`. |
| `created_at` | first writer | Never changed. |
| `extensions` | per key | Namespaced keys (`"client.x"`, `"server.y"`) belong to their namespace owner. Each writer copies the others through. |

## Top-level fields

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `schema_v` | integer | required | Manifest schema version. This document describes `1`. Readers refuse a higher major version. |
| `layout` | string | required | Object layout version, `"v1"` (`layout::LAYOUT_VERSION`). It must match the `v1/` path segment. |
| `trace_id` | string (UUIDv7) | required | The session's trace id. It is minted once, when the session is first ingested, and never changes. Every chunk object carries it in `trace-id` metadata. Spans `ingest`, `deliver.put`, `storage.event`, `transform`, `index.upsert` and `fetch` use it. |
| `revision` | integer ≥ 1 | required | Global manifest revision. The first write is `1` and each successful write by either writer adds exactly `1`. Compare-and-swap puts all writes in a single order, so it is strictly increasing. This is the pointer-index fencing token. |
| `created_at` | string (RFC 3339, UTC) | required | When revision 1 was written. |
| `updated_at` | string (RFC 3339, UTC) | required | When this revision was written, by either writer. |
| `session` | object | required | Client-owned identity and descriptive metadata ([below](#session)). |
| `writer` | object | required | The client delivery instance that last wrote ([below](#writer)). |
| `raw` | object | required | Client-owned raw chunk stream ([below](#raw)). |
| `server` | object or `null` | optional | Server-owned derived state ([below](#server)). Absent or `null` until the server first writes. |
| `extensions` | object | optional | Namespaced forward-compatible data. |

Readers ignore unknown fields. Writers preserve them.

### `session`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `workspace_id` | string | required | Must equal the `ws=` path segment. |
| `machine_id` | string | required | Must equal the `m=` path segment. |
| `harness` | string | required | Harness/provider id, e.g. `claude`, `codex`, `cursor`, `opencode`. Must equal `h=`. |
| `session_id` | string | required | The harness session id as RelayHistory identifies it. Must equal `s=`. |
| `started_at` | string (RFC 3339) | required | Session start. Its UTC date is the `d=` segment and never changes, even when the session runs past midnight. |
| `ended_at` | string (RFC 3339) | optional | Last harness activity, set when `raw.state` becomes `"closed"`. |
| `harness_version` | string | optional | Harness version, if the log records one. |
| `cwd_sha256` | string (hex) | optional | SHA-256 of the working directory. The plaintext path is never stored, because local paths leak usernames and project names. |
| `parent_session_id` | string | optional | Parent session for subagent or delegated sessions, when known. |
| `repo` | object | optional | Repository evidence for a stable repo identity, absent when the working directory is not in a git repository ([below](#sessionrepo)). |

#### `session.repo`

The server uses this to give the session a repo identity that survives repo
renames, transfers, owner renames and SSH/HTTPS remote differences
(relayhistory-cloud `sessions.resolve_repo`). Both fields are derived offline;
the client never calls a provider API for them. The host builds the block with
`manifest::repo::session_repo(remote, root)`: the remote is the `project_key` of
an `ai_hist::project_identity` that resolved by `Remote`, and the root commit
comes from the work tree's `git` (off the ingest path; `None` if `git` is
missing or the clone is shallow).

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `remote_url` | string | optional | The `origin` remote canonicalized to `host/path`, exactly as `ai_hist::project_identity::canonicalize_remote_url` returns it: lowercase host, owner/repo case kept, no scheme, no credentials, no port, no trailing `.git`. Never a raw remote URL, because those can carry tokens. |
| `root_commit` | string (40 or 64 lowercase hex) | optional | The repository's root commit (`manifest::repo::root_commit`, cached per work tree by `RootCommitCache`). Omitted for shallow clones, whose oldest commit is not the root. Forks share it with their upstream. |

### `writer`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `delivery_instance_id` | string | required | The client destination instance that last wrote. |
| `plugin_version` | string | required | `relayhistory-object-store` version of that write. |
| `host_version` | string | optional | Host application version (desktop app / CLI). |

### `raw`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `owner` | string | required | `delivery_instance_id` of the client instance that owns this session's raw stream: the client fencing token ([Stale writers](#stale-writers)). |
| `state` | `"open"` or `"closed"` | required | `"closed"`: the harness session ended and the client saw no new bytes. A later client write may reopen it. The server never writes it. |
| `updated_at` | string (RFC 3339) | required | When the client last changed anything in `raw`. Server lifecycle is computed from this. |
| `format` | string enum | required | `"jsonl"`, `"ndjson"` or `"sqlite-rows"` (`layout::RawFormat`): the encoding of the uncompressed bytes. |
| `compression` | string enum | required | `"zstd"` in v1. Chunk objects are `application/zstd`, with no `Content-Encoding`. |
| `chunk_count` | integer ≥ 0 | required | `chunks.length`. |
| `raw_bytes` | integer ≥ 0 | required | Sum of `chunks[].bytes` (uncompressed). |
| `stored_bytes` | integer ≥ 0 | required | Sum of `chunks[].stored_bytes` (compressed). |
| `content_sha256` | string (64 hex) | required | Hash chain over the ordered chunk list ([definition](#content_sha256)). |
| `epoch` | integer ≥ 0 | required | The current source epoch: the epoch counter ([Epochs](#epochs)). |
| `epochs` | array of [epoch records](#epoch-records) | required | One record per epoch ever opened, `epochs[i].epoch == i`, so `epochs.length == epoch + 1`. |
| `next_offset` | integer ≥ 0 | required | `start_offset + bytes` of the last chunk in the current epoch, or `0` if the current epoch has no chunks yet. The client resumes reading the source here. |
| `chunks` | array of [chunk](#chunk-entries) | required | Every chunk of every epoch, ordered by `seq`. |

#### Epoch records

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `epoch` | integer ≥ 0 | required | Equal to its index in `raw.epochs`. |
| `opened_at` | string (RFC 3339) | required | When the client opened it. |
| `reason` | `"initial"`, `"truncated"` or `"rewritten"` | required | `"initial"` only for epoch 0. `"truncated"`: the source shrank. `"rewritten"`: bytes before `next_offset` changed. |
| `first_seq` | integer ≥ 0 | required | `seq` of the epoch's first chunk, which equals `raw.chunk_count` at the moment it was opened. |
| `opened_by` | string | required | `raw.owner` at the time it was opened. |

### `server`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `updated_at` | string (RFC 3339) | required | When the server last changed this section. |
| `normalized` | object or `null` | required | `null` until the first transform succeeds. |
| `normalized.version` | integer ≥ 1 | required | Transform version `N`. Output lives under `normalized/v{N}/`. |
| `normalized.source_content_sha256` | string (64 hex) | required | The `raw.content_sha256` the transform actually read. |
| `normalized.source_epoch` | integer ≥ 0 | required | The `raw.epoch` it read. By default a transform reads only the current epoch. |
| `normalized.built_at` | string (RFC 3339) | required | When the transform finished. |
| `normalized.manifest_key` | string | optional | The transform's own output index, e.g. `normalized/v{N}/manifest.json`. |
| `lifecycle` | `"active"`, `"idle"`, `"abandoned"` or `"archived"` | required | The server's view of liveness ([State](#state-and-normalized_version)). |
| `lifecycle_at` | string (RFC 3339) | required | When `lifecycle` last changed. |
| `lifecycle_reason` | string | optional | E.g. `"no raw change for 24h"`. |
| `retention` | object | optional | `{ delete_after?: RFC 3339, legal_hold?: boolean }`. The server is the only side that can finish a session's life when the client never returns. |

## Compare-and-swap write protocol (both writers)

Every manifest write, by either side, follows the same steps:

1. **Read.** `get_object` the manifest and keep `meta.version` (S3/Azure ETag,
   GCS generation) **from that same response**. Never pair a body with the
   version from a separate `head`: another writer can commit in between, and
   the next conditional write would then silently discard that writer's
   change.
2. **Modify only owned fields** on that copy (see [Ownership](#ownership)).
   Set `revision = read.revision + 1` and `updated_at = now`.
3. **Write.** First write: `PutOpts::create()`. Every later write:
   `PutOpts::replace(version)` (`If-Match` / `ifGenerationMatch`). Set the
   object's `trace-id` metadata.
4. **On `PreconditionFailed`** (or `AlreadyExists` on a create): the other side
   wrote in between. Go back to step 1 and re-apply **only your own change** to
   the fresh copy. The two sides own disjoint fields, so re-applying always
   merges cleanly. Neither side can lose the other's update, and no field-level
   merge exists.
5. **Bounded retry.** Retry up to **8 attempts**, with exponential backoff and
   full jitter: base 100 ms, cap 5 s. When the attempts run out it is a delay,
   never data loss:
   - The client leaves its local checkpoint unadvanced and retries on its next
     delivery tick.
   - The server re-queues the `storage.event` / transform job with backoff.

### Client-specific rules

- **Chunks before manifest.** Upload new chunks first with
  `PutOpts::create()` and `content_sha256 = stored_sha256`. They are
  content-addressed, so retries are idempotent (`created: false`). Only then
  write the manifest. Advance the local delivery checkpoint only after the
  manifest write is acknowledged. A crash leaves at worst orphan chunks.
- **Re-apply = append.** The client's change is "append my unacknowledged
  chunks (and maybe open an epoch) after the remote chunk list". It is valid
  only if the remote `raw` matches what the client last acknowledged, as
  checked under [Stale writers](#stale-writers). The server never touches
  `raw`, so a server write between read and write never invalidates the
  client's pending chunks.
- **Seq comes from the remote copy.** The client computes `seq` for new chunks
  from the remote copy it read, as `remote.raw.chunk_count + i`. With one owner
  that always equals local state. If it ever does not, the pre-uploaded chunk
  keys (which embed `seq`) are abandoned as orphans, re-uploaded under the
  correct `seq`, and the write retried.

### Server-specific rules

- The server writes only `server` (plus `revision`, `updated_at`). It never
  creates a manifest; a session without one does not exist yet.
- **No feedback loop.** The server's own write fires an object-landed event.
  The handler re-indexes, which is idempotent, but runs a transform only when
  `raw.content_sha256 != server.normalized.source_content_sha256` (or
  `normalized` is `null`).
- A transform that finishes after the client appended still records what it
  read (`source_content_sha256`). The output is then visibly stale and the next
  event rebuilds it. Stale output is never presented as current.

## Epochs

An epoch is one contiguous reading of the harness source from offset `0`.
Harness logs are normally append-only, so most sessions only ever have epoch 0.

**The counter.** `raw.epoch` lives in the manifest and is the only source of
the epoch number. Nobody keeps an independent count. A new epoch number is
always `remote.raw.epoch + 1`, computed from the copy being compare-and-swapped.
Because the write is conditional on that exact copy, two writers can never
commit the same number or skip one.

**Who may open an epoch.** Only the client that is `raw.owner`, and only when
it observes that the source shrank below `next_offset` (`"truncated"`) or that
bytes before `next_offset` changed (`"rewritten"`). It detects the second case
by re-hashing the source range of the last acknowledged chunk and comparing it
with `chunks[-1].sha256`. **The server never opens an epoch.** It has no access
to the source, and it never writes `raw`.

**Opening an epoch** is one manifest write that does all of the following:

- sets `raw.epoch = remote.raw.epoch + 1`
- appends an `epochs` record (`first_seq = remote.raw.chunk_count`,
  `opened_by = raw.owner`)
- appends the new epoch's first chunks (`start_offset` starting at `0`; it may
  be zero chunks if the source is now empty)
- sets `next_offset` to the new epoch's end

Earlier epochs and their chunks stay listed forever, because raw is canonical.

**`seq` across epochs.** `seq` is global and dense: it continues from
`raw.chunk_count` and never resets. Object keys therefore never collide, even
though `start_offset` restarts at `0`.

**On `PreconditionFailed` when the client was opening an epoch:**

- If the other write came from the **server**, the remote `raw` is unchanged.
  Re-apply the same epoch open on the fresh copy: same number, same chunks.
- If the remote `raw` changed (another client instance wrote): apply
  [Stale writers](#stale-writers) first. If this instance is still `raw.owner`
  and the remote already opened epoch `e + 1`, compare the remote's new-epoch
  chunks with the current source.
  - If they match a prefix of the source, adopt that epoch and append only the
    rest.
  - Otherwise open `remote.raw.epoch + 1`.

  The number is still always derived from the remote copy. Both sides "opening
  an epoch" at once cannot happen, because only the raw owner opens epochs.

### Epoch invariants (readers check)

1. `epochs[i].epoch == i`, `raw.epoch == epochs.length - 1`, and
   `epochs[0].reason == "initial"`.
2. For every chunk, `chunk.epoch` is non-decreasing along `chunks`, and the
   first chunk of epoch `e` has `seq == epochs[e].first_seq` and
   `start_offset == 0`.
3. Within an epoch, offsets are contiguous:
   `chunks[i].start_offset == chunks[i-1].start_offset + chunks[i-1].bytes`.
4. `next_offset` = end of the last chunk whose epoch is `raw.epoch`, or `0` if
   that epoch has no chunks.

## Stale writers

The client has a fencing token, `raw.owner`, and a last-acknowledged state
that it keeps locally:

```text
ack = (revision, raw.chunk_count, raw.content_sha256, raw.epoch)
```

`ack` is recorded from its last successful manifest write.

On every read before a write, the client classifies the remote copy:

| Remote copy | Meaning | Action |
| ----------- | ------- | ------ |
| `raw.owner != self` | Another instance took over (reinstall, new machine identity) | **Fenced.** Stop writing this session, mark it read-only locally and report it. Never write `raw` again. |
| `revision < ack.revision`, or `raw.chunk_count < ack.chunk_count`, or `ack.content_sha256` is not on the remote hash chain at position `ack.chunk_count` | The manifest went backwards: restored from backup, or corrupted | **Stop and alert** (`ManifestRolledBack`). Do not overwrite. |
| `raw` identical to `ack` (only `server` / `revision` moved) | The server wrote | Re-apply and continue. |
| `raw.epoch < ack.epoch` | The manifest went backwards | **Stop and alert**, as above. |
| `raw` extends `ack` (more chunks and/or a newer epoch, even with no new chunks) and this instance is the owner | Its own earlier write that it did not see acknowledged, e.g. a crash after the PUT | Adopt it as the new `ack`, including the epoch, and append only what is missing. Never open the epoch a second time. |

**Ownership transfer.** A new client instance takes over only through an
explicit takeover write: it sets `raw.owner` to itself under compare-and-swap,
which is recorded in `writer`. It does this only when it holds the source, for
example a reinstalled desktop re-reading the same logs. The previous owner
detects it on its next read and fences itself.

The server is never "stale" in a harmful way. Its writes only replace its own
section, and it recomputes that section from the copy it just read.

## State and normalized_version

- **Effective state** for the pointer row:
  - `"closed"` if `raw.state == "closed"`
  - otherwise `server.lifecycle` (`"active"` / `"idle"` / `"abandoned"` /
    `"archived"`)
  - `"active"` if `server` is `null`

  The client states facts. The server states its view. The server never writes
  `raw.state`.
- **Lifecycle** is computed by the server from `raw.updated_at` versus now:
  - **active**: less than 24 h since the last raw change
  - **idle**: 24 h or more
  - **abandoned**: 14 days or more, still `open`
  - **archived**: set by the retention policy

  The thresholds are configuration. When the client appends again, it bumps
  `raw.updated_at`, and the next server write returns `lifecycle` to
  `"active"`. The server never needs to coordinate with the client to do this.
- **`normalized_version` is current** only when
  `server.normalized.source_content_sha256 == raw.content_sha256` **and**
  `server.normalized.source_epoch == raw.epoch`. Opening an epoch adds no chunk
  until the source has new bytes, so the digest alone cannot show that the
  output belongs to the previous epoch. Otherwise the pointer row carries the
  version with `normalized_stale = true` until the rebuild lands.

## Client offline for days

1. The server keeps working with no contention:
   - transforms run on whatever raw data exists
   - `lifecycle` goes active, then idle, then abandoned
   - retention fields are set
   - the pointer row stays current
2. When the client returns, its first manifest write gets `PreconditionFailed`
   because the ETag moved. It re-reads:
   - `raw` still equals its `ack`, because the server never wrote `raw`
   - it keeps the server's section
   - it appends its pending chunks, which were already uploaded or are
     uploaded now
   - it writes
3. The next server pass sees the new `raw.updated_at` and `content_sha256`.
   It sets `lifecycle = "active"` and rebuilds the normalized output.
4. If the client never returns, the server alone finishes the session's life:
   abandoned, then archived, then deleted by retention. The session is never
   blocked on the client.

## Chunk entries

Each entry describes one immutable object at
`raw/{seq:010}-{start_offset}-{sha256_8}.{format}.zst`
(`layout::SessionLocation::raw_chunk`).

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `seq` | integer ≥ 0 | required | Global, dense chunk number: `chunks[i].seq == i`. It never resets, including across epochs. Zero-padded to 10 digits in the key, so listing order equals `seq` order. |
| `epoch` | integer ≥ 0 | required | Source epoch of this chunk. |
| `start_offset` | integer ≥ 0 | required | Offset of the first byte in the epoch's uncompressed source stream: a byte offset in the log file for `jsonl`/`ndjson`, and in the serialized row stream for `sqlite-rows`. |
| `bytes` | integer ≥ 1 | required | Uncompressed length. |
| `stored_bytes` | integer ≥ 1 | required | Compressed object size. It equals the object's `Content-Length`. |
| `sha256` | string (64 hex) | required | SHA-256 of the **uncompressed** bytes. |
| `sha256_8` | string (8 hex) | required | `sha256[0..8]`, exactly as it appears in the key. |
| `stored_sha256` | string (64 hex) | required | SHA-256 of the **compressed** object. It equals the object's `content-sha256` metadata, so a download can be verified before decompressing. |
| `compression` | string enum | required | `"zstd"`. |
| `records` | integer ≥ 0 | optional | Complete lines or rows in the chunk. |
| `first_ts` / `last_ts` | string (RFC 3339) | optional | First and last record timestamps, when the format has them. |

Chunk invariants, besides the [epoch invariants](#epoch-invariants-readers-check):

1. `chunks[i].seq == i`.
2. A chunk ends on a record boundary: a newline for `jsonl`/`ndjson`, a row
   for `sqlite-rows`. A partial trailing line stays local until it completes.
3. Entries are never edited or removed once written. New revisions only append.

### Chunk size

Advisory, not part of the schema. The client seals a chunk at a record
boundary once it reaches **4 MiB uncompressed**, or when the session goes idle
or closes. Chunks use a single `put`. `multipart` is reserved for objects over
64 MiB.

## `content_sha256`

`raw.content_sha256` is a hash chain over the ordered chunk entries of **all**
epochs. It is computable incrementally and verifiable from the manifest alone:

```text
h_0 = 32 zero bytes
h_i = SHA-256( h_{i-1}
             ‖ u32be(chunks[i-1].epoch)
             ‖ u64be(chunks[i-1].start_offset)
             ‖ u64be(chunks[i-1].bytes)
             ‖ hex_decode(chunks[i-1].sha256) )        // 32 bytes
content_sha256 = lowercase_hex(h_{chunk_count})
```

An empty list gives 64 zeros. Because it is a chain, "is `ack` a prefix of the
remote copy" is answered by recomputing `h_{ack.chunk_count}` from the remote
chunks and comparing it with `ack.content_sha256`.

## Fences

There are two independent fences.

1. **Object-store fence: ETag / generation.** Every write by either writer is
   compare-and-swap ([protocol](#compare-and-swap-write-protocol-both-writers)).
   It guarantees no lost update between the client and the server, or between
   two client instances.
2. **Index fence: `revision`.** Object-landed events can arrive late, out of
   order or more than once, from either writer's writes. `index.upsert` always
   re-`get`s the manifest named in the event, rather than trusting the event
   payload, and applies it only if it is newer:

   ```sql
   -- Conflict key as in relayhistory-cloud#171's session_index:
   -- (org, workspace, harness, session_id, machine_id).
   insert into sessions (...) values (...)
   on conflict (org_id, workspace_id, harness, session_id, machine_id)
   do update set ...
   where excluded.manifest_revision > sessions.manifest_revision;
   ```

   `machine_id` is part of the key because the same harness session id can
   occur on two machines. Each machine has its own manifest under its own
   `m=` prefix and its own `revision` sequence, so revisions are compared only
   within one machine's row.

   `revision` is global across both writers and strictly increasing, so one
   fence covers both. A stale event is a no-op.

## Fields that feed the Supabase `sessions` pointer row

| Pointer column | Source |
| -------------- | ------ |
| `workspace_id`, `machine_id`, `harness`, `session_id` | `session.*`. Must match the key's path segments. |
| `started_at`, `ended_at` | `session.started_at`, `session.ended_at` |
| `state` | Effective state ([rule](#state-and-normalized_version)) |
| `trace_id` | `trace_id` |
| `manifest_key` | The event's object key |
| `manifest_revision` | `revision`, the upsert fence |
| `manifest_etag` | The object version the handler read: S3/Azure ETag or GCS generation, as an opaque string. Not a manifest field, because an object cannot contain its own ETag. |
| `raw_bytes`, `stored_bytes`, `chunk_count`, `raw_epoch` | `raw.raw_bytes`, `raw.stored_bytes`, `raw.chunk_count`, `raw.epoch` |
| `content_sha256` | `raw.content_sha256` |
| `normalized_version` | `server.normalized.version`, or `null` |
| `normalized_stale` | `normalized` exists and is not current (digest or epoch differs) |
| `lifecycle` | `server.lifecycle`, or `"active"` when `server` is `null` |
| `git_remote_url`, `git_root_commit` | `session.repo.remote_url`, `session.repo.root_commit`. The upsert resolves `repo_id` from them. |

## Example

A Claude Code session at revision 5: the client has written 3 times and the
server twice. The harness rewrote its log once, so there is an epoch 1. The
normalized output is current. The digests are illustrative. The sizes, offsets,
seqs and epochs are consistent.

```json
{
  "schema_v": 1,
  "layout": "v1",
  "trace_id": "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
  "revision": 5,
  "created_at": "2026-10-25T18:02:11.204Z",
  "updated_at": "2026-10-25T18:47:30.112Z",
  "session": {
    "workspace_id": "ws_7f3a2c",
    "machine_id": "mach-linuyc-desktop",
    "harness": "claude",
    "session_id": "4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718",
    "started_at": "2026-10-25T18:01:58.000Z",
    "harness_version": "2.4.1"
  },
  "writer": {
    "delivery_instance_id": "dest_objstore_01",
    "plugin_version": "0.1.0",
    "host_version": "2026.10.60"
  },
  "raw": {
    "owner": "dest_objstore_01",
    "state": "open",
    "updated_at": "2026-10-25T18:41:07.990Z",
    "format": "jsonl",
    "compression": "zstd",
    "chunk_count": 3,
    "raw_bytes": 9437184,
    "stored_bytes": 1873402,
    "content_sha256": "5d1c0b7e2a9f4c3d8e6b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a291807",
    "epoch": 1,
    "epochs": [
      {
        "epoch": 0,
        "opened_at": "2026-10-25T18:02:11.204Z",
        "reason": "initial",
        "first_seq": 0,
        "opened_by": "dest_objstore_01"
      },
      {
        "epoch": 1,
        "opened_at": "2026-10-25T18:33:13.004Z",
        "reason": "rewritten",
        "first_seq": 2,
        "opened_by": "dest_objstore_01"
      }
    ],
    "next_offset": 1048576,
    "chunks": [
      {
        "seq": 0,
        "epoch": 0,
        "start_offset": 0,
        "bytes": 4194304,
        "stored_bytes": 812455,
        "sha256": "a3f19c0e7b2d4e6f8a1c3e5b7d9f0a2c4e6b8d0f1a3c5e7b9d1f3a5c7e9b0d2f",
        "sha256_8": "a3f19c0e",
        "stored_sha256": "0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d",
        "compression": "zstd",
        "records": 1288
      },
      {
        "seq": 1,
        "epoch": 0,
        "start_offset": 4194304,
        "bytes": 4194304,
        "stored_bytes": 845113,
        "sha256": "7e24b6d1c08f3a5e9b2d4f6a8c0e1b3d5f7a9c1e3b5d7f9a0c2e4b6d8f0a1c3e",
        "sha256_8": "7e24b6d1",
        "stored_sha256": "1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e",
        "compression": "zstd",
        "records": 1102
      },
      {
        "seq": 2,
        "epoch": 1,
        "start_offset": 0,
        "bytes": 1048576,
        "stored_bytes": 215834,
        "sha256": "c5b8e0f2a4d6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c4e6b8d0f2a4c6e8",
        "sha256_8": "c5b8e0f2",
        "stored_sha256": "2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f",
        "compression": "zstd",
        "records": 311
      }
    ]
  },
  "server": {
    "updated_at": "2026-10-25T18:47:30.112Z",
    "normalized": {
      "version": 1,
      "source_content_sha256": "5d1c0b7e2a9f4c3d8e6b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a291807",
      "source_epoch": 1,
      "built_at": "2026-10-25T18:47:29.870Z",
      "manifest_key": "normalized/v1/manifest.json"
    },
    "lifecycle": "active",
    "lifecycle_at": "2026-10-25T18:02:15.000Z"
  }
}
```

The chunk keys for this manifest are:

```text
v1/ws=ws_7f3a2c/m=mach-linuyc-desktop/h=claude/d=2026/10/25/s=4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718/raw/0000000000-0-a3f19c0e.jsonl.zst
.../raw/0000000001-4194304-7e24b6d1.jsonl.zst
.../raw/0000000002-0-c5b8e0f2.jsonl.zst
```

## Size and limits

- Each chunk entry is about 450 bytes, so a 10,000-chunk session (~40 GiB raw)
  has a ~4.5 MiB manifest. If sessions grow past that, `schema_v: 2` would page
  the chunk list into immutable `chunks/{n}.json` segments and keep only the
  tail inline.
- The manifest is plain JSON (`application/json`, not compressed). Its own
  `trace-id` and `content-sha256` object metadata are set like any other
  object's.
- A reader that sees a higher `schema_v` refuses it and does not guess.
  Writers on `schema_v: 1` preserve fields they do not recognize, which is how
  `server` was added without a version bump.
