# Session manifest (`manifest.json`), schema v1

- **Status:** Proposed (draft for review)
- **Object:** `{prefix}/v1/ws={workspace_id}/m={machine_id}/h={harness}/d={yyyy}/{mm}/{dd}/s={session_id}/manifest.json`,
  built by `layout::SessionLocation::manifest()`
- **Writer:** the local delivery coordinator's ObjectStorage destination for that
  machine. It is the only writer.
- **Readers:** the server-side `storage.event` handler, the normalize transform,
  the Supabase `sessions` pointer upsert, and `fetch`.

The manifest is the only mutable object under a session prefix. Raw chunks are
immutable and content-addressed. The manifest is the authoritative list of
which chunks make up the session, in what order, and with which digests. A chunk
object that no manifest lists does not exist as far as readers are concerned.
Orphan chunks can appear after a crash between the chunk upload and the
manifest write, and they are harmless.

## Top-level fields

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `schema_v` | integer | required | Manifest schema version. This document describes `1`. Readers must reject a major version they do not know. |
| `layout` | string | required | Object layout version, `"v1"` (`layout::LAYOUT_VERSION`). It must match the `v1/` path segment. |
| `trace_id` | string (UUIDv7) | required | The session's trace id. It is minted once, when the session is first ingested, and never changes. Every chunk object carries the same value in `trace-id` metadata. Spans `ingest`, `deliver.put`, `storage.event`, `transform`, `index.upsert` and `fetch` use it. |
| `revision` | integer ≥ 1 | required | Monotonic manifest revision. The first write is `1` and every successful write increases it by exactly `1`. This is the server-side fencing token (see [Fencing](#revision-and-etag-fencing)). |
| `created_at` | string (RFC 3339, UTC) | required | When revision 1 was written. |
| `updated_at` | string (RFC 3339, UTC) | required | When this revision was written. |
| `session` | object | required | Identity and descriptive metadata ([below](#session)). |
| `writer` | object | required | Which delivery instance wrote this revision ([below](#writer)). |
| `state` | string enum | required | `"open"`: more chunks are expected. `"closed"`: the harness session ended and the writer saw no new bytes. A closed session may be reopened by a later revision if the harness appends again. |
| `raw` | object | required | Raw chunk stream ([below](#raw)). |
| `extensions` | object | optional | Namespaced (`"vendor.key"`) forward-compatible data. Readers must ignore unknown keys here and unknown top-level keys. |

### `session`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `workspace_id` | string | required | Must equal the `ws=` path segment. |
| `machine_id` | string | required | Must equal the `m=` path segment. |
| `harness` | string | required | Harness/provider id, e.g. `claude`, `codex`, `cursor`, `opencode`. Must equal `h=`. |
| `session_id` | string | required | The harness session id as RelayHistory identifies it. Must equal `s=`. |
| `started_at` | string (RFC 3339, UTC) | required | Session start. Its UTC date is the `d=` segment and never changes, even when the session runs past midnight. |
| `ended_at` | string (RFC 3339, UTC) | optional | Last activity, set when `state` becomes `"closed"`. |
| `harness_version` | string | optional | Harness version, if the log records one. |
| `cwd_sha256` | string (hex) | optional | SHA-256 of the session's working directory. The plaintext path is not stored, because local paths leak usernames and project names. A hash still lets sessions be grouped by project. |
| `parent_session_id` | string | optional | Parent session for subagent or delegated sessions, when RelayHistory knows it. |

### `writer`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `delivery_instance_id` | string | required | The destination instance that owns this checkpoint (plan 009: checkpoints are scoped per destination instance). |
| `plugin_version` | string | required | `relayhistory-object-store` version that wrote this revision. |
| `host_version` | string | optional | Version of the host application (desktop app / CLI). |

### `raw`

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `format` | string enum | required | `"jsonl"`, `"ndjson"` or `"sqlite-rows"` (`layout::RawFormat`). This is the encoding of the uncompressed bytes. Each chunk object's extension repeats it. |
| `compression` | string enum | required | `"zstd"` in v1. It applies to every chunk. Chunk objects are `application/zstd`, with no `Content-Encoding`. |
| `chunk_count` | integer ≥ 0 | required | `chunks.length`. |
| `raw_bytes` | integer ≥ 0 | required | Sum of `chunks[].bytes` (uncompressed). |
| `stored_bytes` | integer ≥ 0 | required | Sum of `chunks[].stored_bytes` (compressed, as billed by the bucket). |
| `content_sha256` | string (64 hex) | required | Hash-chain digest over the ordered chunk list ([definition](#content_sha256)). It identifies the exact raw content of this revision. |
| `next_offset` | integer ≥ 0 | required | `start_offset + bytes` of the last chunk in the current epoch, or `0` if the epoch has no chunks. The writer resumes from here. |
| `epoch` | integer ≥ 0 | required | The current source epoch ([below](#epochs)). |
| `chunks` | array of [chunk](#chunk-entries) | required | Every chunk, ordered by `seq` ascending. May be empty while `state` is `"open"`. |

## Chunk entries

Each entry describes one immutable object at
`raw/{seq:010}-{start_offset}-{sha256_8}.{format}.zst`
(`layout::SessionLocation::raw_chunk`).

| Field | Type | Req. | Meaning |
| ----- | ---- | ---- | ------- |
| `seq` | integer ≥ 0 | required | Chunk sequence number. It is unique and dense across the whole session: `chunks[i].seq == i`. It never resets, including across epochs, so object keys never collide. It is zero-padded to 10 digits in the key so lexicographic listing equals `seq` order. |
| `epoch` | integer ≥ 0 | required | The source epoch this chunk belongs to. |
| `start_offset` | integer ≥ 0 | required | Offset of the chunk's first byte in the epoch's uncompressed source stream. For `jsonl`/`ndjson` this is the byte offset in the harness log file. For `sqlite-rows` it is the byte offset in the serialized row stream. |
| `bytes` | integer ≥ 1 | required | Uncompressed length. |
| `stored_bytes` | integer ≥ 1 | required | Compressed object size. It must equal the object's `Content-Length`. |
| `sha256` | string (64 hex) | required | SHA-256 of the **uncompressed** bytes. |
| `sha256_8` | string (8 hex) | required | `sha256[0..8]`, exactly as it appears in the key. Redundant on purpose, so a reader can rebuild the key without string slicing rules. |
| `stored_sha256` | string (64 hex) | required | SHA-256 of the **compressed** object. It equals the object's `content-sha256` metadata (`META_CONTENT_SHA256`), so a reader can verify a download before decompressing it. |
| `compression` | string enum | required | `"zstd"`. Repeated per chunk so a future format can change it per chunk without a schema bump. |
| `records` | integer ≥ 0 | optional | Complete lines/rows in the chunk. |
| `first_ts` / `last_ts` | string (RFC 3339, UTC) | optional | Timestamps of the first and last records, when the format has them. These support time-range fetches without decompressing. |

Chunk invariants, which a reader must check and the writer must uphold:

1. `chunks[i].seq == i`.
2. Within an epoch, offsets are contiguous: `chunks[i].start_offset == chunks[i-1].start_offset + chunks[i-1].bytes`. The first chunk of an epoch starts at `0`.
3. `epoch` is non-decreasing along `chunks`, and `raw.epoch` equals the last chunk's `epoch`.
4. A chunk ends on a record boundary (newline for `jsonl`/`ndjson`, row for `sqlite-rows`). A partial trailing line stays local until it completes. Each chunk therefore decompresses to whole records, and the normalize transform can process chunks independently.
5. Entries are never edited or removed once written. A new revision only appends entries. The exception is the 30-day remote retention policy, if one is ever defined; v1 defines none.

### Epochs

Harness logs are normally append-only. If the source shrinks, or its bytes before
`next_offset` change (a harness rewrote or compacted its log), the writer
starts a new epoch. It increments `raw.epoch` and uploads the full current
source from offset `0` as new chunks with new `seq` numbers. Earlier epochs stay
listed, because raw is immutable and canonical. Readers that want the current
content read only the chunks where `epoch == raw.epoch`. Readers that want full
history read all of them.

### Chunk size

The writer seals a chunk at a record boundary once it reaches **4 MiB
uncompressed**, or when the session goes idle or closes. It never seals below
one record. The value is advisory and not part of the schema. Chunks below the
S3 multipart minimum use single `put`. `multipart` is reserved for objects over
64 MiB.

## `content_sha256`

`raw.content_sha256` is a hash chain over the ordered chunk entries. It is
computable incrementally, so the writer never re-reads earlier chunks, and it
can be verified from the manifest alone:

```text
h_0 = 32 zero bytes
h_i = SHA-256( h_{i-1}
             ‖ u32be(chunks[i-1].epoch)
             ‖ u64be(chunks[i-1].start_offset)
             ‖ u64be(chunks[i-1].bytes)
             ‖ hex_decode(chunks[i-1].sha256) )        // 32 bytes
content_sha256 = lowercase_hex(h_{chunk_count})
```

An empty chunk list gives 64 zeros. Two revisions with the same
`content_sha256` reference exactly the same raw bytes in the same order. The
pointer row uses this to skip re-normalizing a manifest whose content did not
change, for example a revision that only flipped `state`.

## Revision and ETag fencing

There are two independent guards. The ETag guards the **object store**. The
revision guards the **server-side index**.

**Writer protocol (local delivery coordinator):**

1. Upload new chunks first, each with `PutOpts::create()` and
   `content_sha256 = stored_sha256` (chunk keys are content-addressed, so
   retries are idempotent: `created: false`). The coordinator does not touch the
   manifest until every new chunk is acknowledged.
2. Read the current manifest (`head` + `get`) and keep its `ObjectVersion`. It
   is the S3/Azure ETag, or the GCS generation.
3. Build revision `r + 1`: append the new chunk entries, then recompute
   `chunk_count`, `raw_bytes`, `stored_bytes`, `next_offset`, `content_sha256`
   and `updated_at`.
4. Write it:
   - first write: `PutOpts::create()`
   - every later write: `PutOpts::replace(version)`
   Set `trace-id` metadata on both.
5. If the write fails with `PreconditionFailed` (or `AlreadyExists` on the
   first write), another write landed. With one writer per session this means a
   previous run of this coordinator committed after it crashed. Go back to step
   2, reconcile, and retry:
   - The remote chunk list must be a prefix of the local one, or equal to it.
     Re-append only what is missing.
   - If the remote list diverges from the local one, which should never
     happen, stop and surface `ManifestConflict`. Do not overwrite.
6. Advance the local delivery checkpoint only after the manifest write is
   acknowledged. The acknowledged `PutAck.version` is the new `manifest_etag`.

The manifest therefore never loses an update and never references a chunk that
is not durable. A crash at any step leaves either an orphan chunk, which is
harmless, or a manifest that is re-applied idempotently.

**Server protocol (pointer index):** object-landed events can arrive late, out
of order or more than once. The `index.upsert` step is fenced on `revision`:

```sql
insert into sessions (...) values (...)
on conflict (workspace_id, session_id, harness)
do update set ...
where excluded.manifest_revision > sessions.manifest_revision;
```

A stale event, with a lower or equal revision, is a no-op. The handler always
re-`get`s the manifest named in the event, instead of trusting an event
payload, and records the `manifest_etag` it read. `fetch` can then
`get` the manifest with that version and detect that a newer revision exists
since indexing.

## Fields that feed the Supabase `sessions` pointer row

| Pointer column | Source | Notes |
| -------------- | ------ | ----- |
| `workspace_id`, `machine_id`, `harness`, `session_id` | `session.*` | Must match the key's path segments. A mismatch is rejected as corrupt. |
| `started_at`, `ended_at` | `session.started_at`, `session.ended_at` | |
| `state` | `state` | |
| `trace_id` | `trace_id` | |
| `manifest_key` | the event's object key | |
| `manifest_revision` | `revision` | The upsert fencing token. |
| `manifest_etag` | object version read by the handler | S3/Azure ETag or GCS generation, as an opaque string. Not a manifest field, because an object cannot contain its own ETag. |
| `raw_bytes` | `raw.raw_bytes` | |
| `stored_bytes` | `raw.stored_bytes` | Optional column, for billing/quotas. |
| `chunk_count` | `raw.chunk_count` | |
| `raw_epoch` | `raw.epoch` | |
| `content_sha256` | `raw.content_sha256` | Lets the transform skip unchanged content. |
| `normalized_version` | **not from this manifest** | Set by the transform step when it finishes writing `normalized/v{N}/` for a given `content_sha256`. Keeping it out of the client manifest keeps the manifest single-writer: the server never CAS-races the client. The transform records its own output in `normalized/v{N}/manifest.json`, which is out of scope for this doc. |

## Example

A Claude Code session at revision 3, with three chunks in one epoch. The
digest values (`sha256`, `stored_sha256`, `content_sha256`) are illustrative; the sizes and offsets are consistent.

```json
{
  "schema_v": 1,
  "layout": "v1",
  "trace_id": "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18",
  "revision": 3,
  "created_at": "2026-10-25T18:02:11.204Z",
  "updated_at": "2026-10-25T18:41:07.990Z",
  "session": {
    "workspace_id": "ws_7f3a2c",
    "machine_id": "mach-linuyc-desktop",
    "harness": "claude",
    "session_id": "4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718",
    "started_at": "2026-10-25T18:01:58.000Z",
    "harness_version": "2.4.1",
    "cwd_sha256": "9b0e4c8a1d2f3e4a5b6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8f9a0b1c2"
  },
  "writer": {
    "delivery_instance_id": "dest_objstore_01",
    "plugin_version": "0.1.0",
    "host_version": "2026.10.60"
  },
  "state": "open",
  "raw": {
    "format": "jsonl",
    "compression": "zstd",
    "chunk_count": 3,
    "raw_bytes": 9437184,
    "stored_bytes": 1873402,
    "content_sha256": "5d1c0b7e2a9f4c3d8e6b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a291807",
    "next_offset": 9437184,
    "epoch": 0,
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
        "records": 1288,
        "first_ts": "2026-10-25T18:01:58.000Z",
        "last_ts": "2026-10-25T18:19:40.512Z"
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
        "records": 1102,
        "first_ts": "2026-10-25T18:19:41.007Z",
        "last_ts": "2026-10-25T18:33:12.380Z"
      },
      {
        "seq": 2,
        "epoch": 0,
        "start_offset": 8388608,
        "bytes": 1048576,
        "stored_bytes": 215834,
        "sha256": "c5b8e0f2a4d6c8e0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c4e6b8d0f2a4c6e8",
        "sha256_8": "c5b8e0f2",
        "stored_sha256": "2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f",
        "compression": "zstd",
        "records": 311,
        "first_ts": "2026-10-25T18:33:13.004Z",
        "last_ts": "2026-10-25T18:41:06.771Z"
      }
    ]
  }
}
```

The chunk keys for this manifest are:

```text
v1/ws=ws_7f3a2c/m=mach-linuyc-desktop/h=claude/d=2026/10/25/s=4f1e9b2a-6c3d-4e8f-a1b2-c3d4e5f60718/raw/0000000000-0-a3f19c0e.jsonl.zst
.../raw/0000000001-4194304-7e24b6d1.jsonl.zst
.../raw/0000000002-8388608-c5b8e0f2.jsonl.zst
```

## Size and limits

- Each chunk entry is about 450 bytes, so a 10,000-chunk session (~40 GiB raw)
  has a ~4.5 MiB manifest. That is acceptable for v1. If sessions grow past
  that, `schema_v: 2` would page the chunk list into immutable
  `chunks/{n}.json` segments and keep only the tail inline. That would be a
  layout addition, not a rewrite.
- The manifest is plain JSON (`application/json`, not compressed), so `fetch`
  and debugging need no zstd step. The manifest's own `trace-id` and
  `content-sha256` object metadata are set like any other object's.
- Unknown fields: readers ignore unknown top-level and `extensions` keys. A
  reader that sees a higher `schema_v` major version refuses the manifest and
  does not guess.
