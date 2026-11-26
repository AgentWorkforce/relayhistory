# W1 local retention measurements

Measured on 2026-10-25 on Apple M3, Rust 1.94.0, release profile. Baseline is
`d7ed1560`; changed code is `01085a94`. Measurements were repeated after local
verification finished. Ordinary desktop processes remain running.

The fixture seeds 2,000 synthetic catalog sessions. Each run warms up 50
iterations and measures 500 cached list calls and inserts followed by listing
the new session. Five baseline/changed runs alternate order. The table reports
the median of each run's percentile, in milliseconds.

| Operation | Version | P50 | P95 | P99 |
| --- | --- | ---: | ---: | ---: |
| list50 | before | 0.2065 | 0.2282 | 0.2537 |
| list50 | after | 0.2112 | 0.2257 | 0.2550 |
| write_to_list1 | before | 0.3654 | 0.4688 | 1.1169 |
| write_to_list1 | after | 0.3632 | 0.4642 | 1.1255 |

Cached-list P50 changes by +0.0047 ms in this fixture.
Its SQL performs no retention or acknowledgement lookup. Timings vary with host load and do not guarantee a deadline.

## Concurrent maintenance

Two changed-code runs start a maintenance worker with default pacing and
upload acknowledgements. The worker yields/backoffs on busy/deferred reports.

| Run | Catalog sessions evicted | list50 P95 | list50 P99 | write-to-list P95 | write-to-list P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 | 0.3047 | 0.5228 | 0.5785 | 5.9701 |
| 2 | 2 | 0.2946 | 0.3738 | 0.5812 | 1.8002 |

This catalog-only fixture does not measure payload cleanup throughput.
Retention tests separately exercise message/prompt/marker cleanup and WAL reads
completing before a held pruning transaction releases its writer lock.

## Additional index cost

The review fix adds ten session/revision covering indexes and two history
activity indexes to keep validation bounded under the writer lock. Five further
alternating pairs run the same changed binary with these twelve indexes present
or removed from its own synthetic database. The control opens the store read-only
to prevent recreating them. Each measured write atomically inserts a catalog
session, event, tool call and prompt, then lists the new session. This comparison
isolates index cost on the same implementation; it is not a parser benchmark.

| Index configuration | Write-to-list P50 | P95 | P99 |
| --- | ---: | ---: | ---: |
| without_indexes | 0.8849 | 1.7149 | 2.9848 |
| with_indexes | 1.0181 | 2.2232 | 3.8547 |

Regression tests seed 30,000 acknowledged events, 30,000 nullable tool calls and
30,000 prompts. Each actual upload-validation/latest-activity query uses fewer
than 100 SQLite VM steps. Separate tests bound anonymous selection before its
acknowledgement filter and seek past 30,000 pre-existing tombstones. Continuity
reopening remains bounded by the removed edge batch. These operation counts do
not bound filesystem latency or the size of one payload row.

## Index footprint and initial construction

Measured against merged SDK code `63d61c9a` on Apple M3 in
release mode, using three fresh synthetic stores. The fixture has 1,000 sessions
with 36-byte session identities, 30,000 events, 10,000 tool calls, 5,000 edits,
2,000 markers, 6,000 linked and 4,000 anonymous prompts, 1,000 observations,
5,000 observation-evidence rows, 1,000 commit links and 1,000 relationships.
Event text is 128 bytes and prompts are 64 bytes; this is not a representative
payload-size distribution for every harness.

| Measurement | Result |
| --- | ---: |
| Allocated database pages before indexes | 7,432 × 4,096 bytes (29.03 MiB) |
| Allocated pages after all twelve indexes | 8,393 × 4,096 bytes (32.79 MiB) |
| Added index storage, identical across three runs | 3,936,256 bytes (3.75 MiB; +12.93%) |
| Index DDL plus transaction commit, median | 382.53 ms |
| Index DDL plus commit, range | 304.24–405.79 ms |
| Full SDK writable upgrade open, median | 488.45 ms |
| Full writable upgrade open, range | 428.22–785.47 ms |

SQLite `dbstat` independently attributes all 3,936,256 added bytes to
the twelve index B-trees. The event revision index is 1568 KiB;
per-index sizes and construction times are in the
[raw footprint data](retention-index-footprint-results.json).

For each run, the harness drops only its temporary store's twelve retention
indexes, seeds the fixture, compacts it and checkpoints its WAL. It copies that
baseline for a timed `open_db` upgrade, then separately times the twelve captured
index definitions in one transaction on the seeded connection. Both paths assert
that the evidence clock does not advance. Page counts and `dbstat` measure stored
index pages; the final file is checkpointed for comparison. Seeding, compaction
and checkpoint time are excluded from the build timers. The footprint does not
measure peak WAL or temporary-sort storage, or process memory.

These are one-time database construction costs. Writable migration holds the
SQLite writer lock while building the indexes, so initial upgrade can delay
other writers. It belongs off the UI thread. This timing is not the steady-state
session visibility measurement or a 5 ms guarantee. The percentage depends on
row counts, identity lengths and payload sizes; larger real evidence bodies can
change it substantially. Timings use warmed fixture/OS caches with ordinary
desktop processes running and are not cold-disk guarantees.

## Measurement limits

The 5 ms goal remains open: one concurrent catalog run has a 5.9701 ms P99,
and an individual with-index payload run has a 9.5506 ms P99 even though its
five-run median is 3.8547 ms. These runs do not establish a strict 5 ms gate.
W0 owns the complete capture/desktop visibility gate; these are local SQLite
measurements.

An earlier attempt overlapped another `npm ci`, while the disk had about 4.6 GiB
free (99% used). Baseline and changed catalog write-to-list P99 were 20.8988 ms
and 19.3549 ms. That attempt cannot establish an idle-machine latency result.
The raw data includes that attempt under `loaded_attempt`.

## Reproduce

```sh
cargo test -p ai-hist --test retention_perf --features unstable-internal --release -- --ignored --nocapture
RETENTION_PERF_PRUNE=1 cargo test -p ai-hist --test retention_perf --features unstable-internal --release -- --ignored --nocapture
RETENTION_PERF_PAYLOAD=1 cargo test -p ai-hist --test retention_perf --features unstable-internal --release -- --ignored --nocapture
RETENTION_PERF_PAYLOAD=1 RETENTION_PERF_DROP_INDEXES=1 cargo test -p ai-hist --test retention_perf --features unstable-internal --release -- --ignored --nocapture
cargo test -p ai-hist --test retention_index_footprint --features unstable-internal --release -- --ignored --nocapture
```

The baseline uses the same catalog fixture, timers, warmup and sample counts,
with the optional maintenance worker omitted because its API does not exist
there. Compile both binaries before alternating runs.

See [raw run data](retention-benchmark-results.json) and the
[integration contract](retention.md).
