# Selected-session upload verification

Measured on Apple M2 Max, debug Rust build, 2026-09-21. Fixtures and receivers
are synthetic; no real history or live uploads are involved. Timings exclude
fixture construction and filesystem orchestration. The selected session has one
fixed event and is placed both before and after unrelated sessions.

| Unrelated sessions | Original inclusion | Probe-owned inclusion | Original first preparation | Probe-owned first preparation |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 4.82–4.84 ms | 0.389–0.392 ms | 36.34–36.57 ms | 1.244–1.262 ms |
| 10,000 | 428–434 ms | 0.391–0.863 ms | 1.57–22.13 s | 1.231–1.519 ms |
| 50,000 | 2.09–2.16 s | 0.416–0.429 ms | 7.90 s / >30.97 s | 1.396–1.695 ms |

The original late 50,000-session case exhausted its preparation budget without a
batch. Every final case performs 17 SQLite row changes, 1,416 inclusion VM
instructions, 3,638 preparation VM instructions and one payload visit. These
counts include the atomic inclusion API’s indexed exclusion lookup, generic
subscription coordination and persisted member fairness.
The fake receiver stores and acknowledges the actual prepared batch.

Reproduce from this repository:

```sh
TMPDIR=/private/tmp cargo test --manifest-path plugins/relayhistory/rust/Cargo.toml \
  --test durable_delivery scoped_backfill_has_constant_unrelated_record_visits \
  -- --nocapture --test-threads=1
TMPDIR=/private/tmp node scripts/benchmark-sync.mjs --gate
```

The cold sync/hydration gate passes all ten checks without changed thresholds.
The merged PR had failed Linux CI's cold-sync floor (180.7 records/s versus
239 required). A local sampling profile found approximately 68% of samples in
SQLite statement preparation, repeatedly compiling capture triggers for each
insert. Caching the six hot ingestion statements preserves the SQL and live
trigger predicates. A regression test warms the cache before a subscription,
then checks that capture starts and stops with that subscription.

On the same Apple M2 Max debug-build fixture (2,846 records), the two-round
benchmark reported 4,670 ms / 609 records/s before and 1,375 ms / 2,070 records/s
after (3.4× throughput). Cold hydration improved from 1,026 ms to 186 ms.
Concurrent local builds make these timings indicative; the unchanged Linux CI
gate remains the release check. These measurements concern evidence ingestion;
metadata discovery and selected-session measurements are separate.

Verification also covers legacy jobs opened by core before the probe restarts,
exact prepared bytes, leased/retry/paused/blocked queues, unread snapshot
preimages, tombstones, stale acknowledgments, child re-inclusion, and membership
fairness across restart. Local export creates no upload tables. A build without
export support refuses destructive migration under an unread subscription.
Compaction examines a bounded range and advances past retained rows.

The historical 77-/86-second capture failures cannot be diagnosed from the old
logs, which discarded the causes. New diagnostics record allowlisted categories
without provider errors, paths, credentials or session contents.
