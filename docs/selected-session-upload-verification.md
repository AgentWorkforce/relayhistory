# Selected-session upload verification

Measured on Apple M2 Max, debug Rust build, 2026-09-21. Fixtures and receivers
are synthetic; no real history or live uploads are involved. Timings exclude
fixture construction and filesystem orchestration. The selected session has one
fixed event and is placed both before and after unrelated sessions.

| Unrelated sessions | Original inclusion | Probe-owned inclusion | Original first preparation | Probe-owned first preparation |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 4.82–4.84 ms | 0.357–0.384 ms | 36.34–36.57 ms | 1.217–1.296 ms |
| 10,000 | 428–434 ms | 0.376–0.443 ms | 1.57–22.13 s | 1.223–2.335 ms |
| 50,000 | 2.09–2.16 s | 0.366–0.383 ms | 7.90 s / >30.97 s | 1.194–1.239 ms |

The original late 50,000-session case exhausted its preparation budget without a
batch. Every final case performs 17 SQLite row changes, 1,399 inclusion VM
instructions, 3,638 preparation VM instructions and one payload visit. These
counts include generic subscription coordination and persisted member fairness.
The fake receiver stores and acknowledges the actual prepared batch.

Reproduce from this repository:

```sh
TMPDIR=/private/tmp cargo test --manifest-path plugins/relayhistory/rust/Cargo.toml \
  --test durable_delivery scoped_backfill_has_constant_unrelated_record_visits \
  -- --nocapture --test-threads=1
TMPDIR=/private/tmp node scripts/benchmark-sync.mjs --gate
```

The cold sync/hydration gate passes all ten checks. Its unchanged-sync latency
was 154 ms on this machine, above the 140 ms reference ceiling but within the
configured 280 ms allowance for a different CPU class; the gate reports this as
an advisory. This throughput gate is separate from session-metadata discovery.

Verification also covers legacy jobs opened by core before the probe restarts,
exact prepared bytes, leased/retry/paused/blocked queues, unread snapshot
preimages, tombstones, stale acknowledgments, child re-inclusion, and membership
fairness across restart. Local export creates no upload tables. A build without
export support refuses destructive migration under an unread subscription.
Compaction examines a bounded range and advances past retained rows.

The historical 77-/86-second capture failures cannot be diagnosed from the old
logs, which discarded the causes. New diagnostics record allowlisted categories
without provider errors, paths, credentials or session contents.
