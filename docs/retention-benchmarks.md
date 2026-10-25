# W1 local retention measurements

Measured on 2026-10-25 on Apple M3, Rust 1.94.0, release profile. Baseline is
`d7ed1560`; changed code is `6afd8d4e`. Other local check processes had finished.

The fixture seeds 2,000 synthetic catalog sessions. Each run warms up 50
iterations and measures 500 cached list calls and inserts followed by listing
the new session. Five baseline/changed runs alternate order. The table reports
the median of each run's percentile, in milliseconds.

| Operation | Version | P50 | P95 | P99 |
| --- | --- | ---: | ---: | ---: |
| list50 | before | 0.2159 | 0.2618 | 0.3646 |
| list50 | after | 0.2259 | 0.3539 | 0.5220 |
| write_to_list1 | before | 0.3833 | 0.6068 | 1.5992 |
| write_to_list1 | after | 0.4037 | 0.7551 | 1.2821 |

The cached-list median changes by 0.0100 ms; the catalog query itself is
unchanged and performs no retention/acknowledgement lookup. Both versions' P95
listing time is below 0.4 ms in this fixture. Write-to-list P99 is 1.5992 ms
before and 1.2821 ms after.

## Concurrent maintenance

Two further changed-code runs start a maintenance worker using default pacing
and upload acknowledgements. The worker yields/backoffs on busy/deferred reports.

| Run | Catalog sessions evicted | list50 P95 | list50 P99 | write-to-list P95 | write-to-list P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 0 | 0.3354 | 0.4843 | 1.1147 | 3.0997 |
| 2 | 4 | 0.5234 | 0.9617 | 1.1798 | 1.8274 |

The first run yields to foreground writers without completing an eviction;
the second completes four catalog evictions. These catalog-only fixtures do
not measure payload cleanup throughput. The retention tests separately verify
actual message/prompt/marker cleanup, bounded restartable batches and reads
completing before a held pruning transaction releases its writer lock.

The focused write-to-list P99 is below 5 ms in these final runs. This is a
local SQLite measurement, not the complete desktop visibility gate: W0 owns
the capture/desktop integration measurement. Wall-clock percentiles are not a
hard real-time guarantee.

## Reproduce

```sh
cargo test -p ai-hist --test retention_perf --features unstable-internal --release -- --ignored --nocapture
RETENTION_PERF_PRUNE=1 cargo test -p ai-hist --test retention_perf --features unstable-internal --release -- --ignored --nocapture
```

The baseline uses the same harness with its optional maintenance-worker branch
omitted, because that API does not exist on the base commit. It uses the same
fixture, timers, warmup and sample counts. Compile both binaries first and run
them alternately after build/check processes finish.

See [raw run data](retention-benchmark-results.json) and the
[integration contract](retention.md).
