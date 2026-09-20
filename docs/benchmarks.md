# Native-path benchmarks

Run benchmarks from the repository root against the default RelayHistory
database:

```bash
npm run benchmark
```

Benchmarks require a release build of the native addon and refuse a debug one
(`npm run build --prefix crates/ai-hist-napi` rebuilds it as release).

Write the report to a file with:

```bash
npm run benchmark -- --output=foo.md
```

Show a compact terminal table with only the benchmark name and milliseconds:

```bash
npm run benchmark -- --pretty
```

`--pretty` can be combined with `--output` to save the full report while
showing the compact table in the terminal.

The output format follows the extension: `.md` writes a Markdown report and
any other extension writes JSON. Omit `--output` to print JSON to stdout. npm
requires the `--` separator before benchmark-specific options.

Useful overrides:

```bash
npm run benchmark -- --output=foo.md --db=/path/to/ai-history.db
AI_HIST_DB=/path/to/ai-history.db npm run benchmark -- --output=foo.md
```

## Targeted hydration benchmark

Select five recent local catalog sessions and measure the first hydration plus
five unchanged checkpoint hits for each:

```bash
npm run benchmark:hydration -- --pretty
```

The selector prefers shallow sessions, rotates across providers, and never
prints transcript content or provider paths. It reads only the cached catalog
while selecting. Hydration itself updates the configured RelayHistory database,
so use `--db` to choose the intended catalog. Provider source files and stores
are opened read-only.

Useful controls:

```bash
npm run benchmark:hydration -- --count=10 --iterations=20 --source=claude --source=codex
npm run benchmark:hydration -- --output=hydration.md --include-related
```

`--count` sets the number of selected sessions, `--iterations` sets the number
of unchanged calls after each first call, repeated `--source` values constrain
selection, and `--include-related` includes linked child evidence. Without that
flag, related-session work is disabled so provider comparisons stay bounded.
The JSON and Markdown reports record each session's prior discovery state,
first-call status and latency, unchanged p50/p95 latency, evidence counts, and
provider diagnostic work counters. Only repeat calls that actually return
`unchanged` contribute to unchanged percentiles, so a live provider update
during the run cannot be mislabeled as a checkpoint hit. A first call can
report `unchanged` when no shallow session remains for that provider; the
report preserves that status rather than describing it as cold.

## Benchmark definitions

Discovery benchmarks use a generated home directory containing 1,000 valid,
minimal Claude session transcripts and an OpenCode SQLite store
(`.local/share/opencode/opencode.db`) holding 1,000 minimal sessions with one
message and one part each. Setup, fixture creation, and source-file changes
happen outside the timed regions. The fixture makes each run repeatable and
lets the changed-source case avoid modifying real provider history. The
opencode store is written with `node:sqlite`, so benchmarks need Node 22.13+.

Unprefixed discovery benchmarks read the Claude fixture; the
`opencode`-prefixed ones read the opencode fixture through the same
SDK -> N-API -> Rust path. The number at the end of a benchmark name is the
requested session or event limit, not a cache size.

| Benchmark | Setup outside timing | Timed work |
|---|---|---|
| `cold shallow discovery N` | Fresh fixture; database does not exist | Create/migrate the database, enumerate the 1,000 candidates, shallow-read the newest N files, and upsert N catalog rows through SDK -> N-API -> Rust |
| `unchanged shallow discovery N` | Run one cold discovery of N into a fresh database | Enumerate candidates, match source stamps, and return N cached rows without opening unchanged transcripts |
| `cold->changed shallow discovery N` | Run one cold discovery of N, then append a valid assistant record to those N fixture transcripts | Enumerate candidates, detect changed stamps, shallow-read the N changed files, and update their catalog rows |
| `opencode cold shallow discovery N` | Fresh RelayHistory catalog; provider fixture already exists | Open one live read-only transaction, fetch at most N candidates, run indexed session-keyed prompt/model queries, and upsert N catalog rows |
| `opencode unchanged shallow discovery N` | Run one cold discovery of N into a fresh catalog | Open a new coherent read snapshot, fetch at most N candidates, match source stamps, and return N cached rows without message/part queries |
| `opencode cold->changed shallow discovery N` | Run one cold discovery of N, then insert an assistant message and bump `time_updated` for those N sessions | Open the live store read-only, detect the N changed stamps, run selected-session queries, and update those catalog rows |
| `warm session events N` | Select a full session from the configured real database and make one untimed 200-event request | Open the database and return up to N cached events through SDK -> N-API -> Rust; provider transcripts are not read |
| `CLI startup + cold shallow discovery 20` | Fresh database and the generated fixture | Start a new Node process, load the CLI and native addon, then perform cold discovery of 20 sessions and serialize JSON |
| `MCP cold shallow discovery 20` | Start and initialize the MCP server with a fresh database and the generated fixture | Perform one MCP `tools/call` round trip that cold-discovers 20 sessions; MCP process startup and initialization are excluded |

The report includes the real database file size used by the event benchmarks,
so a database larger than 2 GiB can demonstrate that event-query cost follows
the requested page rather than total database bytes.

For a sparse validation fixture, copy a real migrated database and extend the
file sparsely on a filesystem that supports sparse files. Do not run this test
in ordinary CI; use the opt-in `AI_HIST_LARGE_DB` path and verify catalog and
event queries against it.

## 2026-08-31 baseline

Measured on macOS arm64 (Apple M2 Max) with Node.js 22.22.2 against a real
3,151,933,440 byte RelayHistory database with an active WAL:

| Operation | Time | Rows/work |
|---|---:|---:|
| cold shallow discovery | 9.19 ms | 20 rows |
| cold shallow discovery | 10.29 ms | 100 rows |
| cold shallow discovery | 47.67 ms | 1,000 rows |
| unchanged shallow discovery | 4.72 ms | 20 rows; zero files opened |
| unchanged shallow discovery | 5.69 ms | 100 rows; zero files opened |
| unchanged shallow discovery | 17.69 ms | 1,000 rows; zero files opened |
| cold->changed shallow discovery | 5.77 ms | 20 changed rows |
| cold->changed shallow discovery | 9.43 ms | 100 changed rows |
| cold->changed shallow discovery | 55.22 ms | 1,000 changed rows |
| opencode cold shallow discovery | 6.94 ms | 20 rows |
| opencode cold shallow discovery | 8.18 ms | 100 rows |
| opencode cold shallow discovery | 43.76 ms | 1,000 rows |
| opencode unchanged shallow discovery | 3.18 ms | 20 rows; zero shallow reads |
| opencode unchanged shallow discovery | 3.33 ms | 100 rows; zero shallow reads |
| opencode unchanged shallow discovery | 12.20 ms | 1,000 rows; zero shallow reads |
| opencode cold->changed shallow discovery | 3.90 ms | 20 changed rows |
| opencode cold->changed shallow discovery | 7.29 ms | 100 changed rows |
| opencode cold->changed shallow discovery | 60.46 ms | 1,000 changed rows |
| warm session events | 0.61 ms | 20 rows |
| warm session events | 1.53 ms | 200 rows |
| CLI startup + cold shallow discovery | 46.73 ms | 20 rows |
| MCP cold shallow discovery | 16.23 ms | 20 rows |

The event queries did not load or copy the 2.9 GiB database: Rust opened it
directly and returned a bounded indexed page. The OpenCode numbers in this
2026-08-31 table are the historical pre-live-query baseline; the implementation
no longer creates a SQLite backup or reports the provider database size as
`bytesRead`.

## 2026-09-01 OpenCode fixed-limit scaling

Run only this benchmark with:

```bash
cargo test -p ai-hist-cli --test discovery_bench \
  opencode_fixed_limit_scaling_report -- --ignored --nocapture
```

The two WAL-mode fixtures contain the same newest 20 sessions. The larger one
adds 9,000 unrelated sessions and grows unrelated message/part history from
roughly 4.9 MB to 49.3 MB. A writer commits concurrently throughout each
measurement. These debug-build wall clocks are supporting evidence; the
operation counts and query-plan assertions are the acceptance gates.

| Provider store | Median cold discovery, limit 20 | Candidates | Provider queries | Records returned by provider SQL | SQLite bytes claimed |
|---:|---:|---:|---:|---:|---:|
| 1,000 unrelated sessions (4.9 MB) | 3.263 ms | 20 | 41 | 60 | 0 |
| 10,000 unrelated sessions (49.3 MB) | 3.332 ms | 20 | 41 | 60 | 0 |

Representative query plans:

```text
candidate: SCAN session USING INDEX session_time_updated_id_idx
prompt:    SEARCH p USING INDEX part_session_idx (session_id=?)
           SEARCH m USING INDEX sqlite_autoindex_message_1 (id=?)
```

The selected-session assertions fail if `message` or `part` regresses to a
full table scan. The prompt query may use a temporary B-tree to order the few
parts belonging to the selected session; it never sorts unrelated history.

## Sync and hydration throughput

The tables above cover shallow discovery and warm catalog reads — the paths
that deliberately do not open a transcript. This section covers the write path:
a full `sync`, the incremental `sync` a `watch` tick performs, and
`hydrate_session`. It exists because `burn`'s `ingest --watch` runs on one-second
ticks and `burn summary --ingest` inherits whatever full ingestion costs, so the
cost has to be a published number rather than an impression.

Three pieces:

| File | Role |
|---|---|
| `scripts/gen-synthetic-history.mjs` | Fabricates a deterministic Claude/Codex/Cursor/Grok (and, on Node 22.13+, OpenCode) store of a requested size from a seed. |
| `crates/ai-hist-cli/tests/sync_bench.rs` | The `#[ignore]`d harness that runs one phase and reports what it cost. |
| `scripts/benchmark-sync.mjs` | Generates the store, runs each phase in its own process, renders the table, and applies `scripts/benchmark-thresholds.json`. |

```bash
# the fast subset, exactly as CI runs it on every pull request
node scripts/benchmark-sync.mjs --gate

# the published matrix (release build, 100 MB store)
cargo test --workspace --all-features --test sync_bench --release --no-run
node scripts/benchmark-sync.mjs --profile full --output sync-bench.md

# one store on disk, to poke at by hand
node scripts/gen-synthetic-history.mjs --out /tmp/bench-home --target-bytes 104857600
```

Nothing real is read and nothing generated is committed. The record shapes are
modelled on what the parsers in `crates/ai-hist/src/ingest.rs` accept — a Claude
turn is a user prompt, an assistant record carrying `thinking`/`text`/`tool_use`
blocks, and a user record carrying the matching `tool_result` blocks — so the
corpus exercises every row the hot loop writes. A seed plus a byte target
reproduces a store exactly; session count is an outcome of the target, not an
input.

### The phases

| Phase | Setup outside timing | Timed work |
|---|---|---|
| `cold_sync` | Generated store; the database does not exist | Create and migrate the database, walk every provider location, and ingest every transcript |
| `incremental_sync` | A completed `cold_sync`; one 1 KiB assistant record appended to one Claude transcript | Walk every provider location, match stamps, re-read the one changed file, and ingest its new record |
| `unchanged_sync` | A completed `incremental_sync` | The `watch` tick with nothing changed: walk every location, match every stamp, write nothing |
| `hydrate_cold` | A completed sync, so the session is in the catalog | `hydrate_session` on the largest transcript in the store: full parse, evidence written |
| `hydrate_unchanged` | One untimed `hydrate_session` on the same session | The checkpoint hit: stamp matches, nothing is re-parsed |
| `calibration` | A fixed 120-file tree in a temp directory | The reference workload — walk and read the tree, parse JSON, insert into a WAL+FTS5 database, checkpoint. Touches nothing the other phases touch; see "Why the gate is not machine-dependent" |

`records` is the exact row delta across `history`, `session_events`,
`tool_calls`, `file_edits` and `sessions`, counted from the database before and
after — not a parser's own estimate.

The oversized transcript the hydration phases target belongs to the first
source `--sources` names, not always to Claude: slipping a Claude transcript
into a store that did not ask for one would put a whole provider into the
ingested byte count while the report still called the run codex-only.
`incremental_sync` is the one phase that does require Claude, because the
harness appends a Claude-shaped record and no other provider has an equivalent
yet.

**`--phases` is an ordered list, not a set.** Every phase but `cold_sync` reads
a database `cold_sync` created, and `cold_sync` itself insists on a database
that does not exist yet, so `--phases hydrate_cold` or
`--phases incremental_sync,cold_sync` cannot be measured at all. The driver
refuses such a list by name, and it refuses twice:

* against the **request**, before anything is generated — a phase whose setup
  the order cannot provide, a phase listed twice, a provider the plan never
  asked for;
* against the **generated manifest**, before the harness is even built —
  because a source appearing in `--sources` is not a promise that a session of
  it was written. When the oversized session alone already meets the byte
  target, the round-robin loop never runs, so `--sources codex,claude` can
  produce a store containing no Claude transcript. Only the manifest knows.

Both refusals name the phase and what it lacked, which is the point: an
unmeasurable combination should cost a second and a sentence, not a compile
followed by a panic inside the timed region. For the hydration phases it is the
`records_parsed` the hydration diagnostic reports.

### How peak RSS is measured

`getrusage(RUSAGE_SELF).ru_maxrss`, read inside the harness at the end of the
phase. **Linux reports kibibytes and macOS reports bytes**; the harness
normalizes both to bytes, and that one line is the whole cross-platform story.
No `/usr/bin/time` is involved — it is absent from some Linux images entirely,
and its `-v` (GNU) and `-l` (BSD) output formats do not agree.

Two consequences worth knowing before trusting a number:

* Linux does **not** reset `ru_maxrss` across `execve`. A harness launched
  through `cargo test` therefore inherits cargo's own high-water mark and
  reports roughly 55 MiB for every phase. The driver builds the harness once
  with `cargo test --no-run` and then executes the test binary directly, which
  is why its numbers are a tenth of that. A phase run by hand under
  `cargo test` produces a valid report whose `peakRssBytes` is cargo's, and it
  must not be compared against a driver-produced baseline.
* Windows has no `getrusage`; the field is `null` there and the gate does not
  run on Windows.

`bytesRead` and `readSyscalls` are the `rchar` and `syscr` deltas from
`/proc/self/io`, which count every byte the process received from a `read`,
page cache included. macOS has no unprivileged equivalent, so those two columns
are empty there rather than filled with a guess.

### The CI gate

`node scripts/benchmark-sync.mjs --gate` runs in the `verify` job of `ci.yml` on
every pull request, immediately after `cargo test --workspace --all-features` so
that the harness is already built and the step spends its time measuring. It
uses the `ci-debug` profile: an unoptimized build against a 1.25 MiB store, two
rounds per phase, the faster round reported. It takes roughly 15 s on a 4-core
machine — well inside the 60 s budget — and the `full` matrix runs separately on
`workflow_dispatch` through `.github/workflows/benchmark-sync.yml`.

#### Why the gate is not machine-dependent

A throughput floor recorded on one machine says nothing on another, and a shared
runner under load can be several times slower than the same runner idle — which
is exactly the shape of the regression the gate is looking for. Measured
directly, the first version of this gate went red on a developer box simply
because a neighbouring build was running.

So every run measures a **calibration phase** beside the real ones: a fixed
reference workload built from the same materials as the code under test — walk
and read a small file tree, parse JSON, insert into a WAL-mode SQLite database
with an FTS5 index, commit periodically, checkpoint. Throughput and elapsed
checks are then scaled by `stored_calibration / measured_calibration` before
being compared, so what the gate compares is "what this phase would have cost on
the machine the baseline came from". Peak RSS is not scaled: memory does not
grow because the box is busy.

The calibration deliberately touches neither the synthetic store nor the
benchmark database and shares no code with the ingestion path, so a real
slowdown in `sync` or `hydrate_session` cannot normalize itself away. A 3×
slowdown patched into `ingest_claude_transcript_as` leaves the calibration at
1.00× and turns three checks red; the same 3× applied to the whole machine
leaves every check green. Both cases are covered by
`scripts/benchmark-sync.test.mjs`.

The factor is clamped to 0.2×–8×. A calibration outside that range is evidence
that the measurement is wrong, not a licence to normalize an arbitrary amount of
regression away, and the gate says when it clamped.

Bounds come from `scripts/benchmark-thresholds.json`:

* throughput must stay at or above **half** its stored baseline (a > 2×
  regression in records/s is red),
* peak RSS must stay at or below **1.5×** its stored baseline,
* elapsed time on the phases where records/s is meaningless (an unchanged tick
  parses nothing) must stay at or below **2×**.

A ceiling derived from a very small baseline measures the runner rather than the
code, so `policy.absoluteFloors` can raise a ceiling — never lower one, and
never a throughput floor. On the PR store the peak-RSS check is consequently a
blow-up guard: it catches "the transcript is now buffered whole" and not a 1.5×
drift. The proportional rule bites on the `full` profile, whose store is two
orders of magnitude larger.

A phase that a profile names but that produced no measurement is a failure, not
a skip. A run that measured nothing must not read as a pass.

Baselines are re-measured, never nudged:

```bash
node scripts/benchmark-sync.mjs --profile ci-debug --update-baselines
```

Re-measuring records the commit, machine and toolchain it came from in the
thresholds file, and the pull request has to say why the number moved.

### 2026-09-19 baseline

Measured on commit `2a1e81a`, before any of the parity work in #160 lands, so
those pull requests can show a before and after. Release build
(`rustc 1.98.1`), Linux x86_64, Intel Xeon @ 2.80 GHz, 4 cores, 15.7 GiB RAM,
Node 22.22.2. Synthetic store, seed 176. Commands:

```bash
cargo test --workspace --all-features --test sync_bench --release --no-run
# rows 1, 3, 4 and the 1 MB hydration rows
node scripts/benchmark-sync.mjs --profile full --large-session-bytes 1048576
# the 50 MB and 200 MB hydration rows
node scripts/benchmark-sync.mjs --profile full --target-bytes 1048576 \
  --large-session-bytes 52428800 --phases cold_sync,hydrate_cold,hydrate_unchanged
node scripts/benchmark-sync.mjs --profile full --target-bytes 1048576 \
  --large-session-bytes 209715200 --phases cold_sync,hydrate_cold,hydrate_unchanged
```

Rows 1–7 were measured at `2a1e81a` and rows 8–9 at `3672810`; neither commit on
this branch touches the ingestion or hydration path, so the numbers describe
`main`'s behaviour either way.

| Operation | Source | Time | Records | Records/s | MB/s | Bytes read | Peak RSS | DB | WAL |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cold `sync`, 100 MB store (4,399 files, 3,518 sessions) | 100.3 MiB | 513.93 s | 172,218 | 335 | 0.20 | 47.1 GiB | 20.9 MiB | 162.1 MiB | 0 B |
| cold `sync`, 1 GB store | — | not measured; see below | — | — | — | — | — | — | — |
| incremental `sync`, 1 KiB appended to one transcript | 1,023 B | 10.85 s | 1 | — | — | 1.7 GiB | 21.7 MiB | 162.1 MiB | 0 B |
| `watch` tick, nothing changed | 0 B | 11.64 s | 0 | — | — | 1.7 GiB | 20.5 MiB | 162.1 MiB | 0 B |
| `hydrate_session`, 1 MB transcript, cold | 1,020.7 KiB | 1.41 s | 720 | 512 | 0.74 | 17.7 MiB | 12.4 MiB | 162.4 MiB | 0 B |
| `hydrate_session`, 1 MB transcript, unchanged | 1,020.7 KiB | 7.8 ms | 720 | 91,845 | 133.32 | 13.5 MiB | 12.4 MiB | 162.4 MiB | 0 B |
| `hydrate_session`, 50 MB transcript, cold | 50.0 MiB | 70.48 s | 36,054 | 512 | 0.74 | 364.1 MiB | 59.6 MiB | 116.2 MiB | 0 B |
| `hydrate_session`, 50 MB transcript, unchanged | 50.0 MiB | 135.4 ms | 36,054 | 266,198 | 387.38 | 120.4 MiB | 57.4 MiB | 116.2 MiB | 0 B |
| `hydrate_session`, 200 MB transcript, cold | 200.3 MiB | 335.48 s | 144,216 | 430 | 0.63 | 1.6 GiB | 209.9 MiB | 463.9 MiB | 0 B |
| `hydrate_session`, 200 MB transcript, unchanged | 200.3 MiB | 518.0 ms | 144,216 | 278,405 | 405.46 | 481.3 MiB | 206.5 MiB | 463.9 MiB | 0 B |

The 50 MB and 200 MB hydration rows were measured against a store holding that
one transcript and nothing else, so their `DB` column is that store's database
and not the 100 MB store's. Their cold syncs, for reference: 50 MB in 192.89 s
(127,129 rows, 659 rows/s, 858.6 MiB read, 59.4 MiB peak RSS) and 200 MB in
798.50 s (508,294 rows, 637 rows/s, 3.7 GiB read, 209.4 MiB peak RSS).

`MB/s` is decimal megabytes of provider source per second, so it is directly
comparable to a transcript's size on disk. An `incremental sync` row writes one
row and a `watch` tick writes none, so records/s is meaningless for them and is
left blank rather than reported as zero.

**The 1 GB cold-sync row is run on demand.** At the rate above, 1 GB is several
hours of wall clock and a multi-gigabyte database, which is more than the
machine this was measured on had spare. Dispatch
`.github/workflows/benchmark-sync.yml` with `target_bytes: 1073741824` to fill
it in, or run the `full` profile locally with that override.

### What the baseline says

Four things worth carrying into the parity work, all read off the table rather
than inferred:

1. **A `watch` tick with nothing changed costs 11.6 s against a 100 MB store**,
   and reads 1.7 GiB to decide that nothing changed. burn's `ingest --watch`
   runs on one-second ticks; relayhistory's full `sync` cannot serve that
   directly at this size. Shallow discovery, whose unchanged case is measured
   in milliseconds in the tables above, is a different path.
2. **An incremental sync costs essentially the same as an unchanged one**
   (10.85 s vs 11.64 s). The cost is the walk and the stamp comparison, not the
   one changed file. That is where the incremental-cursor work has room.
3. **Cold sync reads far more than the store.** 47.1 GiB read for a 100 MB
   store across 12.3 million read syscalls — roughly 470× the corpus. The store
   is read once; the rest is the growing database being paged back in.
4. **Hydration's peak RSS tracks the transcript.** 12.4 MiB for 1 MB,
   59.6 MiB for 50 MB, 209.9 MiB for 200 MB — the transcript is read whole
   (`fs::read_to_string`), so the memory ceiling for hydration is the largest
   transcript, not a bounded window. Full sync shows the same shape: 20.9 MiB
   against 4,399 small files, 209.4 MiB against one 200 MB file.

Throughput itself is flat in the size of the corpus — 512 rows/s cold hydration
at both 1 MB and 50 MB, 430 at 200 MB — so none of the above is quadratic in
records. The unchanged hydration path does what it claims: 7.8 ms, 135 ms and
518 ms for 1 MB, 50 MB and 200 MB, against 1.41 s, 70.5 s and 335.5 s cold.

None of this is optimized here. Recording it is the point: the parity issues in
#160 can now show a before and an after.
