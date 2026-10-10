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


## 2026-09-20 incremental transcript hydration

Before this, a transcript that changed by one byte was re-read and re-parsed in
full. The numbers that matter are therefore about *what a pass reads*, not
about throughput: the win is asymptotic, not constant-factor.

`hydrateSession` returns `bytesRead` and the figures below come from the tests
that assert them, so they are checked on every CI pass rather than measured
once and written down.

| Pass | Bytes read |
|---|---:|
| First hydration of a transcript | the whole file |
| Append of *n* bytes, then re-hydrate | 2*n* — the metadata walk and the record walk each read the appended region |
| Codex: append of *n* bytes, then re-hydrate | *n* + one `session_meta` record from the head |
| Append to a sidecar | 2*n* for that sidecar; the parent is not read |
| Append to a sidecar beside an unchanged parent | the sidecar's *n*, and nothing for the parent |
| Deleted metadata sidecar | the transcript is re-read and the metadata it owned is cleared |
| Nothing changed | the bounded validation windows — two per cursor, at most 128 KiB each, and nothing else. Not zero: the stamp says the size and mtime have not moved, which is not the same claim as "these are the same bytes" |
| Cursor rejected (truncated, replaced, rewritten head or tail) | the whole file, with a `HYDRATION_SOURCE_ROTATED` diagnostic |
| First pass after a `HYDRATION_PARSER_VERSION` bump | the whole file, once |

Validating a cursor on open costs two seeks and at most 128 KiB, independent of
file size: `prefix_hash` covers a bounded window rather than the whole
committed prefix (see `docs/session-catalog.md`).

### Memory

Peak RSS growth over one pass, measured with
`getrusage(RUSAGE_SELF).ru_maxrss` on Linux (kilobytes there, bytes on macOS;
`peak_rss_bytes` normalizes), sampled before and after so the figure is the
growth of the process high-water mark:

| Path | 100 MB transcript | 200 MB transcript |
|---|---:|---:|
| Incremental reader alone, no rows written | 0 bytes | — |
| Incremental reader + per-record writes, autocommit | — | under 64 MiB (asserted) |
| Incremental reader inside one `Immediate` transaction | ~52 MB (≈ 0.5 × file) | ~169 MB |

The reader is O(1) in file size. The third row is the transaction, not the
reader: a single transaction spanning a whole session accumulates its own state
in proportion to what it writes. Bounding that term is chunked commits — scope
2's "commit every `JSONL_CHUNK_LINES`" — which is deferred, because hydration's
one-transaction-per-session boundary is what several existing rollback tests
rely on and splitting it is a separate change.

The 200 MB test is `#[ignore]`d, not because a CI runner cannot host the file
but because `ru_maxrss` is a per-*process* high-water mark while `cargo test`
runs the suite as threads of one process, so a neighbouring test's allocation
would make it fail for reasons that have nothing to do with the reader. Run it
alone:

```text
cargo test -p ai-hist --all-features --lib -- --ignored --exact \
  --test-threads=1 \
  ingest::hydrate::tests::reading_a_200mb_transcript_stays_under_a_memory_ceiling
```

The `bytesRead` assertions run on every CI pass at sizes that cost nothing.

The throughput baseline below was measured on `main` before this change. Its
unchanged-hydration figures now carry the bounded validation windows described
above: a fixed handful of digests per walk, each at most 128 KiB, which is what
a skip costs in exchange for not serving rows from bytes that are no longer
there.

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

`--target-bytes` is the size of the **whole** store. When `opencode` is one of
the sources its SQLite database takes a share of that budget rather than being
added on top, so two plans with the same target measure the same amount of work;
an empty OpenCode schema is already tens of kilobytes, so a very small target
overshoots and the manifest reports what actually landed. A source may not be
listed twice — each is written once per round, so a duplicate would overwrite
its own files while still being counted, leaving the store smaller than the byte
target it reports.

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

#### Baselines belong to the machines that produced them

A throughput floor recorded on one machine says nothing on another. The gate's
first design tried to bridge that with a **calibration phase** — a fixed
reference workload beside the real ones (walk and read a small file tree, parse
JSON, insert into a WAL+FTS5 database, commit periodically, checkpoint) — and
scaled every throughput and elapsed check by
`stored_calibration / measured_calibration`.

**That did not work, and the numbers say why.** Two `ubuntu-latest` runners
measured on this branch have different CPUs, and they do not agree about which
of them is faster:

| measurement | Xeon 6973P-C | Xeon Platinum 8573C | faster | spread |
|---|---:|---:|:--:|---:|
| calibration (time) | 129.0 ms | 166.8 ms | 6973P-C | 1.29× |
| `cold_sync` (rec/s) | 478.6 | 631.0 | 8573C | 1.32× |
| `incremental_sync` (time) | 423.3 ms | 272.9 ms | 8573C | 1.55× |
| `unchanged_sync` (time) | 47.1 ms | 50.8 ms | 6973P-C | 1.08× |
| `hydrate_cold` (rec/s) | 406.3 | 296.7 | 6973P-C | 1.37× |
| `hydrate_unchanged` (time) | 10.5 ms | 14.1 ms | 6973P-C | 1.34× |

The reference agrees with three phases and contradicts two — and the two it
contradicts are `cold_sync` and `incremental_sync`, precisely the checks that a
scaled comparison failed on a completely healthy runner (PR #202, job
106023782674: 479 rec/s scaled down to 320 against a floor of 321). No single
scalar can correct this, because the phases themselves disagree about which
machine is faster. It is a property of the hardware, not of the reference's
composition, and no amount of re-weighting the reference fixes it.

So the gate now works the other way round:

* **Baselines are measured on the machine class the gate runs on** — GitHub's
  `ubuntu-latest` — and each one is the worse of the observed runs: the lowest
  throughput, the longest time, the largest RSS, rounded away from the bound it
  produces. A limit set by one CPU therefore cannot fail the other, and rounding
  cannot make a limit stricter than the run it came from.
* **The 2× and 1.5× margins are the contention allowance.** They already absorb
  a busy runner; stacking a second, sometimes anti-correlated corrector on top
  made the gate worse rather than better.
* **The calibration is still measured, and still reported** — as a diagnostic.
  Set `policy.calibration` to `"applied"` in the thresholds file to restore the
  scaling; it defaults to `"diagnostic"`.

Two warnings, neither of which fails the run, exist so that a failure is not
silently read as a regression when it is really the hardware:

* the running CPU is not one of `measuredOn.cpusSeen`;
* the reference took more than `calibrationWarnBand` times as long as it did in
  the baseline runs (0.7×–1.4×, which comfortably contains the observed pool at
  0.87× and 1.13×).

Both observed runners clear every bound by at least 2.00×, and a 3× regression
turns the gate red on both — asserted against their recorded measurements in
`scripts/benchmark-sync.test.mjs`, not argued. The calibration still touches
neither the synthetic store nor the benchmark database and shares no code with
the ingestion path, so switching it back to `"applied"` cannot let a real
slowdown normalize itself away.

One wording note, because it cost a real investigation: the factor is a ratio of
*durations*, so a value below 1 means the machine was **faster**. The gate used
to print it as "0.67x its speed", which reads as slower; it now spells out both
halves.

#### A runner the baselines were never measured on

A warning was not enough. GitHub later added an **AMD EPYC 7763** to the
`ubuntu-latest` pool, and on 2026-09-21 the gate failed three pull requests on
it — #194 (run 35543384881), #199 (35543305734) and #204 (35547876206) — each
on `unchanged_sync.elapsedMs` alone, with every other check green and nothing
in the diff touching the sync path. The gate printed "a failure here may be the
hardware rather than the code", measured the reference at 1.33×–1.36×, said
"the checks below are not scaled by it", and went red anyway. An advisory that
blocks a merge is not an advisory.

So off the baseline class the bounds are now **widened**, on these rules. None
of it runs when the CPU is one of `measuredOn.cpusSeen`: the on-class gate is
byte-for-byte the gate it was.

| bound | off class |
|---|---|
| derived from a stored baseline (`baseline × factor`) | widened by the calibration ratio, clamped to `[1, offClassCalibrationCap]` |
| an elapsed ceiling set by `absoluteFloors` / `absoluteFloorsByPhase` | widened by the full cap; a breach *inside* that widening is advisory (printed, annotated, does not fail) |
| `peakRssBytes` | not widened at all |

Four things that rule deliberately does **not** do:

* **It never tightens.** A ratio below 1 says this machine is *faster* than the
  baseline machines; scaling a ceiling down on that reading is precisely the
  corrector that failed a healthy runner in #202. The clamp starts at 1.
* **It never exceeds the cap** (`offClassCalibrationCap`, 2×). On a bound that
  comes from a baseline, that puts the furthest an off-class run can go at 4×
  the baseline — on top of the policy's own 2× and 0.5× margins — so a real
  slowdown cannot hide behind a slow runner. Where the ceiling comes from a
  noise floor the same cap puts the ledge at twice that floor (280 ms for
  `unchanged_sync`, against its 140 ms ceiling), and past it the check fails
  off class as well. Each of the three recorded off-class runs, tripled, is
  red off class; so is a `watch` tick that takes 1,020 ms. Both are asserted in
  `scripts/benchmark-sync.test.mjs` rather than argued.
* **It does not scale memory.** RSS does not grow because the CPU is slower,
  and the 64 MiB floor is a blow-up guard that means the same thing everywhere.
* **It does not scale a noise floor by a CPU reading.** Where a ceiling comes
  from `absoluteFloors` rather than from the baseline, it is not a proportional
  statement about the code at all — it is the jitter a `watch` tick shows on one
  machine class. The calibration does not describe that jitter: #204 spent
  179 ms on `unchanged_sync` against the 120 ms floor of the day while the
  reference said only 1.36×, so scaling would have kept it red for the wrong
  reason. Off class such a check gets the cap and reports a breach as `warn`
  rather than `FAIL`, up to 2× the floor — past that it fails again.

What a reader sees: the `unknown-cpu` warning is unchanged, an `off-class:`
paragraph states what was applied, every bound prints as `raw -> widened`, and
a check that passed only because of the widening prints `warn` instead of `ok`
with an `advisory:` line naming both numbers.

```text
warning [unknown-cpu]: these baselines were measured on … and this is AMD EPYC 7763 …
off-class: this CPU is not one the baselines were measured on, so the bounds below are
widened, never tightened, and never past 2.00x. …
ok   incremental_sync.elapsedMs: 203.7 (baseline 424, bound 848 -> 1155 off-class x1.36)
warn unchanged_sync.elapsedMs: 179.2 (baseline 51, bound 140 -> 280 off-class x2.00)
ok   unchanged_sync.peakRssBytes: 12984320 (baseline 9687040, bound 67108864)
```

The widening is a bridge, not a destination. Two of the profile's three elapsed
ceilings (`unchanged_sync`, `hydrate_unchanged`) are floor-derived and can
therefore go advisory off class, which is weaker coverage than an on-class run
gets; the fix is to fold the new
CPU into the baselines — re-measure on it, take the worse of each metric across
the pool, and add it to `cpusSeen`. Once it is in that list the widening stops
applying to it and the gate is strict there again.

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

For the tiniest elapsed-time phases the thresholds file can also raise a ceiling
for one named phase without loosening the others. Today `unchanged_sync` uses a
140 ms phase-specific ceiling floor; a lower ceiling measures runner jitter
instead of a real no-op `watch` tick slowdown. That number is jitter measured on
the CPUs in `cpusSeen`, which is why a ceiling of this kind is treated
differently off that class — see "A runner the baselines were never measured
on" above.

It was 120 ms, set because healthy `ubuntu-latest` runs had already reached
about 113 ms there. [#166](https://github.com/AgentWorkforce/relayhistory/issues/166)
moved it to 140, and the reason is a store change rather than a slower no-op
tick: making Cursor an event-level source means the synthetic store's Cursor
transcripts now produce 192 `session_events` rows that no earlier release
stored, so the database grows about 0.3 MiB and an unchanged tick reads about
0.4 MiB more of it. On one machine, across four commits, the phase went ~100 ms
on the pre-#166 main, ~102 ms with continuity relationships on top, and ~112 ms
with #166 — and on a github-hosted `ubuntu-latest` (AMD EPYC 7763) the same head
measured 125.3 ms. 120 gave about 6% of headroom over the runs it was set
against; 140 keeps about 12% over the 125.3 ms worst case observed on the
slowest CPU in the pool.

The stored `ci-debug` baseline for the phase is still 51 ms and was **not**
touched, because re-measuring it means running on the machine class the gate
runs on. Raising the phase ceiling is the sanctioned move here — these floors
"raise such a ceiling and never lower one" — but the baseline is now far enough
from what the phase actually costs that it is worth a re-measure on
`ubuntu-latest` the next time someone is in a position to take one.

A phase that a profile names but that produced no measurement is a failure, not
a skip. A run that measured nothing must not read as a pass.

Baselines are re-measured, never nudged:

```bash
node scripts/benchmark-sync.mjs --profile ci-debug --update-baselines
```

Re-measuring records the commit, machine and toolchain it came from in the
thresholds file, and the pull request has to say why the number moved.

Re-measure **on the machine class the gate runs on**, not on a developer box.
`--update-baselines` rounds each baseline away from the bound it produces, so
the run it was taken from cannot fail on it; the rest — `measuredOn.cpusSeen`,
the run ids, and taking the worse value when more than one machine has been
observed — is currently done by hand, because harvesting a runner's numbers
means reading them out of a CI log.

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

### 2026-09-28 sweep profile and the tokscale techniques (#215)

Issue #215 asked whether relayhistory should adopt three techniques from
[tokscale](https://github.com/junhoyeo/tokscale): a parallel `rayon` + `walkdir`
scan, typed (and possibly SIMD) JSON deserialization, and sampled-content file
fingerprints. The answer depends on where a sweep's time actually goes, so this
section measures that first.

**How it was measured.** Apple M4 Pro (12 cores, 24 GiB), macOS, release build,
the `full` profile's 100 MB store (seed 176, 4,399 files, 3,518 sessions).
`Bytes read` comes from `/proc/self/io` and so is Linux-only; on this machine
the attribution is by CPU sampling instead — macOS `sample` against the release
`ai-hist sync` process on the same store, frames aggregated inclusively. These
numbers are not comparable with the Linux baseline above; the before/after
pairs below are each from one machine.

```bash
node scripts/benchmark-sync.mjs --profile full --large-session-bytes 1048576
```

| Phase (harness) | `main` @ `66ed973` | with the probe fix |
|---|---:|---:|
| `cold_sync` | 59.98 s | 42.59 s |
| `incremental_sync` (1 KiB appended) | 6.97 s | 2.12 s |
| `unchanged_sync` | 65.2 ms | 76.4 ms |
| `hydrate_cold` (1 MB) | 147.8 ms | 178.0 ms |
| `hydrate_unchanged` (1 MB) | 3.0 ms | 3.4 ms |

The unchanged and hydration rows are within run-to-run noise; neither touches
the changed code.

#### Where an incremental sweep went

The source fingerprint already makes an unchanged tick stat-only (65 ms), so
the interesting tick is one where something *did* change and the whole sweep
runs. Share of samples in the release CLI's sync after one Claude append:

| Frame (inclusive) | `main` | after the fix |
|---|---:|---:|
| Claude walk, of which the "has this path left evidence?" probe | 75%, **72%** | 12%, <1% |
| `refresh_project_identity` (after sync + inside discovery) | 11% | 43% |
| Discovery (`discover_sessions_with_providers`) | 12% | 39% |
| Transcript cursor window digests (`prefix_window_digest_counted`) | 2.2% | 9% |
| Directory walk (`collect_matching_files`) | 1.7% | 2% |
| Flat-log whole-prefix SHA-256 (`hash_file_prefix`) | 0.3% | 0.8% |
| JSON parsing (`serde_json`) | <0.5% | <0.5% |

The 72% was one query. `claude_transcript_events_exist` joins `sessions` to
`session_events` on a path; with no `sqlite_stat1` (the crate never runs
`ANALYZE`) SQLite chose `session_events` as the outer table, so answering it
for a path with no catalog row — every subagent sidecar, every new transcript —
walked every Claude event: 12 ms per file at 138 K events, against 0.03 ms keyed
on `idx_sessions_raw_path`. That is O(files × events), so it grows with the
database, which is the shape #42 reported on a 930 MB store. The fix pins the
join order (`CROSS JOIN`) on that probe and the two backfill probes built the
same way, and adds `idx_session_relationships_locator` for the sidecar probe,
which otherwise scanned every Claude relationship per file. A plan test
(`claude_walk_probes_are_keyed_searches`) checks all four with and without
statistics.

#### Where a cold sweep goes

Share of samples in the release CLI's cold sync, after the fix:

| Frame (inclusive) | Share |
|---|---:|
| `sqlite3_step` | 89% |
| `pwrite` (WAL frames) | 38% |
| `fsync` | 35% |
| WAL checkpoint | 12% |
| File `read` | 6% |
| Claude record scan (`scan_claude_session_file_resumed`) | 3.5% |
| JSON parsing (`serde_json`) | <1% |

A cold sweep is write-bound: every evidence row is its own autocommit at the
default `synchronous = FULL`. Two measured prototypes, neither shipped:

| Prototype (cold sync, CLI, same store) | Time |
|---|---:|
| as shipped here | 76.1 s |
| `PRAGMA synchronous = NORMAL` for the sweep | 60.1 s (−21%) |
| one transaction per Claude transcript ingest | 71.7 s (−6%) |

(The CLI is slower than the harness above because it also runs catalog
discovery and project-identity maintenance.) `synchronous = NORMAL` is what
discovery already uses for its own reconstructible writes; extending it to the
sweep is a durability decision — on power loss the last commits can vanish
while `.sync-state.json` stamps survive — that the destination marker is meant
to detect, and it wants its own review rather than riding along here.

#### Decisions

| Technique | Decision | Why |
|---|---|---|
| `rayon` parallel parse | **Reject for now** | Parsing is under 1% of a cold sweep and under 0.5% of an incremental one; writes are serialized behind one connection and are ~90% of the cold cost. Parallelism has nothing to speed up. |
| `walkdir` with trusted `file_type()` | **Already adopted** | `collect_matching_files_inner` trusts `DirEntry::file_type()` and stats only symlinks. The walk is ~2% of a sweep. |
| Typed / SIMD JSON envelopes | **Reject for now** | `serde_json::Value` construction is under 1% of any phase measured. Revisit only if a profile shows it. |
| Sampled-content fingerprints | **Adopted for transcripts; defer for flat logs** | Transcript cursors already hash a head-and-tail window (`prefix_window_digest_counted`), not the whole prefix. The whole-prefix SHA-256 remains on the three flat logs (`CompleteJsonlReader`), at under 1% of a sweep on this store; it is the byte-exact resume guard, so it stays until a profile says otherwise. |

The next costs, in order, are the global `refresh_project_identity` passes
(each a scan of `session_events`, run twice per sweep: 43% of the remaining
incremental tick) and the cold sweep's per-row commits. The CI gate thresholds
are unchanged: they are baselined per runner class, and these numbers are from
a developer machine.

### 2026-09-29 read amplification (#42, #215)

The section above attributed CPU. This one attributes **bytes read**, per
reader, which is what #215 asked for, and removes the largest readers of a
sweep over files that did not change — the sweep a `watch` event tick (always
forced) and a periodic Reflex `syncAndPush()` run whenever anything at all
moved (#42).

**How it was measured.** Apple M4 Pro, macOS, release build, the `full`
profile's 100 MB store (seed 176, 4,399 files). macOS has no `/proc/self/io`,
so the harness's `Bytes read` column is empty here; instead each phase ran with
a `DYLD_INSERT_LIBRARIES` shim that counts the bytes every `read`, `pread` and
`readv` returned, per file (the same quantity as Linux `rchar`, page cache
included), and a temporary marker between the sweep's stages. Neither is
committed. `main` is `f71bc63`.

```bash
node scripts/benchmark-sync.mjs --profile full --large-session-bytes 1048576
```

#### Where an incremental sweep's bytes went

After a 1 KiB append to one Claude transcript, by reader:

| Reader | `main` | this change |
|---|---:|---:|
| Claude walk: window digests proving each unchanged transcript (two per file) | 89.1 MiB | 0.4 MiB (the appended file) |
| Cursor: whole-prefix SHA-256 of each unchanged transcript, twice | 35.8 MiB | 0 |
| Discovery: per-candidate locator lookup (a scan of the source's observations) and its identity refresh | 325.6 MiB | 10.6 MiB |
| Identity refresh after the sweep (a scan of `session_events`, twice) | 315.2 MiB | 38.8 MiB, once |
| `.sync-state.json` re-read and merged at each of 9 checkpoints | 11.0 MiB | 2.6 MiB |
| Destination marker (per-session counts, three times) | 33 MiB | 33 MiB |
| Facade catalog digest before and after (`SessionStore::sync`) | 15 MiB | 15 MiB |
| **Total** | **838 MiB (7.7x the store)** | **114 MiB (1.05x)** |

| Phase (harness, same machine) | `main` | this change |
|---|---:|---:|
| `cold_sync` | 39.92 s | 30.72 s |
| `incremental_sync`, first sweep after the cold one | 1.77 s | 0.77 s |
| `incremental_sync`, every sweep after that | 1.77 s | 0.39 s |
| `unchanged_sync` | 63.3 ms, 28.4 MiB read | 59.5 ms, 22.3 MiB read |
| `hydrate_cold` / `hydrate_unchanged` (1 MB) | 154.1 / 2.9 ms | 155.9 / 2.9 ms |

The first sweep after ingestion still proves every file by its digest once and
records that it did; from the second on, unchanged files cost a `stat`. The
unchanged row is the source-fingerprint fast path, which never walked files;
what it lost is the per-session recount (below) — the rest is the harness's own
row counts and the facade's catalog digest.

What changed, largest first:

1. **The identity refresh stopped reading every event.** Pass 3 (bring each
   event's key in line with its session's) scanned `session_events` with three
   correlated catalog lookups per row to find nothing stale. It is now driven
   from the catalog into `idx_session_events_project`, a covering index on
   `(source, session_id, project_key, project_key_method)`; pass 4 is driven
   from the relationship ledger; pass 1 reads the catalog once instead of once
   per distinct directory (an unindexed `cwd IS ?` probe, O(directories x
   sessions)). A sweep runs the refresh once, not twice — discovery's copy and
   the sweep's ran back to back.
2. **Discovery's locator lookup is a search again.** With no `sqlite_stat1`,
   SQLite served `ORDER BY session_id` from the primary key and walked every
   observation of the source for every candidate — O(files x sessions), the
   shape #42 reported on a 930 MB store. `ORDER BY +session_id` leaves it to
   `idx_observation_locator`.
3. **Unchanged transcripts are proven by `ctime`, where it is real.** Size,
   mtime and inode do not prove bytes (a writer can restore an mtime; a
   coarse clock can give two writes one tick), which is why every skip hashed
   a window. On APFS, HFS+, ext2/3/4, XFS, Btrfs, ZFS, tmpfs and F2FS —
   an allowlist read from `statfs`, cached per device — `ctime` cannot be set
   from user space, so once a digest has proven a cursor and the file's
   `ctime` is more than three seconds old (past those filesystems' timestamp
   granularity, the "racy" case) the cursor records it, and an unchanged
   `ctime` proves the file without a read. FAT and exFAT report the mtime as
   the change time, so a same-size rewrite with the mtime restored leaves it
   equal too; there, on any filesystem not on the list and on Windows,
   nothing settles and the digest stays. A settle expires after six hours and
   the digest is taken again, so no miss is permanent. Bound to the prefix
   hashes it proved, so a cursor that moved is not vouched for; any write,
   truncate, chmod or rename falls back to the digest. The stamp is written
   best-effort and compare-and-swap on the document it was proven from. The same `settled` stamp covers
   Cursor transcripts and the flat prompt logs, whose whole-prefix SHA-256 is
   now paid once per change rather than once per sweep — and a Cursor
   transcript that did not advance is no longer hashed a second time to build
   a generation only an advanced scan uses.
4. **Checkpoints and the marker skip what cannot have changed.** A per-source
   checkpoint whose in-memory state is unchanged since the last one is not
   re-read and merged; a tick whose change-feed head (read before the marker's
   counts) is the one recorded with the marker skips recounting every
   session's evidence.

One trap found on the way, and pinned by
`a_session_scoped_retirement_seeks_the_session_not_the_role`: the covering
index first *replaced* `idx_session_events_session`, which it covers as a
prefix. Without statistics the planner then preferred the one-column
`idx_session_events_role` for `source = ? AND session_id = ? AND role = ?`, and
the Codex user-message retirement turned a cold sync quadratic (70 s and
100 GB of reads in the Codex stage alone). Both indexes stay.

#### Where a cold sweep's bytes go

| Reader (cold, this change) | Bytes |
|---|---:|
| SQLite pages (DB + WAL) re-read while writing 214 K rows | 2.0 GiB (`main`: 2.5 GiB) |
| Claude transcripts, 46 MiB on disk | 313 MiB (6.8x) |
| Codex rollouts, 21 MiB | 92 MiB (4.4x) |
| Cursor transcripts, 21 MiB | 89 MiB (4.3x) |
| Grok sessions, 21 MiB | 35 MiB (1.7x) |

A 53 KB Claude transcript is read seven times on its first sweep: the metadata
fold and the record walk each open it (window digest), read it, and re-hash
the window at commit, and discovery reads its head. Each digest is a
deliberate guard — the one at commit is what catches a rewrite during the walk
— and a first sweep is not what a watch loop pays, so this is recorded, not
changed. Folding the metadata walk into the record walk would halve it. The
cold sweep is write-bound regardless: `fsync` and `pwrite` are two thirds of
the Claude walk's samples, and a 64 MiB page cache moved neither the time nor
the reads.

#### Decisions, revisited

| Technique | Decision |
|---|---|
| `rayon` parallel parse | **Reject.** Unchanged: an incremental sweep now reads 0.4 MiB of provider bytes, and parsing was never more than 1% of anything. |
| `walkdir` / trusted `file_type()` | **Already adopted.** The walk and the fingerprint's `stat`s are most of what an unchanged sweep has left. |
| Typed / SIMD JSON | **Reject.** Nothing in the profiles moved it. |
| Sampled-content fingerprints | **Superseded by `ctime`.** A sample is still a read of every file per sweep; a settled `ctime` proves the whole file with none, and the window digest and whole-prefix hash remain the proof it is taken over. |

The CI gate's `ci-debug` thresholds are not tightened here: they are baselined
per runner class (`ubuntu-latest`), and these numbers come from a developer
machine. The gate passes with room (`incremental_sync` 89 ms against an 848 ms
bound, `unchanged_sync` 17 ms against 140 ms locally); re-baselining on the
gate's machine class is the follow-up.

### 2026-10-01 sweep tick and cold-sweep write cost (#215)

A `watch` driven by filesystem events (200 ms debounce) forces a sweep for
every transcript append, so a forced tick's cost is the floor of
write-to-cloud latency, and the cold sweep's write cost bounds a first
install's backfill. This section measures five candidate fixes against both,
keeps the ones that paid, and records the rest with their numbers.

**How it was measured.** Apple M4 Pro, macOS, release build, the `full`
profile's 100 MB store (seed 176, 4,399 files, 3,518 sessions), the harness
run with `--repeat 2`. The harness's `incremental_sync` is the first sweep
after the cold one; a steady-state `watch` tick is measured separately by a
scratch driver (not committed) that opens the harness's database through
`SessionStore`, settles it with two forced syncs, then times `sync` with and
without `force` and after appending a 1 KiB record (unique uuid each time),
median of seven, three interleaved rounds of the base and this change.
Attribution is by macOS `sample` of that driver, frames aggregated
inclusively. Base is `7533d65` (#310).

```bash
node scripts/benchmark-sync.mjs --profile full --large-session-bytes 1048576 --repeat 2
```

| Phase (harness) | base | this change |
|---|---:|---:|
| `cold_sync` | 37.34 s (5,743 rec/s) | 13.44 s (15,950 rec/s) |
| `incremental_sync` (first sweep after the cold one) | 729.6 ms | 663.9 ms |
| `unchanged_sync` | 60.0 ms | 51.8 ms |
| `hydrate_cold` / `hydrate_unchanged` (1 MB) | 163.5 / 3.0 ms | 157.4 / 2.9 ms |
| Peak RSS, `cold_sync` | 38.5 MiB | 37.5 MiB |

| Steady-state tick (driver, median) | base | this change |
|---|---:|---:|
| unchanged, not forced | 50.6 ms | 46.5 ms |
| unchanged, forced (an fs event that changed nothing a sweep reads) | 337.8 ms | 301.3 ms |
| forced, after a 1 KiB Claude append | 363.8 ms | 332.6 ms |
| forced, after a 1 KiB append to a 2 MB Codex rollout | 728 ms | 466 ms |

#### Where a cold sweep's time went

| Frame (inclusive share of the cold sync's samples) | base | per-transcript transactions | + in-memory journal |
|---|---:|---:|---:|
| `fsync` | 49% | 8% | 12% |
| `pwrite` | 22% | 36% | 10% |
| of which statement-journal spills (`subjournalPageIfRequired`) | — | 30% | 3% |
| WAL checkpoint | 21% | 9% | 12% |
| Cold sync (driver, same store) | 38.1 s | 20.8 s | 14.2 s |

The base column samples the first 25 s of the run, which is the Claude walk;
the other two cover the whole sweep. Every column is the same store and the
same driver.

Every evidence row was its own autocommit at `synchronous = FULL`: one WAL
`fsync` per statement, several per record. Writing a transcript as one
transaction removed most of those, and exposed the next cost: inside a
transaction every upsert whose triggers write keeps a statement journal so
it can be undone alone, and past 64 KiB SQLite spills it to a temporary file
-- `pwrite`s of pages discarded when the statement ends. The sweep's
connection now keeps temporary files in memory (`temp_store = MEMORY`); they
hold nothing durable. What remains of a cold sweep is SQLite's own b-tree and
FTS5 work: FTS5 flushes its pending terms at every statement savepoint, about
a quarter of each event insert's samples.

The 2026-09-28 prototype of "one transaction per Claude transcript" measured
−6% against the CLI; with Codex in the same per-transcript transactions, the in-memory statement
journal, and the harness rather than the CLI, it is −64%.

#### Decisions

| Candidate | Decision | Numbers |
|---|---|---|
| Continuity's parent-record lookup (`session_holding_record`): `message_id = ? OR event_uid = ?` scanned every event of the source, per pending transcript, per sweep | **Fixed.** Two keyed searches, lowest answer wins; the uid half on a new partial index, `idx_session_events_claude_uid_unmatched`, holding only the rows the message search cannot see (empty on a store this parser wrote). Plan tests with and without `ANALYZE`; equivalence test against the old query. | 17 ms → <0.1 ms per pending transcript on 75,601 Claude events (linear in events before). The benchmark store has no pending evidence, so its phases do not move. Building the index on an existing store: 0.76 s for 214 K events, once. |
| Whole-session event reads ordered on `ts_ms IS NULL, ts_ms, id` | **Fixed.** `ts_ms` is `NOT NULL` in every schema this table has had (Rust and the Python original), so the order is `ts_ms, id`, delivered by `idx_session_events_source_page` / `idx_session_events_page` instead of a temp b-tree of full rows. Plan test. | Not on the sweep path; a read of `SessionStore::session` no longer copies every row, text included, into a sort. |
| Autocommit Claude and Codex writes | **Fixed.** One `BEGIN IMMEDIATE` unit per Claude transcript (chunked every 2,000 records) and per Codex rollout (unchunked: the parser-upgrade repair needs it whole), cursors inside the unit, `.sync-state.json` still checkpointed after the source. `synchronous` deliberately unchanged -- that is a durability decision for its own review. | Cold 37.3 s → 13.4 s together with the in-memory journal; Codex append tick 728 → 466 ms. |
| Per-event `sessions` subselects and per-event `session_presences` insert in `insert_session_event_with_provenance` | **Deferred.** After the per-transcript transactions, all `sessions`/presence seeks under the event insert are under 4% of a cold sweep's samples (presence alone 0.8%), and hoisting the subselects needs invalidation whenever a walk writes the catalog row mid-transcript (Codex writes events before its session). Not worth the risk at this size. | ≤ 4% of cold; 0% of a tick. |
| `catalog_fingerprint` before and after every sync and watch tick | **Fixed.** The digest records the change-feed head it was read at; an unmoved head (every catalog insert/update/delete moves it through the feed's triggers) skips the after-scan. `changed` is still computed from row digests whenever anything was written, so it is exact. | 3% of a forced append tick still pays it (the head moved); an unchanged tick does not. |
| (Found while measuring) the destination shortfall named sessions with `sessions UNION session_events`, a second walk of every event per swept tick | **Fixed.** The grouped holdings reads collect the names; only the Muse arm is still queried. Same named set. | Forced unchanged tick 346 → 320 ms in the single-run attribution pass on the same database (the median-of-seven table above, which includes this fix, reads 337.8 → 301.3 ms for the same scenario). |

#### Where a forced tick goes now

After a 1 KiB Claude append, share of the tick's samples:

| Frame (inclusive) | Share |
|---|---:|
| Shallow discovery (`discover_sessions_for_sweep`) | 34% |
| of which per-candidate catalog and observation lookups (`fetch_observed_candidate`, each statement prepared per call) | 16% |
| of which Grok enumeration | 12% |
| of which the path-key upgrade (`upgrade_cached_project_identity`) | 9% |
| Project-identity refresh | 13% |
| Directory walk (`collect_matching_files`) | 13% |
| `.sync-state.json` checkpoints (`SweepCheckpoints::save`) | 9% |
| Source fingerprint (`source_fingerprint_with`) | 8% |
| Grok session inventory (`grok_source_inventory`) | 6% |
| Destination marker: shortfall at the start and recount at the end | 7% |
| Catalog digest (`catalog_row_digests`; the head moved) | 3% |
| Ingesting the appended record | 0.3% |

None of it is the appended bytes. These are filed as their own issues rather
than widened into this change; see the pull request.

The CI gate's `ci-debug` thresholds are not re-baselined: they are baselined
per runner class (`ubuntu-latest`), these numbers come from a developer
machine, and every gated phase moved in the safe direction.

### 2026-10-01 change-feed changes for unchanged rows (#215)

relay-desktop's probe uploads whatever `changes_since` reports, so a change
for a row whose content did not move is paid again in transport, scrubbing,
digesting and the cloud's projection lock. This counts the changes each
operation emits and how many carry a row identical, column for column, to the
one the feed last reported for that key.

**How it was measured.** A scratch driver (not committed) over the harness's
`full` store (seed 176, 100 MB, 3,518 sessions, release build) and over the
checked-in fixture corpus staged into one home (51 sessions, with sidecars,
forks, resumes, markers and Codex rollouts). It replays the feed from `START`
into a map, then after each operation drains from the head before it and
compares every upsert's `columns` with the map.

| Operation | 100 MB store, base | this change | fixture corpus, base | this change |
|---|---:|---:|---:|---:|
| Forced tick, nothing changed | 0 | 0 | 17 (0 identical) | 15 (0 identical) |
| Forced tick after a 1 KiB Claude append | 4 (0 identical) | 4 | 22 (0 identical) | 19 |
| First hydration of a Claude session the sweep indexed | 117 (115 identical) | 2 | 8 (5 identical) | 2 |
| First hydration of a Codex session the sweep indexed | 27 (25 identical) | 2 | 8 (6 identical) | 4 (2 identical) |
| Repeat hydration (`unchanged`) | 0 | 0 | 0 | 0 |
| `refresh_project_identity` after a sync | 0 | 0 | 0 | 0 |
| `UPDATE sessions SET project_key = project_key` | 3,518 (all identical) | 0 | 51 (all identical) | 0 |
| Forced tick after one Grok chat line | 40 (36 identical but for `id`) | 40 (36) | — | — |

What is left, and why it stays:

- The fixture corpus's remaining forced-tick changes are real: five session
  ids are each claimed by two top-level transcripts, and every forced sweep
  rewrites the catalog, presence and observation rows from whichever it read
  last (#328). A subagent sidecar, which carries its parent's session id, is
  read as the parent's related transcript and does not do this.
- The fixture corpus's two other forced-tick changes (17 → 15), and the three
  per append (22 → 19), were relationships re-recorded with a new
  `updated_ms` and nothing else; the relationship and observation upserts
  now skip such a write.
- The two Codex events a hydration still re-reports are written with no
  usage and patched a record later -- a real change and a change back.
- Grok replaces a session's evidence wholesale on every re-read, so every
  row comes back under a new `id`: a delete and an insert, not an update the
  guard can see. Filed with the other delete-and-reinsert writers.

Cost: the guard adds no work to an insert, and on an update it replaces the
trigger body with a column comparison whenever nothing changed. The probe's
cold sync of the 100 MB store took 15.2 s on the base and 15.4 s with this
change (one run each, within run-to-run noise), and the `--gate` subset
passes. `schema_is_current` now also reads each fed table's column list and
its update trigger's text: 24 schema reads, under the 10 ms resolution of a
`sqlite3` CLI timing of the same queries, process start included.

### 2026-10-01 the post-sweep checkpoint and a pinned reader (#336)

Every sweep, every watch tick included, ended with `wal_checkpoint(TRUNCATE)`
under the sweep connection's ~30 s busy handler. A `TRUNCATE` takes the WAL
write lock and then waits for readers to leave, so one read open as a sweep
finished held every other writer — an embedder's own tables included — for
the handler's full budget. The sweep now checkpoints `PASSIVE` (no write
lock, no waiting) and escalates to `TRUNCATE` only when that pass copied
every frame and the WAL is still past 4 MiB (`WAL_WARN_BYTES / 16`, SQLite's
own auto-checkpoint size), under a 100 ms busy budget for that one call.

The budget bounds the `TRUNCATE`'s wait, not its copy, which also runs under
the write lock. An earlier revision escalated after a short pass too: with
82.8 MB of frames held back by a reader that left 30 ms into the escalated
call, a third connection's `BEGIN IMMEDIATE` waited 70–80 ms behind the copy
(macOS, where `fsync` is not a full flush; the cost grows with the backlog
and the disk). Escalating only after a full pass leaves the `TRUNCATE` just
what another connection committed in between: the same case now waits
40 µs, and the next sweep's `PASSIVE` copies the backlog without the lock
before truncating (50–80 ms, nobody waiting).

**How it was measured.** A scratch driver (not committed) through the public
`SessionStore` over the harness's `full` store (seed 176, 100 MB, 1,776
sessions, release build, Apple M4 Pro): one cold sync, then 8 forced syncs,
each after appending an 8 KiB turn to one Claude transcript. A second
connection optionally holds a read transaction open from after the cold sync
to the end; a third runs `BEGIN IMMEDIATE; INSERT; COMMIT` into its own table
every 20 ms throughout and records its slowest lock wait.

| 8 forced sweeps, 100 MB store | base, no reader | this change, no reader | base, pinned reader | this change, pinned reader |
|---|---:|---:|---:|---:|
| Mean sweep | 0.25 s | 0.23 s | 32.45 s | 0.25 s |
| Slowest `BEGIN IMMEDIATE` on a third connection | 12 ms | 1.5 ms | 32.4 s | 0.06 ms |
| Lock waits over 1 s | 0 | 0 | 8 (every sweep) | 0 |
| WAL after the cold sync | 0 | 0 | 0 | 0 |
| WAL after sweep 8 | 0 | 3.1 MB (steady) | 5.9 MB | 5.9 MB |

- With no reader the WAL no longer drops to zero after each small sweep: it
  stays at its high-water mark below 4 MiB (3.1 MB here, unchanged across all
  8 sweeps) and SQLite reuses it from the start. A sweep that leaves more than
  4 MiB — the cold sync above — is still truncated to zero.
- With a reader pinned for the whole run neither version can fold back the
  frames the reader may still need, so the WAL grows by what each sweep
  writes either way; `[wal] checkpoint partial` reports it and the next
  quiet sweep truncates it. The difference is only who waits: before, every
  sweep and every other writer, for ~32 s; now nobody, because a pass the
  reader cuts short never escalates. A reader whose snapshot is current
  still lets the pass finish, and the escalated reset then waits on it for
  at most its 100 ms (93–133 ms for a third connection in the unit test).
- `compact` keeps its `TRUNCATE` with the full busy handler: it is an explicit
  maintenance action that already holds the sync lock and rewrites the file
  with `VACUUM`, so a wait there is expected.

### 2026-10-10 a no-change sweep over a 45,000-session OpenCode store

On a copy of a real store — `ai-history.db` at 6.9 GB, `opencode.db` at
1.1 GB holding 44,615 sessions, 139,824 messages and 290,822 parts, plus
17 GB of Codex and 1.8 GB of Claude transcripts — a sweep with nothing to
read cost half an hour. Four causes, each a cost proportional to the store
rather than to what changed:

- **The OpenCode pass re-read and rewrote every session.** It now skips a
  store whose database and WAL stamps are unchanged, and in a store that
  moved reads only the sessions whose row, `message` and `part` stamps moved
  (one aggregate pass per table; see
  [session-catalog](session-catalog.md#adding-a-provider)).
- **Edge lookups walked the source.** Every OpenCode edge cites the one
  store it was read from, so `idx_session_relationships_locator` holds
  44,000 rows under one key, and without statistics SQLite answered the
  superseded-unlinked `DELETE`, the parent-or-child continuity read, the
  child-keyed `ORDER BY parent_session_id` reads and the session delete
  trigger by reading every edge of the source — 81% of a sweep's samples.
  Each now seeks (plan tests in `relationship_graph/plan_tests.rs`, with
  and without `ANALYZE`).
- **The fingerprint counted `-shm`.** Reading a WAL store rewrites its
  `-shm`, so the sweep's own read moved the fingerprint and the
  unchanged-sources fast path never held.
- **Every process's first identity refresh was full.** The refreshed-through
  point lived in process memory, so every `ai-hist sync` re-walked every
  delegated child: 30 s of a 42 s forced sweep once the first three were
  fixed. It is kept in the database now.

**How it was measured.** A scratch driver (not committed) calling
`SessionStore::sync` through `ProviderRoots::from_home` over APFS clones of
the store and provider roots, release builds, Apple M4 Pro, `/usr/bin/time
-l`. The machine was shared with other agents (load average 30–160), so
wall times are high; CPU time is the steadier figure.

| Real store | main | this change |
|---|---:|---:|
| Sync, nothing changed (unforced) | 2,014.7 s wall; 323.6 s user + 390.4 s sys; 147 MB | 0.49–0.75 s; 0.3 s CPU; 74 MB (fast path) |
| Sync, nothing changed (forced) | the same sweep as unforced | 7.1–7.6 s; 2.1–2.4 s CPU; 128 MB |
| Sync after one OpenCode message is appended | the same sweep | 14.6–14.8 s; 3.8 s CPU; 141–148 MB |
| First sync of a store main last wrote | — | reads every OpenCode session once: 232.6 s; 81 s CPU; 329 MB |
| Cold sync, OpenCode store only | 920.5 s; 245.5 s user + 262.9 s sys; 323 MB | 705.3 s; 196.7 s user + 84.6 s sys; 296 MB |

The harness (`perf-ab`, 3 interleaved rounds against main) over a generated
store with 30,250 OpenCode sessions (`gen-rich.mjs --opencode-sessions
30000`, 87,000 messages, 151,500 parts) and a new `opencode_append_sync`
phase, CPU medians:

| Phase | main | this change |
|---|---:|---:|
| `cold_sync` | 86.9 s | 77.2 s (−11%) |
| `forced_sync` | 28.7 s | 1.05 s (−96%) |
| `incremental_sync` (one Claude turn) | 27.2 s | 0.58 s (−98%) |
| `opencode_append_sync` | 25.1 s | 0.78 s (−97%) |
| `unchanged_sync` | 92 ms | 105–116 ms |
| `read_small_sessions` | 303 ms | 131 ms (−57%) |

- The read phases moved within the run's noise; an A/B of each build over
  each build's store, and of one store with and without `sqlite_stat1`,
  put `feed_full`, `export_full`, `catalog_page` and `search` within 3% of
  each other.
- `unchanged_sync` parses the per-session stamps in `.sync-state.json`
  (2.8 MB for 30,000 sessions): about 15 ms and 13 MB more peak RSS on the
  fast path, 29 MB more on a cold sync.
- The remaining forced-sweep time on the real store is evenly spread:
  Codex existence probes, discovery's per-candidate stamp reads, the OpenCode
  holdings scan and the catalog digest, each 1–2 s of mostly I/O.
