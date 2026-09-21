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
| Nothing changed | 0 — the stamp short-circuit does not open the file |
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
