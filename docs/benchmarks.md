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
