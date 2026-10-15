# Existing-provider characterization (#427)

Status: **partial characterization; do not close #427**. Inspected RelayHistory
`d3bbd3d1` (0.44.0) on macOS arm64, 2026-10-14. No operator history, agent
execution, authentication or model requests were used. This is a compatibility
baseline for the checked-in formats, not certification of current upstream
releases. Six rollout rows are Claude, Codex, OpenCode, Cursor, Muse and Grok;
Devin remains a seventh regression source outside that rollout count.

## Evidence and provenance

The source of admission is `providers/history-sources/*.json`; parser ownership
is `crates/ai-hist/src/providers/<id>` with existing normalization under
`ingest.rs`, `ingest/` and `ingest/hydrate.rs`. The existing corpus and reviewed
snapshots are retained byte-for-byte. Its Burn fixtures have independent reader
assertions inherited from Burn; authored RelayHistory fixtures pin current
behavior but do not establish a new upstream writer version. Reference fixtures
are not executable admission evidence.

| Source | Formats/version evidence | Messages/tools/edits | Usage | Relationships and timestamps | Remaining oracle/platform limit |
|---|---|---|---|---|---|
| Claude | JSONL; `simple-turn` records `2.1.96`; primary and nested `subagents/*.jsonl` plus `.meta.json` | Native message/request IDs; block/tool-result joins and explicit edit/result facts; sidecars acquired with parent | Per-message; repeated request copies deduplicated, missing counters remain unavailable | Explicit native parent/fork/subagent facts; record timestamps | Version is fixture metadata. No current CLI writer/export was run. Sidecar, request-copy and provenance assertions live in existing corpus/unit tests. |
| Codex | Rollout JSONL including `.codex/archived_sessions`; version metadata varies, including `0.121.0` in the usage fixture | Synthesized IDs anchored to native record positions/turn facts; tool calls/results and explicit edit evidence | Cumulative-delta; resets/unusable spans/reasoning overlap characterized, not blindly summed | Explicit parent-thread/subagent/fork facts; native timestamps and persisted usage boundaries | Several Burn fixtures contain usage/control only, not a readable conversation. Archived-session and fork fixtures provide actual user/assistant assertions. No current writer oracle. |
| OpenCode | Live SQLite plus legacy `session/message/part` JSON trees; additional native CLI 1.18.31 offline importer capture with separate export oracle | Provider message/part IDs and explicit tool/edit facts | Public normalized usage unavailable; do not infer billing from stored token-like metadata | Persisted parent/session facts; native database timestamps | Native 1.18.31 importer/export probe proves synthetic text/tool rows on macOS arm64. Existing WAL/legacy corpus proves its shapes, not every channel/release schema or actual model/resume writer behavior. |
| Cursor | Admitted prompt-transcript tree; separate authored `observed-3.13.25`, legacy-string and extended-unverified references | Mixed IDs; only persisted text/tool/edit facts. No claim that unverified reference fields exist in real installations | Unavailable | No relationship capability; native framing when available, filesystem fallback otherwise | Cursor fixture README says authored, not captured. `3.13.25` is a third-party corpus description, not a tested installed version here. JSONL references must not be counted as admitted acquisition coverage. |
| Muse | Native event-stream main/child trees; `tools-session` metadata `1.3.0`; separate CLI-capture provenance retained | Mixed IDs; main/child messages, native tools/results/edit payloads | Per-request only when persisted, missing remains unavailable | Explicit child-stream relationship; millisecond record timestamps | Corpus contains a CLI capture and authored cases; fresh 1.4.7 (`d8c53d25bd`) official offline exporter independently confirms all three prompts, assistant texts and tool IDs in the older CLI capture. No new writer/model/resume capture. |
| Grok | `events-session`, legacy `full-session`, `unified-usage`; real-install shape confirmed, no published writer version pin | Synthesized identities for persisted transcript/update facts; tools/edits when written | Per-request turn/unified records; context `totalTokens` is a separate proxy, not billing usage | Explicit persisted relationships; update/native timestamps, documented fallback | [#212](https://github.com/AgentWorkforce/relayhistory/issues/212) is closed. Current catalog records observed `sid`, ISO `ts`, and `ctx` counters, session join and inclusion semantics on one Grok Build install. Its synthetic corpus still includes older accepted `session_id` / `usage` spellings; parser tests cover observed spelling. No new install or all-version claim. |
| Devin | SQLite `sessions.db` plus `transcripts/<id>.json`; schema fixture, writer version unknown | Mixed IDs; transcript messages and persisted tool/edit facts | Unavailable | No relationship capability; persisted seconds converted to milliseconds | SQL/JSON fixture is preserved as regression evidence. Current upstream version and other OS layouts not established. |

Resume capability is separate from historical acquisition. This work neither
adds nor verifies native resume for any source. Missing richer evidence remains
unavailable even when ordinary text acquisition succeeds.

## Roots and acquisition boundaries

All new characterization tests use `ProviderRoots::from_home`, which ignores the
host environment, and an isolated temporary SQLite destination. Production
resolution remains: Claude `CLAUDE_CONFIG_DIR`, Codex `CODEX_HOME`, Grok
`GROK_HOME`; Muse and Devin use `XDG_DATA_HOME`; OpenCode supports `OPENCODE_DB`
and `OPENCODE_STORAGE_DIR`, channel databases and default data roots. Cursor's
configured roots remain the existing `.cursor/projects` transcript layout.
These precedence rules are code inventory, not new cross-platform test claims.
No root resolver or shared dispatch changed. Custom/denied/symlink roots and
sidecar/WAL behavior continue to be covered by their existing tests.

## Repeat/restart contract

`tests/provider_baseline.rs` reuses admitted descriptors/fixtures and the shared
public `assert_history_conformance` helper. Nine existing-format positive tests exercise discover,
selected hydrate, read, reopen, unchanged hydrate, forced full sync, repeated
forced sync and paged export with the export feature. They compare complete
message/block identities and contents; unchanged selected hydration and repeat
full sync must emit **zero SessionEvent changes**. This does not assert that all
other change kinds are silent, nor that a new writer version has the same format.

- `claude_existing_format_repeat_acquisition`
- `codex_archived_format_repeat_acquisition`
- `opencode_sqlite_format_repeat_acquisition`
- `cursor_admitted_transcript_repeat_acquisition`
- `muse_main_child_format_repeat_acquisition`
- `grok_documented_events_format_repeat_acquisition`
- `devin_preserved_sqlite_format_repeat_acquisition`
- `grok_legacy_format_repeat_acquisition`
- `grok_unified_usage_format_repeat_acquisition`

Use the corpus for Claude sidecars/request dedup, Codex compaction/cumulative
usage, OpenCode WAL/legacy JSON, Muse main/child streams and all three Grok
variants. The representative public tests supplement that corpus; they do not
replace it or expand its format claims.

## Existing issue reconciliation

[#326](https://github.com/AgentWorkforce/relayhistory/issues/326),
[#327](https://github.com/AgentWorkforce/relayhistory/issues/327), and
[#328](https://github.com/AgentWorkforce/relayhistory/issues/328) are closed.
The current `change_feed` suite includes
`a_forced_sweep_over_unchanged_files_reports_nothing`,
`a_forced_sweep_keeps_a_shared_session_on_its_recorded_transcript`, and four
`codex_reread_*` tests: unchanged tools emit no event, changed usage emits once,
removed usage clears once, and refusal replaces a stale measurement once.
These cover the previously excluded same-ID catalog case and usage rereads;
this baseline adds no competing parser fix. They do not establish compatibility
with a different upstream writer version.

[#358](https://github.com/AgentWorkforce/relayhistory/issues/358) is closed; parser
generation/retired-key protection remains owned by its existing tests and setup.
No generation key, descriptor, snapshot, public API or runtime schema changed.

## Fresh installed-version and oracle probes

All version commands were passive `--version` on 2026-10-14/macOS arm64;
none read operator transcripts or ran a model. An installed version identifies
what could be probed, not a tested transcript writer.

| Provider | Installed pin | Fresh offline evidence and concrete next probe/blocker |
|---|---|---|
| Claude | 2.1.296 | CLI help provides resume/background logs and config import, not an offline historical transcript writer/importer. The proprietary writer cannot be invoked to create assistant/tool facts without a model turn under this task's constraints. Existing `2.1.96` synthetic fixture is not 2.1.296 certification. A separate official SDK transcript-reader probe is possible research work; it would establish reader compatibility, not a fresh writer capture. A maintainer-produced sanitized current ordinary/tool/resume capture is still missing. |
| Codex | 0.161.0 | Public writer code and generated app-server protocol make an offline path feasible: `thread/start` → `thread/inject_items` → `thread/read` without `turn/start`. Current probe artifacts/status are recorded separately below; this is not an authentication blocker. Do not conflate the older rollout JSONL corpus with current paginated storage. |
| OpenCode | 1.18.31 | **Executed:** native `import --pure` wrote synthetic user/assistant/tool evidence; a new `export --pure` process read it back. Versioned native SQL, input, export oracle, binary digest and reproducible capture script are checked in under `fixtures/baseline/opencode-1.18.31/`. Public conformance compares all exported text and native tool-call IDs. Missing: live ordinary/tool/resume writer behavior, other release channels and platforms; no model was run. |
| Cursor agent | 2026.01.02-80e4d9b | Help exposes resume/list, not a passive transcript import/export writer. Proprietary installed writer requires an agent interaction to produce a fresh assistant/tool session. Need a sanitized transcript capture for this exact CLI/version plus independent official reader/export. Existing authored transcript references do not provide that oracle. |
| Muse | 1.4.7-R6101.1; exporter 1.4.7 `d8c53d25bd` | **Executed:** documented offline `export --session <explicit fixture path>` against preserved `cli-capture`; isolated HOME and auto-update disabled. Versioned official-export projection and reproducible script under `fixtures/baseline/muse-1.4.7/` verify all 6 user/assistant texts and 3 native tool names/arguments/results. This is a new reader oracle for the old capture. Current proprietary writer/model/resume capture remains missing. |
| Grok | 1.0.52 `0920c8169e27` stable | #212 already confirms the real-install `sid`/`ctx` shape and counter inclusion; reuse that result. Current CLI help offers agent runs/continue, not an offline transcript writer/export. Need an upstream-versioned sanitized transcript plus independent transcript oracle; this task does not re-run model-backed capture or repeat the solved usage investigation. |
| Devin | 2026.4.24-10 `d9c02fe` | Help exposes session list, cloud and ACP, not offline transcript import/export. Its proprietary native writer needs a real agent turn for new assistant/tool evidence. The existing SQLite/transcript pair remains preserved regression evidence; a sanitized pinned capture and independent reader are missing. |

The OpenCode native capture deliberately lives with baseline-owned research
fixtures, so it needs no shared descriptor/registry change. The native SQLite
schema/rows come from the CLI, and expected texts/tool IDs come from its separate
export process rather than RelayHistory parser output. Muse projection is selected
from official exporter fields and retains the complete-export digest. Reproduction
scripts accept an explicit installed binary and build a minimal environment
(system PATH, TMPDIR, isolated HOME/XDG and offline flags), excluding inherited
provider config/root overrides. They never install dependencies or start a model. Neither probe silently upgrades richer capability claims.

## Copilot integration reconciliation

[#432](https://github.com/AgentWorkforce/relayhistory/issues/432) is closed by
[#495](https://github.com/AgentWorkforce/relayhistory/pull/495), merged at
`d3bbd3d1`. This baseline does not reimplement that adapter. Its fixture README,
research report and `provider_copilot` test own CLI identity and format evidence:
CLI 1.0.95's offline importer produced the journal, and an independent headless
`session.getMessages` reader produced `official-reader.json`. The acceptance
suite compares all supported official-reader messages/events, plus public
source-filtered discovery, hydration, read, restart, change feed, paged export
and repeated full sync. Additional synthetic 1.0.86-shaped usage fixtures
protect native tool joins, subagent scopes and verbatim output-only usage.
CLI history coverage does not imply IDE or hosted Copilot history coverage.

The remaining limitation is no new authenticated ordinary/tool/resume capture
in this baseline run. The checked-in importer oracle is version-pinned offline
writer/reader evidence; it does not certify all installed CLI releases. The
merged PR's private-data performance/real-data observations are not reproduced
or used as this baseline's acceptance oracle.

## Recorded checks

Results below are recorded for this branch, not inherited from the old baseline.
The baseline's content-loss negative control removes Claude's assistant record
inside an isolated staged fixture home and requires the public content assertion
to reject it. Discovery or a successful hydration alone cannot distinguish lost
assistant content from complete acquisition. A second negative control erases only OpenCode tool-result output, retaining its native call ID/name/arguments; **one official-export result assertion fails when that payload is lost**. No committed corpus fixture changes.

- `cargo test -p ai-hist --all-features --test provider_baseline --test provider_copilot --test fixture_corpus --test source_registry` — 67 corpus, 10 registry, 6 Copilot, and the initial 10 baseline tests succeed. After adding mixed-source exclusion, `cargo test -p ai-hist --all-features --test provider_baseline` — 11 succeed.
- `cargo test -p ai-hist --all-features --test change_feed --test codex_usage_snapshots` — 15 change-feed and 12 Codex usage tests succeed; one unrelated session-drain test remains ignored by its existing annotation.
- `cargo test -p ai-hist --no-default-features --test provider_baseline` — final 15 succeed, with existing library unused-code warnings. Full no-default run recorded below.
- Negative control: **one public content assertion fails when the Claude assistant record is removed** (`missing Assistant text "Hello!"`). `claude_assistant_content_loss_is_detected` requires that failure; discovery/hydration still reach the assertion. Restoring complete fixture staging restores the positive case. No production parser registration was removed in this baseline.
- `source_filter_excludes_another_admitted_provider` stages nonempty Claude and Grok catalogs together and pages each allowlist at one row; neither filtered result admits the other source, and the empty allowlist admits no rows. Single-provider discovery alone cannot distinguish a working filter from a filter that admits every row.
- `npm run providers:check` — deterministic generated output and 9 generator tests succeed.
- `cargo fmt --all -- --check`, `git diff --check` — succeed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — succeeds after the final test addition; no lint allowance added.
- `/private/tmp/rh-baseline-quality/bin/rust-oleum` (CI-pinned 0.1.2) — gate succeeds. The globally installed 0.1.3 rejects unchanged production baseline ceilings and newly measured categories, so its output is not substituted for the pinned gate; no shared ceiling was changed.
- `cargo publish --dry-run -p ai-hist --allow-dirty` — packaging and verification succeed; upload explicitly aborted by dry-run. Existing package/no-default unused-code warnings remain. The preliminary offline attempt could not resolve the registry and is not the successful result.
- Full workspace and no-default results: `TMPDIR=/private/tmp cargo test --workspace --all-features` outside the sandbox — 1,899 succeed across 52 suite summaries, 5 existing ignored (12-case baseline revision; final 15-case target checked separately). Initial sandbox attempt had 1,420 successes/9 failures/1 ignored in library tests; Unix socket EPERM/long `SUN_LEN`, restricted `ps`, a linked-worktree Git condition, and three watcher tick waits were all eliminated by the corrected environment. `TMPDIR=/private/tmp cargo test -p ai-hist --no-default-features` outside the sandbox — 1,547 succeed across 22 suite summaries, 2 existing ignored (final 15-case target checked separately). The initial sandbox no-default run had 1,413 successes/6 failures/1 ignored (four socket cases, restricted `ps`, linked-worktree Git condition).
- Fresh OpenCode and Muse oracle tests: `cargo test -p ai-hist --all-features --test provider_baseline` — final 15 succeed after adding the OpenCode native writer/export oracle, Muse official-export oracle, Codex native writer characterization, and tool-result-loss control. OpenCode and Muse capture reproduction scripts are verified separately with minimal environments.

These checks prove retained content/identity and the fixture-scoped repeat
contract. They cannot distinguish compatibility with unobserved writer schemas
or other operating systems. No native/SDK/API contract changed, so no public-API
snapshot or binding regeneration is part of this test/report-only change.

## Codex offline probe status

The isolated native Codex 0.161.0 app-server accepted `thread/start` and
`thread/inject_items`, persisted a rollout with synthetic user/assistant and
function-call/result response items, and accepted another injected pair after
restart/resume. No `turn/start`, authentication or model request was used. The
remaining independent-reader blocker is specific: `thread/read` with
`includeTurns`, `thread/turns/list`, and `thread/items/list` all returned empty
visible history despite persisted native response items (`historyMode:
"paginated"`). Injected model-visible history is not projected as visible turns
by these APIs. Those empty reader responses do not prove an empty transcript.

Versioned native writer/input and exact attempted reader artifacts are retained
under `fixtures/baseline/codex-0.161.0/`. `codex_0_161_native_writer_repeat_acquisition` verifies all five authored injected
texts plus the native call/result join through discovery, hydration, restart,
change feed and export. They improve writer-format provenance but do not satisfy
a successful independent transcript-reader oracle. The next
probe is an upstream-supported way to materialize/read injected turns, or an
independent public rollout reader pinned to this writer. This remains feasible
public-source research, not an authentication requirement.

## Remaining acceptance work

Keep #427 open. Use the per-provider matrix above for the remaining writer/export probes and
sanitized ordinary/tool/resume captures. OpenCode now has a pinned offline native
writer/export oracle, and Muse has a pinned independent reader oracle for its
older capture. Neither result supplies current model-backed resume evidence. Fixture
metadata alone cannot close this requirement. Cursor's actual installed writer
must establish the admitted transcript shape before reference JSONL formats can
be promoted. For proprietary sources needing authentication, the missing input
is a maintainer-produced sanitized versioned capture plus official export or
separate reader output; this task does not access private history or authenticate.

Grok's #212 investigation is resolved; the remaining Grok limit is a published
writer/platform pin and a sanitized independent transcript oracle for the
observed installation, not another investigation of `sid` or token inclusion.
Re-run corpus/registry after each new integration. Shared descriptor/ADR and
changelog changes remain integration-owned. This report establishes repeat
acquisition of admitted formats, not 6/6 current-version certification or the
completed 41-provider rollout.
