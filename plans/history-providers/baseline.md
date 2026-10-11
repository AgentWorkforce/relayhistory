# Existing-provider characterization (#427)

Status: **partial characterization; do not close #427**. Inspected RelayHistory
`cc11ea2a` (0.40.1) on macOS arm64, 2026-10-11. No operator history, agent
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
| OpenCode | Live SQLite plus legacy `session/message/part` JSON trees; fixture DB schema, no pinned product version | Provider message/part IDs and explicit tool/edit facts | Public normalized usage unavailable; do not infer billing from stored token-like metadata | Persisted parent/session facts; native database timestamps | Existing WAL/legacy corpus proves these schema shapes, not all channel/release schemas. |
| Cursor | Admitted prompt-transcript tree; separate authored `observed-3.13.25`, legacy-string and extended-unverified references | Mixed IDs; only persisted text/tool/edit facts. No claim that unverified reference fields exist in real installations | Unavailable | No relationship capability; native framing when available, filesystem fallback otherwise | Cursor fixture README says authored, not captured. `3.13.25` is a third-party corpus description, not a tested installed version here. JSONL references must not be counted as admitted acquisition coverage. |
| Muse | Native event-stream main/child trees; `tools-session` metadata `1.3.0`; separate CLI-capture provenance retained | Mixed IDs; main/child messages, native tools/results/edit payloads | Per-request only when persisted, missing remains unavailable | Explicit child-stream relationship; millisecond record timestamps | Corpus contains a CLI capture and authored cases; no new install or latest-version compatibility claim. |
| Grok | `events-session`, legacy `full-session`, `unified-usage`; no real-install version pin | Synthesized identities for persisted transcript/update facts; tools/edits when written | Per-request turn/unified records; context `totalTokens` is a separate proxy, not billing usage | Explicit persisted relationships; update/native timestamps, documented fallback | [#212](https://github.com/AgentWorkforce/relayhistory/issues/212) owns real-install characterization. Unified fields/session join are inferred. Reuse its fixture and evidence; do not start a second investigation. |
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
public `assert_history_conformance` helper. Seven named tests exercise discover,
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

Use the corpus for Claude sidecars/request dedup, Codex compaction/cumulative
usage, OpenCode WAL/legacy JSON, Muse main/child streams and all three Grok
variants. The representative public tests supplement that corpus; they do not
replace it or expand its format claims.

## Existing issue reconciliation

[#326](https://github.com/AgentWorkforce/relayhistory/issues/326) is closed;
[#327](https://github.com/AgentWorkforce/relayhistory/issues/327) and
[#328](https://github.com/AgentWorkforce/relayhistory/issues/328) remain open in
GitHub at inspection. Main already includes [#466](https://github.com/AgentWorkforce/relayhistory/pull/466)
(`c531d125`) stable evidence rereads. Do not duplicate that fix. The new public
repeat checks characterize current admitted examples; the existing
`a_forced_sweep_over_unchanged_files_reports_nothing` test addresses the broader
all-kind silence for non-colliding sessions and parent/sidecar acquisition.
It explicitly excludes duplicate top-level transcripts with the same session ID;
#328's top-level collision is not covered. An open issue title is not proof its original
symptom persists on this head.

[#358](https://github.com/AgentWorkforce/relayhistory/issues/358) is closed; parser
generation/retired-key protection remains owned by its existing tests and setup.
No generation key, descriptor, snapshot, public API or runtime schema changed.

## Recorded checks

Initial untouched baseline:
`TMPDIR=/private/tmp cargo test -p ai-hist --all-features --test fixture_corpus --test source_registry`
— **66 corpus + 10 registry pass**.

Added public characterization:
`TMPDIR=/private/tmp cargo test -p ai-hist --all-features --test provider_baseline`
— **7 pass**.
`TMPDIR=/private/tmp cargo test -p ai-hist --no-default-features --test provider_baseline`
— **7 pass**.
Removing the Claude assistant record makes
`claude_existing_format_repeat_acquisition` fail because `Hello!` is absent;
restoring the fixture makes all seven pass. The fixture is unchanged in Git.

`TMPDIR=/private/tmp cargo test -p ai-hist --all-features --test change_feed a_forced_sweep_over_unchanged_files_reports_nothing`
— **1 pass**, separately covering the existing broader change-feed guarantee.
`cargo fmt --all --check` and `git diff --check` pass. No hosted CI result is
claimed by this local work; no production behavior was changed.

## Next acceptance work

Pin independently readable current writer/export versions for every provider
whose row above is unknown, with sanitized captures and a separate reader
oracle. Grok stays blocked on #212's real-install evidence; Cursor's real writer
format must be verified before moving reference shapes into production corpus.
Re-run unchanged corpus/registry after each admitted-provider integration and
compare meaningful identity, usage and change-feed output. Integration owns any
shared descriptor/ADR/changelog changes. This report does not claim 6/6 new
version characterization or complete the 41-provider rollout.
