# Hermes historical acquisition (#429)

Pinned variant: NousResearch/hermes-agent, commit
`97a9ae8ee55d31bebce49f1fb6b5ef775323976a`, SQLite schema **31**, captured on
macOS arm64. Primary writer: `hermes_state.py`, `hermes_state_sessions.py`,
`hermes_state_messages.py`, `hermes_state_common.py`; official comparison
reader: `hermes_state_portability.py::export_session(include_inactive=True)`.
Source: https://github.com/NousResearch/hermes-agent/tree/97a9ae8ee55d31bebce49f1fb6b5ef775323976a

`capture_fixture.py` beside this adapter creates only synthetic evidence with
SessionDB create/append/end/reopen/archive_and_compact calls in an explicitly
isolated HERMES_HOME. Run with the pinned checkout on PYTHONPATH and its Python
dependencies installed. It writes upstream table DDL and rows as SQL and the
independent official JSON export under `crates/ai-hist/tests/fixtures/hermes`.
No authenticated service, model turn or private user history is required.

The captured session ID `chat/not-a-uuid:α` proves arbitrary string identity;
the fixture contains user/assistant text, a native tool-call ID and result,
reasoning, resumed turns, in-place compaction archives, and a compression child.
The official export comparison tests role, content, tool-call identity and row
counts. All timestamps come from provider epoch seconds converted to milliseconds.

Root precedence: freeze explicit HERMES_HOME at ProviderRoots construction,
otherwise the recorded home's `.hermes` (plus upstream HERMES_DATA_DIR_SUFFIX
when environment resolution is enabled). The Windows resolver uses LOCALAPPDATA/hermes
as upstream does; the fixture/gates were run on macOS. Process overrides trim
whitespace and expand `$VAR`, `${VAR}` and `~/` paths; `~other-user` expansion is
not supported. Context-local overrides inside a live Hermes process are not
observable by a passive importer. `from_home` ignores every environment override. Scan `state.db` and direct
`profiles/*/state.db` beneath that frozen root. Profile directories are enumerated
at scan time, so later-created profiles are discoverable. Watches use narrow
database/sidecar filters plus a watch on the profiles parent directory; the
watcher backstop refresh installs narrow watches for newly created profiles.
Nested writes before that refresh are picked up by the backstop scan. Existing
profile configuration and credential writes do not trigger a sweep. No active-profile
selection, config, credential file, blob path or legacy transcript is opened.
Named profiles under the default root use profile-name namespaces; the default
uses `default`. Native IDs are losslessly hex encoded after that namespace and
also retained verbatim in session metadata. External custom homes use a SHA-256
path namespace: moving those homes changes ledger IDs; default/profile homes are
portable. A custom HERMES_HOME that selects a default-tree named profile keeps
its normal profile identity.

Acquisition validates the database and every existing SQLite sidecar before
opening a read-only, WAL-visible transaction. Metadata and message queries use
that same snapshot; normalization consumes immutable captured rows. Database
bytes-read is unknown (zero), never confused with file size. Discovery reads
bounded metadata/first-prompt queries; no transcript allocation occurs there.
An unsupported schema (including older schemas), invalid role/type/timestamp,
malformed tool-call container, denied path or escaped symlink is an actionable
error. Absence of a root means no sessions; an actual empty schema-31 database
is valid empty history. No migration, journal-mode change or provider write
occurs. Full sweep uses the same normalizer and an atomic transaction per session.

Evidence limits: schema 31 only; legacy per-session JSONL is not supported.
Session model/title/parent/end-reason are metadata, not per-turn inferences.
Native integer message-row IDs retain archived generations as separate records.
Active/compacted/observed/compressed-summary/display-kind flags remain in markers;
summary, hidden, delegation-notice, observed and undone user-role rows are synthetic controls, not
human prompt entries. System records become markers. Tool arguments/results stay
verbatim with their actual call IDs; error status is unknown unless recorded in
an admitted contract. Paths in tool payloads are never dereferenced. External
blobs/media and structured reasoning sidecars are not read. Session token totals,
message token_count, and aggregated session_model_usage are not billable request
usage and are deliberately unavailable. Edits, delegation, native resume and live
execution are unavailable. Compression parent IDs are retained as metadata;
no delegation relationship is invented and no compression lineage is flattened.

Verification and exact gate results are recorded below as the implementation
settles. The SDK/CLI/MCP proof is `node crates/ai-hist/src/providers/hermes/smoke.mjs`
after native and SDK rebuild; it creates its own isolated synthetic fixture home.

Negative control: replacing the Hermes normalizer with an empty successful
return causes exactly these four named checks to fail:

- `hermes_public_history_conformance`: hydration emits no evidence.
- `compression_summary_is_not_a_human_turn_and_parent_is_not_delegation`:
  expected synthetic records are absent.
- `malformed_evidence_rolls_back_without_pruning_previous_rows`: malformed
  evidence is no longer rejected.
- `rewritten_metadata_and_retired_events_preserve_remote_shares`: catalog
  evidence is absent.

Restoring the normalizer passes all seventeen focused unit checks and the public
conformance test. This separates working evidence acquisition from successful
registration or an empty sweep. Existing 66 corpus tests and 10 source-registry
tests also pass; no pre-existing fixture snapshot changed.

Final provider validation (macOS, `TMPDIR=/private/tmp`; socket/process suites
ran outside the restrictive macOS sandbox):

- `cargo test -p ai-hist --all-features providers::hermes --lib`: 17 pass.
- `cargo test -p ai-hist --all-features --test provider_hermes --test fixture_corpus --test source_registry`: 1 + 66 + 10 pass.
- `cargo test -p ai-hist --test provider_hermes` and `cargo test -p ai-hist --features export --test provider_hermes`: pass.
- `npm run providers:check`: pass; `node --test scripts/*.test.mjs`: 173 pass.
- `cargo fmt --all -- --check`, explicit `rustfmt --check` for this adapter,
  `cargo clippy --workspace --all-targets --all-features -- -A clippy::too_many_arguments -D warnings`, and `rust-oleum`: pass.
- `cargo build -p ai-hist --no-default-features` and `cargo publish --dry-run -p ai-hist --allow-dirty`: pass. The latter aborts upload as intended.
- Native rebuilt with worktree-local dependencies; native contract 26 verifies.
  `sdk-ts/node_modules/ai-hist-native` resolves into this worktree's native crate.
- `node scripts/check-public-api.mjs`: matches the updated 949-item snapshot.
  The API adds `Source::Hermes`; rustdoc also splits the existing capabilities
  method into a separate impl listing without changing its signature.
- After the shared fixes, `cargo test --workspace --all-features` passes with
  no command-level exclusions (including the sourcing population assertion,
  66 corpus tests, 10 source-registry tests and 75 live-capture tests).
- `cargo test -p ai-hist --no-default-features` also passes with no
  command-level exclusions, including the sourcing population assertion.

Final integration base: setup commit
`562bac904dda3c8b7962b9f4e4b29c5bfed1ed32`, composed with main `e0827b98`
(#465). This includes the shared repair, SDK admission, generator, formatting,
and migration test-isolation corrections. The full workspace and no-default
runs above preceded this final mechanical rebase; affected checks below were
rerun on the final composition.
The descriptor now drives the generated sourcing population row. The SDK accepts
`session_marker`, uses the existing badge fallback, and isolates provider root
overrides in its test environments. No provider-owned handwritten shared edits
were needed.

After integrating those changes:

- Provider generation: 8 tests; repository scripts: 173 tests.
- Native rebuilt; contract 26 and public API 949 verified.
- SDK lint passes; SDK suite: 278 pass, one filesystem capability skip (invalid
  UTF-8 filename support), zero failures.
- The strict checked-in synthetic SDK/CLI/MCP smoke passes historical user and
  assistant search, source filtering, and Hermes capability reads.
- Formatting, Clippy and rust-oleum pass.

Setup PR #464 is merged in main `cc11ea2a`; this provider is rebased on that merged setup before pilot integration.

Cancellation follow-up: database/profile enumeration, snapshot session/message
collection, and normalization now check the existing cooperative stop token.
Cancelling midway through normalization rolls back its current session transaction.
Disabling cancellation propagation produces two named failures:
`cancellation_interrupts_snapshot_collection_before_it_finishes` and
`cancellation_inside_normalization_rolls_back_partial_evidence`. Restoring the
checks passes both and all 17 focused provider tests.

Final-base affected verification:

- `cargo test -p ai-hist --all-features providers:: --lib`: 30 pass,
  including all 17 Hermes checks and the setup adapter repair checks.
- `cargo test -p ai-hist --all-features an_upgraded_store_gains_the_requests_of_standalone_records --lib`: 1 pass.
- `cargo test -p ai-hist --all-features --test provider_hermes --test fixture_corpus --test source_registry --test sourcing_sdk_doc`: 1 + 66 + 10 + 1 pass.
- `npm run providers:check`: 8 pass; formatting, Clippy, rust-oleum and
  public API checks pass (949 items).
- Worktree native debug rebuild, SDK lint/build, native contract 26 and
  `node crates/ai-hist/src/providers/hermes/smoke.mjs`: pass.

The final rebase changed no Hermes source behavior and required no conflict
resolution. Existing provider snapshots remain unchanged.

Out-of-tree consumer acceptance on the final provider checkout used an isolated
copy at `/private/tmp/hermes-rust-consumer` with only a local `ai-hist` patch.
`cargo tree --manifest-path /private/tmp/hermes-rust-consumer/Cargo.toml -p ai-hist --offline`
proved resolution to this checkout. `RELAYHISTORY_FIXTURES=/private/tmp/relayhistory-provider-hermes/crates/ai-hist/tests/fixtures TMPDIR=/private/tmp cargo run --manifest-path /private/tmp/hermes-rust-consumer/Cargo.toml --offline`
passed: six stock synthetic sessions, five usage rows and 104 change-feed records.
The stock consumer exercises Claude/Codex; Hermes-specific evidence is proved
by the public conformance test above. No repository example or lockfile changed.

Review corrections (schema-31 parser generation v2): discovery uses latest message
activity and globally ranks candidates across profiles before applying the limit.
Fingerprint inputs stat only the database, WAL and rollback journal, without opening
SQLite. Changed-path sweeps select the database/profile named by a database,
sidecar or ancestor event. Each selected complete database snapshot commits
normalization and native-session deletion reconciliation in one ledger transaction;
unread and unvisited profiles cannot retire prior evidence. Remote evidence
and remote session presence survive local deletion. Shallow prompt controls match
full normalization, and prompt ownership/cwd rewrites refresh the project search
projection, including a surviving fork with a different or cleared cwd.

Reverting prompt controls, deleted-session reconciliation and project refresh causes
three named regressions to fail: `shallow_prompt_excludes_undone_async_and_observed_controls_like_full_capture`,
`deleted_sessions_retire_only_after_complete_selected_profile_capture`, and
`prompt_project_follows_final_owner_and_cwd_rewrites`. Restoring the fixes passes
all 22 Hermes unit tests and public history conformance. The stat-only fingerprint
regression uses a deliberately corrupt database: fingerprints still succeed while
SQLite discovery rejects it, distinguishing metadata polling from hidden reads.

Reverting global profile ranking and retiring disappeared sessions before new
surviving evidence causes exactly two further failures:
`discovery_ranks_profiles_by_latest_message_and_fingerprint_never_opens_sqlite`
and `replacement_snapshot_preserves_shared_prompt_identity_and_new_owner_project`.
The replacement regression proves that a newly observed fork retains the prior
prompt row ID and projects its own cwd. Surviving sessions normalize before local
absence reconciliation within the same database transaction.

Affected validation: 22 Hermes units, one public conformance test, 66 corpus tests,
10 registry tests and the sourcing documentation population check pass. Generator
check (8 tests), public API (949 items), formatting, Clippy and rust-oleum pass.
The worktree-local native addon and SDK rebuild pass the strict SDK/CLI/MCP smoke.
Only Hermes snapshot source stamps change for parser generation v2.

Root identity correction (parser generation v3): native Windows LOCALAPPDATA and
HERMES_DATA_DIR_SUFFIX trees keep portable default/profile IDs using explicit
resolver provenance frozen beside paths. An explicit HERMES_HOME override remains
path-namespaced, including when it spells the same path as a native default. This
deliberate distinction survives serialization; an empty provenance map is retained.
Old serialized roots lacking provenance infer only the canonical no-environment
default and never read current environment variables. Complete sweeps retire local
rows under the former path-hashed identity when a proven native root is reclassified,
while retaining remote evidence. Platform branches are exercised with injected
environment inputs rather than changing process environment during parallel tests.

Root-provenance acceptance: reverting `profile()` to its former path comparison
fails `native_suffix_and_windows_roots_keep_portable_ids_but_same_path_overrides_do_not`.
Restored resolver/provenance passes 39 provider/setup units, 5 paths units,
94 discovery tests (including explicit root construction),
public Hermes conformance, 10 registry cases, sourcing population documentation,
9 generator cases, public API (unchanged 949 items), formatting and Clippy.
`with_all_roots` now delegates default construction to `ProviderRoots::from_home`;
its reduced discover.rs LOC ceiling is lowered from 3424 to 3411.

Late review correction (parser generation v4): a complete sweep now retires local
history for a confirmed missing default/profile `state.db`. The catalog locator
must remain inside an accessible configured Hermes root with the native layout;
existing ancestors are containment-validated and readable. Missing/unavailable roots,
unconfigured old locators, unrelated watch paths, failed snapshots, and escaped
symlinks never authorize retirement. Remote shares remain readable. Removal of an
entire profile directory is reconciled when its configured root remains accessible.

Tool-use events now retain the same native call ID as the result and public
`ToolCall`. Full capture uses the coherent snapshot's latest timestamp, bounded by
session start. Clearing cwd withdraws path-derived catalog/event project keys;
stronger remote/inherited identities and remote-only event path projections remain
intact.

Negative evidence: on the prior implementation,
`deleted_database_retires_only_confirmed_scoped_local_evidence` retains local
presence, `hermes_public_history_conformance` exposes a null tool-use block ID,
and `full_snapshot_bounds_activity_and_clears_path_project_identity` records
activity before start. Reverting only the new cwd reconciliation causes that last
regression to retain `/fixture/project`. Restoring the fixes passes these checks;
`deleted_profile_database_preserves_unvisited_default_and_rejects_escaped_parent`
additionally proves scoped profile deletion and containment rejection. The corpus
snapshot changes only the tool-use ID and parser source stamps. The native smoke
resolves its session from actual discovery so explicit custom-root identity is
verified rather than assuming the default namespace, and checks native call/event
ID linkage before SDK/CLI/MCP search assertions.

Absence proof is filesystem-local: a still-readable empty mountpoint is
indistinguishable from an intentionally emptied root. A missing or inaccessible
configured root is preserved; this adapter does not infer operating-system mount
state from directory readability.

Removing only the event-location guard fails the remote-only path assertion
(`project_key` becomes NULL). Restoring it preserves the imported remote cwd/key.

Final v4 checks: `cargo test -p ai-hist --all-features providers::hermes --lib`
passes 27; public Hermes conformance, corpus, registry and sourcing documentation
pass 1 + 66 + 10 + 1. Provider generator checks pass 9; public API remains 949.
Workspace Clippy with all targets/features and warnings denied, workspace/adapter
formatting, and rust-oleum pass without baseline changes. The final native debug
addon and SDK builds pass the stricter synthetic SDK/CLI/MCP smoke. Independent
read-only review signed off the deletion trust/scope checks, remote-only path guard,
canonical project preservation, timestamp bound and public block identities.

Frozen namespace is part of the stat-only fingerprint stamp. Native and explicit
custom roots selecting identical database paths/bytes now invalidate the source
cache when their ledger identity contract changes. Prefixing the namespace leaves
the terminal file length available to the shared byte fold; no SQLite open is
introduced and normalization generation remains v4. The regression
`identical_database_path_namespace_change_invalidates_unforced_sync_fingerprint`
first fails on identical old fingerprints. With that assertion bypassed, restoring
the old stat-only stamp also fails the actual public unforced sync: it skips the
required sweep after reopening the ledger under custom provenance. The fix proves
unchanged repeats still skip, provenance changes sweep, old local IDs retire, and
only new custom IDs remain. All 28 Hermes units, including the corrupt-database
no-open fingerprint control, pass; formatting, Clippy and quality gates pass.

Final composition follow-up: Pi #468 merged into main. Commit `52492960`
carries four Pi review corrections (generation `pi_v3_4`) into this branch:
confirmed missing-file retirement, retained prompt project refresh, complete
first-prompt replacement/clearing, and observed shallow recency. Independent
Sol review signed off. Combined checks passed: 64 provider/setup unit tests,
Pi/Hermes public suites (6/1), corpus (66), registry (10), sourcing docs (1),
and both public suites without default features (6/1). Formatting, provider
generation checks and rust-oleum passed. See the Pi plan for negative controls.

A final review-thread audit found the prior reply to the quadratic-query
finding had addressed ordering but not query cost. Commit `daf5df9f` replaces
the correlated per-session message scan with one grouped timestamp aggregate
joined to sessions. Selected capture filters that aggregate by native session.
The new EXPLAIN regression fails with `CORRELATED SCALAR SUBQUERY` when only
the old query is restored; it also verifies indexed selected-session lookup,
limited recency ordering and empty-session end-time fallback. Independent Sol
review signed off. Final query checks: `cargo test -p ai-hist providers::hermes
--lib` (29), fixture corpus with `unstable-internal` (66), workspace all-target
all-feature Clippy with warnings denied, formatting, and rust-oleum all pass.
The previous composed head `4fe3a8aa` passed all nine hosted CI jobs; the final
head requires a fresh run.


## Transport compatibility acceptance (#521)

The schema-31 native SQLite fixture now runs through discovery, hydration,
actual paged `ExportSnapshot`, public remote observation/evidence intake, and
local hydration in both acquisition orders. Recorded call IDs, result text,
message blocks and markers survive; unknown usage stays unknown. Remote-first
rows become `both` after matching local hydration. Local-first rows keep their
local canonical ownership while retaining the remote observation. Repeats do
not change canonical evidence; observation bookkeeping can still advance.

A separate checkout at `d6211bf47f8fc8f72047b8b720c1840525cbe686` produced the
checked-in old index SQL dump and actual export archive. That revision rejects
its own exported native tool-use IDs with the result-only validation error.
The current intake accepts that unmodified evidence. The restored old index
reads and exports without staging native history, syncing or reindexing; its
`hermes_schema31_v4` checkpoint remains intact. Fixture provenance and exact
reproduction commands are in `tests/fixtures/hermes/transport/README.md` under
the crate. The checked-in dump is reproducible from the captured SQLite file.

The malformed-identity regression fails without rejecting empty/whitespace
call IDs; result-only fields remain forbidden on tool-use/text/thinking events.
The conflicting-native-rewrite regression fails when the remote-backed payload
guard is absent: hydration succeeds instead of rejecting the rewrite. Kimi's
existing guard is now shared and Hermes runs it inside the same transaction
as normalization, preserving imported evidence on conflict. No normalization
generation bump or forced reindex is needed.

Focused acceptance: 5 Hermes, 29 Kimi and 22 source-intake tests pass with
`cargo +1.99.0 test -p ai-hist --all-features --test provider_hermes
--test provider_kimi --test source_intake`. Broader and independent review gates
are recorded in the pull request once completed.
