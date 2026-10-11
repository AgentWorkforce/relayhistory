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

Restoring the normalizer passes all twelve focused unit checks and the public
conformance test. This separates working evidence acquisition from successful
registration or an empty sweep. Existing 66 corpus tests and 10 source-registry
tests also pass; no pre-existing fixture snapshot changed.

Final provider validation (macOS, `TMPDIR=/private/tmp`; socket/process suites
ran outside the restrictive macOS sandbox):

- `cargo test -p ai-hist --all-features providers::hermes --lib`: 15 pass.
- `cargo test -p ai-hist --all-features --test provider_hermes --test fixture_corpus --test source_registry`: 1 + 66 + 10 pass.
- `cargo test -p ai-hist --test provider_hermes` and `cargo test -p ai-hist --features export --test provider_hermes`: pass.
- `npm run providers:check`: pass; `node --test scripts/*.test.mjs`: 171 pass.
- `cargo fmt --all -- --check`, explicit `rustfmt --check` for this adapter,
  `cargo clippy --workspace --all-targets --all-features -- -A clippy::too_many_arguments -D warnings`, and `rust-oleum`: pass.
- `cargo build -p ai-hist --no-default-features` and `cargo publish --dry-run -p ai-hist --allow-dirty`: pass. The latter aborts upload as intended.
- Native rebuilt with worktree-local dependencies; native contract 26 verifies.
  `sdk-ts/node_modules/ai-hist-native` resolves into this worktree's native crate.
- `node scripts/check-public-api.mjs`: matches the updated 949-item snapshot.
  The API adds `Source::Hermes`; rustdoc also splits the existing capabilities
  method into a separate impl listing without changing its signature.
- The unmodified workspace gate reaches and fails
  `the_guide_population_table_is_what_the_crate_declares`: parent-owned
  `docs/sourcing-sdk.md` needs the Hermes population row. The preceding library
  run had 1263 passes and one ignored test. Both the full workspace/all-features suite and the no-default suite pass
  with only that known documentation check explicitly skipped. The required
  unmodified gates must be rerun after the shared generated documentation fix.

Shared integration blockers (outside this provider's handwritten ownership):

1. `sdk-ts/src/operations.ts` still types its seven legacy icons as
   `Record<Source, string>`, so adding Hermes fails SDK lint/build. Its existing
   fallback supports unknown icons; the map should be partial.
2. `sdk-ts/src/normalization.ts::hydrationCoverage` rejects `session_marker`.
   Hermes truthfully declares marker coverage; its final SDK hydration fails
   with `NATIVE_CONTRACT_MISMATCH` until the shared vocabulary/types are fixed.
   Before adding that truthful coverage declaration, the isolated smoke proved
   historical user/assistant search, source filtering and capabilities through
   SDK, CLI and MCP. This is not a final green SDK result. Rerun the checked-in
   `smoke.mjs` and complete SDK suite after the shared fixes.
3. Parent must add the sourcing SDK population row, root changelog/Rust API
   note, and include HERMES_HOME in shared SDK test-environment scrubbing
   (alongside the other pilot's override). Tests here use only isolated homes.

Do not label the provider integration ready or close #429 until those shared
changes and all gates pass. No publication, merge, push or PR creation was done
by this provider worker. Setup #464 must merge before pilot integration.
