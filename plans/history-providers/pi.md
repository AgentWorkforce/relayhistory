# Pi historical acquisition (#428)

Pinned product: `@earendil-works/pi-coding-agent@1.1.0`, native JSONL session
version 3. Published npm tarball integrity is
`sha512-SeEi/4hdcHNgA9UWlefZl7ZZpm3dzi2OoxNjDHsBJ9o298LNOtbL4DGKgitlEj6uCTccvtw6f2hlCkTPVJ2RXg==`;
the installed `dist/core/session-manager.js` SHA-256 is
`046b6a1109ac3f0ed893bb85bf0648709362fa926a5da75761216cf2fcf9d926`.
The registry supplies no `gitHead`; independently inspected public upstream `badlogic/pi-mono` revision
`89e5c2c075535e2f427c30fc74720d65b2944ff9`. Implemented and tested on macOS 26.6.2 (25G83), arm64;
paths use Rust platform paths. No private histories, authentication, model turns,
or agent execution were used.

Primary sources:
- https://github.com/badlogic/pi-mono/blob/89e5c2c075535e2f427c30fc74720d65b2944ff9/packages/coding-agent/src/core/session-manager.ts
- https://github.com/badlogic/pi-mono/blob/89e5c2c075535e2f427c30fc74720d65b2944ff9/packages/coding-agent/src/config.ts
- https://github.com/badlogic/pi-mono/blob/89e5c2c075535e2f427c30fc74720d65b2944ff9/packages/coding-agent/src/main.ts
- https://pi.dev/docs/latest/session-format
- https://pi.dev/docs/latest/environment-variables

The CLI chooses explicit `--session-dir`, then `PI_CODING_AGENT_SESSION_DIR`,
then project/user settings `sessionDir`, then `<agentDir>/sessions/<encoded cwd>`.
The agent directory is `PI_CODING_AGENT_DIR` or `~/.pi/agent`; tilde overrides
are expanded. RelayHistory freezes the two environment overrides once, and
accepts explicit roots via serialized `ProviderRoots.provider_roots.pi`. It does
not read settings/configuration files to discover additional roots. Explicit
session paths and settings roots must be supplied explicitly. `from_home`
ignores all operator environment variables.

The upstream writer emits a header followed by newline-terminated records and
flushes pending records when an assistant response first arrives. Every entry
has native `id`, `parentId`, timestamp and type. Session IDs can be custom
strings. `SessionManager.getEntries()` reads the full persisted tree;
`buildSessionContext()` projects only the active branch and compaction window.
RelayHistory captures the full tree. Compaction/context edits never erase raw
historical turns. `parentSession` is a file path, not a session ID: retain it in
the header marker and never dereference it or invent a relationship identity.

The checked-in synthetic fixture is written with the real upstream
`SessionManager.create`, `appendMessage`, `branch`, `appendUsage`,
`appendCompaction`, and `appendSessionInfo` APIs. Reopening the original file
with `SessionManager.open(...).getEntries()` produces the independent oracle
in `crates/ai-hist/src/providers/pi/oracle.json`. The reproduction script beside
it replaces only random entry IDs and wall-clock entry timestamps consistently;
message timestamps, tree structure, content and usage are retained. Runtime Rust
consumers need neither Node nor network. Run the script only with the pinned
package installed in its documented temporary tool directory.

Capture limits are explicit: v1/v2 and future versions are rejected rather than
migrated; persisted v3 only, no ephemeral/in-memory sessions. Assistant usage is
per message, cache-exclusive input plus distinct cache read/write fields.
Standalone usage and summary-generation usage remain raw markers, outside
assistant request totals. `tokensBefore` is context occupancy, never usage.
Tool calls/results join by actual IDs; file modifications are not inferred from
a tool name. Images/unknown blocks retain bounded raw metadata; external files,
spill paths and parent-session files are never opened. No native resume or live
execution capability is advertised.

Duplicate native session IDs at distinct live locators fail with
`PI_ID_COLLISION` naming both paths rather than mixing profiles. Moving a file
is permitted once its old locator is absent. Rewrites reconcile stable keys
transactionally; partial final records are deferred and prevent pruning.

## Verification and integration status

Implementation is stacked on setup commit
`562bac904dda3c8b7962b9f4e4b29c5bfed1ed32` (setup PR #464, composed on main `e0827b98`), rebased from the original verified setup commit `276bd3dba51312c7b78e7379cd79b04e5d313ff1`. Setup is not merged.
No package publication, push, merge or provider PR was performed.

Provider-level checks:

- `TMPDIR=/private/tmp cargo test -p ai-hist --all-features --lib providers::pi`: 9 pass. Covers schema validation, usage counters, bounded headers, immutable capture, explicit/legacy/default roots, denied roots, symlink/spill containment, scoped watch sweeps, partial rewrites, removed tool results, and shared multi-block fork prompts.
- `TMPDIR=/private/tmp cargo test -p ai-hist --test provider_pi`: 5 pass.
- `TMPDIR=/private/tmp cargo test -p ai-hist --features export --test provider_pi`: 5 pass.
- `TMPDIR=/private/tmp cargo test -p ai-hist --all-features --test provider_pi --test fixture_corpus --test source_registry`: 5 + 66 + 10 pass. Existing corpus and registry snapshots are unchanged; only the new Pi snapshot is added.
- `npm run providers:check`: pass. `node --test scripts/*.test.mjs`: 171 pass.
- `cargo fmt --all -- --check`, workspace Clippy with all targets/features and `-A clippy::too_many_arguments -D warnings`, and `rust-oleum`: pass without raising any baseline.
- `cargo build -p ai-hist --no-default-features` and `cargo publish --dry-run -p ai-hist --allow-dirty`: pass; no upload occurred.
- `node scripts/check-public-api.mjs`: matches 949 items. Expected addition is `Source::Pi`; the generator/rustdoc also moves the unchanged `capabilities` method into its own existing implementation block.
- `npm --prefix crates/ai-hist-napi run build:debug` and `node scripts/verify-native-contract.mjs ./crates/ai-hist-napi/index.js`: pass, contract remains 26. Dependencies were installed locally in this worktree; SDK native resolution points to this worktree's `crates/ai-hist-napi`.
- Out-of-tree consumer built and ran from `/private/tmp/pi-rust-consumer` with the checkout patched as `ai-hist` and `RELAYHISTORY_FIXTURES` pointing to this fixture corpus. `cargo tree` proves the local checkout was the dependency.

Mutation proof: temporarily disabling Pi selected ingest makes these four named
public acceptance tests fail with `PI_MUTATION_DISABLED_PARSER`:
`pi_discover_hydrate_read_reopen_changes_export_fullsync`,
`pi_append_partial_tail_rewrite_and_metadata_changes`,
`pi_messages_match_independent_upstream_reader_oracle`, and
`pi_missing_empty_unsupported_malformed_and_collision_are_truthful`.
Restoring the parser returns all five public tests to green. The symlink test
remains green under the mutation because it tests rejection before ingest.

The strict checked-in `sdk-smoke.mjs` now passes against the rebuilt native
addon and SDK: discovery, hydration, public user/assistant search including the
abandoned branch, source filtering, events, SDK/CLI/MCP search, marker capability
and partial coverage. The shared setup fixes accept the existing
`session_marker` vocabulary, use a source badge fallback, and generate the
population table from each provider descriptor. Pi adds its generated population
row; no provider-specific shared switch was introduced.

Rebased validation:

- `npm run providers:check`: 8 pass; `node --test scripts/*.test.mjs`: 173 pass.
- `npm --prefix sdk-ts run lint`: pass. `npm test` in `sdk-ts` with provider environment overrides cleared: 278 pass, 1 skip, 0 failures.
- Native rebuild, native contract 26, public API 949 items, formatting, Clippy, rust-oleum, no-default build, packaging dry run and out-of-tree Rust consumer: pass.
- `TMPDIR=/private/tmp cargo test -p ai-hist --no-default-features`: pass, fully unskipped with normal socket/process permissions.
- `cargo test -p ai-hist --all-features --test provider_pi --test fixture_corpus --test source_registry --test sourcing_sdk_doc`: 5 + 66 + 10 + 1 pass.
- The initial unskipped workspace run reported 1264 pass, 1 ignored and 1 failure in the shared `an_upgraded_store_gains_the_requests_of_standalone_records` test. Its raw cursor equality compared a newly added time-dependent `settled` envelope with an unsettled cursor; the exact isolated rerun passes. The setup follow-up disables only that optional stat-cache transition within the regression test using its existing thread-local guard, preserving the exact cursor assertions.

`TMPDIR=/private/tmp RUST_TEST_THREADS=4 cargo test --workspace --all-features`
passes without exclusions on setup `588d91f3`, including the slow CLI
contention case and all 75 live-capture checks. The no-default suite also passed
unskipped before the final shared test-isolation/helper-extraction follow-up.
Issue #428 remains open until the setup dependency lands and integration review
is complete. No publication, push, PR or merge was performed.

The final setup rebase onto main carries the shared guarded-write changes from
#465. The integration owner validated that composition independently; Pi is
revalidated on `562bac90` with its unit/public conformance, corpus/registry/doc,
generator, formatting, Clippy, complexity, public API and rebuilt native
SDK/CLI/MCP smoke gates. Full Rust suites were not redundantly repeated after
this final composition; their precise revisions are recorded above.
