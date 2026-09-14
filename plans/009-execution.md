# Plan 009 execution record

Base: `b20fd1e` (0.16.0), reconciled from plan baseline `3155117`.
Only release version/lockfile changes occurred between those commits.

## Baseline

- `cargo test --workspace`: 460 passed, 2 existing benchmark tests ignored.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -A clippy::too_many_arguments -D warnings`: passed.
- `node --test scripts/*.test.mjs`: 13 passed.
- Native debug build and contract 11 verification: passed; declarations unchanged.
- SDK lint and `npm test`: 130 passed on Node 22.
- Tests using localhost/process inspection were rerun outside the filesystem/network
  sandbox after initial permission failures. No production service was exercised.

## Characterization matrix

| Contract | Coverage | Follow-up |
| --- | --- | --- |
| Canonical session identity, local versus remote presence | Existing discovery tests plus new core scan-order tests | Replace remote connector overwrite behavior in observation migration |
| Connector-specific hydration progress | New checkpoint-key characterization | Add authoritative observation checkpoints |
| Retry after reopening unchanged storage | New outbox test | Preserve through coordinator extraction |
| Retry after evidence changes | New known-limitation characterization | Persist immutable queued payloads before dispatch |
| SDK cached scope/auth behavior | Existing remote-auth tests and new public-contract tests | Remove commercial auth from cached queries |
| Evidence identity and tied cursor ordering | New SDK public-contract fixtures | Preserve through export/native boundaries |
| Native auth, refresh, atomic replacement, redaction | Existing cloud/auth suites pass | Preserve in RelayHistory plugin |
| Transcript cursor with interleaved sessions | Reproduced A1/B2/A3 budget=1 data loss outside baseline | Separate cursor fix PR before extraction |

## PR sequence

1. Characterization tests and implementation plan.
2. Transcript cursor data-loss fix.
3. Explicit source connector selection and offline query semantics.
4. Observation migration, Rust extraction, durable delivery, plugin packaging,
   and isolation gate in dependent reviewable changes as specified by plan 009.

The plan remains IN PROGRESS until all acceptance criteria are implemented and
verified. Characterization intentionally records several existing violations;
passing those tests does not mean the boundary is already fixed.

## Active execution state

- PR #139: https://github.com/AgentWorkforce/relayhistory/pull/139 — characterization; CI passed.
- PR #140: https://github.com/AgentWorkforce/relayhistory/pull/140 — transcript cursor fix; CI passed.
- PR #141: https://github.com/AgentWorkforce/relayhistory/pull/141 — explicit
  connector selection/offline queries; branch `codex/local-history-explicit-connectors`,
  worktree `stage2`, head ea421cb. Native contract 12. Review correction restores
  quiet native sync; fresh-process Rust/SDK regressions pass. Integrated checks:
  479 Rust tests, 139 SDK tests, 13 scripts; clippy/fmt/lint/native verification passed.
- PR #142: https://github.com/AgentWorkforce/relayhistory/pull/142 — Rust CLI
  extraction; branch `codex/local-history-cli-boundary`, worktree `stage4-cli`,
  head a919497. Integrated 479 Rust tests/clippy/fmt/native contract12 passed.
- Worker `explicit_connectors`: observation provenance/injectable source registry,
  branch `codex/history-source-observations`, worktree `observations`.
- Core durable queue/journal/export APIs committed a0a4435, worktree `delivery`;
  reviewer reran155 core tests/clippy. Worker `rust_characterization` now validates
  real ingestion checkpoint recovery on `codex/history-delivery-capture`, worktree
  `capture`, based on CLI extraction plus core. Not yet published.
- Worker `sdk_characterization`: delivery host/NAPI/plugin APIs on
  `codex/history-delivery-host`, worktree `delivery-host`; Stage2 corrective test is integrated in PR #141.
- Worktree parent: `/private/tmp/rh-local-history-20260913`.
- Original checkout remains on main with its untracked plans/review files preserved.
- No PR has been merged and no release has been published.

Still required: complete/review durable storage+host; observation migration;
finish Rust/cloud/source extraction; optional RelayHistory plugin and cursor
migration; physically absent-cloud isolation CI; all integration/recovery gates.


## Approved server extension

User approved a companion server fix after the legacy API's missing revision
fence was demonstrated from source. Worktree `server` in the same temporary
parent, branch `codex/durable-history-delivery`, based on freshly fetched
`AgentWorkforce/relayhistory-cloud` main `8f4b8af`. Original server checkout is
clean and unchanged. No deployments authorized or performed. Baseline checks
are running; assign an implementation worker after the current bounded task.


## Further verified PRs

- PR #143 https://github.com/AgentWorkforce/relayhistory/pull/143: durable core
  and ingestion capture recovery, branch `codex/history-delivery-capture`, head
  8fa4817. Integrated501 Rust tests and139 SDK tests/clippy/fmt/native12 pass;
  all platform CI jobs pass.
- PR #144 https://github.com/AgentWorkforce/relayhistory/pull/144: destination
  plugins/background driver/NDJSON/CLI/MCP, branch
  `codex/local-history-delivery-host`, worktree `stage5-host`, head cb39523.
  Integrated151 SDK tests,8 architecture tests, native13/clippy/fmt/lint pass.
  Review fixes: active DB export alias protection, stale receiver revisions,
  and verbatim plugin CLI arguments after an explicit separator.
- Rust cloud worker: `cloud-rust`, branch `codex/relayhistory-rust-plugin`.
  Own commits4eee324 and402fa69 add typed scans/optional helper then remove
  localCLI/native cloud operations (contract14). Dependency host commitb74fbca
  is already represented by PR144. Core/engine cloud originals still await
  transport extraction; no absence claim yet.
- Source worker: `observations`; authoritative provenance and injected registry
  tests implemented; full review still in progress. Export evidence is being
  normalized per observation record to avoid unbounded session-sized blobs.
- SDK worker now owns server companion implementation in `server`. Server
  baseline351 tests/24files and package typecheck passed. No deployment.
- Next: integrate/review source observations and transport extraction, finish
  local TS/packaging isolation, optional RelayHistory destination transport and
  credential/cursor/service migration, server/client conformance, absence CI.

## Server review and corrective follow-up

- Companion server PR #45:
  https://github.com/AgentWorkforce/relayhistory-cloud/pull/45, head0867377.
  Reviewer reran372/372 tests and package typecheck successfully. Actual migrated
  PostgreSQL function tested in PGlite; no production migration or deployment.
- Rust destination transport5027518 implemented exact persisted-body retries,
  account assertions, receipt validation and versioned readback. Review requires
  matching server record limits and byte-bounded read pages; joined helper/server
  conformance is next.
- PR144 Node22 and Windows pass; Linux/Node20 verify failed three delivery timing
  tests. Worker is investigating lease/deadline behavior. Additional validated
  fixes: nonexistent database aliases through symlinked output parents, native
  JSON escaping overhead, plugin CLI global flags and conservative MCP metadata.
- PR141 review fixes underway for Relaycast lock ordering/independent connector
  progress and hydrate capability preflight.
- PR143 review fixes underway for atomic state transitions; pause/resume must
  not bypass a permanent blocked failure or race with failure recording.
- SDK extraction worker assigned `sdk-plugin` on codex/relayhistory-sdk-plugin.
  Source registry/native intake and physical remote extraction remain in flight.

## Integrated review corrections

- PR141 head9a55b90: Relaycast lock acquisition precedes DB initialization;
  a busy Relaycast does not skip selected Codex discovery. Hydration capability
  filtering excludes commercial recall before reading auth. Reviewer reran the
  two fresh-process preflight tests, auth-read counter regression, and CLI lock test.
- PR142 headed74b85 includes fs2 test dependency after CLI test relocation.
- PR143 head959520a includes atomic blocked-state fixd41540f; reviewer reran all
  157 core tests and formatting successfully. Two-connection race regression
  and each permanent failure category require explicit retry.
- PR144 head0c6dd6f includes correctionacc2a0a. Reviewer rebuilt native13 and ran
  all154 SDK tests/lint successfully. Node20/22 focused tests pass in worker.
  Corrections pushed to existing PRs with ancestry-preserving merges.
- Observation foundation integrated75c82be, latest3bed542, branch
  codex/history-observation-boundary in stage3-observations. Full workspace review
  underway. Events-only external intake explicitly reports partial capability;
  complete normalized records/reconciliation follows before the final boundary.
- Full source plugin work now source-plugins / codex/history-source-plugins.
  Includes Rust optional helper dependencies; sourceworker owns final core/engine
  cloud deletion and generic native intake, SDKworker owns TS orchestration.
- Joined actual Rust helper/Hono/PGlite tests pass in worker for receipt loss,
  out-of-order revisions and wrong authenticated account. Server byte-bounded
  readback follow-up is being verified; root review of source completed.

## Provenance and composed server verification

- PR145 https://github.com/AgentWorkforce/relayhistory/pull/145 head3bed542:
  reviewer ran521 Rust tests, clippy/fmt, rebuilt native13 and ran154 SDK tests
  plus lint. All passed. Full normalized source/plugin completion remains next.
- Server PR45 follow-up43b2bb2: reviewer ran377/377 tests with the actual Rust
  helper enabled (374 server +3 composed), and typecheck. No skipped tests in
  that run. Readback returns bounded3MiB pages with no omission; unsupported
  transformed size fails before commit. Follow-up pushed.
- Latest PR142/143/144 Linux, Node22 and Windows CI all pass. PR141 Windows
  exposed a missing cfg(unix) on a new shell-fixture test; tiny correction is
  underway. Its Linux/Node22 checks pass. PR145 CI is in progress.

## Final package integration underway

- PR145 corrections09b952a/211db1a preserve opaque acquisition locators and
  local OpenCode database paths; reviewer523 workspace tests plus16 focused
  OpenCode tests, clippy/fmt pass. Pushed through211db1a.
- Server PR45 a2ea43e rejects unknown mapping versions before writes; reviewer
  26 focused/helper tests and typecheck pass. c4d40fe adds actual SDK/native/plugin
  process-restart recovery against Hono/PGlite; reviewer independently reran all
  four composed tests successfully. Persisted request bytes match after lost
  commit receipt, one server receipt remains, local pending state clears only
  after acknowledgment, and SDK readback returns the delivered event. Pushed.
- Final integration branch codex/local-history-plugin-packages in final-packages
  starts at PR145. Optional Rust adapters and physical removals are being combined
  with normalized source intake and the SDK package split. Sourceworker owns
  source conflict resolution. Physical absence and installed tarball gates remain
  required before declaring the plan complete.

## Additional integration review

- Reviewer independently ran188 optional RelayHistory Rust tests and26 optional
  provider-adapter Rust tests on final-packages681f301; all passed. Local workspace
  test found one obsolete built-in remote wrapper expectation; sourceworker is
  updating it to test structured capability directly plus public plugin-required
  preflight behavior. The final local check will rerun after that correction.
- PR1457978355 preserves unknown legacy Codex diff rows without adding a duplicate
  connector-prefixed canonical projection; fresh bytes/checkpoints remain in the
  selected observation. Four focused legacy tests pass; correction pushed.
- Legacy Claude aggregate rows with unknown provenance remain conservatively
  retained. Fresh connector evidence is independent; extraction does not invent
  an owner for rows whose old presence was overwritten. This compatibility limit
  is being documented and characterized rather than silently deleting evidence.
- SDK review requires unknown selectors rejected before local work, isolation
  of failed source instances, deterministic handling of duplicate canonical
  records from multiple cloud origins, and explicit metadata-only capability.
  Helper review requires cancellation to stop descendant provider processes.
  Workers own these corrections and their regression tests.

## Final Rust boundary verified

- Source cleanup6674caa integrated asaefe9e2; latest provenance parent7978355
  merged cleanly (final head ea2ef04 before remaining SDK work).
- Reviewer reran complete local workspace:324 passed,0 failed,2 pre-existing
  ignored benchmark tests. Workspace strict clippy and formatting passed.
- Worker cloud-absent fresh workspace contained only unmodified Cargo manifests,
  lockfile and crates:322 tests passed, then the added migration intake test
  passed in its nine-test suite. Dependency metadata contains no HTTP transport
  or optional cloud/provider package reachable from core, engine or native.
- Final native14 rebuild and SDK/package verification are underway. Remaining
  reviews concern source aggregation, helper process-tree cancellation, docs,
  and installed/release artifacts; the overall plan remains in progress.

## Final SDK and packaging verification

- SDK extractionfe205ab integrated as1042235; docs397ac4d integrated ase19759f;
  packaging/CIbcd3883 integrated as10f9683.
- Reviewer local SDK:125 tests passed, no skips; install, lint and native14
  contract/declaration checks passed. A fresh committed checkout with `plugins/`
  physically removed and unchanged manifests also passed install, lint and all125
  SDK tests. Root rechecked the absent workspace dependency closure for core,
  engine and native: no cloud/provider package or HTTP transport.
- Reviewer fresh local tarball installation passed on darwin-arm64. Four packaging
  regressions passed, covering all14 optional plugin/platform metadata fixtures,
  independent versions and rejecting helper failure despite valid JSON output.
- Fresh optional SDK `npm ci --ignore-scripts --omit=optional` exposed missing
  platform entries in both lockfiles; worker owns correction without removing
  required optional dependencies. The provider helper's invalid selector error
  code is also being corrected/tested before final composed verification.

## Final implementation outcome

- Final package branch `codex/local-history-plugin-packages` includes all source,
  SDK, helper, packaging and review corrections through f0ef088.
- Reviewer final local workspace:324 Rust tests passed, strict clippy/fmt passed;
  native14 source/addon/SDK contract and generated declarations match.
- Reviewer local SDK125 passed on Node22; worker Node20 also125 passed. Fresh
  cloud-absent unchanged-manifest install/lint/SDK125 and release-script tests pass.
- Reviewer optional SDK45 RelayHistory +6 provider tests passed with real helpers.
  Optional Rust188 RelayHistory +24 provider unit +5 provider helper tests passed.
- Reviewer installed local and composed tarballs pass on darwin-arm64. All20
  script/packaging regressions pass. Final SDK/native/helper/Hono/PGlite composed
  recovery4/4 passes, including a new process after lost committed receipt.
- PR1457978355 and server PR45c4d40fe CI pass. Final PR adds Windows helper
  process-tree and installed-package CI; nonlocal platform execution is pending.
- Published-core gate rejects actual ai-hist0.16.0/native11 before any optional
  publishing. Compatible core must be released first and plugin peer minimums
  updated. Server migration0009 and a non-production Neon smoke remain release
  gates. No publishing, deployment, merging or user scheduler changes performed.
- Local verification found and fixed: per-instance source failure isolation,
  pre-I/O option validation, multi-origin canonical collisions/tombstones, partial
  capability aggregation, discovery recency, process-tree cancellation, helper
  error classification, immutable payload size blocking, optional lockfiles and
  esbuild clean installation. Original checkout and unrelated review files remain
  preserved. Implementation is complete; PR review/release gates remain explicit.
