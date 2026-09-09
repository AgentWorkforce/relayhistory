# WS-12 validation

Drafts: https://github.com/AgentWorkforce/relayhistory/pull/117 and
https://github.com/AgentWorkforce/relayhistory-cloud/pull/43. No merge or
production deployment has been performed.

## Added scope: npm token and replay

The branch is rebased on published 0.15.0's source (`865a536`). It reuses #111's
`cloud::access_token` and refactors the existing Rust replay entrypoint into a
shared result-returning operation. NAPI contract **9** exposes both to the public
async SDK (`accessToken`, `replay`) and the npm CLI (`token`, `replay`).

Validation on code head `8969793`:

- All 66 SDK tests passed after the rebase, including first-run bootstrap,
  architecture constraints, the 525-record cloud fixture and both new commands.
- Existing Rust integration suites passed: 11 token tests and 8 replay tests.
  Workspace Clippy passed before the rebase; the rebase only brought forward
  the release metadata and existing bootstrap behavior.
- Fresh local npm tarballs for the SDK, native loader and Darwin ARM64 addon
  were installed in an isolated directory. The command suite passed against
  that installed artifact, including exact token stdout, proactive rotation,
  redacted refresh/parser failures, explicit/environment stage selection and
  ambiguity refusal, short-page pagination, opaque cursors, truncation markers,
  repeated cursor rejection, and preservation of existing output on failure.
- The same test suite now runs in CI's installed SDK/CLI smoke step against
  Linux tarballs. Source and built addon agree on native contract 9.
- Live dev, using the earlier isolated synthetic session's auth: the installed
  npm `token --base-url https://dev.history.agentrelay.com` exited 0, returned
  exactly one service-token line and emitted no stderr. The token was captured
  in memory and was not printed in the validation log.
- Installed npm `replay ws12-cloud-demo-1788873511810 --base-url
  https://dev.history.agentrelay.com --limit 1 --json` exited 0 and returned all
  three events, including exact prompt `prompt:1788873511858:558f0daa0735f957`.
  It used the Rust pagination path and never imported into SQLite.

These are locally built artifacts, not a new npm publication. Cloud PR #42's
instructions become usable from the registry after the owner merges/releases
the client. This lane has not merged or published anything. No additional
sharing implementation was undertaken for this scope addition.

## Artifact and CI evidence

- Native source, SDK and built addon agree on contract 8.
- Client head e98b4ec passed all required CI jobs in
  https://github.com/AgentWorkforce/relayhistory/actions/runs/34236408938:
  Rust formatting, Clippy and workspace tests; native contract; Node 20 SDK;
  installed npm tarball SDK/CLI smoke; Node 22 SDK; Windows state replacement.
- The Node 22 SDK suite passed all 61 tests. Its actual built npm command fixture
  runs with fresh isolated HOME/state and a simulated Agent Relay device-flow
  service: all 525 unique synthetic prompts arrive in two batches; token refresh,
  stale legacy SDK auth, stage isolation, failed pushes, Git notes, prior hook
  chaining, external shared-hook rejection and native sharing are exercised.
  The final built addon also passed this focused fixture locally on September 8.
- Local Rust transport tests: 50 passed. Local outbox tests: 29 passed.
- Server head f475d7c passed formatting, typecheck and the full test suite in
  https://github.com/AgentWorkforce/relayhistory-cloud/actions/runs/34232491784.
  Four PGlite sharing route tests cover visibility, owner/tenant isolation,
  escaping, frozen content and revocation; 15 session-link tests also pass.

## Live dev demonstration — September 8, 2026

The dev workflow on the cloud draft branch succeeded, including credential
preflight, all tests, additive Neon migrations, SST diff, dev deployment and
post-deploy stage-binding/health verification:
https://github.com/AgentWorkforce/relayhistory-cloud/actions/runs/34236389791.

1. With fresh isolated ai-hist HOME/state, the built npm command
   `ai-hist enable-cloud --base-url https://dev.history.agentrelay.com --once --json`
   exchanged an existing real Agent Relay Cloud identity and returned
   `sent=1, accepted=1`. This demonstrates fresh ai-hist setup, not a fresh browser
   device approval. The trusted-dev opt-in was set in this isolated fixture.
2. After deployment, authenticated `GET /v1/events?session=ws12-cloud-demo-1788873511810`
   returned HTTP 200 and the exact persisted synthetic event
   `prompt:1788873511858:558f0daa0735f957`, with content
   `Synthetic WS-12 acceptance fixture ws12-cloud-demo-1788873511810: prove one-command cloud push.`
3. The final SDK installed a repository-local hook for that indexed session and
   https://github.com/AgentWorkforce/relayhistory/pull/117. An actual Git commit
   (`c965e86c7a54a354d8c7a7917a487da474e3adcb`) appended the session note. The next
   `pushCloud()` returned `sent=2, accepted=2`.
4. `GET /v1/sessions/ws12-cloud-demo-1788873511810/thread?source=claude`
   returned HTTP 200 with the persisted session-link projection:

   ```json
   {
     "linkKind": "github_pr",
     "linkRef": "AgentWorkforce/relayhistory#117",
     "linkUrl": "https://github.com/AgentWorkforce/relayhistory/pull/117",
     "confidence": 1
   }
   ```

5. `createShareableTrace(sessionId, {visibility: 'direct-link', source: 'claude', baseUrl})`
   returned a frozen three-event snapshot at this demo URL:
   https://dev.history.agentrelay.com/s/be1f610eace1cb5debf2bb6cb97dd61d9e979ec801690ae1862b21a869c93c64
   An anonymous HTTP GET returned 200, the exact synthetic prompt in rendered
   HTML, and `X-Robots-Tag: noindex, nofollow, noarchive`. Browser visual inspection
   was unavailable because the browser runtime import was rejected by its tool.

## Practical limits and environment notes

The SDK push loop is foreground/in-process. Agent Relay must be installed for
interactive device login. Hooks use an existing indexed session; PRs created
after installation require rerunning installation. Shares contain frozen
convergence events; private URL reads currently require an authenticated HTTP
client. See enable-cloud.md and the cloud PR's docs/sharing.md.

Early broad local runs were interrupted by a broken Homebrew Node, ENOSPC and
host overload. Subsequent commands used mise Node 22.22.2; only this worktree's
reproducible Rust cache was removed to recover space. Local SST attempts failed
on missing/expired Cloudflare credentials; the authorized dev workflow supplied
its existing environment credentials and completed deployment successfully.
Earlier dev recall 404s were resolved by this deployment; persistence is now
verified by exact event content and the session-link read above.

## Rebase onto 0.15.1 and re-verification against the installed artifact

The branch was rebased from its original base (`865a536`, release 0.15.0) onto
`5188b13` (release 0.15.1), picking up the four changes that landed underneath
it: `2fd5bbc` (Rust `token`), `3d559ed` (first-run bootstrap), `5f3246b`
(npx first-search verification and pretty session output) and `d56f64d` (the
native Linux glibc floor). The only conflicts were in `sdk-ts/src/cli.ts` and
both were unions rather than competing edits: `BOOLEAN_FLAGS` now carries both
main's `pretty` and this branch's `once`, and the usage block lists both
`enable-cloud` and `sessions list --pretty`.

The bootstrap seam was checked explicitly: `bootstrapLocal` runs only on a bare
`ai-hist` invocation with no command, so it cannot write to stdout ahead of
`token`. `token` writes the token and nothing else to stdout; the
secret-in-scrollback warning goes to stderr and only when stdout is a TTY.

Post-rebase evidence, all on darwin-arm64 with mise Node 22.22.2:

- `cargo fmt --all -- --check` clean; `cargo clippy --workspace --all-targets
  -- -A clippy::too_many_arguments -D warnings` clean.
- Native contract: `verify-native-contract.mjs` reports contract version 9
  agreeing across the Rust binding source, the TypeScript SDK source and the
  built addon; `git diff --exit-code -- crates/ai-hist-napi/index.d.ts` clean,
  so the checked-in declarations match what the build regenerates.
- `sdk-ts`: `tsc --noEmit` clean, `npm test` 73/73 passing.
- `node --test scripts/*.test.mjs`: 10/10 passing.

### Verified against the artifact, not the source

Packing and installing the tarballs the way CI does — SDK, loader and the
darwin-arm64 platform addon — into a scratch project, and then running the
installed `./node_modules/.bin/ai-hist`:

- The installed CLI's usage block lists `ai-hist token [--base-url URL]` and
  `ai-hist replay SESSION_ID [--base-url URL] [--limit N] [--max-content N]
  [--json] [--out PATH]`.
- For contrast, the same check against the actually-published `ai-hist@0.15.1`
  from the registry lists neither: zero matches for `token` or `replay`. This
  independently reproduces the QA report that the published CLI cannot reach
  these commands.
- `cloud-commands.test.js` was rerun with `AI_HIST_TEST_PACKAGE_DIR` pointed at
  the installed package and passed, covering token stdout exactness, proactive
  refresh with credential rotation, secret-safe failures, stage selection,
  replay pagination and atomic `--out` replacement.
- The specific failure QA reported — `$(ai-hist token)` silently yielding an
  empty string — was reproduced as a shell command substitution against a
  fixture cloud using the installed binary. It captured a 27-character token
  identical to the one the fixture issued, with a non-zero length.

## Review round: four independent reviewers converged on the hook-install path

Rebased again onto `6e6acac` (#118's README rewrite). The README conflict was
resolved in main's favour: #118's landing copy is kept, and the cloud entrypoint
is reintroduced lower down described accurately, since `enable-cloud`
authenticates and syncs but does not install Git hooks.

Six review threads landed on the PR. Four of them — from CodeRabbit and
cursor — pointed at `crates/ai-hist/src/git_sdk.rs`. That convergence was
correct; all four described real defects in hook installation.

**P1, installing fixed hooks from a linked worktree.** Confirmed against real
Git rather than by reading: in a linked worktree
`git rev-parse --git-path hooks/post-commit` resolves to the *main* worktree's
shared `.git/hooks/post-commit`, so the existing containment check passed and
installation proceeded. The written hook embeds one fixed `sessionId` and `repo`,
so every other worktree's commits would have fired it and been attributed to the
wrong session — corrupting exactly the linkage the feature exists to record. Now
refused by comparing `--git-dir` with `--git-common-dir`, and scoped so it only
fires for hooks that are actually shared.

**P2, resolving a nonexistent hooks path.** `core.hooksPath` containing `..`
satisfied the containment check lexically while `create_dir_all` resolved
outside the repository. The nearest existing ancestor is now canonicalized and
the remaining components applied before comparison.

**P2, preserving existing hooks.** A compiled or non-UTF-8 `post-commit` made
`read_to_string` fail; `unwrap_or_default` turned that into an empty string, so
no backup was taken and the hook was overwritten, contrary to the stated
preservation contract. Only a missing file now counts as nothing to preserve.

**cursor, repo-local hooks rejected.** The guard treated any `core.hooksPath`
outside the common dir as external, so legitimate repository-local directories
such as husky's `.husky` were refused — while the error told the user to
configure exactly such a directory. Hook directories inside the work tree are
now accepted; only paths belonging to neither the common dir nor the work tree
are refused. This also makes the worktree guard meaningful rather than dead:
work-tree-local hooks are per-worktree and stay allowed.

The two CodeRabbit minors are fixed as well: `--interval` is validated in the
seconds the caller typed rather than reporting a millisecond bound, and the
`enable-cloud` copy no longer claims one command threads sessions to PRs. A
nitpick about `URL.pathname` not decoding percent-encoding was taken too, so a
checkout path containing a space resolves `cli.js`.

A regression test covers all four hook behaviours. It is not vacuous: it first
failed for an unrelated reason, which proved the guarded code path was reached,
then passed on the specific assertions once the fixture seeded a session.

### Re-verified after the fixes

`cargo fmt --check` 0, `clippy -D warnings` 0 with zero warnings,
`cargo test --workspace` 0 with 438 passed, `tsc --noEmit` 0, `npm test` 0 with
74/74, `scripts/*.test.mjs` 0 with 10/10, native contract 9 agreeing across all
three sources, `index.d.ts` diff clean.

The artifact was repacked and reinstalled, and both commands were run from the
installed binary:

```
$ ai-hist replay sess-117
Session sess-117 — 2 event(s), oldest first

[2026-09-09T10:00:00Z] claude / prompt (e1)
rebase 117 onto current main

[2026-09-09T10:00:05Z] claude / response (e2)
rebased; two union conflicts in cli.ts

$ ai-hist replay sess-117 --json --out transcript.json
(stdout empty; file holds 2 events e1, e2; pagination exercised)
```

`$(ai-hist token)` again captured a 27-character token rather than an empty
string, and `cloud-commands.test.js` passed against the installed package.

## The split auth store: token and replay must migrate the legacy npm credentials

cursor flagged that `accessToken()` and `replay()` loaded credentials with
`load_auth`, which never reads or migrates `~/.config/ai-hist/auth.json`, while
the four other cloud entrypoints in this same change — `enableCloud`,
`pushCloud`, `loadStoredRelayhistoryAuth` and `createShareableTrace` — go through
`load_sdk_auth`, which does import it.

That inconsistency defeated the purpose of the PR. An existing npm SDK user has
credentials in the legacy TypeScript store by definition and not in the Rust
stage store, so after upgrading they would still have found `token` and `replay`
unusable — the exact population these commands exist to serve. Both now use
`load_sdk_auth`.

The regression test is deliberately a negative control rather than a passing
assertion alone. With the fix reverted it fails with the precise user-visible
symptom:

```
code: 1
stderr: 'ai-hist: CLOUD_TOKEN_FAILED: not authenticated — run `ai-hist login` …'
```

and with the fix it serves the legacy token and migrates it into the canonical
stage store. It runs against the installed package under
`AI_HIST_TEST_PACKAGE_DIR`, so it proves the behaviour in the shipped CLI.

### A test-isolation defect this exposed

`crates/ai-hist/tests/token.rs` and `tests/replay.rs` pinned `RELAYHISTORY_HOME`
but not `HOME`, so once `token` began consulting the legacy store these tests
read the developer's real `~/.config/ai-hist/auth.json`. Two of them failed
locally for that reason. They would have passed in CI, where no such file exists,
which makes this a latent machine-dependent flake rather than a new break. Both
harnesses now pin `HOME`, `USERPROFILE` and `AI_HIST_CONFIG_DIR` to the test's
temporary directory.

### Re-verified

`cargo fmt --check` 0, `clippy -D warnings` 0 with zero warnings,
`cargo test --workspace` 0 with 438 passed and 0 failed, `tsc --noEmit` 0,
`npm test` 0 with 75/75, `scripts/*.test.mjs` 0 with 10/10, native contract 9
agreeing across all three sources, `index.d.ts` diff clean. Against the repacked
and reinstalled artifact both suites pass, including the new legacy-store case,
and `replay` and `$(ai-hist token)` still run from the installed binary.

Note for follow-up, outside this PR's surface: `cloud::recall_auth` also uses
`load_auth` and would show the same gap for legacy npm users. It is a CLI-side
path not exposed by this change, so it was left alone rather than expanding scope.

## Follow-up: the legacy-store fix regressed the stage-ambiguity probe

cursor caught a regression introduced by the fix above. `access_token` calls
`load(None)` purely as a probe: it exists to raise push's multi-stage refusal
when no destination has been selected. Routing that probe through
`load_sdk_auth` broke it, because the two functions disagree on what counts as
a selection:

- `access_token` treats a stage selector as chosen only if it *normalizes*
  (`find_map(normalize_base_url)`).
- `load_sdk_auth` treats *any non-empty* `RELAYHISTORY_BASE_URL` or
  `AI_HIST_BASE_URL` as a selection and falls back to production
  (`.any(|v| !v.trim().is_empty()).then(default_base_url)`).

So with several stages configured and a malformed selector such as
`RELAYHISTORY_BASE_URL=not-a-url`, the refusal was skipped and `token` printed
the production credential — the wrong stage's secret, silently.

The probe now uses `load_auth` again while destination resolution keeps
`load_sdk_auth`, so legacy migration is preserved. Migration is irrelevant to a
probe that only asks whether the destination is ambiguous.

The regression test was written before the fix and observed to fail in the
telling way: not on the assertion message but on `!output.status.success()` —
`token` *succeeded* where it had to refuse.

Re-verified: `cargo fmt --check` 0, `clippy -D warnings` 0 with zero warnings,
`cargo test --workspace` 0 with 439 passed and 0 failed, `tsc` 0, `npm test` 0
with 75/75, `scripts` 10/10, contract 9, `index.d.ts` clean. Against the
repacked and reinstalled artifact both suites pass, including the legacy-store
case, and `$(ai-hist token)` still captures a real token.
