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
