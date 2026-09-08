# WS-12 validation

Drafts: https://github.com/AgentWorkforce/relayhistory/pull/117 and
https://github.com/AgentWorkforce/relayhistory-cloud/pull/43. No merge or
production deployment has been performed.

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
