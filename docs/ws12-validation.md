# WS-12 validation and remaining rollout

Drafts: https://github.com/AgentWorkforce/relayhistory/pull/117 and
https://github.com/AgentWorkforce/relayhistory-cloud/pull/43. No merge or
production deployment has been performed.

## Evidence

- Native source, SDK and built addon agreed on contract 8.
- Rust cloud transport: 50 tests passed (stage isolation, refresh rotation,
  atomic auth/cursor writes, failed pushes and transcript watermarks).
- Rust outbox: 29 tests passed.
- Initial built npm fixture: all 525 unique synthetic prompts arrived in two
  batches; the Rust Agent Relay login handoff ran; SDK auth adopted the refreshed
  token; stale SDK auth did not override it; a failed second-stage push left the
  first-stage cursor unchanged; a real commit generated Git notes and a PR event.
- Follow-up hook checks found inherited broker `core.hooksPath` pointing at a
  shared temporary directory. The installer now rejects external shared hook
  paths, and tests use isolated Git configuration. It also resolves an existing
  PR at install time through repository config or gh. The final fixture and full
  Node 22 SDK suite passed in GitHub CI on head 1b91b25.
- Server: 4 sharing route tests and 15 existing session-link tests passed, package
  typecheck passed, Wrangler deployment dry-run passed. Sharing tests use real
  PGlite DDL and verify anonymous/private access, owner and tenant isolation,
  frozen snapshots, escaped HTML, and revocation. Full server CI (formatting,
  typecheck and all tests) passed on f475d7c in run 34232491784.
- Live dev: the npm command with fresh isolated ai-hist state exchanged an
  existing real Agent Relay Cloud identity and reported sent=1, accepted=1 for
  synthetic session `ws12-cloud-demo-1788873511810`. This was not a fresh browser
  device approval. The deployed service returned 404 for all recall/session/turn
  routes while /health returned 200, so persistence is NOT verified from the ack.

## Environment failures

Homebrew Node 26 stopped loading mid-run because libada.3.dylib disappeared.
Subsequent commands use the existing mise Node 22.22.2 installation. The disk
then filled; the broad SDK run had 44 passes and 17 failures including ENOSPC and
SQLite shared-memory I/O failures. Broad server runs stalled/timed out. These
runs are not claimed green. Only WS-12's reproducible Rust target cache was
removed to recover space; the built addon and committed sources were retained.

SST dev plan attempts initially stalled in provider dependency installation. A
direct npm install of the generated platform dependencies completed. SST then
failed explicitly: Cloudflare API not initialized; CLOUDFLARE_API_TOKEN (or API
key plus email) must be configured. No plan, deploy, or migration was applied.

## Remaining acceptance

1. Client Node 22 SDK and Windows CI passed. The main verify job found a Clippy
   item-order warning; the SDK cloud functions have now been moved before the
   test module. Verify the subsequent CI run. `node --test sdk-ts/dist/cloud.test.js` exercises the public
   SDK and actual npm CLI, including an offline post-commit and native sharing.
2. Review `sst diff --stage dev`, deploy the cloud companion to dev, and apply
   its additive `0010_trace_shares.sql` to the verified dev Neon branch.
3. Run `ai-hist enable-cloud --base-url https://dev.history.agentrelay.com --once`
   with the existing trusted-dev opt-in. Query the exact synthetic event IDs,
   then commit through the installed hook and query
   session_links.link_kind='github_pr' (the actual column is link_kind).
4. Call createShareableTrace and open the returned dev /s/<token> URL. The hosted
   PR-link row and share URL demonstrations are still outstanding.

The SDK push loop is foreground/in-process. Agent Relay must be installed for
interactive device login. Hooks use an existing indexed session; PRs created
after installation require rerunning installation. Shares contain frozen
convergence events; private URL reads currently require an authenticated HTTP
client. See enable-cloud.md and the cloud PR's docs/sharing.md.
