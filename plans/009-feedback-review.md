# PR feedback review

Reviewed all inline comments, review bodies and PR discussions for client PRs 139–146 and server PR 45. Snapshot fetched from GitHub for this review. All endpoints returned fewer than 100 entries, so no additional pages were needed. General comments contain review status/walkthroughs rather than additional actionable findings. No messages or review replies were posted.

There are 22 inline findings, including one duplicate. All seven newly identified findings are fixed. Thirteen findings were already addressed (including one duplicate); one Promise.race report is a false positive, and one legacy-ownership behavior is intentionally conservative.

| PR | Finding | Disposition |
| --- | --- | --- |
| #141 | [Lock skip drops remote discovery](https://github.com/AgentWorkforce/relayhistory/pull/141#discussion_r4001007394) | Fixed previously: Relaycast lock skip no longer skips independent discovery. |
| #141 | [Exclude cloud from remote hydration availability checks](https://github.com/AgentWorkforce/relayhistory/pull/141#discussion_r4001009780) | Fixed previously: unsupported hydration connectors filtered before auth/DB. |
| #141 | [Acquire the Relaycast sync lock before opening SQLite](https://github.com/AgentWorkforce/relayhistory/pull/141#discussion_r4001009785) | Fixed previously: advisory lock precedes SQLite open. |
| #143 | [Prevent pause/resume from retrying blocked batches](https://github.com/AgentWorkforce/relayhistory/pull/143#discussion_r4001037253) | Fixed previously: atomic transition rejects bypass of permanent blocked failures. |
| #143 | [Pause resumes blocked delivery jobs](https://github.com/AgentWorkforce/relayhistory/pull/143#discussion_r4001043527) | Duplicate of 4001037253; fixed by the same atomic transition. |
| #143 | [Un-exclude skips already consumed records](https://github.com/AgentWorkforce/relayhistory/pull/143#discussion_r4001099048) | Fixed in `4f80e89`: reject removal of a persistent exclusion while affected noncancelled jobs exist; cancel/recreate supplies a fresh baseline. `afd4305` preserves the recovery code through the native SDK API. Regressions cover consumed and queued exclusions plus unaffected selections. |
| #144 | [Abort races leak unhandled rejections](https://github.com/AgentWorkforce/relayhistory/pull/144#discussion_r4001049430) | Rejected: Promise.race attaches rejection handlers to both promises. Both losing-rejection orders pass Node strict unhandled-rejection mode. |
| #144 | [Plugin separator ignores leading flags](https://github.com/AgentWorkforce/relayhistory/pull/144#discussion_r4001049434) | Fixed previously: leading global flags preserve plugin separator handling. |
| #144 | [Reject output aliases through symlinked parent directories](https://github.com/AgentWorkforce/relayhistory/pull/144#discussion_r4001051995) | Fixed previously: canonicalize existing parents and recheck before rename. |
| #144 | [Allow JSON expansion for valid prepared payloads](https://github.com/AgentWorkforce/relayhistory/pull/144#discussion_r4001051997) | Fixed previously: native envelope cap includes worst-case JSON expansion. |
| #144 | [Do not mark every plugin tool idempotent](https://github.com/AgentWorkforce/relayhistory/pull/144#discussion_r4001051999) | Fixed previously: arbitrary plugin tools use conservative annotations. |
| #144 | [Delivery usage hides subcommand errors](https://github.com/AgentWorkforce/relayhistory/pull/144#discussion_r4001096799) | Fixed in `b24ae93`: delivery help, missing and unknown subcommands report the right usage without creating a database. |
| #145 | [Persist the candidate locator for acquisition](https://github.com/AgentWorkforce/relayhistory/pull/145#discussion_r4001083139) | Fixed previously: observation stores candidate locator separately from display path. |
| #145 | [Include the observation location in Codex evidence keys](https://github.com/AgentWorkforce/relayhistory/pull/145#discussion_r4001083142) | Fixed previously: Codex evidence prefix includes observation location. |
| #145 | [Codex diff IDs break upgrades](https://github.com/AgentWorkforce/relayhistory/pull/145#discussion_r4001098666) | Fixed previously: retain unknown legacy projection without duplicate namespaced rows. |
| #145 | [Legacy rows block remote cleanup](https://github.com/AgentWorkforce/relayhistory/pull/145#discussion_r4001098667) | Intentional: unknown legacy ownership cannot safely authorize deletion. Characterized and documented. |
| #146 | [Fall back to USERPROFILE in the Windows helper](https://github.com/AgentWorkforce/relayhistory/pull/146#discussion_r4001174746) | Fixed in `7856c4b`: use nonempty HOME, then USERPROFILE. Unit cases and real Codex helper discovery cover profile-only environments. |
| #146 | [Apply rewrites other connector snapshots](https://github.com/AgentWorkforce/relayhistory/pull/146#discussion_r4001175330) | Fixed in `fa406e7`: persist canonical ownership protection separately from connector evidence. A refresh leaves sibling snapshots/revisions untouched, including after reopen and disappearance of local provenance. |
| #146 | [Hydrate hides plugin failure codes](https://github.com/AgentWorkforce/relayhistory/pull/146#discussion_r4001175334) | Fixed in `f535911`: preserve public authentication/session failure classes and codes, sanitize plugin messages/causes, and retain errors through all-failed public acquisition paths. |
| #146 | [Hydrate timeout cuts off large snapshots](https://github.com/AgentWorkforce/relayhistory/pull/146#discussion_r4001175336) | Fixed in `f535911`: configurable per-operation 300-second default, up to one hour, passed through SDK/CLI/MCP and both optional source helpers. Simulated 45-second snapshot completes; timeout/cancellation remain distinct and never commit partial evidence. |
| server #45 | [Reject unsupported mapping versions](https://github.com/AgentWorkforce/relayhistory-cloud/pull/45#discussion_r4001068678) | Fixed previously: unsupported mapping versions rejected before mutation/receipt. |
| server #45 | [Composed test exceeds default timeout](https://github.com/AgentWorkforce/relayhistory-cloud/pull/45#discussion_r4001130592) | Fixed in `56da306`: explicit 120-second test budget covers sequential 15-second subprocess limits; track/reap children before fixture reuse. Four composed tests pass with every SDK process delayed six seconds (27.44 seconds overall). |

PRs 139, 140, 142 have no inline findings. CodeRabbit skipped or rate-limited substantive review on several PRs; that does not constitute independent approval.

## Validation

- Core delivery regressions: 25 tests at PR 143; 27 including observation capture at PR 145.
- Source intake: 10 tests, including sibling revision/snapshot preservation and canonical protection persistence/cleanup.
- Optional provider Rust: 25 unit tests and 5 helper integration tests.
- SDK acquisition regressions cover typed failures, secret redaction, healthy-source isolation, long snapshots, cancellation, timeout, invalid budgets and no partial native commit.
- Provider SDK regression confirms both discovery and hydration budgets reach the spawned helper.
- Server typecheck passes. Real SDK/native/helper/Hono/PGlite composed suite: 4 tests pass, including the deliberately slow-process run.
- Final merged workspace: 327 Rust tests passed, 2 existing benchmark tests ignored; formatting and Clippy passed.
- Final rebuilt native contract 14 verified; generated declarations unchanged. Core SDK: 130 tests passed.
- Optional SDK suites: 45 RelayHistory and 7 provider tests passed.
- Installed local tarball smoke test passed on darwin-arm64.
- Server CI first caught a formatting violation in the test helper; corrected with Prettier, then formatting and typecheck passed locally. Hosted reruns are pending as this report is committed.

No PR was merged or deployed. Existing release gates remain in place.
