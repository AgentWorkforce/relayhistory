# Implementation plans

Boundary plan prepared on 2026-09-13 against main `3155117`.
Existing plans are retained; this index does not certify their completion.

| Plan | Subject | Priority | Dependency | Status |
| --- | --- | --- | --- | --- |
| [006](006-native-only-production-architecture.md) | Native production architecture | Existing | — | Existing; not re-audited |
| [007](007-targeted-session-hydration.md) | Targeted hydration | Existing | 006 | Existing; not re-audited |
| [008](008-session-delegation-topology.md) | Session delegation topology | Existing | 006, 007 | Existing; not re-audited |
| [009](009-local-history-cloud-boundary.md) | Local history core with optional source/destination plugins | P1 | Characterization first; preserve 006–008 contracts | DONE — implemented and locally verified; PRs open |

Execute plan 009 in order: characterize → explicit connector selection/offline
semantics → observation migration → Rust boundaries → durable delivery coordinator + thin plugins →
physical isolation gate. Keep these as separate reviewable stages.

The untracked review under `reviews/` was preserved. Plan 009 covers its selected
cloud-coupling finding and the auth/local-I/O prerequisites needed to fix it.
It does not update or certify the other findings.

Rejected approaches: a default-on cloud feature, a subpath export, or a dynamic
import alone cannot prove independent compilation/installation. Reimplementing
TS authentication would reverse the newly unified Rust auth ownership.

The plugin model supersedes the earlier proposal for paired local/cloud native
addons and separate CLI/MCP distributions. Core runs locally; optional destination
plugins or NDJSON export deliver history elsewhere. RelayHistory is one plugin.

Dependable background delivery is part of the design: the local coordinator owns
durable queue/checkpoint state, retries, leases, and status. Plugins translate
auth/payloads/acknowledgments. NDJSON remains a one-off export path, not a durable
delivery guarantee. Background work requires explicit enablement.

Implementation PRs opened:

- [#139: Characterization tests](https://github.com/AgentWorkforce/relayhistory/pull/139)
- [#140: Interleaved transcript cursor fix](https://github.com/AgentWorkforce/relayhistory/pull/140), stacked on #139.

Execution is ongoing in isolated `codex/` branch worktrees under
`/private/tmp/rh-local-history-20260913`. The original checkout and review files
are preserved. These PRs do not complete the entire plan.
