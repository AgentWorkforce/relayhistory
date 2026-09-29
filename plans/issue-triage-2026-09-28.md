# Open-issue triage — 2026-09-28

Triage of every open issue against main `66ed973` (release 0.30.1), after the
capture plugin was retired (`14e382a`) and the probe moved to
[`relay-desktop/probe/`](https://github.com/AgentWorkforce/relay-desktop/tree/main/probe).

## Moved to relay-desktop

These concern the probe's cloud client, delivery journal or install
management, which no longer live here. Each relayhistory issue carries a
comment linking its replacement and is left open for a maintainer to close.

| relayhistory | relay-desktop | Subject |
| --- | --- | --- |
| #243 | AgentWorkforce/relay-desktop#77 | Decommissioned probe installs retain journal, database and logs |
| #250 | AgentWorkforce/relay-desktop#78 | Journal capture: narrow root subscriptions |
| #251 | AgentWorkforce/relay-desktop#74 | Reclaim decommissioned installs off the listing path |
| #56 | AgentWorkforce/relay-desktop#75 | Cloud calls send the bearer to `base_url` without a scheme check |
| #68 | AgentWorkforce/relay-desktop#76 | Reuse agent-relay auth instead of a separate credential pair |

## Fixed by a PR

_Filled in as the per-issue PRs open._

## Labelled `needs-investigation`

Issues that no longer appear to apply to this repository as it stands. Each
carries a comment with the evidence.

_Filled in as triage completes._

## Not triaged

- #160 is the sourcing-migration epic; its children (#163, #171, #177, #181,
  #183, #184) are triaged individually above.
