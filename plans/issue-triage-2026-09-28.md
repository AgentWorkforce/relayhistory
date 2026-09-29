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

| Issue | PR | Notes |
| --- | --- | --- |
| #181 | #274 | Part of: change feed through napi and the TS SDK; native contract 23 |
| #73 | #272 | `export` refuses to overwrite the database it opened; atomic write |
| #211 | #278 | Merge streamed Claude Code usage per request; `<synthetic>` becomes a marker |
| #208 | #280 | Part of: skip subagent `journal.jsonl`; `~/.claude/transcripts/` still to characterize |
| #66 | #277 | napi/SDK/MCP search share the CLI's Rust query; native contract 23 |
| #67 | #282 | Keyset cursors and inclusive time windows; stacked on #277; native contract 24 |
| #171 | #276 | Tool-result fidelity for Cursor, Grok and OpenCode; hydration parser version 13 (12 is #284's) |
| #177 | #281 | Part of: local JS source plugins; registry consolidation and collector issues remain |
| #210 | #273 | Part of: Codex fork edges from `session_meta`; replay gate remains |
| #209 | #279 | OpenCode channel-suffixed databases; `ProviderRoots::opencode_db_pinned` |
| #212 | #284 | Part of: Grok per-turn usage from `updates.jsonl`; `unified.jsonl` remains |
| #42 | #271 | Part of: Codex metadata backfill scoped to re-read sessions (2.9s → 0.96s) |
| #215 | #283 | Part of: pinned join order + locator index (incremental 6.97s → 2.12s); rayon/SIMD rejected with measurements |
| #53 | #285 | Part of: `ai-hist compact` and reclaimable space in `doctor`; pruning policy remains |
| #47 | #275 | ADR: writers stay direct, no append-only spools |

## Still relevant, blocked

| Issue | Blocked on |
| --- | --- |
| #183 | burn's `relayhistory-source` feature and parity suite (burn #555, #557, #558) |
| #184 | #183's parity report, burn #562 cutover, #177 |

## Merge order

The PRs are stacked, each based on the one before it, so they merge bottom-up
without conflicts:

#270 → #275 → #272 → #280 → #278 → #279 → #273 → #284 → #276 → #271 → #283 →
#285 → #277 → #282 → #274 → #281

The native contract is 23 at #277, 24 at #282 and 25 at #274.

## Labelled `needs-investigation`

Issues that no longer appear to apply to this repository as it stands. Each
carries a comment with the evidence.

| Issue | Why |
| --- | --- |
| #119 | Fixed by #120: Linux GNU builds target glibc 2.28 and are smoke-tested on Bookworm |
| #41 | `syncAndPush` was removed with cloud push; native `sync()` is already silent |
| #64 | sql.js and the snapshot write path were removed in `4fe7aef`; reads open fresh rusqlite connections |
| #99 | Shipped as `getSessionUsage` / `getSessionRequestsPage` / `ai-hist sessions usage` (`590fdae`, `b509a9c`) |
| #163 | Delivered by #192 (its `Closes #163` was in backticks, so it never auto-closed) |
| #214 | Reference record with no code to write; its use depends on #177's collector issues |
| #40 | Hosted ingestion is cloud/relay-desktop scope; needs live Codex Cloud experiments |

## Not triaged

- #160 is the sourcing-migration epic; its children (#163, #171, #177, #181,
  #183, #184) are triaged individually above.
