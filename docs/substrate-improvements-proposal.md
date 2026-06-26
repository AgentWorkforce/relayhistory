# relayhistory — substrate improvements proposal

**Date:** 2026-06-26 · **Author:** lead (synthesized with burn-expert, cloud-expert, reflex-agent) · **Status:** proposal for Khaliq

## Framing

relayhistory is the **open, lossless event-ledger substrate** — capture → normalize →
search → export — that the private **Reflex** intelligence layer consumes. The
competitive scan (Traces, Honcho, Paxel, SkillOps, MS-Coach, **agentmemory**)
made the lane crisp:

> **agentmemory/Honcho/mem0 own *recall*. relayhistory owns the *lossless ledger*.
> Reflex owns *outcome*.** Three non-overlapping axes — only the outcome axis
> compounds with proprietary data.

So relayhistory's job is **not** to become a memory-injection brain (agentmemory
already leads that, ~24k stars). Its job is to be the **boring, trustworthy,
diff-level, replayable record** that nobody else keeps — because outcome
attribution (intent → commit → PR → prod) *requires* lossless evidence that a
compress-and-forget memory engine structurally throws away. **Their forgetting is
our moat's prerequisite.** Do the substrate extremely well; let Reflex score.

## Proposal — prioritized

### P0 — foundational / urgent (do first)
1. **Live capture + import-before-cleanup** — `ai-hist import --watch` + lifecycle
   hooks (`SessionStart`/`PostToolUse`/`Stop`/`PreCompact`). *Why:* agentmemory
   flags that Claude JSONL gets **cleaned up** — raw transcripts can vanish. If the
   evidence disappears, the entire ledger/outcome thesis collapses. **This is the
   single most urgent item.** Status: the Rust `import --watch` live-capture alias
   is landing in PR #36; lifecycle hook adapters remain follow-up work. Hooks also
   capture tool *errors* inline (lower latency than after-the-fact sync). *Effort: M.*
2. **Governance primitives** — `redact` / `delete` / `export` with audit, + **secret
   redaction at ingest** (we store full transcripts + diffs). *Why:* required before
   relayhistory is a shared/team substrate, and for the cloud data-trust posture.
   *Effort: M.*

### P1 — high leverage (unlocks Reflex + matches the recall UX)
3. **MCP server (upgrade `ai-hist-mcp`)** — expose `search` / `recall` / `session` /
   `replay` over schema v2 so **agents query relayhistory mid-session**, not just
   humans via CLI. *Why:* the biggest capability gap vs agentmemory; turns
   relayhistory from an archive into a live, queryable substrate + a distribution
   surface (`connect <agent>` one-liners). *Effort: M–L.*
4. **session → commit → PR linkage** — `ai-hist setup git` (thin, no-network
   `post-commit` → optional `refs/notes/ai-hist` + `session_commit_links` table),
   plus `ai-hist link commit` and `ai-hist export commit-links --jsonl`. Status:
   the deterministic hook/link/export path is landing in PR #36; broader PR and
   outcome joins remain follow-up work. `match_method` and `confidence` are linkage
   metadata only, not scoring or attribution. *Why:* the **bridge to outcome** —
   Reflex can't attribute anything without it. (Borrow Traces' git-notes; this was
   "edit #2".) *Effort: M.*
5. **Provenance-preserving recall-pack** — `ai-hist recall "<query>" --budget N
   --with-source-ids --json` → top sessions/files/PRs + intent snippets + **raw-evidence
   pointers + match confidence**, token-budgeted. *Why:* the genuinely good part of
   agentmemory, delivered **as data, not auto-injection** — and exactly what Reflex/Pair
   consume. *Effort: M.*
6. **Session replay** — `ai-hist replay <session>` over `session_events` (prompts →
   tool calls → results → edits, in order). *Why:* cheap, high-utility, not
   intelligence-heavy; agentmemory has it, we have richer data for it. *Effort: S.*

### P2 — depth (after the above prove out)
7. **Optional hybrid index** — vector + graph edges over `session→file→tool→commit`,
   fused (RRF), **keeping FTS5 canonical**. *Why:* better recall; cloud adds pgvector.
   *Effort: L.* (Local stays SQLite/FTS — don't add a server dependency.)
8. **Outcome-source ingestion substrate** — the data side of `prod_health`: PR
   feedback (gh), CI, then Sentry/incident/support joins, as **score-independent
   labels** Reflex reads. *Why:* the deepest moat enabler. *Effort: L, integration-gated.*
9. **Retrieval benchmark** — given "restore previous session" / "why did PR #232
   fail", does recall surface the right session/files/PR in top-K? *Why:* makes
   recall quality measurable (agentmemory publishes scorecards; we should too).
   *Effort: S–M.*
10. **Connector UX** — `ai-hist connect <agent>` + health-check, boring and verifiable;
    matches the multi-agent capture story. *Effort: S.*

## Guardrails (what NOT to do)
- Don't make relayhistory the auto-injection brain (that's a layer above / Reflex).
- Don't let LLM-summarized memory become the source of truth — raw events stay canonical.
- Don't bury raw evidence under summaries; don't require an always-on server for the local path.
- Keep storage standard/inspectable (SQLite/FTS) — agentmemory's proprietary
  native KV engine lock-in is a differentiator *against* them.

## Recommended first move
**P0.1 (live capture / import-before-cleanup) + P1.4 (session→commit→PR linkage).**
Together they protect the evidence and create the join that turns the ledger into
outcome-attributable data — the two things everything else (recall, Reflex scoring,
the flywheel) depends on. Everything in P1/P2 builds on a durable, linkable ledger.
