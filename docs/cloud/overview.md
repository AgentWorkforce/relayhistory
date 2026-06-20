# Cloud Hosting Strategy: Overview

## What We're Building

A hosted cloud layer on top of the existing local-first `ai-hist` tool. The Python CLI and TypeScript SDK remain unchanged and fully functional without a cloud account. The cloud is an opt-in sync target that adds persistence, cross-device access, team collaboration, and a web UI.

The core promise is simple: **we store your data, we don't read it.** This is enforced architecturally, not just by policy.

---

## The Problem We Solve

AI coding agents (Claude Code, Codex, Cursor, and others) are proliferating rapidly. Developers use multiple tools across multiple projects, often on multiple machines. Their AI conversation history is:

- Scattered across local JSONL files on one device
- Lost when a laptop is replaced or a disk fails
- Invisible to the rest of the team
- Unauditable for organizations that need to track AI usage
- Gone when an employee leaves

No existing tool aggregates multi-agent history with team visibility. The LLM observability platforms (Langfuse, Helicone) target *app developers* logging API calls, not *developers using* AI coding agents. This is an unserved niche at exactly the moment when it is becoming a real organizational need.

---

## Privacy-First Positioning

This is the non-negotiable core principle:

> **We never access your data. Your data is yours to remove from our platform at any time. We do not read it, share it, or use it for any purpose.**

This is not a policy promise — it is an architectural guarantee enforced by end-to-end encryption (see [encryption.md](./encryption.md)). The server stores ciphertext. We hold no decryption keys. Even if compelled by law or breached by an attacker, there is nothing readable to produce.

This differentiates us from every competitor in the adjacent space, all of whom require reading your data to deliver their features. We do not.

---

## Selling Points

### For Individual Developers
- **Cross-device continuity** — sync history to a new machine instantly; never lose a conversation
- **Persistent backup** — local files can be corrupted or deleted; cloud history is durable
- **Unified search** — one place to search across all AI tools you use
- **Hosted MCP endpoint** — point your Claude config at a URL instead of running a local server

### For Teams
- **Shared organizational memory** — search your teammates' prior AI interactions to avoid solving the same problem twice
- **Onboarding acceleration** — new hires can search how the team approached similar problems
- **Knowledge retention** — when someone leaves, their AI-assisted work history stays with the org
- **Decision audit trail** — trajectory records capture *why* agents made choices, not just what they did

### For Enterprises
- **AI usage auditing** — compliance, cost attribution, security review
- **IP protection** — E2E encryption means your proprietary code never exists in plaintext on our servers
- **Data residency controls** — regional storage options
- **On-premises deployment** — for organizations that require it (the open source base makes this credible)
- **GDPR/CCPA compliance by design** — not retrofitted

---

## Competitive Landscape

| Tool | What they do | Gap vs. ai-hist cloud |
|---|---|---|
| **CASS Memory System** | Procedural memory for AI agents — extracts rules and patterns from session history | Single-agent focus; no multi-tool aggregation; no team layer; no cloud sync; no privacy-first model |
| Langfuse | LLM tracing for app developers | Reads your data; targets API integrators not CLI users |
| Helicone | Proxy-based API logging | Reads your data; no CLI agent support |
| Braintrust | LLM eval + logging | Enterprise, reads your data, no multi-tool CLI aggregation |
| Pieces for Developers | AI snippet capture | Single-tool, no team layer, no trajectory tracking |
| Rewind.ai | Screen recording with AI search | Reads everything on your screen; significant privacy concerns |
| mem.ai | Personal AI memory | Not coding-specific, no multi-tool, no team sharing |

### CASS in Depth

CASS (385 stars, alpha) is the closest thing in the market to a direct competitor and is worth understanding in detail.

**What CASS does well:**
- Three-layer memory: raw session logs → structured diary summaries → distilled procedural rules ("playbook")
- Confidence decay (90-day half-life) and evidence-gated rule acceptance — rules have to prove themselves
- Anti-pattern tracking: consistently harmful patterns get inverted into warnings
- Cross-agent learning: a pattern found in Cursor can surface in Claude Code

**Where ai-hist cloud is different:**
- **Multi-tool aggregation** — CASS focuses on a single agent at a time; we aggregate Claude Code, Codex, Cursor, Agent Relay, and trajectories into one corpus
- **Raw history as the product** — CASS treats session logs as input to distill *away from*; we treat them as the primary value (searchable, sharable, resumable)
- **Team layer** — CASS has no concept of a team; we're building shared organizational memory
- **Privacy-first** — CASS reads your sessions to extract rules; we never read content
- **Trajectory records** — we capture structured decision logs from agentic workflows; CASS works with conversational logs only

**The relationship:** These are more complementary than directly competing. CASS is an intelligence layer on top of history; we are the history layer itself. A user could run both — CASS extracting rules from sessions that ai-hist stores and makes searchable. A future "Insights" feature in ai-hist cloud (see roadmap) would move into CASS's territory, but that is not V1 scope.

**The gap:** Nobody aggregates AI coding agent history across Claude Code, Codex, Cursor, etc. into an encrypted, team-searchable cloud layer. The multi-tool aggregation + decision trajectory tracking + privacy-first model is unique. CASS confirms there is real user appetite for AI agent memory tools; it does not solve the problems we solve.

---

## Success Metrics (Early Stage)

- Active syncing users (weekly sync at minimum)
- Retention: % of users who sync again after 30 days
- Team conversion: % of individual users who add a second seat
- Deletion requests completed within SLA (target: same-day)
- Export requests completed within SLA (target: immediate)
