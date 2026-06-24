# Agent Integration Guide — Agent Relay Loop

**For agents and tool authors: how to feed your work into the convergence store and consume Pair (in-session warnings).**

This is the agent-facing counterpart to the human setup guide (`docs/cloud-sync.md`, `docs/pair-hooks.md`). It assumes the CLI is built and authenticated (see `docs/cloud-sync.md`).

> **Secret hygiene (non-negotiable):** every command below reads secrets from env vars (`$ADMIN_MINT_SECRET`, etc.) — **never paste a real session token, admin-mint secret, or internal auth identifier into a file, prompt, log, or PR.** The server scrubs content at ingest, but don't rely on it for credentials in commands.

---

## 1. The loop, from an agent's seat

```
your session → Capture (local) → push → convergence store → Pair (warnings back to you)
```

- **Capture:** your tool's history + trajectory (decisions/retrospectives) land in the local `ai-hist` store.
- **Push:** `ai-hist push` syncs new rows to the cloud convergence store, scrubbed.
- **Pair:** before a risky action, you query the store and get back cited warnings drawn from your team's own prior work.

---

## 2. How your work becomes convergence events

When you `ai-hist push`, local trajectory/history rows map to typed **convergence events**. The contract (full detail in the `trajectories` repo's `docs/convergence-integration.md`):

| Your work | → event `kind` | eventId shape |
|---|---|---|
| a decision (chose X over Y, why) | `decision` | `decision:<traj>:<i>` |
| a retrospective **learning** | `finding` | `finding:<traj>:learning:<i>` |
| a retrospective **suggestion** ("remember to…") | `reflection` | `reflection:<traj>:suggestion:<i>` |
| a retrospective **challenge** | `finding` | `finding:<traj>:challenge:<i>` |
| summary / approach | `reflection` | `reflection:<traj>:summary` / `:approach` |
| a prompt / thinking / tool-call | *(deferred — chapter event stream, post-v1)* | `trajevent:<chapterId>:<i>` |

Each event carries: `content` (readable, **scrubbed**), `significance`, `confidence` (source-native float; server stores basis-points), `tags`, `filesTouched`, plus tenancy (`orgId`/`workspaceId`/`machineId`, server-derived from your token — you never assert them).

**What is NOT sent:** raw tool payloads / `raw` blobs are dropped on the default tier. Secrets/PII (`ghp_…`, `sk-…`, JWTs, AWS keys, credential URLs, home-dir usernames) are redacted server-side at ingest. Suggestions are the highest-value signal — they become Pair warnings.

---

## 3. Sync your work (push)

After authenticating (see `docs/cloud-sync.md`):

```bash
ai-hist push --json    # incremental; only new rows since the cursor; idempotent (safe to re-run / cron)
```

`--incognito` excludes sessions you don't want synced. Cost/usage stays burn-owned; this carries the recall/reasoning lens.

---

## 4. Consume Pair — in-session warnings

Pair answers: *"before I do this, what has my team learned here?"* Two ways to wire it:

### a) Pull — the `pair_check` MCP tool
Register the `ai-hist` MCP server (see `docs/pair-hooks.md`). Then, before a risky step, the agent calls `pair_check` with the current context. Good when the agent decides when to consult.

### b) Push — a Claude Code / Codex hook (automatic)
A `PreToolUse` / `UserPromptSubmit` hook calls `ai-hist pair check` and injects the top warning inline. Good for automatic, every-action coverage. The hook is **advisory-only and fail-open** — if the endpoint errors, your session continues unaffected.

Both shell to the same primitive:

```bash
ai-hist pair check \
  --file src/auth/middleware.ts \      # files in scope / about to edit
  --task "refactor auth middleware token check" \
  --tool Edit --target src/auth/middleware.ts \   # optional pending action
  --json
```

### Request contract (`POST /v1/pair/check`, `rth_at_` bearer, scope `rth:read`)
Send **only** bounded context — files, task summary, pending tool/target, a short prompt summary. **Never file contents or full transcripts.** `projectId` optional (server infers from `repoPath`/`cwd`/`gitRemote` when absent).

### Response
```jsonc
{
  "decision": "warn" | "allow",          // "warn" iff warnings exist; NEVER blocks
  "warnings": [{
    "text": "Prior reflection: update the permissions config when editing the auth middleware",
    "kind": "reflection|finding|decision",
    "lens": "trajectories|history",
    "score": 0.0-1.0,
    "evidence": [{ "machineId","source","sessionId","kind","eventId","ts","snippet" }]  // scrubbed snippet
  }],
  "correlationId": "…"
}
```
Empty result → `{"decision":"allow","warnings":[]}` → the hook no-ops cleanly.

**How to treat warnings:** advisory context, not commands. Surface them; let the engineer/agent decide. Pair never blocks an action (v1).

---

## 5. Security & privacy model (what to tell users)

- **Tenant isolation:** retrieval is hard-scoped to your `orgId` (the token's org, never client-supplied). You only ever see your own org's history — verified at the API, not just at rest.
- **Scrub at the boundary:** secrets/PII are redacted server-side both at ingest (stored content) and on Pair response snippets (egress). Client-side redaction is defense-in-depth, not the boundary.
- **No raw transcript:** Pair requests carry paths + summaries only.
- **Advisory-only:** Pair warns, never blocks.
- **Retrieval (v1):** lexical/FTS over scrubbed `content` + file-overlap + kind/significance ranking. (Semantic/vector retrieval is a future upgrade once embeddings are populated.)

---

## 6. Quick reference

```bash
# one-time: build + authenticate (see docs/cloud-sync.md)
ai-hist login                                             # prod; uses canonical Agent Relay Cloud auth
ai-hist admin-mint --base-url <dev-url> --org <org> --user "$USER"   # dev only; reads $ADMIN_MINT_SECRET from env (never pass via --flag → argv/ps exposure)

# every session
ai-hist push --json                                          # sync your work
ai-hist pair check --file <path> --task "<summary>" --json   # ask for warnings before a risky step
```

See also: `docs/cloud-sync.md` (human setup), `docs/pair-hooks.md` (hook/MCP wiring), and the `trajectories` repo `docs/convergence-integration.md` (full event-mapping contract).
