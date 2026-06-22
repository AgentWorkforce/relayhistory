# Getting Started — Agent Relay Loop (cloud sync + Pair)

A step-by-step setup for **humans**. By the end you'll have your local AI-agent history
**captured**, **synced to the cloud**, and **Pair in-session warnings** wired into Claude
Code / Codex so your agents get advisory nudges from your team's own past work.

> Agents: see the companion [`agent-integration.md`](agent-integration.md) for how an
> agent's work becomes convergence events and how to consume Pair programmatically.

The loop has three stages:

1. **Capture** — `ai-hist` records your AI history (Claude Code, Codex, Cursor, Agent Relay,
   Trajectories) into a local SQLite DB.
2. **Cloud sync** — `ai-hist push` maps that history into normalized **convergence events**
   and POSTs them to your team's relayhistory-cloud service (`/v1/ingest`).
3. **Pair** — before a risky action, a hook/MCP tool asks `/v1/pair/check` for advisory
   warnings drawn from your team's prior decisions/findings/reflections.

> **Security note (applies throughout):** never paste a real token, admin secret, or
> RelayAuth key into a shell, file, or chat. Every example below reads secrets from
> environment variables or files with mode `0600`. The client only ever sends **paths +
> short summaries** — never file contents or full prompt bodies.

---

## Prerequisites

- macOS or Linux, `git`, and a Rust toolchain (for building the CLI) **or** the install script.
- Your team's relayhistory-cloud endpoint:
  - **Dev:** `https://relayhistory-api-dev.agent-workforce.workers.dev`
  - **Prod:** `https://history.agentrelay.com`
- For Pair: Node.js (for the hook script + MCP server).

---

## Step 1 — Install the CLI

```bash
curl -fsSL https://raw.githubusercontent.com/AgentWorkforce/relayhistory/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"   # add to ~/.zshrc or ~/.bashrc
ai-hist --help                          # should list: sync, search, login, admin-mint, push, pair
```

Or build from source:

```bash
git clone https://github.com/AgentWorkforce/relayhistory && cd relayhistory
cargo build --release -p ai-hist-cli
cp target/release/ai-hist ~/.local/bin/ai-hist
```

---

## Step 2 — Capture your history locally

```bash
ai-hist sync          # ingest from all detected sources into the local SQLite DB
ai-hist search "auth" # confirm it captured something
```

The local DB lives at `~/.local/share/ai-hist/ai-history.db`. This is what `push` sends.

---

## Step 3 — Cloud sync

Cloud sync authenticates once per target, then `push` uses the stored context. **Use a
separate `RELAYHISTORY_HOME` per target** (dev vs prod) so their auth + resume cursors
don't overwrite each other.

### Dev (no RelayAuth token needed)

Dev allows `admin-mint` (prod does not). The admin secret comes from a local `0600` file —
never typed inline:

```bash
set -a; source ~/.agentworkforce/secrets/relayhistory-cloud-dev-github-secrets.env; set +a

# `ai-hist admin-mint` reads --admin-secret from $ADMIN_MINT_SECRET. Keep it in the env and
# omit the flag — a flag value is visible in `ps`/the process table; an env var is not.
export ADMIN_MINT_SECRET="$RELAYHISTORY_DEV_ADMIN_MINT_SECRET"

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-dev" \
  ai-hist admin-mint \
    --base-url https://relayhistory-api-dev.agent-workforce.workers.dev \
    --org <your-org> --workspace <your-workspace> --user "$USER"

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-dev" ai-hist push --json
```

### Prod (RelayAuth login)

Prod returns **404** on `admin-mint` by design — it uses the RelayAuth login path:

> **First-time RelayAuth setup:** the steps below assume you already have an `identityId`
> and `workspaceId`. Creating a **new** identity requires a **`sponsorId`** — `POST
> /v1/identities` is rejected without one. Request an identity (or a `sponsorId`) from your
> RelayAuth admin before this step.

```bash
# 1. issue a RelayAuth access JWT (audience "relayhistory") — key read from a 0600 file.
ACCESS_TOKEN=$(curl -sS -X POST https://api.relayauth.dev/v1/tokens \
  -H "x-api-key: $RELAYAUTH_API_KEY" -H "content-type: application/json" \
  -d '{"identityId":"<id>","workspaceId":"<ws>","audience":["relayhistory"],"expiresIn":3600}' \
  | jq -r .accessToken)

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-prod" \
  ai-hist login --base-url https://history.agentrelay.com --token "$ACCESS_TOKEN"

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-prod" ai-hist push --json
```

`push` is incremental + idempotent: it only sends new rows past the cursor, dedupes
server-side, and advances the cursor only after the server accepts the batch. Re-run it (or
cron it) safely. Exclude a session with `ai-hist push --incognito <sessionId>`.

> Full cloud-sync reference (automation, troubleshooting, internals):
> [`cloud-sync.md`](cloud-sync.md).

---

## Step 4 — Set up Pair (in-session warnings)

Pair shells out to the CLI primitive, which reuses the same stored auth as Step 3:

```bash
ai-hist pair check --json --task "refactor auth middleware" --file src/auth/middleware.ts
```

Wire it into your agent so it fires automatically:

### MCP tool (`pair_check`)

The `ai-hist-mcp` server exposes a `pair_check` tool that shells to `ai-hist pair check
--json`. Add the server to your MCP client config; the agent can then call `pair_check`
before risky steps. Set `AI_HIST_PAIR_CHECK_BIN=/path/to/ai-hist` if `ai-hist` isn't on
`PATH`.

### Claude Code / Codex hook (automatic)

Add a `PreToolUse` / `UserPromptSubmit` hook that runs the example script. It returns
`hookSpecificOutput.additionalContext` only — **advisory, never blocks**:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Edit|Write|Bash",
        "hooks": [
          { "type": "command",
            "command": "node /absolute/path/to/relayhistory/examples/hooks/pair-check-hook.mjs",
            "timeout": 10 }
        ]
      }
    ]
  }
}
```

(Codex uses the same command in `.codex/hooks.json`; run `/hooks` to trust it.)

> Full Pair setup (Codex config, MCP details, request/response contract):
> [`pair-hooks.md`](pair-hooks.md).

---

## How Pair works (and what it protects)

When you're about to edit a file or run a tool, the hook sends **minimal context** (the
files in scope, a short task summary, the pending tool — **never file contents or your full
prompt**) to `/v1/pair/check`. The server:

- **Hard-scopes to your org** — you only ever see your own organization's history (tenant
  isolation enforced in the query, verified by independent cross-tenant tests).
- Retrieves matching **convergence events** (your team's prior `decision` / `finding` /
  `reflection` rows) via full-text + file-overlap relevance — prompts are excluded.
- **Scrubs** every snippet on the way out (secrets/PII redacted to `[REDACTED]`).
- Returns **ranked, cited** advisory warnings — most-relevant first, with actionable
  *suggestions* ("remember to…") surfaced ahead of comparably-relevant findings (relevance
  always decides first — a clearly more-relevant finding still wins). Each warning names the
  source event and a scrubbed snippet — or `{decision:"allow",warnings:[]}` when nothing
  relevant.

The marquee moment: you start editing `auth/middleware.ts` and Pair surfaces *"a prior
retrospective suggested updating the permissions config when editing this"* — pulled from
your team's own past reasoning, and ranked ahead of a drier related finding because it's the
more actionable nudge. Advisory only: it adds context, it never blocks the action.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `command not found` / no `login`/`pair` | rebuild from `main` (Step 1) |
| `not authenticated …` | run `login` (prod) or `admin-mint` (dev) first |
| `Nothing new to push.` | `ai-hist sync` first, then `push` |
| `HTTP 404 admin mint disabled` | you hit prod with `admin-mint` — prod is login-only |
| `HTTP 401` | token expired/invalid — re-auth |
| Pair returns nothing | confirm `/v1/pair/check` is deployed on your target + you've `push`ed history |

Deeper references: [`cloud-sync.md`](cloud-sync.md) · [`pair-hooks.md`](pair-hooks.md) ·
[`agent-integration.md`](agent-integration.md).
