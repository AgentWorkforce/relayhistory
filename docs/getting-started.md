# Getting Started — Agent Relay Loop (cloud sync + Pair)

A step-by-step setup for **humans**. By the end you'll have your local AI-agent history
**captured**, **synced to the cloud**, and **Pair in-session warnings** wired into Claude
Code / Codex so your agents get advisory nudges from your team's own past work.

> Agents: see the companion [`agent-integration.md`](agent-integration.md) for how an
> agent's work becomes convergence events and how to consume Pair programmatically.

The loop has three stages:

1. **Capture** — `ai-hist` records your AI history (Claude Code, Codex, Cursor, Grok,
   Agent Relay, Trajectories) into a local SQLite DB.
2. **Cloud sync** — `ai-hist push` maps that history into normalized **convergence events**
   and POSTs them to your team's relayhistory-cloud service (`/v1/ingest`).
3. **Pair** — before a risky action, a hook/MCP tool asks `/v1/pair/check` for advisory
   warnings drawn from your team's prior decisions/findings/reflections.

> **Security note (applies throughout):** never paste a real token, admin secret, or
> provider key into a shell, file, or chat. Every example below reads secrets from
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

### Dev / team-internal admin mint

Dev allows `admin-mint` for maintainers and internal test environments only. It requires
an admin secret and is **not** an end-user auth path. The admin secret comes from a local
`0600` file — never typed inline:

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

### Prod (Agent Relay Cloud login)

Prod auth is owned by **Agent Relay Cloud**. End users should not mint auth tokens, call
internal auth services directly, or handle internal API keys. Sign in through Agent Relay
Cloud, then let the Cloud/relayhistory login handoff populate the `ai-hist` prod session:

```bash
# 1. Sign in to Agent Relay Cloud in the browser / Cloud CLI.
#    Your org/workspace and relayhistory session are provisioned by Cloud.
npx agent-relay cloud login

# 2. Push uses the stored prod relayhistory session.
RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-prod" \
  ai-hist push --json
```

If your local build still asks for a manual auth handoff, treat that as a temporary
Cloud-login gap, not as an instruction to mint anything yourself. Ask your Agent Relay
Cloud admin to provision the relayhistory session, or wait for the first-class Cloud login
handoff in the CLI. Internal auth details belong in operator runbooks, not human setup.

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

Install the MCP tool and advisory hooks from the project you want Pair scoped to:

```bash
npx -y ai-hist-mcp setup
```

The installer writes `.mcp.json`, `.claude/settings.json`, and `.codex/hooks.json`
idempotently. It scopes Pair to the current project and writes no tokens or secrets to
config. After it runs, restart your agent session, then ask your agent to use the
`pair_check` MCP tool before risky edits.

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
| `not authenticated …` | run Agent Relay Cloud login (prod) or team-internal `admin-mint` (dev) first |
| `Nothing new to push.` | `ai-hist sync` first, then `push` |
| `HTTP 404 admin mint disabled` | you hit prod with `admin-mint` — use Agent Relay Cloud login for prod |
| `HTTP 401` | token expired/invalid — re-auth |
| Pair returns nothing | confirm `/v1/pair/check` is deployed on your target + you've `push`ed history |

Deeper references: [`cloud-sync.md`](cloud-sync.md) · [`pair-hooks.md`](pair-hooks.md) ·
[`agent-integration.md`](agent-integration.md).
