# Cloud Sync — push your local history to Agent Relay Loop

`ai-hist` captures your AI-agent history **locally** (Claude Code, Codex, Cursor, Agent
Relay, Trajectories) into a SQLite database. **Cloud sync** pushes that history to your
team's **Agent Relay Loop** service (`relayhistory-cloud`) so it feeds the shared
Capture → Learn → Plan → Pair loop.

This is the single source of truth for cloud-sync usage (dev + prod, end-to-end).

- **Dev API:** `https://relayhistory-api-dev.agent-workforce.workers.dev`
- **Prod API:** `https://history.agentrelay.com`

> Pair (in-session warnings) is documented separately in [`docs/pair-hooks.md`](pair-hooks.md).

---

## What gets sent (and what doesn't)

`ai-hist push` maps your local rows into the normalized **convergence event** contract and
POSTs them to `/v1/ingest`. Two lenses are sent:

- **History (prompts):** your prompts across tools → `kind: "prompt"`, `lens: "history"`.
- **Trajectories (reasoning):** the *distilled* `decisions` + `retrospective` per run (not
  the full transcript) → `decision` / `finding` / `reflection` events, `lens: "trajectories"`.

Privacy model (default **Team/Growth** tier — vendor-readable):
- Cost/usage data is **not** sent by this client — that's `burn`'s job.
- Client preflight normalizes home-dir paths (`/Users/<you>/…` → `/Users/~/…`) before send.
- The **server** is the compliance boundary: scrubs secrets/PII, drops raw payloads,
  minimizes records before storage. Scrubbed readable content is retained for Learn/Plan.
- **Incognito** excludes any session from sync.
- E2E / self-host custody is the separate **Enterprise** tier.

---

## Step 0 — Build & install the CLI (required first)

The cloud commands (`login` / `admin-mint` / `push`) are in the **Rust** binary. The
`~/.local/bin/ai-hist` you may already have is the **Python** CLI and does **not** include
them — rebuild from `main`:

```bash
cd <relayhistory repo>            # e.g. ~/Projects/AgentWorkforce/ai-hist
git checkout main && git pull --ff-only origin main
cargo build --release -p ai-hist-cli
cp target/release/ai-hist ~/.local/bin/ai-hist
ai-hist --help                    # should list: login, admin-mint, push
```

Both CLIs read the **same** local DB (`~/.local/share/ai-hist/ai-history.db`), so your
existing captured history is what `push` sends. If you have nothing captured yet, run
`ai-hist sync` first. (If `push` ever says `Nothing new to push.`, run `ai-hist sync`.)

---

## DEV — team-internal admin mint

Dev allows `admin-mint` for maintainers and internal test environments only. It requires
an admin secret and is **not** an end-user auth path. Two commands:

```bash
# load the dev admin secret from the secure local file (never echo it)
set -a; source ~/.agentworkforce/secrets/relayhistory-cloud-dev-github-secrets.env; set +a

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-dev" \
  ai-hist admin-mint \
    --base-url https://relayhistory-api-dev.agent-workforce.workers.dev \
    --admin-secret "$RELAYHISTORY_DEV_ADMIN_MINT_SECRET" \
    --org org-a --workspace workspace-a --user "$USER"

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-dev" \
  ai-hist push --json
```

(Against a *local* `wrangler dev` instead of the deployed dev Worker, use
`--base-url http://localhost:8787`.)

---

## PROD — Agent Relay Cloud login

Prod auth goes through **Agent Relay Cloud**. End users should not mint tokens, call
internal auth services, or handle internal API keys.

```bash
# 1. Sign in to Agent Relay Cloud. Cloud provisions the relayhistory session.
npx agent-relay cloud login

# 2. Push with the stored prod relayhistory session.
RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-prod" \
  ai-hist push --json
```

Current implementation note: older `ai-hist` builds may still expose a manual auth handoff.
That handoff value is internal Cloud/relayhistory plumbing, not something a user should
mint manually. If your build still requires it, ask your Agent Relay Cloud admin for the
Cloud-provisioned handoff or wait for the first-class browser/device login flow.

---

## Push semantics & key behaviors

`ai-hist push` output (example): `{"sent":N,"accepted":N,"batchId":"b_…","cursor":{…}}`

- **Auth + base_url are stored** in `$RELAYHISTORY_HOME/auth.json` (mode `0600`) by
  `login`/`admin-mint`. Authenticate once per target, then just `ai-hist push` — **`push`
  takes no `--base-url`** (it uses stored context).
- **Use a separate `RELAYHISTORY_HOME` for dev vs prod** (as above): `auth.json` holds one
  base_url + token + the cursor, so a shared home would make dev/prod overwrite each other's
  auth and resume state.
- **Incremental + idempotent:** the cursor only sends new rows; re-running `push` (or a
  cron) is safe — duplicates dedupe server-side on the event PK + batch id. The cursor
  advances only after the server accepts the batch.
- **Exclude sessions:** `ai-hist push --incognito <sessionId> --incognito <trajectoryId>`.
- To switch targets, use the per-target `RELAYHISTORY_HOME`; for prod, re-run Agent Relay
  Cloud login if the stored session expires.

### Automation (cron / launchd)

```cron
*/5 * * * * RELAYHISTORY_HOME=$HOME/.agentworkforce/relayhistory-prod ai-hist sync && \
            RELAYHISTORY_HOME=$HOME/.agentworkforce/relayhistory-prod ai-hist push --json >> /tmp/ai-hist-push.log 2>&1
```

`push` exits non-zero on transport/auth failure (cursor not advanced → safe retry next run).

---

## Troubleshooting

| Message | Fix |
|---|---|
| `ai-hist: command not found` / no `login`/`push` | rebuild from `main` (Step 0) — old binary is Python |
| `not authenticated …` | run Agent Relay Cloud login (prod) or team-internal `admin-mint` (dev) first |
| `Nothing new to push.` | `ai-hist sync` first, then push |
| `HTTP 404 admin mint disabled` | you hit prod with `admin-mint` — use Agent Relay Cloud login for prod |
| `HTTP 401` | session expired/invalid — re-auth |
| connection refused | wrong `--base-url`, or (local) `wrangler dev` not running |

---

## Internals

- Mapping: `ai-hist-core::convergence` → WS-1 `ConvergenceEnvelope`.
- Batching/cursor: `ai-hist-core::outbox::build_outbox_batch`.
- Transport/auth: the `ai-hist` binary `cloud` module — `ureq` HTTP, `rth_at_` bearer,
  `SyncCursor` persistence. Network I/O lives here, not in the WASM-bound core.
- Convergence contract: `relayhistory-cloud/docs/decisions/2026-06-21-normalized-agent-event-schema.md`;
  trajectory-lens mapping: `trajectories/docs/convergence-integration.md`.
