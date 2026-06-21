# Cloud Sync — push your local history to Agent Relay Loop

`ai-hist` captures your AI-agent history **locally** (Claude Code, Codex, Cursor, Agent
Relay, Trajectories) into a SQLite database. **Cloud sync** pushes that history to your
team's **Agent Relay Loop** service (`relayhistory-cloud`) so it feeds the shared
Capture → Learn → Plan → Pair loop.

This is the single source of truth for cloud-sync usage (dev + prod, end-to-end).

- **Dev API:** `https://relayhistory-api-dev.agent-workforce.workers.dev`
- **Prod API:** `https://history.agentrelay.com`

> Pair (in-session warnings) is a later phase. What's shipped is Capture → push → store.

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

## DEV — usable right now (no RelayAuth token needed)

Dev allows `admin-mint` (it's fail-closed only in prod). Two commands:

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

## PROD — once you issue a RelayAuth token

Prod returns **404** on `admin-mint` by design, so prod uses the RelayAuth login path:

```bash
# 1. issue a RelayAuth access JWT for audience "relayhistory" (your RelayAuth infra).
#    NOTE: the param is expiresIn (seconds, integer) — not ttl.
ACCESS_TOKEN=$(curl -sS -X POST https://api.relayauth.dev/v1/tokens \
  -H "x-api-key: $RELAYAUTH_API_KEY" -H "content-type: application/json" \
  -d '{"identityId":"<id>","workspaceId":"<ws>","audience":["relayhistory"],"expiresIn":3600}' \
  | jq -r .accessToken)

# 2. login + push
RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-prod" \
  ai-hist login --base-url https://history.agentrelay.com --token "$ACCESS_TOKEN"

RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-prod" \
  ai-hist push --json
```

RelayAuth JWT must be RS256, issuer `https://relayauth.dev`, audience `relayhistory`, with
mappable user/org/workspace claims.

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
- To switch targets, re-run `login`/`admin-mint` against the other `--base-url` (or just use
  the per-target `RELAYHISTORY_HOME`).

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
| `not authenticated …` | run `login` (prod) or `admin-mint` (dev) first |
| `Nothing new to push.` | `ai-hist sync` first, then push |
| `HTTP 404 admin mint disabled` | you hit prod with `admin-mint` — prod is login-only |
| `HTTP 401` | token expired/invalid — re-auth |
| connection refused | wrong `--base-url`, or (local) `wrangler dev` not running |

---

## Internals

- Mapping: `ai-hist-core::convergence` → WS-1 `ConvergenceEnvelope`.
- Batching/cursor: `ai-hist-core::outbox::build_outbox_batch`.
- Transport/auth: the `ai-hist` binary `cloud` module — `ureq` HTTP, `rth_at_` bearer,
  `SyncCursor` persistence. Network I/O lives here, not in the WASM-bound core.
- Convergence contract: `relayhistory-cloud/docs/decisions/2026-06-21-normalized-agent-event-schema.md`;
  trajectory-lens mapping: `trajectories/docs/convergence-integration.md`.
