# Cloud Sync — push your local history to Agent Relay Loop

`ai-hist` captures your AI-agent history **locally** (Claude Code, Codex, Cursor, Agent
Relay, Trajectories) into a SQLite database. **Cloud sync** is the optional next step: push
that history to your team's **Agent Relay Loop** service (`relayhistory-cloud`) so it feeds
the shared Capture → Learn → Plan → Pair loop.

> **Status:** the `login` / `admin-mint` / `push` commands below are built and tested. They
> require a running `relayhistory-cloud` endpoint — either your team's deployed service or a
> local `wrangler dev` instance. Pair (in-session warnings) is a later phase.

---

## What gets sent (and what doesn't)

`ai-hist push` maps your local rows into the team's normalized **convergence event**
contract and POSTs them to `/v1/ingest`. Two lenses are sent today:

- **History (prompts):** your prompts across tools → `kind: "prompt"`, `lens: "history"`.
- **Trajectories (reasoning):** the *distilled* `decisions` + `retrospective` from each run
  (not the full turn-by-turn transcript) → `decision` / `finding` / `reflection` events,
  `lens: "trajectories"`.

Privacy model (default **Team/Growth** tier — vendor-readable):
- Cost/usage data is **not** sent by this client — that's `burn`'s job.
- The CLI does a **defense-in-depth preflight**: home-directory paths are normalized
  (`/Users/<you>/…` → `/Users/~/…`) before anything leaves your machine.
- The **server** is the compliance boundary: it scrubs secrets/PII, drops raw payloads,
  and minimizes records before storage. Readable content is retained (scrubbed) so the team
  can search and learn from it.
- **Incognito** lets you exclude any session from sync entirely (see below).
- End-to-end-encrypted / self-hosted custody is the separate **Enterprise** tier.

---

## Quickstart (humans)

### 1. Authenticate

**Real use** — log in with your Agent Relay token (the service mints a local
`rth_at_`/`rth_rt_` session, stored `0600` under `~/.agentworkforce/relayhistory/`):

```bash
ai-hist login --base-url https://history.agentrelay.com --token "<your-agent-relay-token>"
```

**Local dev** — against a `wrangler dev` instance, mint a token directly (no browser flow;
needs the server's `ADMIN_MINT_SECRET`):

```bash
ai-hist admin-mint \
  --base-url http://localhost:8787 \
  --admin-secret "$ADMIN_MINT_SECRET" \
  --org org-a --workspace workspace-a --user me
```

### 2. Push

```bash
ai-hist push                 # send everything new since the last push
ai-hist push --limit 200     # cap rows scanned per source
ai-hist push --json          # machine-readable result
```

Output:

```
Pushed 6 record(s), 6 accepted (cursor → history #4821, trajectory rowid 37).
```

`push` is **incremental and idempotent**: it tracks a per-source cursor
(`~/.agentworkforce/relayhistory/cursor.json`) and only sends rows past it. The cursor
advances **only after the server accepts the batch**, and each batch carries a deterministic
id, so a retry after a network blip never double-writes.

### 3. Incognito (exclude sessions)

```bash
ai-hist push --incognito <session-id> --incognito <trajectory-id>
```

Excluded sessions are skipped *and* the cursor still advances past them — they are never
sent, now or on a later push.

---

## Automation (agents / CI / continuous sync)

`push` is safe to run on a timer — the cursor makes repeated runs cheap and idempotent.

**macOS launchd** (every 5 min):

```bash
# in your LaunchAgent ProgramArguments, after `ai-hist sync` (local import):
/usr/bin/env ai-hist push --json >> /tmp/ai-hist-push.log 2>&1
```

**Linux cron:**

```cron
*/5 * * * * ai-hist sync && ai-hist push --json >> /tmp/ai-hist-push.log 2>&1
```

Notes for automated callers:
- `--json` emits `{ "sent", "accepted", "batchId", "cursor" }` — parse `sent`/`accepted`.
- Exit code is non-zero on transport/auth failure; the cursor is **not** advanced on failure,
  so the next run retries the same batch safely.
- Token storage and cursor are per-machine under `~/.agentworkforce/relayhistory/`
  (override the home with `RELAYHISTORY_HOME`).
- The CLI sends source-native data; the server owns normalization (confidence scaling,
  `Task:` content enrichment, scrub). Don't pre-transform on the client.

---

## Local end-to-end (against `wrangler dev`)

To exercise the full client→cloud path locally:

1. Stand up `relayhistory-cloud` on `wrangler dev` with a **dedicated non-prod Neon branch**
   in `.dev.vars` (see `relayhistory-cloud/docs/deployment-flow.md`).
2. `ai-hist admin-mint --base-url http://localhost:8787 --admin-secret <secret> --org org-a`
3. `ai-hist push --json`
4. Verify the rows landed (the server-side quickstart shows the `SELECT … FROM
   convergence_events` checks).

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `not authenticated …` | Run `ai-hist login` or `ai-hist admin-mint` first. |
| `ingest failed: HTTP 401` | Token expired/invalid — re-auth. |
| `ingest failed: HTTP 404` on admin-mint | The server fails admin-mint closed in production — it's dev-only. Use `login` against a deployed service. |
| `Nothing new to push.` | Cursor is already current; capture new sessions (or `ai-hist sync`) first. |
| Connection refused (localhost) | `wrangler dev` isn't running, or wrong `--base-url` port. |

---

## How it works (internals)

- Mapping: `ai-hist-core::convergence` turns local rows into the WS-1 `ConvergenceEnvelope`.
- Batching/cursor: `ai-hist-core::outbox::build_outbox_batch` selects rows past the cursor.
- Transport/auth: `ai-hist` (binary) `cloud` module — `ureq` HTTP, `rth_at_` bearer,
  `SyncCursor` persistence. Network I/O lives here, not in the WASM-bound core.
- The convergence contract is defined in
  `relayhistory-cloud/docs/decisions/2026-06-21-normalized-agent-event-schema.md`; the
  trajectory-lens mapping reference is `trajectories/docs/convergence-integration.md`.
