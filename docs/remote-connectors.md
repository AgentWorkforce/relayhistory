# Remote connectors

Remote acquisition (`sessions discover --remote`, `sync --remote`, and the
remote half of `--all`) runs through provider connectors. A connector
enumerates the sessions a provider keeps on its own service and lands them in
the shared session ledger as catalog rows with a `remote` presence. There are
three connectors:

| Connector | Source | Lists | Interface |
|---|---|---|---|
| `claude-web` | `claude` | Claude Code sessions on claude.ai/code | listing plus the CLI's private teleport-evidence interface, with its stored OAuth token |
| `codex-cloud` | `codex` | Codex cloud tasks | `codex cloud list --json` and `codex cloud diff TASK_ID` |
| `cloud` | Upstream source (`claude`, `codex`, `cursor`, `grok`, `relay`, `trajectory`, `opencode`) | Teammate sessions in the token’s organization | `GET /v1/sessions`, using the stored RelayHistory `rth_at_` session |

Local and remote stay presences of one session ledger: a session observed
both by a local file adapter and by a connector is one catalog row whose
`locations` are `["local", "remote"]`.

## Configuration

The provider connectors use their CLI’s stored sign-in. The `cloud` connector
reuses the RelayHistory session created by `ai-hist login`:

- `claude-web` — `~/.claude/.credentials.json` exists (the claude.ai OAuth
  token the Claude Code CLI stores at sign-in). If your setup keeps
  credentials elsewhere — for example macOS keychain-backed installs — export
  a credentials JSON file and point `RELAYHISTORY_CLAUDE_CREDENTIALS` at it.
- `codex-cloud` — `~/.codex/auth.json` exists (written by `codex login`).
  The `codex` binary must be on `PATH` when the connector runs; the CLI
  handles token refresh itself.
- `cloud` — `cloud::config_dir()` (normally `~/.agentworkforce/relayhistory`,
  overridden by `RELAYHISTORY_HOME`) resolves a stored `rth_at_` session with
  `access_token_expires_at` at least 60 seconds in the future. This RFC 3339
  expiry is persisted from the service's `accessTokenExpiresAt` during login,
  mint, and refresh. Legacy auth without expiry remains usable for push, but
  needs a fresh login/refresh before cloud discovery advertises availability.
  Missing, malformed, expired, or ambiguous credentials report an unconfigured
  reason; status probes never refresh or perform network requests.

Cloud stage selection and credential loading live only in `cloud.rs`.
`RELAYHISTORY_BASE_URL` takes precedence over `AI_HIST_BASE_URL`; without an
explicit override, a single stored stage is used and multiple stages require
selection. The normal service URL is `https://history.agentrelay.com`.
Non-loopback cleartext HTTP is refused, while `http://127.0.0.1:8787` is valid
for development. Recall requests reuse auth refresh on HTTP 401, refuse
redirects, and bound each response to 16 MiB and each request to 30 seconds.
The client never sends an org/workspace selector: tenancy comes from the token.

```bash
ai-hist login
ai-hist sessions discover --remote --source cursor
ai-hist sync --all
```

Cloud is a connector, **not** a new `--source` value. Its adapters query each
upstream source independently, preserving source filters and global recency
ordering. Each source is limited to 100 pages of at most 100 sessions.

Requesting `--remote` acquisition with no connector configured fails loudly
with the same `no remote provider connectors are configured` error as before
connectors shipped, now including the per-connector reason. `--all` runs
whatever is configured and never errors on absence — the acquisition
summary's `locations_run` says what executed.

`claude-web` talks to `https://api.anthropic.com` only. It deliberately does
**not** honor `ANTHROPIC_BASE_URL` — that variable redirects generic
Anthropic API traffic (LLM gateways, dev proxies), and following it here
would send the stored claude.ai OAuth token to whatever host it names.
Redirecting the session-list endpoint is its own explicit decision via
`RELAYHISTORY_CLAUDE_API_BASE_URL`, and plain `http://` is refused except for
loopback addresses, so the token cannot travel cleartext to a remote host. An
expired token is reported before any request is made; running the Claude Code
CLI once refreshes it.

`codex-cloud` pages through the CLI's listing window: `codex cloud list`
accepts `--limit` values of 1–20, so the connector requests bounded pages and
follows the returned `cursor` until the listing (or a requested row cap) is
exhausted. The provider connectors bound one enumeration to 100 pages (10,000 claude
sessions, 2,000 codex tasks) — bounded work is part of the discovery
contract; a later run continues from fresher listings.

## Targeted remote hydration

`hydrateSession({ source, sessionId, scope: "remote" })` addresses exactly one
cataloged remote session. It never runs discovery or enumerates other tasks.
Results report `capability` as `full`, `partial`, or `shallow_only`; a provider
limitation is a successful `capability_limited` result rather than a fabricated
empty transcript. Authentication, missing-session, bounded-partial, and parser
failures use the stable codes `AUTHENTICATION_EXPIRED`, `SESSION_NOT_FOUND`,
`EVIDENCE_PARTIAL`, and `CONNECTOR_FAILURE`.

Claude Code currently fetches a teleported session from
`GET /v1/code/sessions/{id}/teleport-events`, after resolving the signed-in
organization through `GET /api/oauth/profile`. RelayHistory uses the same
provider-owned OAuth credential and normalizes those records through the same
Rust ingestion path as a local Claude transcript. This can include messages,
assistant output, tool calls/results, file edits, model/token fields, and
sidechain markers when Claude supplies them. The endpoint is an observed Claude
Code implementation contract, **not a documented public Anthropic API**. It may
change without notice; malformed or incomplete responses fail explicitly and
never upgrade the remote presence to `full`.

Codex's supported cloud CLI exposes a unified diff, but no task transcript,
tool-result, token/model, or parent/child export. Remote hydration therefore
runs bounded `codex cloud diff TASK_ID`, stores per-file patches in
`file_edits`, and returns `capability: "partial"` with
`discoveryState: "shallow"`. A task without an available diff returns
status `capability_limited` with `capability: "shallow_only"`. RelayHistory
does not call private Codex service APIs.

Both paths cap responses at 16 MiB and execution at 30 seconds where a child
process is involved. Claude pagination is capped at 100 pages of 1,000 records.
HTTP redirects are not followed, preventing authorization headers from moving
to another host. Work is checkpointed by remote presence and repeated
hydration is idempotent.

## What remote rows carry before hydration

Remote listings carry less than a local transcript, and nothing is invented
to fill the gap:

- `session_id` — the provider's identifier (`session_…`/`cse_…` for claude,
  the cloud task id for codex). A Codex cloud task that is later applied or
  resumed locally produces a local rollout with its *own* session id; the two
  are separate catalog rows today.
- `first_prompt` — the provider's session/task **title**, the only
  human-readable identifier the listings offer. Both providers derive it from
  the opening prompt.
- `raw_path` — the session/task URL (`https://claude.ai/code/<id>`, the
  ChatGPT task URL), or `cloud://<orgId>/<sessionId>` for cloud recall.
  Cloud markers also live on the remote presence, preserving a local locator
  when the same `(source, session_id)` already exists locally.
- `repo_url` — for claude-web, the session's `git_repository` source when the
  service records one.
- Timestamps — `created_at`/`last_event_at` (claude), `updated_at` (codex).

Discovery rows stay `discovery_state: "shallow"`. Claude can be upgraded by
targeted hydration through teleport evidence. Codex stays shallow even after
its available diff is indexed because no transcript export exists.

## Remote/local identity correlation

Different remote and local session IDs remain separate rows. Identical
`(source, session_id)` keys, including cloud copies of local sessions, have one
canonical row with both local and remote presences. A Claude transcript containing
the provider-recorded `remoteSessionId` creates a `materialized_local`
relationship from the remote ID to the local session ID, but only when that
exact remote presence already exists. Titles, repositories, prompts, and
timestamps never create canonical relationships. Current Codex CLI apply/diff
output does not record a local rollout ID, so RelayHistory does not guess a
Codex remote-to-local relationship.

Claude Remote Control **bridge** sessions (`environment_kind: "bridge"`) are
views of sessions running in a local terminal; their evidence is local, so
the connector deliberately skips them rather than recording a false remote
presence.

## Change detection

Connectors participate in the same stamp-guarded rescan contract as local
adapters, per presence: the claude stamp tracks `last_event_at`, the codex
stamp tracks `updated_at` plus task `status` (an applied or failed task whose
timestamp did not move is still re-read). An unchanged remote session is
served from the catalog with zero fresh work beyond the listing itself.
Cloud stamps hash the recall rollup and provenance marker, so a changed summary,
event count, or timestamp invalidates that presence's cache.

Cloud discovery and sync populate shallow catalog rows. Cached list/search/recent/
stats APIs retain their existing read semantics; this connector does not ingest
normalized recall events into the local history tables. `cloud.rs` exposes the
same authenticated page helper for `GET /v1/events` and
`GET /v1/sessions/:sessionId/events`, but automatic event ingestion and cloud
transcript hydration are separate work.

## Contract stability

`codex-cloud` speaks supported Codex CLI commands, which are as stable as Codex
chooses to keep them. Claude listing and teleport evidence are observed private
Claude Code interfaces. A change surfaces as a connector diagnostic (or a
failed remote-only run), never as silently missing data mixed into local
results.
