# OpenClaw historical acquisition (#511)

Pinned product: `openclaw@2026.10.4` from npm (tarball integrity
`sha512-+FH0StZF1tlTazM7JJGY6JePeJmhFNSRByi2Q3jXPRiLO7o4dJh2xa9Vy4Dk8Cd2ic8B7Zkf5NfEaCCWUkgqnQ==`),
which writes agent-database schema **24**. Upstream source was reviewed at
`openclaw/openclaw` `84da50d6716633cdd38150cab08d83b49d05e7f8` (main, agent
schema 31). Implemented on Linux; the real-install check below ran on macOS
arm64. No private history was copied; every fixture row is synthetic.

## Storage

OpenClaw keeps one SQLite database per agent at
`<stateDir>/agents/<agentId>/agent/openclaw-agent.sqlite`
(`src/state/openclaw-agent-db.paths.ts`). The state directory is
`OPENCLAW_STATE_DIR` (trimmed, `~` expanded against the effective home), else
`<home>/.openclaw`, or `<home>/.openclaw-<profile>` when `OPENCLAW_PROFILE`
names a non-default profile; `OPENCLAW_HOME` replaces the OS home
(`src/config/state-dir.ts`, `src/infra/home-dir.ts`, `src/cli/profile.ts`).
RelayHistory freezes those variables once; `from_home` ignores them. The native
default root keeps portable IDs (`<agentId>:<sessionId>`); any override is
namespaced by a hash of its path (`root-<12 hex>:<agentId>:<sessionId>`), as
Hermes does.

Every directory under `agents/` holding `agent/openclaw-agent.sqlite` is an
agent, enumerated at scan time so new agents are found after roots freeze.
Not read: the shared `state/openclaw.sqlite` (its `agent_databases` registry
also lists relocated agent databases, but the file holds credentials and
pairing secrets), per-agent `agentDir` config overrides, `OPENCLAW_AGENT_DIR`,
`codex-home`, `auth-profiles`, and any config file.

Schema, verified against the released writer and a real 2026.10.4 install:

- `schema_meta` (`meta_key='primary'`, `role='agent'`, `schema_version`,
  `agent_id`) and `PRAGMA user_version` must agree; the owner must be the
  directory's agent; an `openclaw_engine_anchor` table (PostgreSQL-backed
  store) is refused. Schemas 24–31 are accepted: 25–31 only add columns at
  table ends, triggers and indexes outside the selected columns, and every
  column is selected by name.
- `session_windows`: one row per transcript (`session_id`), with its
  `session_key`, previous window, reason, scope, chat type, channel, account,
  model, harness, parent/spawn keys and timestamps (epoch ms).
  `session_nodes` (by key) adds label/display name and `created_via`.
- `transcript_events (session_id, seq)`: one Pi session entry per row, as
  `event_json` or a plain zstd frame in `event_zstd` whose decoded length must
  equal `event_utf8_bytes` (≤ 4 MiB). The lowest `seq` is the
  `{"type":"session","version":3|4,...}` header. Entries are Pi v3/v4:
  `message` (roles `user`, `assistant`, `toolResult`, `custom`,
  `bashExecution`), `model_change`, `thinking_level_change`, `compaction`,
  `reset`, `branch_summary`, `custom`, `custom_message`, `label`,
  `session_info`, `leaf`.
- `session_transcript_active_events`: the active branch.
- `session_transcript_cold_archives`: optional cold storage (off by default)
  deletes a session's hot rows after writing one zstd JSONL envelope to
  `agents/<id>/sessions/cold/<sha256>.jsonl.zst` or the row's blob
  (`src/config/sessions/session-cold-storage-codec.ts`).
- `transcript_rewrite_watermarks.generation` rotates when admitted content is
  rewritten; `session_windows.transcript_updated_at` advances on every append
  or rewrite.

## Acquisition

Each capture validates the database and every existing sidecar inside the
root, then opens one read-only, WAL-visible transaction
(`sqlite_snapshot`). The listing and every re-read session share it. Nothing
is migrated, checkpointed or written; the journal mode is untouched. A busy or
locked database (rollback-journal writers, network filesystems) counts as
unread and keeps prior evidence. Bytes read are reported as zero.

Discovery stamps each session from its row aggregates (`count`, `max(seq)`,
latest `created_at`, rewrite generation, window update times, cold-archive
identity), so a write to one session does not re-read the others. The
stat-only fingerprint covers the database, WAL and journal and carries the
identity namespace.

A sweep re-reads only sessions whose stamp changed. When a session only grew
(same rewrite generation, still hot, no deletions, and the identity index
still holds the previous last 32 entry IDs), only those 32 entries and the new
ones are normalized; nothing is retired and catalog fields only widen.
Anything else (rewrites, suffix replacement, deletions, cold
archiving/restoring, generation upgrade, repair) re-reads the whole transcript
in one ledger transaction and retires evidence it no longer holds. Sessions
absent from a complete snapshot are retired locally, and a database confirmed
missing inside an accessible root retires its sessions. A missing root,
unreadable database, or failed session never proves deletion. One malformed
session is unread; the rest of its database still syncs.

A cold-archived session is read from its archive after its length, SHA-256,
header, event count, last sequence and raw byte count verify. A missing or
tampered archive is unread, never empty history.

## Normalization

OpenClaw entries are Pi entries, so normalization follows the Pi provider:
the full tree (all branches) with native entry IDs and `parentId`; text,
thinking and `toolCall` blocks; `toolResult` joined by `toolCallId` with
`isError` as the outcome; images and unknown blocks as bounded markers;
non-message entries and `custom`/`bashExecution` messages as markers. Message
timestamps are the integer `message.timestamp`, else the entry's ISO time.

- The header marker carries the session key, agent ID, channel (the key's
  first segment after `agent:<id>:`: `main`, `cron`, `relay`, `telegram`,
  `subagent`, `recovered`, ...), window channel/account/chat type, window
  reason and previous session, parent/spawn keys, harness, model, status and
  schema version.
- A per-message marker keeps OpenClaw's own fields (`__openclaw`,
  `idempotencyKey`, `openclawDelivery`, `openclawDeliveryMirror`,
  `openclawDisplayContent`, `openclawStreamFallback`, `provenance`,
  `excludeFromContext`, `supersedesEntryId`, ...) and flags entries off the
  active branch.
- User messages OpenClaw wrote itself are `control_kind = "synthetic"`:
  `provenance.kind` `inter_session`/`internal_system`, `excludeFromContext`,
  `__openclaw.contextFreeCommand`. `external_user` provenance and Codex mirror
  annotations (which upstream adds to the admitted human prompt in place) stay
  prompts.
- Usage is per message (Pi's counters and reported cost). The provider
  `responseId` is the request identity, so copies of one response on inactive
  branches count once; on the real install 53.8k usage rows collapse to about
  20k requests. Not counted, but kept in the marker: delivery mirrors and
  other transcript-only rows (`provider: "openclaw"`, `delivery-mirror`,
  `gateway-injected`, `native-history`), session-catalog imports,
  all-zero snapshots, and CLI-harness rows (`api: "cli"`), whose usage the
  harness's own native log (Claude Code) already holds. Codex-harness rows are
  counted: their native rollouts live in the agent's private `codex-home`,
  which no provider reads. A Codex harness configured with the user's own
  `~/.codex` (`homeScope: "user"`) would double count.
- Titles: node label/display name, else window display name, else the last
  `session_info` name. Paths in tool arguments are text, never opened.

Unsupported: legacy pre-SQLite `agents/<id>/sessions/*.jsonl` (upstream's
doctor/startup migration imports and moves them), reset/delete archives
(deleted sessions retire, as for every provider), `agentDir` relocation,
PostgreSQL stores, relationships, native resume and live execution.

## Fixtures

`crates/ai-hist/src/providers/openclaw/capture_fixture.mjs` writes synthetic
sessions with the released writer's own plugin-SDK APIs
(`upsertSessionEntry`, `appendSessionTranscriptMessageByIdentity`,
`appendAssistantMirrorMessageByIdentity`) into an isolated state directory,
with a synthetic clock and sequential UUIDs so reruns are byte-identical.
The writer itself zstd-compresses the long tool result. It then dumps the
transcript tables as SQL, and the released reader
(`readSessionTranscriptEvents`) re-reads the result into `oracle.json`. Run:

```sh
npm install --prefix ~/Projects/scratch-openclaw/pkg --ignore-scripts openclaw@2026.10.4
OPENCLAW_WRITER=~/Projects/scratch-openclaw/pkg node crates/ai-hist/src/providers/openclaw/capture_fixture.mjs
```

`upstream-agent-v2026.9.9.sql` is upstream's own MIT-licensed dump of a
database the published 2026.9.9 writers created (openclaw/openclaw
`test/fixtures/sqlite/openclaw-agent-v2026.9.9.sql`), verbatim; it proves the
full upstream catalog (FTS5 tables, triggers, STRICT tables) opens without
extensions.

## Verification

Real install, read-only (macOS, OpenClaw 2026.10.4, schema 24, one agent,
148 sessions, 127k events of which 6.1k zstd): a throwaway probe synced only
OpenClaw into a temporary ledger, printed aggregate counts and deleted the
ledger. No content left the machine.

Scale, synthetic: before a shared fix, normalization was quadratic in session
size (2×10k events: 25.6 s). Without `sqlite_stat1`, the planner walked every
event of the source once per prompt row in `file_history::reconcile_prompts`;
`+e.source` keeps it on the timestamp index (2×10k: 1.2 s; 40k events: 2.5 s).
This also affects Pi and Copilot. A one-event append to a 40k-event session
then took 1.85 s; the append path takes 49 ms (100k events: 7.5 s complete
read, 113 ms append).
