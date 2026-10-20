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
RelayHistory freezes those variables once; `from_home` ignores them.

One ledger session is one OpenClaw **conversation**: a session key and every
transcript window under it, as OpenClaw's own session list shows it. Resets,
rollovers and each isolated cron or heartbeat run open a new window (session
ID) on the same key (`heartbeat-runner-execution.ts`, `cron/isolated-agent`),
and an exact-run key `agent:<id>:cron:<job>:run:<run>` belongs to its job
(`parseCronRunScopeSuffix`). On the real install one cron job had 81 run
windows; one row per window flooded the session list. The ledger ID is
`<agentId>:<key after agent:<agentId>:>` (`main:main`,
`main:telegram:direct:<peer>`, `main:cron:<job>`), or
`<agentId>:key=<key>` for a key outside that prefix. The native default root
keeps these portable; any override is namespaced by a hash of its path
(`root-<12 hex>:<agentId>:...`), as Hermes does. Each window keeps its own
header marker (`session:<sessionId>`) with its native session ID.

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
  store) is refused. Schemas 19–31 are accepted and every column is
  selected by name. Released writers, each diffed against 24 from a database
  it wrote: 19 (2026.8.1–2026.9.4) has no `event_zstd`/`event_utf8_bytes`
  columns and no cold-archive table; 21 (2026.9.5) adds
  `session_transcript_cold_archives` (same codec as 2026.10.4); 23 (2026.9.6)
  adds the zstd columns; 24 (2026.9.7–2026.10.4) only adds
  `session_nodes.snapshot_revision`/`legacy_acp_migration_json`. 20 and 22
  were development schemas between releases. Those two features are detected
  by table and column, not by version. 25–31 only add columns at table ends,
  triggers and indexes outside the selected columns.
- A database before schema 19 (1 in 2026.6.5–2026.7.35: auth profiles and
  caches only) with no transcript tables holds no sessions; its agent's
  sessions are the JSONL store below. An older schema that has transcript
  tables is refused.
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

## Legacy JSONL store (2026.7.x and earlier)

Until 2026.8.1 every agent kept `agents/<id>/sessions/sessions.json` (an
object of session key → entry: `sessionId`, `updatedAt`, `sessionFile`,
`label`, `displayName`, `chatType`, `channel` — `provider` before 2026.2 —,
`model`, `modelProvider`, `agentHarnessId`, `parentSessionKey`,
`spawnedBy`, `status`, `endedAt`, ...) beside one JSONL transcript per
session, `<sessionId>.jsonl` (or `<sessionId>-topic-<topic>.jsonl`). Lines
are the same Pi v3 entries the database rows hold (header first; Pi's own
`SessionManager` wrote them before 2026.6.1, OpenClaw's agent-core after,
both at version 3). Captured from the 2026.7.35 writer; the 2026.2.22 dist
(Pi 0.54.1) was checked for the same header version, index shape and paths.
Each agent's `sessions/` directory is watched.

- Live transcripts follow upstream's `isPrimarySessionTranscriptFileName`:
  `*.jsonl` except `*.trajectory.jsonl` and
  `<id>.checkpoint.<uuid>.jsonl` compaction checkpoints. Reset/delete
  archives (`*.jsonl.reset.<ts>`, `*.jsonl.deleted.<ts>`), index backups and
  `sessions.json.<uuid>.tmp` files are never read; a session whose live file
  is gone retires like any deleted session.
- An index entry names its transcript by the file name of `sessionFile`
  (an absolute path upstream keeps inside the directory), else
  `<sessionId>.jsonl`. The identity is the header `id` under the agent's
  namespace, `<agentId>:<sessionId>`: one session per transcript. The index
  maps a key only to its current transcript, so earlier runs of a key
  cannot be folded the way the database's windows are.
- The active branch is resolved as upstream's session storage does (visible
  leaf after each entry including `leaf` controls and side appends, then the
  path to the root), so off-branch messages carry the same marker as in the
  database.
- Window/node fields map as the doctor's import fills them: title from
  `label` else `displayName`, chat type, channel, account, model, harness,
  parent/spawn keys, status, end time. The header marker records
  `layout: "jsonl"` and no schema version.
- An unterminated final line is deferred (Pi semantics): the complete lines
  only add evidence, as an append does, and nothing of the session retires
  until the file is complete again; the file counts as unread. A changed
  transcript or index entry (its stamp is the file stat plus a digest of its
  entry) is re-read whole. A transcript over 512 MiB or 1M records is unread.
  One parse of `sessions.json` serves every shallow read of its directory.

Upgrade: the 2026.8.1+ doctor (`openclaw doctor --session-sqlite import`,
also run at startup) imports each transcript under the same session ID and
entry IDs, seq 0 being the header, then moves the files to
`agents/<id>/session-sqlite-import-archive/` (checked by running the
2026.9.6 doctor on the 2026.7.35 capture). The database owns every session
it holds a transcript for: discovery and sweeps skip such a JSONL file even
while it is still in place. The imported transcript becomes a window of its
key's database conversation, so its JSONL ledger row (`<agentId>:<sessionId>`)
retires into that conversation once the ledger holds the window (its
`session:<sessionId>` header marker): the same evidence is never counted
twice, and its prompts reattach to the conversation. A JSONL transcript found
missing is checked against its agent's database before anything retires: a
session the database holds keeps its JSONL row until the conversation holds
that window (also when that database failed to normalize it, or was out of a
scoped sweep's reach), and while the database cannot be read nothing of that
agent retires, whether or not its `sessions/` directory still exists. Discovery checks
ownership against every session the database holds, not just the bounded
listing.

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

A sweep re-reads only conversations whose stamp changed, window by window:
an unchanged window is not read, a new window is read whole, and a grown one
only from its tail. When a window only grew
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
- Heartbeats (`src/auto-reply/heartbeat.ts`, `heartbeat-filter.ts`): a
  shared-mode heartbeat appends into the agent's main session. Its user turn,
  upstream's `isHeartbeatUserMessage` for an unconfigured prompt (an internal
  wake marker such as `[OpenClaw heartbeat poll]`, the current or legacy
  default prompt alone or followed by a newline, or the periodic-task prompt),
  is a control row, never a prompt. On the real install 12,781 of 26,953
  main-session user messages were the legacy prompt. The quiet
  acknowledgement (`NO_REPLY`, or `HEARTBEAT_OK` at either edge of at most
  300 characters) stays as an event with its usage (it was a model call) but
  never becomes the session's last assistant text. A custom configured
  heartbeat prompt is not read from config and stays a prompt unless it
  carries `internal_system` provenance.
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

Unsupported: reset/delete archives and the doctor's import archive (deleted
sessions retire, as for every provider), JSONL headers before Pi version 3,
`agentDir` relocation, PostgreSQL stores, relationships, native resume and
live execution.

## Fixtures

`crates/ai-hist/src/providers/openclaw/capture_fixture.mjs` writes synthetic
sessions with a released writer's own plugin-SDK APIs
(`upsertSessionEntry`, `appendSessionTranscriptMessageByIdentity`,
`appendAssistantMirrorMessageByIdentity`) into an isolated state directory,
with a synthetic clock and sequential UUIDs so reruns are byte-identical.
The writer itself zstd-compresses the long tool result. It then dumps the
transcript tables as SQL, and the released reader
(`readSessionTranscriptEvents`) re-reads the result into `oracle.json`.
Writers: 2026.10.4 (schema 24, `main`/`ops`, `oracle.json`), 2026.9.6 (23),
2026.9.5 (21) and 2026.8.1 (19), each `main-schema<N>.sql` with
`oracle-schema<N>.json`, and 2026.7.35, whose sessions directories are copied
to `legacy-jsonl/` (the index's absolute `sessionFile` rewritten to
`/fixture/state`) with `oracle-legacy.json`; that writer only accepts
file-safe session IDs, so its main session is `fixture-main-0001`. Run, per
version:

```sh
npm install --prefix ~/Projects/scratch-openclaw/writers/<version> --ignore-scripts openclaw@<version>
OPENCLAW_WRITER=~/Projects/scratch-openclaw/writers/<version> node crates/ai-hist/src/providers/openclaw/capture_fixture.mjs
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

## Conversation identity and heartbeats (generation v3)

Reported from the dashboard: one row per heartbeat. Isolated cron and
heartbeat runs open a new window on the same session key every run, and the
first implementation emitted one ledger session per window. Generation v3
emits one session per conversation key and treats heartbeat polls as
controls. Real install, read-only, same probe as above: sessions 149 → 96
(cron 59 → 7, one job holding 82 run windows), prompt history 13,979 → 8,143
with no heartbeat prompt left, no session whose last assistant text is a
quiet acknowledgement, and 21,301 usage requests (unchanged: run windows
keep their usage). First sync 47 s, re-sync 0.65 s. Relay keys up to 4,096
bytes long exceed the ledger's identity bound; such keys use a stable
`sha=<SHA-256>` name and are resolved by listing.
