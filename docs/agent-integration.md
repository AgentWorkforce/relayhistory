# Agent integration

RelayHistory 1.0 has one production path: the TypeScript SDK, CLI, and MCP
server call the mandatory Rust Node-API engine. Reads are cache-only; provider
I/O happens only when an integration explicitly requests shallow discovery or
full sync.

All collection operations use the same session ledger and a shared scope enum:
`local`, `remote`, or `all`. `local` is the default. Local and remote are
presences of one logical session; `all` is a deduplicated union, not a second
query followed by concatenation.

Use these public operations:

- `listSessionCatalogPage()` / MCP `list_sessions` for bounded cache-only
  session discovery results.
- `discoverSessions()` / MCP `discover_sessions` to refresh shallow provider
  metadata.
- `search()`, `recent()`, `getSession()`, and `getSessionEventsPage()` for
  indexed history reads.
- `getSessionRelationships()` / MCP `get_session_relationships` and
  `getSessionTree()` / MCP `get_session_tree` for delegation topology, plus
  the SDK-only `getSessionChildrenPage()`, `sessionDescendants()`, and
  `sessionEventsIncludingDescendants()` for walking large trees. Every event
  keeps the session id of the session that produced it.
- `getSessionToolCallsPage()` / MCP `get_session_tool_calls` and
  `getSessionFileEditsPage()` / MCP `get_session_file_edits` for bounded,
  structured reads of one hydrated session's recorded tool calls and file
  edits. Both name a session by `source` **and** `sessionId`; provider session
  ids collide, and these pages never merge two providers' records.
- `sync()` / MCP `sync` for explicit full local ingestion.

Pass `scope` to collection operations when the default local view is not
enough. The CLI spelling is the mutually exclusive `--local`, `--remote`, and
`--all` flags. `sessions list`, `search`, `recent`, and `stats` always query the
cached ledger and never contact a provider. Direct `getSession()` and event-page
lookups are identity-based and scope-independent.

Remote discovery and remote sync run through provider connectors
(claude.ai/code web sessions and Codex cloud tasks — see
[Remote connectors](remote-connectors.md)). A `remote`-only request on a
machine with no connector configured fails explicitly, and integrations must
surface that error rather than retrying locally; an `all` request runs
whatever is configured and skips remote quietly on absence — `locationsRun`
says what executed. Local sync performs full ingestion; remote sync
refreshes shallow connector rows and `remote` presences, because the remote
listings carry no transcripts. `all` runs local adapters plus every configured
connector. Acquisition result `scope` echoes the request and `locationsRun`
reports which connector locations executed; use session `locations` for
observed presences.

The CLI equivalents are `sessions list`, `sessions discover`,
`sessions hydrate`, `sessions relationships`, `sessions tree`,
`sessions tools`, `sessions edits`, `search`, `recent`,
`session`, `events`, `stats`, and `sync`. See
[Session catalog](session-catalog.md) for discovery and pagination contracts
and [Architecture](architecture.md) for the process boundary.

The old cloud push, login, Pair, hook installer, tag, and trajectory convenience
commands were removed in 1.0. They are not available through subprocess or
JavaScript fallbacks; see the [migration guide](native-sdk-migration.md).

## Live capture

Provider transcripts do not live forever. Claude Code cleans up its JSONL
files, so the window in which a session can still be captured losslessly is
bounded — and anything that only learns about a session from a later sweep can
miss that window entirely. Two surfaces close it, and they are complementary:
watch mode notices writes, hooks are told about them.

### `ai-hist watch`

```bash
ai-hist watch                       # fs events, 200ms debounce, 30s backstop
ai-hist watch --debounce-ms 500     # collapse bursts over a longer window
ai-hist watch --interval 30         # polling cadence when fs events are off
ai-hist watch --no-fsevents         # poll only
```

Watch attaches a filesystem watcher to everything a local sweep reads: the
providers' session roots (`~/.claude/projects`, `~/.codex/sessions`,
`~/.codex/archived_sessions`, `~/.cursor/projects`, `~/.grok/sessions`, and the
directory holding the OpenCode database) plus the flat per-harness logs
`~/.claude/history.jsonl` and `~/.codex/history.jsonl` and any `.trajectories`
directory. It then runs one sweep per burst of writes, with a slow poll behind
it. When no root can be watched — a network mount, a container without inotify,
`--no-fsevents` — it falls back to polling at `--interval`.

Startup reports which driver it took **and which roots are not covered yet**. A
root that does not exist cannot be watched, so a provider installed after
`watch` started would otherwise be silently missed; those roots are retried on
every backstop tick, and a loop that started with nothing to watch promotes
itself to filesystem events as soon as one appears. Those retries are on the
*backstop*, not on `--interval`: a `watch --interval 3600` started before a
provider exists picks it up in seconds rather than in an hour, while its
sweeps stay on the hour it was given. That tick also re-checks each
registration against the directory it was made against — a watch is bound to
the directory object, not to its name, so a root deleted and recreated
(`rm -rf ~/.codex/sessions`, then the next session) has a live name and a dead
watch — and re-registers only the ones that changed. The backstop also
re-derives the root set, so a project that grows a `.trajectories` directory
mid-run — a root whose *name* could not have been known at startup — is picked
up too.

A root is watched at the depth it asks for: transcript trees recursively,
because a new session is a new file somewhere inside; the directories holding
the flat logs shallowly, so the todo files and shell snapshots an active
session rewrites constantly do not each wake a sweep; and a `TRAJECTORY_ROOT`
naming a single JSON file as that one file — registered through its parent,
because an atomic rewrite takes a watch on the file itself with it, but
filtered back down to the one name, since that parent is routinely `$HOME`.
That depth is enforced on the events themselves rather than left to the OS,
because the macOS backend has no shallow mode and delivers the whole subtree
regardless.

`watch --remote` installs no local roots at all. Local provider writes are not
what a remote-only run collects, and letting them drive the loop would fire the
remote connectors on every local keystroke instead of at `--interval`.

Two things make this cheap enough to leave running:

- A tick first folds a **stat-only fingerprint** over everything the sweep
  reads — the transcripts discovery enumerates, the Claude subagent
  `agent-*.meta.json` sidecars beside them, the two flat logs, the trajectory
  records, and a generation for the imported Relay rows, which have no file to
  stat but still change. If it matches the previous sweep's, the tick returns
  without opening a single file. Anything the sweep reads has to be in that
  fold: a source left out would sit behind an unchanged fingerprint and never
  be read again. The value is recorded in `.sync-state.json` beside the
  database, after the sweep's cursors, and only when every source was read —
  including the per-file failures a provider absorbs on its way to a
  successful partial run. A file that could not be read this tick keeps the
  fingerprint stale so the next tick retries it, rather than caching the
  failure in place. The value is qualified by the sweep's own parser and
  scanner generations, so a stamp written before an upgrade that bumps one
  cannot skip the re-read that bump exists to force.
- A sweep owes more than ingestion — it also repairs a session whose per-file
  stamp matches but whose evidence is gone. So the stamp is paired with a
  **destination generation** recorded after the sweep, one entry per session:
  if a session holds less than it did, the next tick sweeps instead of
  skipping and that session is re-ingested, however unchanged the sources
  look. A finished rollout's bytes never move again, so without this the loss
  would be permanent. Per session rather than in total, because totals cannot
  tell a loss from a coincidence — one row deleted here and one inserted there
  leaves every total intact. Rows arriving between sweeps — the hook fast
  path, hydration — are growth rather than loss and still skip. The marker
  covers only what a sweep can put back: Claude transcripts and Codex
  rollouts, both re-read when their session is short. Each entry covers that
  session's events, tool calls, file edits and catalog row, since the same
  re-read restores all four. `history` rows are
  deliberately outside it — they come from cursor-backed flat logs sitting at
  EOF, which nothing replays, so counting them would disarm the fast path
  forever over a loss no sweep could undo. And a loss the sweep could *not*
  restore leaves the marker and the fingerprint stale: every tick keeps
  sweeping, loudly, rather than recording the shortfall as the new truth.
- A tick woken by a filesystem event **forces** the sweep past that
  fingerprint. An event can arrive before the write is flushed, so the size and
  mtime it would be compared against are not yet trustworthy. The polling
  backstop does not force, so a quiet machine keeps paying only the fingerprint
  walk.

`ai-hist import --watch` remains an alias for the same loop with the defaults.

### `ai-hist ingest --hook claude`

```bash
echo '{"session_id":"…","transcript_path":"/path/to/session.jsonl"}' \
  | ai-hist ingest --hook claude --quiet
```

Reads one Claude Code hook payload from stdin and hydrates exactly the
transcript it names, instead of sweeping every provider root. The path is
checked against Claude's own root before it is read — a hook payload comes from
another process, and an arbitrary path must never become an ingest target.

**The command always exits 0.** A hook runs inside the agent's tool call, and a
non-zero exit there fails that tool call. A missing or rotated transcript, an
unparseable payload, a locked database: all are reported on stderr and shrugged
off. `--quiet` silences the reporting, not the shrug. A payload without
`transcript_path` (some releases elide it) falls back to a forced full sweep.

Pass `--json` for a machine-readable report on stdout. **`--quiet` outranks
`--json`**: given both, the command prints nothing at all. A hook told to stay
out of the way must not write a JSON document into the stdout of every tool
call; use `--json` on its own when you want to read the report.

### Wiring the Claude Code hooks

In `~/.claude/settings.json`, or a project's `.claude/settings.json`:

```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          { "type": "command", "command": "ai-hist ingest --hook claude --quiet" }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "*",
        "hooks": [
          { "type": "command", "command": "ai-hist ingest --hook claude --quiet" }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          { "type": "command", "command": "ai-hist ingest --hook claude --quiet" }
        ]
      }
    ],
    "PreCompact": [
      {
        "hooks": [
          { "type": "command", "command": "ai-hist ingest --hook claude --quiet" }
        ]
      }
    ]
  }
}
```

`PreCompact` is the one that watch mode cannot replace. Compaction **rewrites
the transcript in place**, and the hook fires *before* that rewrite — it is the
last moment the pre-compaction records still exist on disk. A sweep arriving
afterwards sees only the compacted file, and the earlier evidence is gone for
good. Keep that entry even if you drop the others.

`SessionStart` and `Stop` bracket the session; `PostToolUse` captures tool
errors as they happen rather than after the fact. Re-running any of them over
an untouched transcript is free: hydration compares the source stamp first and
reports `unchanged` without re-reading.

### Codex, OpenCode, Cursor and Grok

Only Claude Code exposes a transcript lifecycle hook to attach to. Codex writes
`~/.codex/sessions/**/rollout-*.jsonl` with no hook surface; OpenCode writes a
SQLite database and exposes none either; Cursor and Grok likewise. For those
providers watch mode *is* the live-capture path — their roots are watched, and
an append wakes the same sweep. `ai-hist ingest --hook <other>` reports the
harness as unsupported and exits 0 rather than pretending. When any of them
grows a lifecycle hook, the payload shape is the only new part: the
single-transcript ingest underneath is provider-agnostic.
