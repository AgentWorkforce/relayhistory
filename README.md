# RelayHistory (`ai-hist`)

**Search, resume, and hand off every coding-agent session — across every harness on your machine.**

Local memory for **Claude Code**, **Codex**, **Cursor**, **Grok**, **OpenCode**, and [**Agent Relay**](https://github.com/AgentWorkforce/relay). Your agent sessions are indexed into one local SQLite database. Search them all at once, jump back into a session with its harness's native resume command, and hand a compact context pack to another agent — or to a teammate's agent — to continue the work.

```sh
ai-hist search "auth rewrite"
# → matches across every codex/claude/cursor session on this machine

ai-hist resume "auth rewrite"
# → prints `codex resume 29284179-c09f-44f2-b9ec-16f678dc1832`

ai-hist pack "auth rewrite" --tokens 1500
# → a token-budgeted context pack, ready to paste into another agent
```

## Use Cases

- **Never search for an old chat session again.** One local index across every harness. `ai-hist search "the thing I was working on"` finds it, whichever CLI you used.
- **Hand off work between agents without losing context.** `ai-hist pack "the feature"` produces a compact context pack; `ai-hist resume "the feature"` prints the native resume command. Great for switching from Claude to Codex mid-work, or picking up a teammate's session.
- **Give your agent access to its own memory via MCP.** Wire `ai-hist-mcp` into Claude Code / Cursor / Codex and the agent can query its own past sessions while it's running — search, session events, tool calls, file edits.

## Get Started

```sh
npm install -g ai-hist
ai-hist                                      # first run discovers and indexes your recent sessions
ai-hist search "the thing i was working on"
```

The bare `ai-hist` command bootstraps a searchable database on first use: it discovers your most recent local sessions and indexes their evidence, then tells you what it found. It leaves an already-populated database alone. Run `ai-hist sync` any time you want a full re-ingest rather than the bounded first-run pass.

Node.js 20 or 22 is required. `npm install` pulls a prebuilt native addon for macOS (arm64, x64), Linux glibc ≥ 2.28 and musl (arm64, x64), and Windows x64 — no Rust toolchain, compiler, or separate binary download. The glibc floor covers Debian 12, Ubuntu 22.04, Amazon Linux 2023, and RHEL/Alma 9; releases are smoke-tested on `node:22-bookworm-slim` and `ubuntu:22.04`.

Rust embedders depend on the `ai-hist` crate (`SessionStore::open` / `sync`). See [crates/ai-hist/README.md](crates/ai-hist/README.md).

## Every command

```sh
ai-hist search "auth rewrite"                        # full-text search across every harness
ai-hist sessions list                                # your most recent sessions, newest first
ai-hist resume "auth rewrite"                        # print the native resume command
ai-hist pack "auth rewrite" --tokens 1500            # compact context to hand another agent
ai-hist sessions tree <harness> <id>                 # walk the parent/subagent tree
ai-hist sessions relationships <harness> <id>        # which sessions spawned which
ai-hist recent 20                                    # the last N prompts, newest first
ai-hist stats                                        # how much history is indexed, by source and project
```

`<harness>` is the session's source — `claude`, `codex`, `cursor`, `grok`, `opencode`, or `relay` — and is required alongside the ID, because session IDs collide across providers. `ai-hist sessions list` prints both.

`ai-hist resume` prints a native resume command for Claude Code, Codex, Cursor, and Grok sessions. OpenCode and Agent Relay sessions are searchable and packable, but have no native resume command to print, so use `ai-hist pack` to carry that context forward instead.

## MCP

```sh
npx -y ai-hist-mcp
```

Exposes `search_history`, `list_sessions`, `get_session_events`, `get_session_tool_calls`, `get_session_file_edits`, `get_session_tree`, `history_stats`, and more as MCP tools. Wire it into any MCP-capable agent so it can query its own history mid-session.

The optional `@relayhistory/capture` plugin adds `get_session_thread` and durable evidence readback when explicitly configured. The default MCP server contains only local history and generic delivery operations.

The optional plugin owns stage credentials and token rotation through its Rust
helper. The default MCP server does not load that helper or read its auth store.

## Team + Cloud

Cached reads such as `search`, `recent`, `sessions list`, `resume`, `pack`, and `stats`,
and acquisition commands such as discovery, hydration, and sync, take a location scope: `--local` (the default), `--remote`, or `--all`.

```sh
ai-hist sessions discover --remote --config history.json  # explicitly installed source plugins
ai-hist sync --all --config history.json                  # local history plus selected plugins
ai-hist search "auth rewrite" --all    # search both at once
```

Commands that address one session by identity do not take a scope, and reject one rather than guessing — they already name a single session. They split by how they take that identity:

```sh
ai-hist sessions tree SOURCE SESSION_ID        # also relationships, tools, edits
ai-hist session SESSION_ID [--source SOURCE]   # session and events take the id alone
ai-hist events SESSION_ID [--source SOURCE]    # --source only narrows a reused id
```

`sessions tree`, `sessions relationships`, `sessions tools` and `sessions edits` require both positionals and fail without `SOURCE`. `session` and `events` take `SESSION_ID` on its own and reject a `SOURCE` positional; pass `--source` only to disambiguate an id two harnesses happen to share. (`sessions hydrate` also takes `SOURCE SESSION_ID`, but it is an acquisition command and does accept a scope.)

Optional: install `@relayhistory/capture` to add authentication, durable delivery, readback, sharing and replay. Other services can implement the same public destination/source interfaces. See [optional cloud setup](docs/enable-cloud.md).

Install `@relayhistory/provider-sources` and configure it explicitly for
remote provider acquisition. Its connectors reuse sign-ins you already have: `claude-web` lists your claude.ai/code sessions from the Claude Code CLI's stored OAuth token, and `codex-cloud` lists Codex cloud tasks through `codex cloud list --json`. With no connector configured, `--remote` fails loudly rather than silently falling back to local. See [remote connectors](docs/remote-connectors.md).

The optional compatibility CLI reads sessions available through the legacy cloud API:

```sh
relayhistory-plugin replay <session-id>                 # print a cloud session's events, oldest first
relayhistory-plugin replay <session-id> --out log.txt   # write that transcript to a file instead
relayhistory-plugin token                               # print a cloud API token for your own tooling
```

`replay` prints the whole transcript. `--limit` is the per-request page size, not a cap: `replay` follows the server's cursor until the session is exhausted, so a 5-event session under `--limit 1` still prints all 5, one request at a time. `--max-content` truncates long events, and truncated ones are marked in the output; `--json` emits the raw event array. (`events --limit N` does cap, because it prints one page and a `nextCursor`.) Without a stored cloud session it stops and names what is missing rather than printing a partial transcript, and `--out` is written atomically only after the whole fetch succeeds, so an interrupted replay never truncates a transcript you already had.

`relayhistory-plugin token` prints a live credential to stdout — treat it like a password, and don't paste its output into a terminal you are sharing or a log.

RelayHistory is one optional cloud integration. See [cloud setup](docs/enable-cloud.md)
for authentication, durable delivery, readback, and the legacy sharing API.

## Why `ai-hist`

- **Every harness, one search.** Claude Code, Codex, Cursor, Grok, OpenCode, Agent Relay — indexed side-by-side. No per-harness silo.
- **Provider-aware evidence.** Prompts, tool calls, and edits are preserved as raw evidence, not summarized away — as much of it as each harness actually exposes. Hydration reports `full`, `partial`, or `shallow_only` per session, so you can tell thin coverage from a thing that never happened. Cursor is the clearest example of the difference: its transcripts do carry the assistant's prose and every tool call it made, all of which are indexed, but they carry no tool *output*, no model id, no token usage and no timestamp field — so those are reported as unavailable rather than missing, and a turn whose injected `<timestamp>` tag cannot be read is stamped from the file mtime with a `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic saying so. The per-field detail is in [the session catalog](docs/session-catalog.md#cursor).
- **Local by default.** SQLite on your machine. Export and delivery require an explicit selection; remote acquisition requires an installed source plugin.
- **Handoff-native.** `pack` and `resume` are first-class commands, not afterthoughts.
- **MCP-native.** Your agent queries its own memory the same way you do.

---

Docs: [getting started](docs/getting-started.md) · [architecture](docs/architecture.md) · [remote connectors](docs/remote-connectors.md) · [migration](docs/native-sdk-migration.md)
