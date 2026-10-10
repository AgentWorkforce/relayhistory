# RelayHistory (`ai-hist`)

**Search, resume, and hand off every coding-agent session — across every harness on your machine.**

Local memory for **Claude Code**, **Codex**, **Cursor**, **Grok**, **Muse Code**, **OpenCode**, and **Devin CLI**. Your agent sessions are indexed into one local SQLite database. Search them all at once, jump back into a session with its harness's native resume command, and hand a compact context pack to another agent — or to a teammate's agent — to continue the work.

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
- **The sourcing layer for [burn](https://github.com/AgentWorkforce/burn).** burn prices and analyzes agent sessions. Today it still ships its own Claude Code, Codex and OpenCode readers; from burn 5.0.0 ([burn #562](https://github.com/AgentWorkforce/burn/issues/562)) it drops them and reads usage, tool calls, file edits and session topology from the same `ai-history.db` through the `ai-hist` crate, so the two tools share one ingest and agree on what a session is. Pricing and cost stay in burn. See the [sourcing ADR](docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md).

## Get Started

```sh
npm install -g ai-hist
ai-hist                                      # first run discovers and indexes your recent sessions
ai-hist search "the thing i was working on"
```

The bare `ai-hist` command bootstraps a searchable database on first use: it discovers your most recent local sessions and indexes their evidence, then tells you what it found. It leaves an already-populated database alone. Run `ai-hist sync` any time you want a full re-ingest rather than the bounded first-run pass.

RelayHistory follows each harness's configured state directory. Set `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, or `GROK_HOME` to relocate Claude Code, Codex, or Grok data; unset or empty values fall back to `~/.claude`, `~/.codex`, and `~/.grok`. Muse Code sessions are read from `$XDG_DATA_HOME/muse/sessions` (`~/.local/share/muse/sessions` when `XDG_DATA_HOME` is unset). OpenCode uses `OPENCODE_DB`, defaulting to `~/.local/share/opencode/opencode.db`. Devin CLI sessions are read from `$XDG_DATA_HOME/devin/cli/sessions.db`, defaulting to `~/.local/share/devin/cli/sessions.db`.

Node.js 20 or 22 is required. `npm install` pulls a prebuilt native addon for macOS (arm64, x64), Linux glibc ≥ 2.28 and musl (arm64, x64), and Windows x64 — no Rust toolchain, compiler, or separate binary download. The glibc floor covers Debian 12, Ubuntu 22.04, Amazon Linux 2023, and RHEL/Alma 9; releases are smoke-tested on `node:22-bookworm-slim` and `ubuntu:22.04`.

## Embedding from Rust

Rust embedders depend on the `ai-hist` crate on crates.io (`SessionStore::open` / `sync` / `sessions` / `session` / `changes_since`) on its default features — no raw database connection, no feature flags. Read [docs/sourcing-sdk.md](docs/sourcing-sdk.md), the embedder guide, alongside [crates/ai-hist/README.md](crates/ai-hist/README.md), and start from [examples/rust-consumer](examples/rust-consumer), a standalone Cargo project that stages the fixture corpus into a throwaway `HOME`, syncs, and prints per-session usage totals by model. CI builds that example against the workspace crate on every pull request and against the published crate nightly, and diffs the crate's public API against `crates/ai-hist/public-api.txt`.

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

`<harness>` is the session's source — `claude`, `codex`, `cursor`, `grok`, `muse`, `opencode`, or `devin` — and is required alongside the ID, because session IDs collide across providers. `ai-hist sessions list` prints both.

`ai-hist resume` prints a native resume command for Claude Code, Codex, Cursor, Grok, and Muse Code sessions. OpenCode and Devin CLI sessions are searchable and packable, but have no native resume command to print, so use `ai-hist pack` to carry that context forward instead.

OpenCode is read from whichever of its two stores the machine has: the SQLite
`opencode.db` that current releases write (`OPENCODE_DB`), or the older JSON
tree under `storage/` (`OPENCODE_STORAGE_DIR`). Either way you get the full
turn — text, tool calls and their results, file edits, per-message token
counts, the provider-qualified model, why the turn stopped, compaction
boundaries, and the link from a subagent session to the session that spawned
it.

## MCP

```sh
npx -y ai-hist-mcp
```

Exposes `search_history`, `list_sessions`, `get_session_events`, `get_session_tool_calls`, `get_session_file_edits`, `get_session_tree`, `history_stats`, and more as MCP tools. `list_relay_agents` lists live Agent Relay participants (not history) through the local desktop app's Unix socket. `relay_status` reports whether the local session hosting the MCP server is reachable; `join_relay` makes that session reachable by teammates and agents, with optional public `name` and `description`; `leave_relay` removes it again. Wire it into any MCP-capable agent so it can query its own history and participate on the relay mid-session.

Workspace handoffs need no installed receiver skill. `create_handoff(intent)`
returns one pointer whose existing `intent` field is itself the continuation
prompt: it tells the receiving agent to call
`resume_handoff(source=..., session_id=...)` through the ai-hist MCP and then
continue the original intent. Send that exact `intent` as the Agent Relay DM
text and the full pointer as `kind="handoff"` metadata; do not add a second text
field or inline the transcript. Before sending, call `resume_handoff` once with
the pointer to verify that Agent Relay desktop has uploaded the session to the
current workspace.

The MCP server reads local history only, plus any source plugins named by `AI_HIST_PLUGIN_CONFIG`. The relay roster, status, join, and leave tools are also local: they use `AGENT_RELAY_SOCKET`, then `~/.agentworkforce/desktop/relay-socket`, then the platform socket defaults. They never load cloud credentials. If the desktop app is absent, the tools return a non-fatal instruction to open it or use the Agent Relay MCP. A connected desktop gets a 15-second local response deadline. Read-only roster and status calls that exceed it report `timeout` instead of claiming the app is absent. A sent `join_relay` or `leave_relay` mutation reports `indeterminate_result` because the desktop may have completed it, and directs the caller to `relay_status`. If the app setting disallows self-registration, `join_relay` returns the desktop's readable `not_allowed` guidance.

## Local and remote history

Cached reads such as `search`, `recent`, `sessions list`, `resume`, `pack`, and `stats`,
and acquisition commands such as discovery, hydration, and sync, take a location scope: `--local` (the default), `--remote`, or `--all`.

```sh
ai-hist sessions discover --remote --config history.json  # explicitly installed source plugins
ai-hist sync --all --config history.json                  # local history plus selected plugins
ai-hist search "auth rewrite" --all    # search both at once
```

Commands that address one session by identity do not take a scope, and reject one rather than guessing — they already name a single session. They split by how they take that identity:

```sh
ai-hist sessions tree SOURCE SESSION_ID        # also relationships, tools, edits, markers, usage
ai-hist session SESSION_ID [--source SOURCE]   # session and events take the id alone
ai-hist events SESSION_ID [--source SOURCE]    # --source only narrows a reused id
```

`sessions tree`, `sessions relationships`, `sessions tools`, `sessions edits`, `sessions markers` and `sessions usage` require both positionals and fail without `SOURCE`. `session` and `events` take `SESSION_ID` on its own and reject a `SOURCE` positional; pass `--source` only to disambiguate an id two harnesses happen to share. (`sessions hydrate` also takes `SOURCE SESSION_ID`, but it is an acquisition command and does accept a scope.)

Install `@relayhistory/provider-sources` and configure it explicitly for
remote provider acquisition. Its connectors reuse sign-ins you already have: `claude-web` lists your claude.ai/code sessions from the Claude Code CLI's stored OAuth token, and `codex-cloud` lists Codex cloud tasks through `codex cloud list --json`. With no connector configured, `--remote` fails loudly rather than silently falling back to local. See [remote connectors](docs/source-plugins.md).

`ai-hist export --selection FILE` writes a selected slice of history as NDJSON for your own tooling. See [export](docs/export.md).

Team uploads come from the [Agent Relay desktop app](https://agentrelay.com), not from this repository.

## Why `ai-hist`

- **Every harness, one search.** Claude Code, Codex, Cursor, Grok, Muse Code, OpenCode, Devin CLI — indexed side-by-side. No per-harness silo.
- **Provider-aware evidence.** Prompts, tool calls, and edits are preserved as raw evidence, not summarized away — as much of it as each harness actually exposes. Hydration reports `full`, `partial`, or `shallow_only` per session, so you can tell thin coverage from a thing that never happened. Where a harness records less than the others, the gap is named. Cursor transcripts carry the assistant's prose and every tool call, but no tool output, model id, token usage or timestamp field — those are reported as unavailable, and a turn whose injected `<timestamp>` tag cannot be read is stamped from the file mtime with `CURSOR_TIMESTAMP_FROM_MTIME`. Older Grok builds log no per-turn billing tokens, so their only token fact is a context-window proxy, and hydration says so; newer builds' per-turn `usage` breakdown is recorded beside it, never added to it. The per-field detail is in [the session catalog](docs/session-catalog.md).
- **Local by default.** SQLite on your machine. Export requires an explicit selection; remote acquisition requires an installed source plugin.
- **Handoff-native.** `pack` and `resume` are first-class commands, not afterthoughts.
- **MCP-native.** Your agent queries its own memory the same way you do.

---

Docs: [getting started](docs/getting-started.md) · [architecture](docs/architecture.md) · [source plugins](docs/source-plugins.md) · [export](docs/export.md) · [migration](docs/native-sdk-migration.md)
