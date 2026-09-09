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

Exposes `search_history`, `list_sessions`, `get_session_events`, `get_session_tool_calls`, `get_session_file_edits`, `get_session_tree`, `get_session_thread`, `history_stats`, and more as MCP tools. Wire it into any MCP-capable agent so it can query its own history mid-session.

`get_session_thread` is the one cloud-backed tool. Given a `source` and a `session_id` it returns the commits that session shipped plus the pull requests, reviews, incidents, tickets, Slack threads, hotfixes and follow-up sessions linked to it — the *lifecycle* fan-out, complementing `get_session_tree`'s *subagent* fan-out. It fetches on every call and caches nothing, because a thread keeps growing as PRs and incidents land. Optional `kinds`, `since`, `limit` and `cursor` narrow and page the links. Tenancy comes from the stored cloud session's token, never from a parameter.

It reads the native `ai-hist enable-cloud` store first (`RELAYHISTORY_HOME`, else `~/.agentworkforce/relayhistory`) and this SDK's `~/.config/ai-hist/auth.json` second, holding a native session to the same preconditions the `cloud` connector applies to recall. An expired session with a refresh token is rotated once and the new pair merged back over the stored session, so a long-running MCP install does not stop working at the token boundary. Without a usable cloud session it returns `UNSUPPORTED_OPERATION`, names the missing precondition, and makes no request.

## Team + Cloud

The search-style read commands — `search`, `recent`, `sessions list`, `sessions discover`, `sessions hydrate`, `resume`, `pack`, `stats` and `sync` — take a location scope: `--local` (the default), `--remote`, or `--all`.

```sh
ai-hist sessions discover --remote     # pull in sessions your providers keep server-side
ai-hist sync --all                     # ingest local and remote together
ai-hist search "auth rewrite" --all    # search both at once
```

Commands that address one session by identity — `sessions tree`, `sessions relationships`, `sessions tools`, `sessions edits`, `session` and `events` — do not take a scope, and reject one rather than guessing: the `SOURCE` + `SESSION_ID` pair already names exactly one session.

Optional: `ai-hist enable-cloud` authenticates and syncs your sessions to RelayHistory Cloud. Threading commits to a PR is a separate opt-in hook install. See [cloud setup, Git hooks, and sharing](docs/enable-cloud.md).

Remote acquisition runs through connectors that reuse sign-ins you already have: `claude-web` lists your claude.ai/code sessions from the Claude Code CLI's stored OAuth token, and `codex-cloud` lists Codex cloud tasks through `codex cloud list --json`. With no connector configured, `--remote` fails loudly rather than silently falling back to local. See [remote connectors](docs/remote-connectors.md).

Two commands read a session back out of the cloud once `enable-cloud` is set up:

```sh
ai-hist replay <session-id>                 # print a cloud session's events, oldest first
ai-hist replay <session-id> --out log.txt   # write that transcript to a file instead
ai-hist token                               # print a cloud API token for your own tooling
```

`replay` takes `--limit` to cap the number of events and `--max-content` to truncate long ones; `--json` emits the raw event array. Without a stored cloud session it stops and names what is missing rather than printing a partial transcript, and `--out` is written atomically only after the whole fetch succeeds, so an interrupted replay never truncates a transcript you already had.

`ai-hist token` prints a live credential to stdout — treat it like a password, and don't paste its output into a terminal you are sharing or a log.

A hosted layer for sharing sessions across a team — so every PR threads back to the session that produced it — is in progress at `history.agentrelay.com`; its connector is not yet wired into the npm CLI. Want early access, or to self-host it? Reach out at hello@agentrelay.com.

## Why `ai-hist`

- **Every harness, one search.** Claude Code, Codex, Cursor, Grok, OpenCode, Agent Relay — indexed side-by-side. No per-harness silo.
- **Provider-aware evidence.** Prompts, tool calls, and edits are preserved as raw evidence, not summarized away — as much of it as each harness actually exposes. Hydration reports `full`, `partial`, or `shallow_only` per session, so you can tell thin coverage from a thing that never happened.
- **Local by default.** SQLite on your machine. Nothing leaves it unless you opt in to a remote scope.
- **Handoff-native.** `pack` and `resume` are first-class commands, not afterthoughts.
- **MCP-native.** Your agent queries its own memory the same way you do.

---

Docs: [getting started](docs/getting-started.md) · [architecture](docs/architecture.md) · [remote connectors](docs/remote-connectors.md) · [migration](docs/native-sdk-migration.md)
