# ai-hist

Sync and search your [Claude Code](https://docs.anthropic.com/en/docs/claude-code), [Codex CLI](https://github.com/openai/codex), [Cursor](https://cursor.com), [Agent Relay](https://github.com/AgentWorkforce/relay), and compacted persona trajectory history into a local SQLite database with full-text search.

**Zero dependencies** — Python 3.8+ standard library only. Single file.

## Install

```bash
mkdir -p ~/.local/bin
curl -o ~/.local/bin/ai-hist https://raw.githubusercontent.com/khaliqgant/ai-hist/main/ai-hist
chmod +x ~/.local/bin/ai-hist
```

Or clone and symlink:

```bash
git clone https://github.com/khaliqgant/ai-hist.git
mkdir -p ~/.local/bin
ln -s "$(pwd)/ai-hist/ai-hist" ~/.local/bin/ai-hist
```

Make sure `~/.local/bin` is in your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"  # add to .zshrc / .bashrc
```

## Usage

```bash
# Import all history (incremental — only reads new bytes on re-run)
ai-hist sync

# Full-text search
ai-hist search "authentication bug"
ai-hist search "refactor" --source claude --limit 10
ai-hist search "deploy" --source relay
ai-hist search "retry policy" --source trajectory
ai-hist search "deploy" --project relay

# Recent prompts
ai-hist recent                             # last 20
ai-hist recent 50                          # last 50
ai-hist recent --source claude --project my-app

# Drill into a specific entry (shows full prompt + metadata + resume command)
ai-hist show 4521

# See surrounding context (same session + nearby entries)
ai-hist context 4521
ai-hist context 4521 --window 15   # ±15 min window (default: 5)

# View all prompts in a session
ai-hist session abc-1234-def
ai-hist session abc-1234-def --full   # no truncation

# Resume a conversation directly (the exact command is shown by `ai-hist show <id>`)
cd /path/to/project && claude --resume <session_id>          # claude
codex --resume <session_id>                                   # codex
cd /path/to/project && cursor-agent --resume=<session_id>    # cursor

# Stats overview
ai-hist stats
```

Search results include entry IDs (`#NNN`) — use them to drill deeper:

```
ai-hist search "deploy" → find #4521
ai-hist show 4521       → see full prompt, session info, resume command
ai-hist context 4521    → see what else was happening in that session + nearby
ai-hist session <id>    → browse the full conversation
```

Example output from `ai-hist stats`:

```
Total entries: 47,665

By source:
  claude: 37,406
  codex: 10,259

Date range:
  2025-10-05 to 2026-03-08

Top 10 projects:
   8,701  /Users/you/Projects/my-app
   4,586  /Users/you/Projects/api-server
   ...
```

## How it works

ai-hist supports five sources:

| Source | How | Key fields |
|--------|-----|------------|
| Claude Code | Local JSONL (`~/.claude/history.jsonl`) | `display`, `timestamp`, `project`, `sessionId` |
| Codex CLI | Local JSONL (`~/.codex/history.jsonl`) | `text`, `ts`, `session_id` |
| Cursor | Per-session JSONL (`~/.cursor/projects/<encoded-path>/agent-transcripts/<uuid>/<uuid>.jsonl`) | `role`, `message.content[].text` (user prompts wrapped in `<user_query>...`) |
| [Agent Relay](https://github.com/AgentWorkforce/relay) | API (`https://api.relaycast.dev/v1`) | `sender`, `content`, `channel`, `timestamp` |
| Trajectories | Compacted per-run JSON (`$TRAJECTORY_ROOT/**/compacted/*.json`) | `personaId`, `projectId`, `task`, `decisions`, `retrospective` |

**Claude Code, Codex & Cursor** are synced from local JSONL files incrementally (byte-offset tracking in `.sync-state.json`). Cursor lines have no per-line timestamp, so the file mtime at sync time is used.

**Agent Relay** is synced via the [Relaycast API](https://github.com/AgentWorkforce/relaycast), pulling workspace messages with cursor-based pagination. Configure with:

```bash
export RELAYCAST_API_KEY="rk_live_..."
export RELAYCAST_WORKSPACE_ID="ws_abc123"
```

**Trajectories** are synced from compacted per-run JSON files. Configure an explicit root with:

```bash
export TRAJECTORY_ROOT="/path/to/repo/.trajectories"
```

ai-hist scans `$TRAJECTORY_ROOT/**/compacted/*.json`. Without `TRAJECTORY_ROOT`, it discovers `~/Projects/**/.trajectories/**/compacted/*.json`.

The runtime contract is one JSON file per completed run:

```json
{
  "id": "run-id",
  "version": 1,
  "personaId": "planner",
  "projectId": "agent-workforce",
  "task": { "title": "Task title", "description": "Task description" },
  "status": "completed",
  "startedAt": "2026-06-06T10:00:00.000Z",
  "completedAt": "2026-06-06T10:05:00.000Z",
  "decisions": [
    {
      "question": "What should we do?",
      "chosen": "Chosen option",
      "reasoning": "Why this option won",
      "alternatives": ["Other option"]
    }
  ],
  "retrospective": {
    "summary": "What happened",
    "approach": "How the work was done",
    "learnings": ["What to carry forward"],
    "confidence": 0.8
  }
}
```

Aggregate `trail compact` artifacts are intentionally not the ai-hist interface; ai-hist indexes the runtime-emitted per-run contract files.

All sources are indexed with [FTS5](https://www.sqlite.org/fts5.html) full-text search. Deduplication uses `INSERT OR IGNORE` on a `UNIQUE(source, timestamp_ms, prompt)` constraint.

## Database location

Default: `~/.local/share/ai-hist/ai-history.db`

Override with the `AI_HIST_DB` environment variable:

```bash
export AI_HIST_DB="$HOME/Dropbox/ai-history/ai-history.db"
```

## MCP server

The TypeScript package exposes a stdio MCP server that wraps the SDK and serves both HOW history and WHY trajectories:

```bash
npx -y ai-hist-mcp
```

Tools include `search_history`, `recent_entries`, `get_session`, `get_context`, `stats`, `search_trajectories`, and `why_for_task`.

## Continuous sync (macOS)

Create a launchd plist to sync every 60 seconds:

```bash
cat > ~/Library/LaunchAgents/com.ai-hist.sync.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ai-hist.sync</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>${HOME}/.local/bin/ai-hist</string>
        <string>sync</string>
    </array>
    <key>StartInterval</key>
    <integer>60</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/ai-hist-sync.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/ai-hist-sync.err</string>
</dict>
</plist>
EOF

launchctl load ~/Library/LaunchAgents/com.ai-hist.sync.plist
```

> Replace `/usr/bin/python3` with your Python path if needed (e.g., from `which python3`).

### Linux (cron)

```bash
# Sync every minute
echo "* * * * * python3 ~/.local/bin/ai-hist sync >> /tmp/ai-hist-sync.log 2>&1" | crontab -
```

### Alternative: watch mode

```bash
ai-hist watch              # syncs every 60s
ai-hist watch --interval 30  # syncs every 30s
```

## Schema

```sql
CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,          -- 'claude', 'codex', 'cursor', 'relay', or 'trajectory'
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    UNIQUE(source, timestamp_ms, prompt)
);

-- FTS5 full-text search index
CREATE VIRTUAL TABLE history_fts USING fts5(prompt, project, content='history', content_rowid='id');
```

Trajectory sync also maintains a structured `trajectories` table for decisions and retrospectives, while inserting a searchable `source='trajectory'` row into `history`.

You can query the database directly with any SQLite client:

```bash
sqlite3 ~/.local/share/ai-hist/ai-history.db "SELECT COUNT(*) FROM history"
```

## License

MIT
