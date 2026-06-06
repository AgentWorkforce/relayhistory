# ai-hist (TypeScript SDK and MCP server)

TypeScript reader for the [ai-hist](../README.md) history database, plus an MCP server for searching local AI coding-agent history from Claude Code, Codex, Cursor, and Agent Relay.

The SDK uses `sql.js`, so it has no native build step. It reads the same SQLite file the Python `ai-hist sync` tool writes, or falls back to scanning local Claude/Codex/Cursor JSONL files and compacted trajectory JSON when the database is missing.

## Install

```bash
npm install ai-hist-mcp
```

## Quick start

```ts
import { openAiHist, resumeCommand } from 'ai-hist-mcp';

const hist = await openAiHist(); // uses $AI_HIST_DB or ~/.local/share/ai-hist/ai-history.db
try {
  const sessions = hist.listSessions({ limit: 20 });
  for (const s of sessions) {
    console.log(`[${s.source}] ${s.firstPrompt.slice(0, 60)} (${s.promptCount} prompts)`);
    console.log('  resume:', resumeCommand(s));
  }
} finally {
  hist.close();
}
```

To require the Python-managed SQLite database instead of JSONL fallback:

```ts
const hist = await openAiHist({ fallback: 'error' });
```

## MCP server

After the package is published, run the local stdio MCP server with:

```bash
npx -y ai-hist-mcp
```

From a local checkout:

```bash
npm install
npm run build
node dist/mcp-server.js
```

The MCP server exposes tools for search, recent history, session lookup, temporal context, evidence packing, stats, trajectory search, and task WHY lookup over stdio. It runs on the user's machine and uses the same data-opening behavior as the SDK: SQLite first, then local fallback scanning.

Contract tools:

- `search_history(query, limit?)`
- `recent_entries(limit?, project?)`
- `get_session(session_id)`
- `get_context(id)`
- `stats()`
- `search_trajectories(query, limit?)`
- `why_for_task(query)`

## API

```ts
openAiHist(opts?: {
  dbPath?: string;
  fallback?: 'jsonl' | 'error';
}): Promise<AiHist>

hist.close(): void
hist.dbPath: string
hist.sourceKind: 'sqlite' | 'jsonl'

hist.recent(opts?): HistoryEntry[]            // newest prompts first
hist.listSessions(opts?): SessionSummary[]    // grouped by session_id, last activity DESC
hist.getSession(sessionId): HistoryEntry[]    // all prompts in a session, oldest first
hist.getEntry(id): HistoryEntry | null
hist.getInTimeWindow(timestampMs, windowMs): HistoryEntry[]
hist.search(query, opts?): HistoryEntry[]     // literal substring search, recent matches first
hist.searchTrajectories(query, opts?): TrajectoryEntry[]
hist.whyForTask(query): TrajectoryEntry | null
hist.stats(): Stats                           // counts + date range
```

All list-style methods accept `{ source?, project?, limit?, beforeMs? }`. `beforeMs` is the cursor for paginating older results.

```ts
resumeCommand(entry): string | null           // shell command per source; null for relay
defaultDbPath(): string                       // resolve env / OS default
```

## Trajectories

ai-hist indexes compacted per-run trajectory JSON files as the decision WHY. Set `TRAJECTORY_ROOT` to an explicit root; the scanner reads:

```text
$TRAJECTORY_ROOT/**/compacted/*.json
```

Without `TRAJECTORY_ROOT`, default discovery scans:

```text
~/Projects/**/.trajectories/**/compacted/*.json
```

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

## Schema

The canonical schema is owned by the Python tool:

```sql
CREATE TABLE history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  session_id TEXT,
  project TEXT,
  prompt TEXT NOT NULL,
  prompt_hash TEXT,
  timestamp_ms INTEGER NOT NULL,
  UNIQUE(source, timestamp_ms, prompt)
);

CREATE VIRTUAL TABLE history_fts USING fts5(prompt, project, content='history', content_rowid='id');
```

Trajectory sync also creates a structured `trajectories` table and inserts each per-run compact file into `history` with `source='trajectory'`, so general history search and WHY-specific lookup both work.

## License

MIT
