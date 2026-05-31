# ai-hist (TypeScript SDK and MCP server)

TypeScript reader for the [ai-hist](../README.md) history database, plus an MCP server for searching local AI coding-agent history from Claude Code, Codex, Cursor, and Agent Relay.

The SDK uses `sql.js`, so it has no native build step. It reads the same SQLite file the Python `ai-hist sync` tool writes, or falls back to scanning local Claude/Codex/Cursor JSONL files when the database is missing.

## Install

```bash
npm install ai-hist
```

## Quick start

```ts
import { openAiHist, resumeCommand } from 'ai-hist';

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

```bash
npx ai-hist-mcp
```

The MCP server exposes tools for search, recent history, session lookup, temporal context, evidence packing, and stats. It uses the same data-opening behavior as the SDK: SQLite first, then local JSONL fallback.

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
hist.stats(): Stats                           // counts + date range
```

All list-style methods accept `{ source?, project?, limit?, beforeMs? }`. `beforeMs` is the cursor for paginating older results.

```ts
resumeCommand(entry): string | null           // shell command per source; null for relay
defaultDbPath(): string                       // resolve env / OS default
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

The SDK reads the table columns directly. In JSONL fallback mode it creates an in-memory table with the columns it needs for the public reader API.

## License

MIT
