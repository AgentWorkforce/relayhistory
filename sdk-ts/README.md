# ai-hist (TypeScript SDK)

Thin TypeScript SDK for the [ai-hist](../README.md) SQLite database. Reads the same file the Python `ai-hist sync` tool writes — this package never writes, sync stays the Python tool's job.

Built to let Electron / Node consumers (e.g. the `pear` desktop app) render a Codex/Claude-style "previous conversations" list backed by your local ai-hist history.

## Install

```bash
npm install ai-hist
```

`better-sqlite3` is a native dep — when bundling into an Electron app, run `electron-rebuild` (or your bundler's equivalent) after install.

## Quick start

```ts
import { AiHist, resumeCommand } from 'ai-hist';

const hist = new AiHist();              // uses $AI_HIST_DB or ~/.local/share/ai-hist/ai-history.db
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

## API

```ts
new AiHist(opts?: { dbPath?: string })       // readonly open
hist.close()
hist.dbPath: string

hist.recent(opts?): HistoryEntry[]            // newest prompts first
hist.listSessions(opts?): SessionSummary[]    // grouped by session_id, last-activity DESC
hist.getSession(sessionId): HistoryEntry[]    // all prompts in a session, oldest first
hist.search(query, opts?): HistoryEntry[]     // FTS5 query, recent matches first
hist.stats(): Stats                           // counts + date range
```

All list-style methods accept `{ source?, project?, limit?, beforeMs? }`. `beforeMs` is the cursor for paginating older results.

```ts
resumeCommand(entry): string | null           // shell command per source; null for relay
defaultDbPath(): string                       // resolve env / OS default
```

## Schema (canonical, owned by the Python tool)

```sql
CREATE TABLE history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,         -- 'claude' | 'codex' | 'cursor' | 'relay'
  session_id TEXT,
  project TEXT,
  prompt TEXT NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  UNIQUE(source, timestamp_ms, prompt)
);

CREATE VIRTUAL TABLE history_fts USING fts5(prompt, project, content='history', content_rowid='id');
```

If the Python sync tool's schema changes, bump this SDK's major version. Consumers pin the SDK version they were built against.

## License

MIT
