# Export history

History stays local. `ai-hist export` writes a selected slice of it as NDJSON
built from versioned Rust evidence records, for your own programs to consume.

Create an explicit selection file:

```json
{
  "all_sources": false,
  "sources": ["claude", "codex"],
  "sessions": [],
  "kinds": ["history", "session_event", "tool_call", "file_edit", "presence"],
  "excluded_sessions": []
}
```

Sources and individual session identities form a union. Select every source
with `all_sources: true`; evidence kinds must still be explicit. A session in
`excluded_sessions` is left out, and so is a relationship that names it at
either end. Each record is one stored row: `payload` holds every column as
SQLite stores it, `record_id` is the SHA-256 of the change feed's key for the
row, and `revision` is the row's change-feed revision, so an export and the
feed name a record the same way. `origin_id` is the store's change-feed epoch.

```bash
ai-hist export --selection selection.json > history.ndjson
ai-hist export --selection selection.json --out history.ndjson
```

Stdout contains one complete JSON record per line, with errors on stderr. File
output is replaced only after a complete export. A successful pipe means export
completed; it does not mean the downstream consumer accepted the history.

```ts
import { exportHistory } from 'ai-hist';
for await (const record of exportHistory(selection, { dbPath })) {
  await consume(record);
}
```

The iterator reads a snapshot of the rows present when it began, not a live
change feed: a row added later is outside it, a row rewritten meanwhile is
exported as it then stands, and a row deleted before its page is not exported.
It closes the snapshot when finished or stopped. For resumable paging, use
`beginHistoryExport`, `readHistoryExportPage`, and `closeHistoryExport`. Opaque
cursors can be replayed until the snapshot expires (one hour by default).
Abandoned snapshots expire.
