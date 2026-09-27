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
with `all_sources: true`; evidence kinds must still be explicit. Incognito and
excluded sessions are filtered by core. Presence records carry observed location
provenance. Source/session identity, stable record/revision IDs, original fields,
timestamps, and raw evidence strings are preserved.

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

The iterator captures a bounded historical snapshot, not a live change feed.
It closes the snapshot when finished or stopped. For resumable paging, use
`beginHistoryExport`, `readHistoryExportPage`, and `closeHistoryExport`. Opaque
cursors can be replayed until the snapshot expires (one hour by default).
Abandoned snapshots expire.
