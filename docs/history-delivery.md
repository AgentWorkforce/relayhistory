# Export and durable history delivery

History can stay local, be exported as NDJSON, or be delivered to an explicitly
enabled destination. Export and delivery share versioned Rust evidence records.
Delivery adds a durable queue, immutable mapped payloads, acknowledgments, retry
times, and fenced worker leases. Native contract 13 is required.

## Export a snapshot

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
Snapshot retention uses the same bounded storage budget as delivery. A worker
expires abandoned snapshots during maintenance.

## Implement a destination

A destination is ordinary trusted JavaScript, with no native rebuild or SQL.
It declares its mapping version, supported evidence, deletion capability, and
idempotency guarantee. `prepare` maps a generic batch to a body; core persists
that exact body before `send` can run. Retries reuse the stored bytes, batch ID,
and revision IDs. Credentials belong in transient transport headers, never in
the prepared body or durable job config.

```ts
import { HistoryPluginRegistry, HistoryDeliveryError } from 'ai-hist';

const registry = new HistoryPluginRegistry();
registry.register({ destinations: [{
  instanceId: 'archive',
  destination: {
    id: 'example', mappingVersion: '1', idempotency: 'revision',
    orderedRevisions: true, supportedKinds: ['history'], supportsTombstones: false,
    async prepare(batch) {
      return { content_type: 'application/json', body: JSON.stringify(batch.records) };
    },
    async send(payload, { batch, signal, idempotencyKey }) {
      // Authenticate only for this explicit operation. Verify the authenticated
      // remote account equals batch.account_id before writing. Honor signal.
      const result = await uploadExactBody(payload.body, { signal, idempotencyKey });
      if (result.needsLogin) throw new HistoryDeliveryError('authentication_required');
      return {
        batch_id: batch.batch_id,
        accepted_revision_ids: result.durablyAcceptedRevisionIds,
        unsupported_revision_ids: [],
        acceptance_level: 'durable',
      };
    },
  },
}] });
```

The receiver must deduplicate revision IDs and reject stale revisions, or apply
revisions in order. Declare `idempotency: 'none'` if it cannot deduplicate: it
may observe duplicates after lost responses. Delivery is at least once, not
exactly once. Acknowledgments must list exact revision IDs. Partial acceptance
retains the entire batch for safe retry; unsupported evidence blocks delivery.
An asynchronous receipt is insufficient until the destination confirms durable
acceptance. `indexed` is available only when remote indexing is actually confirmed.

`HistoryDeliveryError` classifies transient/rate-limit failures and permanent
auth/permission/schema problems. `deliveryRetryAfter` converts an HTTP
`Retry-After` value to its absolute timestamp for that error's second argument.
Core owns persisted exponential backoff and jitter. Arbitrary plugin exception
text is not written to progress output. Unattended workers never start login.

## Enable and operate a job

Job configuration includes non-secret destination, instance, remote account, and
mapping identities. It also includes the explicit selection and bounded limits.
`createHistoryDelivery(config)` enables capture but performs no network I/O.
`drainHistoryDelivery(registry)` runs a bounded one-shot drain;
`runHistoryDelivery(registry, { signal })` repeatedly uses that same path.

For CLI use, configure only the installed modules to load. Each module exports
`createHistoryPlugin(options)`, returning a `HistoryPlugin`. The factory must be
inert; loading a module is not permission to log in or upload. Package names are
resolved from the config directory, and relative module paths are supported.

```json
{
  "plugins": [{ "module": "your-history-destination", "options": { "instanceId": "archive" } }],
  "job": {
    "destination_id": "example",
    "instance_id": "archive",
    "account_id": "account-label",
    "mapping_version": "1",
    "selection": {
      "all_sources": false, "sources": ["claude"], "sessions": [],
      "kinds": ["history"], "excluded_sessions": []
    },
    "limits": {
      "max_batch_records": 100, "max_batch_bytes": 1048576,
      "max_scan_records": 400, "max_prepared_bytes": 2097152
    }
  }
}
```

```bash
ai-hist delivery enable --config delivery.json
ai-hist delivery drain --config delivery.json
ai-hist delivery run --config delivery.json
ai-hist delivery status
ai-hist delivery pause --job JOB_ID
ai-hist delivery resume --job JOB_ID
ai-hist delivery retry --job JOB_ID
```

The worker delivers revisions committed to the local history database. Keep
the local indexer running or invoke `ai-hist sync` to ingest changes from provider
files. Ingestion records enabled delivery work transactionally; it never performs
uploads itself. The delivery worker does not scan provider files or install an
OS service. Run it under an existing process supervisor if desired.

Jobs, queue bodies, and retry timestamps survive process termination and sleep.
Workers renew leases and stop claiming work on SIGINT/SIGTERM. Plugins must honor
their abort signals; arbitrary plugin code is not a sandbox. A request already
in flight cannot be revoked by a lease fence, so receiver idempotency remains
necessary. Pausing keeps capture and pending work. `delivery cancel --job JOB_ID`
explicitly discards pending work; it does not delete remote records.

Status distinguishes local capture, queued records/bytes, remote acknowledgment,
suppressed work, and blocked jobs. Core rechecks eligibility immediately before
transport; already accepted history is not remotely erased by a local privacy
change. Deletes are tombstones and require receiver support. Changing a job's
selection, mapping version, or account requires an explicit new generation after
cancellation; existing progress is never reused for a broader selection.

Maintenance removes bounded batches of consumed journal rows, completed receipts,
and expired snapshots. `historyDeliveryRetention` reports used/cap bytes;
`compactHistoryDelivery` runs explicit maintenance and
`setHistoryDeliveryRetention(bytes)` raises the cap when needed. A full budget
fails capture visibly with a retention-limit error; it never silently drops
history. Long offline periods therefore require enough retained storage.

## CLI and MCP extensions

Configured plugins can register commands and tools. Duplicate identifiers,
including collisions with core names, fail atomically. Run a configured command
with `ai-hist plugin COMMAND --config delivery.json -- ARGS`. MCP loads plugins
only when `AI_HIST_PLUGIN_CONFIG` explicitly names that config; core tools work
without it. Plugin tools receive their object arguments through an `input` field.
MCP also exposes delivery status, pause, resume, and retry for existing jobs.

This change supplies generic destination contracts and the durable coordinator.
The existing RelayHistory cloud API still ships in the current distribution.
Optional cloud package extraction and migration of its adapter onto this
coordinator are separate changes; no third-party service compatibility is implied.

Plugin arguments after `--` are passed verbatim, including flags, `-h`, equals signs,
and empty arguments. Core options before that separator remain strictly validated.

Arbitrary plugin MCP callbacks default to non-idempotent, potentially destructive,
open-world annotations. The host does not infer safety from registration. Export
file targets are checked against the active database through existing ancestor
symlinks and again before the final rename.
