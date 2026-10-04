# @relayhistory/uploader

`relayhistory-upload` sends the sessions you select from a machine's local RelayHistory
store (`ai-history.db`) to a History service — a self-hosted server
([`docs/self-hosting.md`](../../docs/self-hosting.md)) or any deployment of the same
engine. It is separate from `ai-hist`: local history keeps working with no network and
no credentials, and nothing leaves the machine until you configure and run this.

It reads the store only through the public `ai-hist` SDK change feed
(`getChangesPage`/`commitChanges`), and names every record exactly as
`ai-hist export` does.

## Configure

```json
{
  "endpoint": "https://history.example.com",
  "tokenFile": "laptop-token.json",
  "dbPath": "/home/me/.ai-history/ai-history.db",
  "instanceId": "laptop",
  "selection": {
    "all_sources": false,
    "sources": [],
    "sessions": [{ "source": "claude", "session_id": "0b6c…" }],
    "kinds": [
      "session",
      "session_event",
      "tool_call",
      "file_edit",
      "session_marker",
      "relationship",
      "history",
      "presence",
      "commit_link"
    ],
    "excluded_sessions": []
  }
}
```

| Field        | Meaning                                                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`   | Service base URL. `https://` is required except for loopback; `allowInsecureHttp: true` permits plain HTTP on a trusted network. |
| `tokenFile`  | The file `relayhistory-server token create` wrote (needs `rth:sync`). Must be mode `0600`.                                       |
| `dbPath`     | The local store. Defaults to the SDK's default path.                                                                             |
| `selection`  | The export selection: sessions and whole sources form a union; `excluded_sessions` always wins; `kinds` are explicit.            |
| `instanceId` | A non-secret label for this machine. Defaults to `default`.                                                                      |
| `limits`     | Optional `maxRecords`/`maxBytes` per batch; the server's advertised limits also apply.                                           |

Relative paths resolve against the config file.

## Run

```bash
# From a clone of this repository (the uploader installs ai-hist from npm):
cd packages/uploader && npm ci && npm run build && npm link
relayhistory-upload check --config upload.json  # token, account and server limits
relayhistory-upload run --config upload.json --dry-run   # what would be sent; nothing leaves
relayhistory-upload run --config upload.json             # upload until drained, print a summary
relayhistory-upload run --config upload.json --sync --watch --interval 300
```

`--sync` captures local sessions (`ai-hist sync`) before each upload. `--watch` keeps
running; a retryable failure waits for the next round, anything else exits.

Exit codes: `0` done, including `--watch` stopped by a signal between rounds; `1`
retryable (server unreachable, rate limited, or a signal during an upload); `2` needs you
(configuration, token, permission, account mismatch, or data the server refuses).

## Guarantees

- **Receipt before cursor.** The cursor is a named consumer inside the local store. A
  page moves it only after every selected record on the page has a durable receipt for
  exactly the batch sent — matching batch id, every revision accepted, nothing
  unsupported. A crash or a lost response leaves the cursor where it was; the next run
  resends the same batch identities and the server replays its receipts.
- **Isolation.** The cursor is per endpoint, account and selection. Another endpoint or
  account never moves it. Changing the selection starts a new cursor from the beginning
  of the local feed, so newly selected history is backfilled and already-sent records
  are acknowledged as replays.
- **Pinned identities.** Batches name the token file's `accountId`; the server refuses
  a token from another tenant (`delivery_account_mismatch`) and the run stops. Records
  carry the store's change-feed epoch as their origin; a store replaced mid-run stops
  the run.
- **Conflicts.** A typed `409 delivery_conflict` skips only the records the server
  proves it holds with different content at that revision, using the SDK's recovery
  helper, and delivers the rest.
- **Deletions.** A local deletion is forwarded as a tombstone only for a session the
  selection names (or a whole selected source). A relationship's deletion, which does
  not carry its child, is held back while its source has excluded sessions. Removing a
  session from the selection, or excluding it, stops future uploads; it never deletes
  what the server already holds.
- **Retries.** Transient failures and `429` back off exponentially with jitter, honoring
  `Retry-After`.
- **Secrets.** The token is sent only in the `Authorization` header and never logged.
  No file name is derived from it.

Logs are JSON lines on stderr; the run summary is one JSON line on stdout.

## Verify

`scripts/e2e.mjs` builds two machines' real local stores through the SDK, uploads their
selections to the real self-hosted server and checks recall, incremental upload,
lost-response replay, account mismatch, per-account cursors, restart persistence,
selection backfill and that no token appears in any output.

```bash
node scripts/e2e.mjs --admin-url postgres://postgres@127.0.0.1:5432/postgres
```
