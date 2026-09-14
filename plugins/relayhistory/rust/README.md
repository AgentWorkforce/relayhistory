# Optional RelayHistory Rust integration

This standalone Cargo workspace owns RelayHistory authentication, legacy convergence/turn mapping, replay, sharing and the `relayhistory-plugin` helper. The local workspace does not list this package as a member or dependency. Local readers in `ai-hist-core::storage` provide typed scans; this package contains no production SQL queries.

Build with `cargo build --manifest-path plugins/relayhistory/rust/Cargo.toml --release`. The SDK plugin distributes the resulting helper separately from the local native addon. An explicit `RELAYHISTORY_PLUGIN_BIN` override can select a development build.

## JSON bridge version 1

With no command-line arguments the helper reads one JSON request from stdin, writes one JSON response to stdout and exits. Arguments use the SDK's camelCase names:

```json
{"version":1,"operation":"cloudLoadAuth","args":{"baseUrl":"https://history.agentrelay.com"}}
```

Success: `{"version":1,"ok":true,"value":null}`. Operation failure: `{"version":1,"ok":false,"error":{"code":"CLOUD_AUTH_FAILED","message":"..."}}`. Operation failures exit zero so the host can decode the typed error; malformed/oversized requests or output failures exit nonzero with a fixed diagnostic. Unknown versions and operations are rejected. Errors never serialize raw response bodies or input credentials. Explicit token/auth operations return credentials as their successful result, so hosts must not log those results.

Operations: `accessToken`, `syncAndPush` (explicit legacy Agent Relay hook), `cloudLoadAuth`, `cloudResolveSession` (requires `now` in Unix milliseconds), `cloudRefreshSession`, `cloudValidateExchangeBaseUrl`, `cloudLogin`, `enableCloud`, `pushCloud`, `replay`, and `createShareableTrace`. Login/push arguments are flattened (no nested `options`). Replay accepts `sessionId`, `baseUrl`, `limit`, `maxContent`, `json`, `out`. Share returns the serialized response string matching the former native method. Auth results use `baseUrl`, `accessToken`, `accessTokenExpiresAt`, `refreshToken`, `orgId`, `workspaceId`.

Input is capped at 16 MiB, output at 32 MiB, and process execution at 300 seconds. Hosts should enforce their own shorter operation deadline and terminate cancelled helpers. Large replay output can use the atomic `out` file option. Interactive Agent Relay login output goes to stderr so stdout remains framed JSON.

## Compatibility and migration

Explicit legacy subcommands `login`, `admin-mint`, `token`, `replay`, `push`, `coverage`, and `pair` remain on this optional binary. Manual legacy push uses the unchanged stage-scoped authentication and cursor files. Existing positional turns and convergence endpoints provide their original guarantees; they are not the generic durable delivery protocol. `enableCloud` and `pushCloud` also remain legacy compatibility operations.

`push --install-service` and `push --uninstall-service` fail with a migration instruction. Before enabling a new durable worker, the optional SDK plugin must detect existing `com.ai-hist.push` launchd jobs and managed `# ai-hist push (managed)` cron entries, then require explicitly stopping/replacing them. Do not alter live services merely by loading a plugin. Preserve existing auth and legacy cursor files unchanged. The new destination job requires an explicit source/selection and persisted exclusions; prior per-run `--incognito` filters cannot be inferred. New delivery progress must never reinterpret or reset legacy cursors.

## Durable protocol transport

`deliveryAccount` returns the expected account assertion `relayhistory:` plus SHA-256 of UTF-8 JSON `[orgId, workspaceId ?? ""]`. It reads only explicit plugin auth state. The server compares the immutable batch account assertion to authenticated tenancy before writing; cache labels cannot grant authority.

`deliveryPrepare` takes `args.batch` and returns a core `PreparedPayload` using mapping version `relayhistory-delivery-v1`. Persist this exact result with the coordinator before transport. `deliverySend` accepts `args.baseUrl` and `args.prepared`, checks the hash/mapping and sends the body unchanged to `/v1/delivery/batches`. Authentication headers may rotate. It returns the core snake_case `DeliveryAcknowledgment`; it does not mutate local delivery progress. The host must validate its lease/eligibility immediately before calling, then pass the exact acknowledgment to the core. The service stores its documented scrubbed/minimized representation; durable acceptance does not promise indexing or retaining raw secrets.

Safe helper failures use `DELIVERY_TRANSIENT`, `DELIVERY_RATE_LIMITED`, `DELIVERY_AUTHENTICATION_REQUIRED`, `DELIVERY_PERMISSION_DENIED`, `DELIVERY_INVALID_PAYLOAD`, `DELIVERY_UNSUPPORTED_EVIDENCE`, or `DELIVERY_MAPPING_VERSION_MISMATCH`. Only transient/rate-limit errors are automatic retries. HTTP404 blocks rather than falling back to positional turns or last-write-wins ingest. Preparation rejects more than 100 records, matching the server; prepared bodies are capped at 2 MiB; HTTP deadlines are 30 seconds; redirects are disabled. The core's immutable generic batch remains available if an explicit limit needs changing.

`deliveryRead` takes `args.baseUrl` and `args.readOptions` (`expectedAccount`, optional `kind`, `source`, `sessionId`, `cursor`, `includeDeleted`, `limit` from 1–100). It sends the expected-account header and returns `{protocolVersion:1,listing:"live",records,nextCursor}`. This is a live keyset listing of current retained records. Refreshes must restart from the beginning; retaining its cursor as an incremental watermark would miss revisions of earlier record IDs. A source adapter must not advertise snapshot/change-feed capability for this endpoint.

The JSON fixture in `tests/fixtures/delivery-native-v1.json` is shared with the companion server tests and was generated through the local native delivery coordinator. The client pins its own mapping version when preparing it. Deploying the new server protocol is a release prerequisite for enabling this destination; this change itself performs no deployment or live service migration.

### Legacy scheduler migration guard

`deliveryMigrationStatus` takes no arguments and returns
`{state:"clear"|"active"|"unknown",jobs:string[]}`. Job labels are fixed safe names;
scheduler commands, credentials, and history are never returned. On macOS it
checks both `~/Library/LaunchAgents/com.ai-hist.push.plist` and the loaded
`com.ai-hist.push` launchd label. On macOS/Linux it checks uncommented user cron
entries bearing `# ai-hist push (managed)`. An unloaded plist still blocks because
it can restart at login. Command failures, timeouts, and oversized output produce `unknown`;
positive evidence takes precedence as
`active`. Each scheduler command has a two-second deadline and a 1 MiB output cap.

The SDK must inspect status before enabling a new durable job and before prepare
or send. `active` always blocks. `unknown` requires an explicit user acknowledgment
that inspection was unavailable and they have checked/stopped legacy scheduling;
that acknowledgment must never be reported as a successful inspection. Platforms
other than macOS/Linux return `clear` with `legacy-installer-unsupported` because
the old automatic installer never supported them.

Stop and remove an active old managed job explicitly using the previous
installation or the platform scheduler, then rerun this check. This operation
never stops a service, changes scheduler files, reads auth, migrates watermarks,
or modifies existing auth/cursor files. New jobs require an explicit selection
and generation; legacy cursors are never interpreted as generic delivery
checkpoints. Arbitrary manually named schedules cannot be discovered by this
check and must be stopped by their owner. A status check cannot revoke a request
that another process has already sent.

### Explicit legacy source operations

`source::CloudProvider` implements the public local-history source interface for
legacy catalog recall. Its account/stage-derived connector instance and locators
are preserved. The helper operation `discover` requires `connectorId:"cloud"`,
accepts `baseUrl`, `source`, `limit` (at most 10000), and optional
`connectorInstance` (the account-derived fingerprint to assert), and returns
`{observations:[ShallowSession]}`. It reads only the explicitly selected stage's
auth. This is catalog-only; the new durable record readback source is separate.

`relaycast::sync_relaycast(connection,state)` preserves the old Relaycast
incremental history importer, using public core insert APIs. The helper operation
`relaycastSync` requires `connectorId:"relaycast"`, `dbPath`, and the previous
`state` object, and returns `{inserted,state,capability:"legacy-incremental-history"}`.
Only this explicit invocation reads `RELAYCAST_API_KEY`,
`RELAYCAST_WORKSPACE_ID`, and `RELAYCAST_BASE_URL`. The caller must persist the
returned state after successful local ingestion; retrying an earlier state keeps
history inserts idempotent. Existing `state.relay` channel/DM high-water marks
retain their original meaning and must not be relabeled as generic delivery
checkpoints. Reuse the old cursor map when migrating an installation.

This legacy importer can retain channel history when DM listing is forbidden and
is not a complete normalized source snapshot. It does not advertise `covered_kinds`
or participate in automatic discovery. New full snapshot support requires a
separately verified Relaycast traversal/permission contract. The local packages
contain no Relaycast transport or implicit credentials-driven acquisition.
