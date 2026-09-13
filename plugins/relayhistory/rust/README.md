# Optional RelayHistory Rust integration

This standalone Cargo workspace owns RelayHistory authentication, legacy convergence/turn mapping, replay, sharing and the `relayhistory-plugin` helper. The local workspace does not list this package as a member or dependency. Local readers in `ai-hist-core::storage` provide typed scans; this package contains no production SQL queries.

Build with `cargo build --manifest-path plugins/relayhistory/rust/Cargo.toml --release`. The SDK plugin distributes the resulting helper separately from the local native addon. An explicit `RELAYHISTORY_PLUGIN_BIN` override can select a development build.

## JSON bridge version 1

With no command-line arguments the helper reads one JSON request from stdin, writes one JSON response to stdout and exits. Arguments use the SDK's camelCase names:

```json
{"version":1,"operation":"cloudLoadAuth","args":{"baseUrl":"https://history.agentrelay.com"}}
```

Success: `{"version":1,"ok":true,"value":null}`. Operation failure: `{"version":1,"ok":false,"error":{"code":"CLOUD_AUTH_FAILED","message":"..."}}`. Operation failures exit zero so the host can decode the typed error; malformed/oversized requests or output failures exit nonzero with a fixed diagnostic. Unknown versions and operations are rejected. Errors never serialize raw response bodies or input credentials. Explicit token/auth operations return credentials as their successful result, so hosts must not log those results.

Operations: `accessToken`, `cloudLoadAuth`, `cloudResolveSession` (requires `now` in Unix milliseconds), `cloudRefreshSession`, `cloudValidateExchangeBaseUrl`, `cloudLogin`, `enableCloud`, `pushCloud`, `replay`, and `createShareableTrace`. Login/push arguments are flattened (no nested `options`). Replay accepts `sessionId`, `baseUrl`, `limit`, `maxContent`, `json`, `out`. Share returns the serialized response string matching the former native method. Auth results use `baseUrl`, `accessToken`, `accessTokenExpiresAt`, `refreshToken`, `orgId`, `workspaceId`.

Input is capped at 16 MiB, output at 32 MiB, and process execution at 300 seconds. Hosts should enforce their own shorter operation deadline and terminate cancelled helpers. Large replay output can use the atomic `out` file option. Interactive Agent Relay login output goes to stderr so stdout remains framed JSON.

## Compatibility and migration

Explicit legacy subcommands `login`, `admin-mint`, `token`, `replay`, `push`, `coverage`, and `pair` remain on this optional binary. Manual legacy push uses the unchanged stage-scoped authentication and cursor files. Existing positional turns and convergence endpoints provide their original guarantees; they are not the generic durable delivery protocol. `enableCloud` and `pushCloud` also remain legacy compatibility operations.

`push --install-service` and `push --uninstall-service` fail with a migration instruction. Before enabling a new durable worker, the optional SDK plugin must detect existing `com.ai-hist.push` launchd jobs and managed `# ai-hist push (managed)` cron entries, then require explicitly stopping/replacing them. Do not alter live services merely by loading a plugin. Preserve existing auth and legacy cursor files unchanged. The new destination job requires an explicit source/selection and persisted exclusions; prior per-run `--incognito` filters cannot be inferred. New delivery progress must never reinterpret or reset legacy cursors.
