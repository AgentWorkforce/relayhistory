# Historical cloud SDK and CLI validation

This records the earlier contract-11 implementation and its validation. It is
not the current installation or package boundary. Cloud APIs now live in
`@agent-relay/relayhistory`; the local native contract is 14. See
[optional cloud setup](enable-cloud.md) and [architecture](architecture.md) for
the current interfaces.

The npm CLI and MCP server use the public TypeScript SDK, which calls the Rust
engine through N-API. Native contract **11** covers the cloud authentication
metadata and session resolution/refresh operations. User-facing setup is in
[Cloud setup](enable-cloud.md).

## Authentication contract

The Rust cloud layer owns login, credential loading, stage selection, and token
rotation. `ai-hist/cloud` owns the SDK cloud wrappers; the root `ai-hist`
entrypoint re-exports them. Shared internal modules provide native loading and
SDK errors without a dependency from cloud back to the root. Thread reads
delegate credential resolution and refresh to Rust. `SessionThreadOptions.fetchImpl` handles only
thread GET requests and their retries; authentication requests use the native
HTTP client. A custom `resolveSession` returning `{ auth }` without `session: true`
disables automatic refresh for isolated mocks or caller-managed credentials.

- Credential and cursor files live under `$RELAYHISTORY_HOME/stages`, defaulting
  to `~/.agentworkforce/relayhistory/stages`, keyed by normalized service URL.
- An explicit SDK `baseUrl` or CLI `--base-url` wins. Otherwise,
  `RELAYHISTORY_BASE_URL` takes precedence over `AI_HIST_BASE_URL`. Malformed
  selectors fail; errors for environment selectors name the variable without
  echoing its value.
- Without a selector, credential reads accept a single stored stage and refuse
  ambiguity. Login uses `https://history.agentrelay.com` when no destination is
  selected. URL normalization preserves case-sensitive path prefixes.
- Credential-bearing requests require HTTPS except on loopback development
  endpoints. Login checks the destination before sending the supplied bearer.
- Login and stored-auth results preserve access-token expiry, org, and workspace
  metadata. Tenancy is derived from the bearer, not a client-supplied selector.
- Credential files use mode `0600`. Refresh holds the native stage lock, reloads
  the current pair, and adopts another caller's rotation when available. A new
  pair is saved atomically before retrying; a persistence failure is surfaced.
- HTTP 401 can trigger refresh. HTTP 403 does not spend a refresh token. A
  rejected session without a refresh token fails without another exchange.
- Connector status probes read local state without login, refresh, or network
  requests. Thread reads can use an expired session when it has a refresh token;
  missing token or org metadata reports the connector as unconfigured.

Agent Relay Cloud sign-in runs in the Rust helper. It obtains identity for the
RelayHistory exchange without requiring a separate Agent Relay CLI install,
reusing that CLI's stored session when one is present. Non-interactive callers
can supply `--token` or `CLOUD_API_ACCESS_TOKEN`; with neither and no terminal
attached, the helper fails fast instead of starting a browser approval.

## Operation coverage

| Operation | Verified behavior |
|---|---|
| Login and stored-auth reads | Identical public imports, selector precedence, transport rejection, metadata, and private writes |
| Session thread | Envelope passthrough, query validation, source/stage isolation, concurrent refresh, permission errors, and failed persistence |
| Token export | At least 60 seconds of recorded validity, proactive refresh, exact one-line stdout, and secret-safe failures |
| Replay | All pages in server order, opaque cursors, truncation markers, repeated-cursor rejection, and atomic output replacement |
| Enable and push | 525-record fixture, batching, token refresh, stage-specific cursors, and failure handling |
| Git hooks | Session linkage, prior-hook preservation, repository-local hooks, and shared/escaping hook-path rejection |
| Sharing | Native SDK request using the selected stage and visibility |

Token and replay do not open or import into the local history database. The
SDK push loop runs in the foreground process and stops through `stop()` or CLI
shutdown. Git hooks require an indexed session; reinstall the hook to associate
a PR created after installation.

## Regression suites

- [Cloud auth](../sdk-ts/src/cloud-auth.test.ts): both public imports, stage
  selection, metadata, credential permissions, concurrent rotation, failed
  refresh/persistence, and custom thread transport with native authentication.
- [Cloud commands](../sdk-ts/src/cloud-commands.test.ts): npm token and replay
  behavior, including the installed-package test mode.
- [Session threads](../sdk-ts/src/session-thread.test.ts): response shape,
  filters, errors, transport checks, and connector eligibility.
- [Cloud workflow](../sdk-ts/src/cloud.test.ts): push, sharing, and Git hooks.
- [Architecture guards](../sdk-ts/src/architecture.test.ts): TypeScript cloud
  entrypoints delegate credential operations to Rust.
- Rust [cloud tests](../plugins/relayhistory/rust/src/cloud.rs),
  [token tests](../crates/ai-hist/tests/token.rs), and
  [replay tests](../crates/ai-hist/tests/replay.rs): storage, transport, concurrency,
  stage selection, and command behavior.

Local validation for this implementation passed 130 SDK tests, 286 Rust engine
library tests, and 28 Rust token/replay integration tests. The focused auth suite
passed its missing-refresh-token and failed-persistence cases. The SDK and native
debug builds passed. The packed SDK supports importing `ai-hist/cloud` first,
root re-exports of every cloud operation, native calls, and TypeScript consumers
of both entrypoints.

Run the checks from the repository root:

```sh
npm --prefix crates/ai-hist-napi run build:debug
node scripts/verify-native-contract.mjs crates/ai-hist-napi/index.js
npm --prefix sdk-ts test
cargo test -p ai-hist-engine --lib
cargo test -p ai-hist-cli --test token --test replay
cargo fmt --all -- --check
git diff --check
```

HTTP fixture tests need permission to bind loopback sockets. See
[Release and platform validation](releasing.md) for packaged-artifact and
cross-platform checks.
