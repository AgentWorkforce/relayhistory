# Connect history to a cloud service

History stays local until you explicitly export it or enable a destination.
Install RelayHistory separately from the local SDK:

```sh
npm install ai-hist @agent-relay/relayhistory
```

Other services can implement the same source and destination interfaces. A source
plugin reads remote history; a destination plugin receives your local history.
Either can be used independently.

## Dependable background delivery

For new installations, follow the [RelayHistory plugin setup](../plugins/relayhistory/sdk/README.md)
to create `history.json` and an explicit `selection.json`, then run:

```sh
relayhistory-plugin login --base-url https://history.agentrelay.com
ai-hist plugin relayhistory-migration-status --config history.json
ai-hist plugin relayhistory-enable --config history.json -- --selection selection.json
ai-hist delivery run --config history.json
ai-hist delivery status
```

These are the npm SDK CLI commands. The standalone Rust `ai-hist-cli` binary
handles local history and does not load JavaScript plugins.

The durable worker stores prepared bytes before sending, retries uncertain
outcomes with the same revision identities, and advances only on exact durable
acknowledgments. Run it under your existing supervisor for background operation.
Stop and remove old managed push schedules before enabling a new generation;
the setup checks for them and does not modify live services automatically.
Existing auth files and legacy cursors remain intact. Legacy positional cursors
are not imported as delivery acknowledgments.

See [export and delivery](history-delivery.md) for selection, exclusions, queue
limits, cancellation, and building a destination for another service.

## Existing cloud API migration

Cloud functions are exported by `@agent-relay/relayhistory`. Neither `ai-hist`
nor the removed `ai-hist/cloud` entrypoint exports them. Git hooks and commit
linking remain in the local SDK:

```ts
import { installGitHooks } from 'ai-hist';
import { enableCloud, createShareableTrace } from '@agent-relay/relayhistory';

// Compatibility with an existing legacy cloud workflow, not durable delivery.
const cloud = await enableCloud({ watch: false });
await installGitHooks({
  repo: process.cwd(),
  sessionId: 'YOUR_SESSION_ID',
  source: 'claude',
  prUrl: 'https://github.com/OWNER/REPO/pull/123',
});
const trace = await createShareableTrace('YOUR_SESSION_ID', {
  source: 'claude', visibility: 'direct-link',
});
console.log(trace.url);
await cloud.stop();
```

`enableCloud`, `pushCloud`, legacy replay, and sharing retain their existing
service protocol. A new durable delivery acknowledgment does not imply the
legacy sharing index has processed a session. Sharing requires a session already
available through that legacy API.

The optional npm package also retains these compatibility commands:

```sh
relayhistory-plugin enable-cloud --once
relayhistory-plugin replay SESSION_ID --out transcript.txt
relayhistory-plugin token --base-url https://history.agentrelay.com
```

Without `--once`, legacy `enable-cloud` repeats in the current process until
stopped; it does not install a daemon. Do not run that loop alongside a new
durable worker for the same history. Token output is a live credential.

## Authentication and stages

The optional Rust helper owns RelayHistory login, credential loading, rotation,
and stage selection. The optional SDK invokes it; the local native binding,
SDK, and default MCP server do not read commercial auth.

Credentials and legacy cursors retain their existing location under
`$RELAYHISTORY_HOME/stages`, defaulting to
`~/.agentworkforce/relayhistory/stages`. Each normalized service URL has its own
files. Rotation holds the stage lock and atomically saves the refreshed pair.

Use `baseUrl` in SDK/plugin options or `--base-url` in the optional CLI. Plugin
configuration pins the service endpoint; durable jobs also bind an authenticated
organization/workspace account. A changed endpoint or account requires a new
instance or job. For remote readback, resolve `deliveryAccount({ baseUrl })` and
pin it as `expectedAccount` before selecting the cloud source.

The optional legacy CLI preserves its existing environment/stage selection
behavior. A bare login defaults to `https://history.agentrelay.com`. Use an
explicit development endpoint for development acceptance tests; exchanging an
Agent Relay identity at a nondefault endpoint also requires the helper's
`RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL=1` opt-in.

## Local Git linkage

Hooks use an explicit, already-indexed session. `installGitHooks()` accepts a PR
URL or finds one through repository configuration or `gh pr view`. It writes Git
notes and local commit linkage without network I/O, preserving the prior
post-commit hook. Notes remain local until you explicitly push
`refs/notes/ai-hist`. Sending commit-link evidence to a destination is a separate
operation, selected through the delivery configuration.
