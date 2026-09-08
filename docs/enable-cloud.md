# Your sessions, threaded to your PRs, in one command

```sh
ai-hist enable-cloud
```

The npm CLI calls the async SDK. It reuses your selected RelayHistory stage or
starts Agent Relay's device login, exchanges that identity for a service-local
`rth_at_*` session, syncs local history, drains the outbox, and keeps pushing once
per minute until Ctrl-C. Install `agent-relay` for the interactive login flow;
a preauthenticated embedding host can pass `relayAccessToken` to the SDK.
The loop runs in the current process; it is not an installed background daemon.

Use `--once` to drain and exit, `--interval 30` to change the interval, and
`--base-url https://dev.history.agentrelay.com` to select development explicitly.
The existing Rust trust gate requires
`RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL=1` for the trusted dev exchange.
Never use production for development acceptance tests.

```ts
import { enableCloud, installGitHooks, createShareableTrace } from 'ai-hist';

const cloud = await enableCloud({ intervalMs: 60_000 });
await installGitHooks({
  repo: process.cwd(),
  sessionId: 'YOUR_SESSION_ID',
  source: 'claude',
  prUrl: 'https://github.com/OWNER/REPO/pull/123',
});
// The next commit writes refs/notes/ai-hist plus a durable local link.
// The next successful push projects it to session_links.link_kind='github_pr'.
const trace = await createShareableTrace('YOUR_SESSION_ID', {
  source: 'claude', visibility: 'direct-link',
});
console.log(trace.url);
await cloud.stop();
```

Hooks use an explicit, already-indexed session. Installation finds an existing PR
through `git config ai-hist.pr-url` or `gh pr view`, or you can pass `prUrl` directly.
The result reports `prUrl: null` when no PR was found. When a PR is created later,
reinstall the hook before the next commit.
Installations with an external shared `core.hooksPath` are rejected rather than
modifying other repositories; configure a repository-local hooks directory first.
Hooks perform no network I/O and preserve the previous post-commit hook in
`post-commit.before-ai-hist`. Git notes append session IDs without replacing other
notes. They are local until you explicitly push `refs/notes/ai-hist`; cloud
linkage travels through the durable outbox independently.
The design follows [Traces' Git-hook documentation](https://traces.com/docs/sharing/git-hooks):
explicit session IDs, Git notes, and separate upload.

Cloud login, refresh, URL normalization, migration, stage hashing, atomic state
writes, cursor locks, and outbox batching remain in Rust. The compatibility
`ai-hist/cloud` export now reads the same Rust store as the engine. It imports the
old `~/.config/ai-hist/auth.json` only if the requested stage lacks canonical
credentials; stale SDK tokens never overwrite refreshed Rust tokens. No new
TypeScript auth store is written. Multiple stages require an explicit selection.
A new stage starts from its own cursor; enabling cloud never seeds it from the
local maximum or another stage's watermark.

Sharing creates a frozen snapshot of already-pushed convergence events. Public
shares permit indexing; direct-link shares are bearer URLs with noindex headers;
private shares require the same user, organization, workspace and read scope.
Private links can be read with an authenticated HTTP client; there is no browser
login page on the share route yet. The creator must own the session. Later events
are excluded. Revoke with authenticated `DELETE /v1/shares/:token`. The server
caps snapshots at 10,000 events and 5 MB and rejects oversized sessions explicitly.
