# ai-hist-mcp

Thin `npx` wrapper for the MCP server shipped by the public `ai-hist` SDK:

```bash
npx -y ai-hist-mcp
```

The server imports only `ai-hist` public functions. It never opens SQLite,
loads the native addon directly, scans provider files, or invokes a CLI.

Tools: `search_history`, `recent_history`, `list_sessions`,
`discover_sessions`, `hydrate_session`, `get_session`, `get_session_events`,
`get_session_relationships`, `get_session_tree`, `get_session_thread`,
`get_session_tool_calls`, `get_session_file_edits`, `history_stats`, and
`sync`. Search, recent history, session listing, discovery, statistics, and sync accept a
`scope` of `local`, `remote`, or `all`; scope defaults to `local`.
`get_session`, `get_session_events`, `get_session_relationships`,
`get_session_tree`, and `get_session_thread` address one session by identity
and take no `scope`.
`get_session_tool_calls` and `get_session_file_edits` are bounded, cursor-paged
reads that require both a `source` and a `session_id`, because provider session
IDs collide.

Cached reads support all three scopes. Remote acquisition runs through
provider connectors (claude.ai/code web sessions, Codex cloud tasks) that are
configured by the provider CLI's own sign-in on the machine; explicit `remote`
acquisition returns `UNSUPPORTED_OPERATION` when none is configured, while
`all` runs local adapters plus every configured connector. The discovery and
sync tools are therefore annotated as open-world writes.
`hydrate_session` is an idempotent write that fully indexes one previously
discovered identity and optionally its related provider-native sessions from
local provider evidence, so it stays annotated as a local, closed-world write.

`get_session_thread` is the *lifecycle* fan-out for one session: the commits it
shipped plus the pull requests, reviews, incidents, tickets, Slack threads,
hotfixes and follow-up sessions the cloud has stitched to it. It complements
`get_session_tree` — that one is the *subagent* fan-out — and an agent may call
both. It is cloud-only and never cached: a thread exists once the cloud has
ingested lens events, and it changes as PRs and incidents land, so every call
fetches. With no stored cloud session it returns `UNSUPPORTED_OPERATION` with
the same `no remote provider connectors are configured` message the sibling
connectors use, without making a request. Tenancy is derived from the token
server-side; there is no org parameter. `kinds` filters `link_kind`, `since`
bounds link event time, and `limit` (1-500, default 100) with `cursor` pages the
links; outcomes come back whole on every page.

Credentials come from whichever store holds a session: the native `ai-hist
login` store (`RELAYHISTORY_HOME`, else `~/.agentworkforce/relayhistory`) is
read first, then this SDK's own `~/.config/ai-hist/auth.json`. With more than
one stage stored and no `AI_HIST_BASE_URL` naming one, the tool refuses to
guess rather than answer about the wrong org.

A native-store session must meet the same bar the engine's `cloud` connector
applies to recall — an `rth_at_` access token, an expiry at least 60s away, and
a recorded org for provenance. A session missing a token prefix or an org is one the
connector itself reports as unconfigured, so the tool reports it the same way
and names the missing precondition rather than issuing a request that would
fail. `ai-hist login` restores them.

Expiry is the exception, because rotation exists to repair it: a rejected
session with a refresh token is rotated once and the new pair persisted over
the file it came from, in that store's own schema, so the CLI and the MCP stay
in step. A pair another process rotated first is adopted rather than spending
the one-time refresh token again. Only a session with nothing left to rotate
reports expiry as unconfigured.

`get_session_relationships` and `get_session_tree` read the delegation topology
recorded by hydration and sync: who delegated to whom, what evidence
established the link, whether the child has a stable identity, and whether its
events are independently addressable. Tree traversal is cycle-safe,
deterministically ordered, and bounded by `max_depth` and `max_nodes`.
