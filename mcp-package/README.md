# ai-hist-mcp

Thin `npx -y ai-hist-mcp` wrapper for the local `ai-hist` MCP server.
It calls public SDK operations; it never opens SQLite or loads cloud auth.

Default tools cover cached search/recent/catalog/statistics, discovery/sync,
identity-addressed events/tool calls/file edits/relationships/session trees, and
generic durable delivery status/control. `create_handoff` and `resume_handoff`
are also enabled by default: the former returns the caller's current-session
pointer and the latter composes one bounded continuation page containing prompts,
normalized events, tool calls, and file edits. Cached scopes local/remote/all do not
read credentials; acquisition defaults to local. Tool and file edit pages require
both source and session ID and use bounded deterministic cursors.

A Relaycast handoff is a pointer, never an inlined transcript. Send the
`create_handoff` result as the payload of structured delivery metadata
`kind="handoff"`:

```json
{"source":"codex","session_id":"…","intent":"continue the fix","origin_agent":"sender","origin_user":"user-id"}
```

On receipt, call `resume_handoff(source, session_id)` immediately. Its initial
response is directly usable continuation context; `next_cursor` is only needed
when an agent requires an older page. Handoffs are workspace-scoped. The
authenticated source connector can acquire a teammate's session in the same
workspace, while cross-workspace and cross-organization loads are rejected.

Remote acquisition and commercial tools are optional. Install a source or
destination package and set `AI_HIST_PLUGIN_CONFIG` to its explicit module config.
Loading a configured plugin is inert. `source_connectors` selects configured
source IDs; an empty array disables remote acquisition. Acquisition tools declare
open-world writes. Arbitrary plugin callbacks receive conservative annotations,
and duplicate/reserved tool names are rejected before registration.

For teammate handoffs, install `ai-hist`, `ai-hist-mcp`, and
`@relayhistory/capture` together, sign in once, and point the MCP process at this
minimal config (resolved beside that installation's `node_modules`):

```json
{"plugins":[{"module":"@relayhistory/capture"}]}
```

```sh
AI_HIST_PLUGIN_CONFIG=/absolute/path/history.json npx ai-hist-mcp
```

No manual account hash is required: the `cloud` source derives and pins the
authenticated organization/workspace account for each acquisition. An explicit
`expectedAccount` remains available for deployments that want a static pin.

The installable auto-resume instructions live in
`skills/agent-relay-handoff`. Install the directory as a skill:

```sh
# Codex
cp -R skills/agent-relay-handoff ~/.codex/skills/

# Claude Code
cp -R skills/agent-relay-handoff ~/.claude/skills/
```

The optional `@relayhistory/capture` package registers `get_session_thread`
and `read_delivered_history`. The former composes freshly delivered evidence
with legacy lifecycle links under one pinned account and reports each outcome;
the latter is an explicit live listing, not an incremental feed. Neither tool
is shipped in the default inventory. See the repository's optional package README
for auth, account pinning and stage selection.
