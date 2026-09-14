# ai-hist-mcp

Thin `npx -y ai-hist-mcp` wrapper for the local `ai-hist` MCP server.
It calls public SDK operations; it never opens SQLite or loads cloud auth.

Default tools cover cached search/recent/catalog/statistics, discovery/sync,
identity-addressed events/tool calls/file edits/relationships/session trees, and
generic durable delivery status/control. Cached scopes local/remote/all do not
read credentials; acquisition defaults to local. Tool and file edit pages require
both source and session ID and use bounded deterministic cursors.

Remote acquisition and commercial tools are optional. Install a source or
destination package and set `AI_HIST_PLUGIN_CONFIG` to its explicit module config.
Loading a configured plugin is inert. `source_connectors` selects configured
source IDs; an empty array disables remote acquisition. Acquisition tools declare
open-world writes. Arbitrary plugin callbacks receive conservative annotations,
and duplicate/reserved tool names are rejected before registration.

The optional `@agent-relay/relayhistory` package registers `get_session_thread`
and `read_delivered_history`. The former composes freshly delivered evidence
with legacy lifecycle links under one pinned account and reports each outcome;
the latter is an explicit live listing, not an incremental feed. Neither tool
is shipped in the default inventory. See the repository's optional package README
for auth, expected-account pinning and stage selection.
