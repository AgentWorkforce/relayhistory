# ai-hist-mcp

Thin `npx -y ai-hist-mcp` wrapper for the local `ai-hist` MCP server.
It calls public SDK operations; it never opens SQLite or loads cloud auth.

Default tools cover cached search/recent/catalog/statistics, discovery/sync,
identity-addressed events/tool calls/file edits/relationships/session trees, and
generic durable delivery status/control. Cached scopes local/remote/all do not
read credentials; acquisition defaults to local. Tool and file edit pages require
both source and session ID and use bounded deterministic cursors.

Remote acquisition is optional. Install a source plugin such as
`@relayhistory/provider-sources` and set `AI_HIST_PLUGIN_CONFIG` to its explicit
module config. Loading a configured plugin is inert and adds no tools.
`source_connectors` selects configured source IDs; an empty array disables
remote acquisition. Acquisition tools declare open-world writes.
