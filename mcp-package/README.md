# ai-hist-mcp

Thin `npx -y ai-hist-mcp` wrapper for the local `ai-hist` MCP server.
It calls public SDK operations; it never opens SQLite or loads cloud auth.

Default tools cover cached search/recent/catalog/statistics, discovery/sync, and
identity-addressed events/tool calls/file edits/markers/requests/usage/
relationships/session trees. `create_handoff` and `resume_handoff` are also
enabled by default: the former returns the caller's current-session pointer and
the latter composes a bounded continuation page containing prompts, normalized
events, tool calls, and file edits. Cached scopes local/remote/all do not
read credentials; acquisition defaults to local. Tool and file edit pages require
both source and session ID and use bounded deterministic cursors.

A Relaycast handoff is a pointer, never an inlined transcript. Send the
`create_handoff` result as structured delivery metadata with `kind="handoff"`:

```json
{"source":"codex","session_id":"…","intent":"continue the fix","origin_agent":"sender","origin_user":"user-id"}
```

Before sending, call `resume_handoff` once with the new pointer and send only
after that workspace read succeeds. This prevents a live pointer from racing
ahead of Agent Relay desktop's team upload.

On receipt, call `resume_handoff(source, session_id)` immediately. Its response
is continuation context; pass the returned `next_cursor` back unchanged when an
additional page is needed. Handoffs are workspace-scoped: resume selects only the
configured `cloud` connector, refreshes its session observation under the
currently authenticated workspace, and rejects unavailable cross-workspace or
cross-organization sessions.

Remote acquisition is optional. Install a source plugin such as
`@relayhistory/provider-sources` and set `AI_HIST_PLUGIN_CONFIG` to its explicit
module config. Loading a configured plugin is inert and adds no tools.
`source_connectors` selects configured source IDs; an empty array disables
remote acquisition. Acquisition tools declare open-world writes.

Agent Relay desktop owns team upload and supplies the workspace-authenticated
`cloud` source configuration used by `resume_handoff`; the retired
`@relayhistory/capture` package is not required. Auto-resume instructions live
in `skills/agent-relay-handoff`.
