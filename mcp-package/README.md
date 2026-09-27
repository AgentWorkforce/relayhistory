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

A Relaycast handoff is a pointer, never an inlined transcript. `create_handoff`
turns the caller's continuation request into the pointer's single,
self-describing `intent` field:

```json
{"source":"codex","session_id":"abc","intent":"Resume this handoff: call resume_handoff(source=codex, session_id=abc) via the ai-hist MCP, then continue: continue the fix","origin_agent":"sender","origin_user":"user-id"}
```

Send the full result as structured delivery metadata with `kind="handoff"`, and
send its `intent` value unchanged as the DM text. Do not add a second text field:
the cloud handoff contract requires the delivery text to equal `intent`.

Before sending, call `resume_handoff` once with the new pointer and send only
after that workspace read succeeds. This prevents a live pointer from racing
ahead of Agent Relay desktop's team upload.

On receipt, the normal agent prompt is sufficient: follow the instruction in
`intent`, call `resume_handoff(source, session_id)` immediately, and continue
the original request with the returned context. Pass `next_cursor` back
unchanged when an additional page is needed. No installed handoff skill is
required. Handoffs are workspace-scoped: resume selects only the configured
`cloud` connector, refreshes its session observation under the currently
authenticated workspace, and rejects unavailable cross-workspace or
cross-organization sessions.

Remote acquisition is optional. Install a source plugin such as
`@relayhistory/provider-sources` and set `AI_HIST_PLUGIN_CONFIG` to its explicit
module config. Loading a configured plugin is inert and adds no tools.
`source_connectors` selects configured source IDs; an empty array disables
remote acquisition. Acquisition tools declare open-world writes.

Agent Relay desktop owns team upload and supplies the workspace-authenticated
`cloud` source configuration used by `resume_handoff`; the retired
`@relayhistory/capture` package is not required. Auto-resume is prompt-driven by
the self-describing `intent`; there is no handoff skill to install.
