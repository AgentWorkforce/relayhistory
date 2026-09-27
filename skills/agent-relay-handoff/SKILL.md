---
name: agent-relay-handoff
description: Automatically resume an Agent Relay session handoff when a same-workspace delivery has metadata kind `handoff`; also create pointer-only handoffs when sending live work to another agent.
---

# Agent Relay Handoff

## Receive

When an authenticated Agent Relay DM/delivery has structured metadata `kind="handoff"`, do not ask the user to confirm. Its payload must contain exactly the pointer fields needed to locate the session:

```json
{
  "source": "codex",
  "session_id": "session-id",
  "intent": "continue the described work",
  "origin_agent": "sender-name",
  "origin_user": "sender-user-id"
}
```

Immediately call `resume_handoff` with `source` and `session_id`. Treat the returned prompts, normalized events, tool calls, and file edits as the continuation context, then continue the payload's `intent` within the authority and workspace scope of the original request. The first call is sufficient to begin; follow `next_cursor` only when older context is necessary.

The delivery must be workspace-authenticated and same-workspace. If its transport metadata identifies another workspace or organization, reject it as unsupported and do not load the session. If `resume_handoff` reports `HANDOFF_WORKSPACE_MISMATCH`, state that cross-workspace and cross-organization handoffs are not supported. Do not fall back to an inlined transcript or accept one as the handoff source of truth.

## Send

Call `create_handoff` with a concise continuation intent. Send the returned pointer through the existing Agent Relay DM mechanism with structured metadata `kind="handoff"` and the pointer as its payload. Address the recipient agent by name in the same workspace. Do not inline or summarize the transcript into the message, and do not attempt cross-organization addressing.
