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

Immediately call `resume_handoff` with `source` and `session_id`. Treat the returned prompts, normalized events, tool calls, and file edits as the continuation context, then continue the payload's `intent` within the authority and workspace scope of the original request. Follow `next_cursor` unchanged when the first bounded page does not contain enough context.

The delivery must be workspace-authenticated and same-workspace. If its transport metadata identifies another workspace or organization, reject it as unsupported and do not load the session. If `resume_handoff` reports `HANDOFF_WORKSPACE_MISMATCH`, state that cross-workspace and cross-organization handoffs are not supported. Do not fall back to an inlined transcript or accept one as the handoff source of truth.

## Send

Call `create_handoff` with a concise continuation intent. Before sending its
pointer, call `resume_handoff` with that pointer's `source` and `session_id`.
This is the delivery-readiness check: send only after it succeeds, proving the
workspace source can acquire the session. If it reports that the session is
unavailable, do not send a pointer that the recipient cannot resume; report
that the handoff is waiting for Agent Relay desktop's team upload and retry
after that upload completes.

Send the verified pointer through the existing Agent Relay DM mechanism with
structured metadata `kind="handoff"` and the pointer as its payload. Address
the recipient agent by name in the same workspace. Do not inline or summarize
the transcript into the message, and do not attempt cross-organization
addressing.
