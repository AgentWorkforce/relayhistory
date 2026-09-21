# Keep session previews and capture progress local

Status: implemented
Date: 2026-09-21

## Context

Desktop setup waits for full capture before sessions can be selected. The local
collector knows file counts and session counts during that wait, but previously
sent those counts to Cloud through onboarding heartbeats. Knowing a session exists
locally is not permission to disclose it.

## Decision

Keep full capture progress on the Mac, in a private `progress.json` sidecar and the
install process's JSON event stream. Capture-only monitors do not send heartbeats.
At the HTTP boundary, remove source names and all local inventory counts from
progress; Cloud may observe only delivery progress for records permitted by the
existing upload policy. Keep the legacy fields empty/zero for wire compatibility.

Expose the latest 100 titles through a local-only `sessions preview` command. Reuse
RelayHistory's shallow discovery adapters and a private metadata catalog separate
from the delivery database. The desktop can read its small JSON cache immediately
while discovery and full capture continue. Preview rows never grant inclusion or
prove readiness; only the target-scoped session list enables upload selection.

## Consequences

No provider parser or Cloud inventory API is added to the desktop. Preview reads
cannot wait behind the full capture writer or add rows to a delivery generation.
Cloud loses the local-scan progress indicator; the desktop displays those details.
Previously received inventory counts are not retroactively deleted by this change.
