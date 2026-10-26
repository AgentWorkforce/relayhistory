"""Write and independently read a synthetic Vibe 3.0.1 Unified session.

Run with mistral-vibe==3.0.1 installed (binary wheel). No CLI, credentials,
network, model, or user session is involved.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys

from mistralai_vibe_local_harness.session_protocol import (
    IdleSessionStatus,
    LatestPublicHistoryPage,
    PublicSession,
    PublicSessionState,
    TurnQueue,
    TURN_QUEUE_MAX_ITEMS,
)
from mistralai_vibe_local_harness.vibe._storage import (
    AppendEntryOp,
    PluginLockV1,
    ProjectionStateV1,
    ReplaceEntryOp,
    UnifiedSessionStore,
    empty_runtime_state,
)

SID = "vibe-synthetic-001"
START = 1792609200000


def entry(entry_id: str, role: str, text: str, tick: int) -> dict:
    return {
        "id": entry_id,
        "sessionId": SID,
        "turnId": "turn-1",
        "inputEntryId": None if role == "user" else "user-1",
        "createdAt": START + tick,
        "updatedAt": START + tick,
        "generationStatus": "completed",
        "relatedEntryId": None,
        "type": "message",
        "role": role,
        "content": [{"type": "text", "text": text}],
        "source": "turn_start" if role == "user" else "harness",
    }


def effect() -> dict:
    # Shape from the pinned Unified projector `_running_effect_entry`, settled
    # under the same native effect id by its tool-result projection.
    return {
        "type": "effect",
        "id": "effect-action-1",
        "sessionId": SID,
        "turnId": "turn-1",
        "createdAt": START + 1000,
        "updatedAt": START + 1250,
        "generationStatus": "completed",
        "relatedEntryId": None,
        "inputEntryId": "user-1",
        "title": "read_file",
        "detail": {
            "kind": "tool",
            "toolName": "read_file",
            "input": {"path": "/synthetic/repository/README.md"},
            "display": {"summary": "Read file", "statusText": "Read file"},
        },
        "state": {
            "status": "completed",
            "outputText": "A synthetic README",
            "output": {"text": "A synthetic README"},
            "durationMs": 25,
            "display": {"success": True, "message": "Read file"},
        },
    }


def write(root: Path) -> None:
    history = [
        entry("user-1", "user", "Explain the synthetic repository", 0),
        effect(),
        entry("assistant-1", "assistant", "It contains one fixture.", 1000),
    ]
    state = PublicSessionState(
        session=PublicSession(
            id=SID,
            status=IdleSessionStatus(),
            created_at=START,
            updated_at=START + 1000,
            title="Synthetic Vibe session",
        ),
        history=LatestPublicHistoryPage(entries=history),
        turn_queue=TurnQueue(items=[], paused=False, max_items=TURN_QUEUE_MAX_ITEMS),
    )
    projection = ProjectionStateV1(
        session_id=SID, snapshot_sequence=0, watermark=2, snapshot=state
    )
    runtime = empty_runtime_state(
        SID,
        snapshot_sequence=0,
        plugin_lock=PluginLockV1(plugins=[]),
        cwd="/synthetic/repository",
        pins={"active_model": "synthetic-model"},
    )
    checkpoint = {
        "checkpoint_version": 1,
        "context": {
            "messages": [
                {"message": {"role": "user", "content": [{"type": "text", "text": "Explain the synthetic repository"}]}},
                {"message": {"role": "assistant", "content": [{"type": "text", "text": "It contains one fixture."}]}},
            ]
        },
    }
    UnifiedSessionStore(root, SID).write_generation(
        checkpoint=checkpoint, runtime_state=runtime, projection_state=projection
    )


def read(root: Path) -> None:
    stored = UnifiedSessionStore(root, SID).load()
    out = {
        "session_id": stored.manifest.session_id,
        "format_minor": 9,
        "cwd": stored.runtime_state.session_metadata.cwd,
        "title": stored.projection_state.snapshot.session.title,
        "entries": stored.projection_state.snapshot.history.entries,
        "interop_roles": [m.role for m in stored.interop_export.history],
    }
    print(json.dumps(out, indent=2, default=str))


def append(root: Path) -> None:
    store = UnifiedSessionStore(root, SID)
    store.advance_projection_delta(
        4,
        (
            AppendEntryOp(entry=entry("user-2", "user", "Check the journal", 2000)),
            AppendEntryOp(entry=entry("assistant-2", "assistant", "Journal captured.", 3000)),
        ),
    )


def write_replace_base(root: Path) -> None:
    running = effect()
    running["updatedAt"] = START + 1000
    running["generationStatus"] = "running"
    running["state"] = {"status": "running"}
    history = [
        entry("user-1", "user", "Explain the synthetic repository", 0),
        running,
        entry("assistant-1", "assistant", "Pending synthetic answer", 1000),
    ]
    state = PublicSessionState(
        session=PublicSession(
            id=SID, status=IdleSessionStatus(), created_at=START,
            updated_at=START + 1000, title="Synthetic Vibe replacement session",
        ),
        history=LatestPublicHistoryPage(entries=history),
        turn_queue=TurnQueue(items=[], paused=False, max_items=TURN_QUEUE_MAX_ITEMS),
    )
    projection = ProjectionStateV1(
        session_id=SID, snapshot_sequence=0, watermark=2, snapshot=state
    )
    runtime = empty_runtime_state(
        SID, snapshot_sequence=0, plugin_lock=PluginLockV1(plugins=[]),
        cwd="/synthetic/repository", pins={},
    )
    checkpoint = {"checkpoint_version": 1, "context": {"messages": []}}
    UnifiedSessionStore(root, SID).write_generation(
        checkpoint=checkpoint, runtime_state=runtime, projection_state=projection
    )


def replace(root: Path) -> None:
    revised = entry("assistant-1", "assistant", "Final synthetic answer", 1000)
    revised["updatedAt"] = START + 1500
    settled = effect()
    settled["updatedAt"] = START + 1500
    settled["detail"]["input"] = {"path": "/synthetic/repository/UPDATED.md"}
    settled["state"]["outputText"] = "A revised synthetic README"
    settled["state"]["output"] = {"text": "A revised synthetic README"}
    UnifiedSessionStore(root, SID).advance_projection_delta(
        4,
        (
            ReplaceEntryOp(id="assistant-1", entry=revised),
            ReplaceEntryOp(id="effect-action-1", entry=settled),
        ),
    )


def rewrite(root: Path) -> None:
    # Native generation replacement: one old assistant entry and the tool
    # effect disappear, rather than a JSON fixture being edited by hand.
    history = [
        entry("user-1", "user", "Explain the synthetic repository", 0),
        entry("assistant-revised", "assistant", "It contains a revised fixture.", 4000),
    ]
    state = PublicSessionState(
        session=PublicSession(
            id=SID,
            status=IdleSessionStatus(),
            created_at=START,
            updated_at=START + 4000,
            title="Revised Vibe session",
        ),
        history=LatestPublicHistoryPage(entries=history),
        turn_queue=TurnQueue(items=[], paused=False, max_items=TURN_QUEUE_MAX_ITEMS),
    )
    projection = ProjectionStateV1(
        session_id=SID, snapshot_sequence=2, watermark=5, snapshot=state
    )
    runtime = empty_runtime_state(
        SID, snapshot_sequence=2, plugin_lock=PluginLockV1(plugins=[]),
        cwd="/synthetic/repository", pins={},
    )
    checkpoint = {
        "checkpoint_version": 1,
        "context": {"messages": [
            {"message": {"role": "user", "content": [{"type": "text", "text": "Explain the synthetic repository"}]}},
            {"message": {"role": "assistant", "content": [{"type": "text", "text": "It contains a revised fixture."}]}},
        ]},
    }
    UnifiedSessionStore(root, SID).write_generation(
        checkpoint=checkpoint, runtime_state=runtime, projection_state=projection
    )


def reorder(root: Path) -> None:
    # Keep native IDs and same-millisecond timestamps, reorder only the
    # assistant and effect entries in a new native generation.
    history = [
        entry("user-1", "user", "Explain the synthetic repository", 0),
        entry("assistant-1", "assistant", "It contains one fixture.", 1000),
        effect(),
        entry("user-2", "user", "Check the journal", 2000),
        entry("assistant-2", "assistant", "Journal captured.", 3000),
    ]
    state = PublicSessionState(
        session=PublicSession(id=SID, status=IdleSessionStatus(), created_at=START,
                              updated_at=START + 3000, title="Synthetic Vibe session"),
        history=LatestPublicHistoryPage(entries=history),
        turn_queue=TurnQueue(items=[], paused=False, max_items=TURN_QUEUE_MAX_ITEMS),
    )
    projection = ProjectionStateV1(session_id=SID, snapshot_sequence=2,
                                   watermark=5, snapshot=state)
    runtime = empty_runtime_state(SID, snapshot_sequence=2,
                                  plugin_lock=PluginLockV1(plugins=[]),
                                  cwd="/synthetic/repository", pins={})
    checkpoint = {"checkpoint_version": 1, "context": {"messages": [
        {"message": {"role": "user", "content": [{"type": "text", "text": "Explain the synthetic repository"}]}},
        {"message": {"role": "assistant", "content": [{"type": "text", "text": "It contains one fixture."}]}},
    ]}}
    UnifiedSessionStore(root, SID).write_generation(
        checkpoint=checkpoint, runtime_state=runtime, projection_state=projection
    )


if __name__ == "__main__":
    command = sys.argv[1]
    root = Path(sys.argv[2])
    if command == "write":
        write(root)
    elif command == "read":
        read(root)
    elif command == "append":
        append(root)
    elif command == "write-replace-base":
        write_replace_base(root)
    elif command == "replace":
        replace(root)
    elif command == "rewrite":
        rewrite(root)
    elif command == "reorder":
        reorder(root)
    else:
        raise SystemExit("expected write or read")
