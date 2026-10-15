#!/usr/bin/env python3
"""Project the native offline Muse export of an existing synthetic corpus log."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("--binary", required=True, type=Path)
parser.add_argument("--out", required=True, type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[5]
fixture = Path("crates/ai-hist/tests/fixtures/muse/cli-capture/.local/share/muse/sessions/2026/09/01/74747474-7474-4747-8747-747474747474/session.jsonl")
with tempfile.TemporaryDirectory(prefix="rh-muse-", dir="/private/tmp" if platform.system() == "Darwin" else None) as temporary:
    home = Path(temporary)
    exported = home / "export.json"
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR": str(home),
           "HOME": str(home), "XDG_DATA_HOME": str(home / ".local/share"),
           "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache"),
           "MUSE_NO_AUTO_UPDATE": "1"}
    subprocess.run([str(args.binary.resolve()), "export", "--session", str(root / fixture),
                    "--out", str(exported)], env=env, cwd=home, check=True, capture_output=True)
    raw = exported.read_bytes()
    data = json.loads(raw)
    if data["exporter_version"]["semver"] != "1.4.7":
        raise SystemExit("Expected pinned Muse exporter 1.4.7")
    messages, calls, results = [], [], []
    for record in data["events"]:
        event = record.get("envelope", {}).get("payload", {}).get("event", {})
        if event.get("kind") == "started" and isinstance(event.get("prompt"), str):
            messages.append({"role": "user", "text": event["prompt"]})
        if event.get("kind") == "assistant_message_committed":
            messages.append({"role": "assistant", "id": event.get("message_id"), "text": event["text"]})
        if event.get("kind") == "assistant_tool_calls_committed":
            calls.extend(event["tool_calls"])
        if event.get("kind") == "tool_result_batch_committed":
            results.extend(event["results"])
    projection = {"exporter_version": data["exporter_version"],
                  "export_schema_version": data["export_schema_version"],
                  "full_export_sha256": hashlib.sha256(raw).hexdigest(),
                  "source_fixture": str(fixture), "messages": messages, "tool_calls": calls, "tool_results": results}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(projection, indent=2) + "\n")
