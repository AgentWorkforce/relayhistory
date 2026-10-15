#!/usr/bin/env python3
"""Offline OpenCode writer/export probe; never starts an agent or calls a model."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import sqlite3
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument("--binary", required=True, type=Path)
parser.add_argument("--out", required=True, type=Path)
args = parser.parse_args()
binary = args.binary.resolve()
args.out.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix="rh-oc-", dir="/private/tmp" if platform.system() == "Darwin" else None) as temporary:
    home = Path(temporary)
    work = home / "work"
    work.mkdir()
    env = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR": str(home),
           "HOME": str(home), "XDG_DATA_HOME": str(home / ".local/share"),
           "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache"),
           "OPENCODE_DISABLE_MODELS_FETCH": "1", "OPENCODE_DISABLE_AUTOUPDATE": "1"}
    def run(*arguments):
        return subprocess.run([str(binary), *arguments], cwd=work, env=env,
                              check=True, capture_output=True, text=True).stdout
    version = run("--version").strip()
    if version != "1.18.31":
        raise SystemExit(f"Expected pinned OpenCode 1.18.31, got {version!r}")
    source = Path(__file__).with_name("input.json")
    run("import", "--pure", str(source.resolve()))
    # A fresh process reads native persistence independently from RelayHistory.
    exported = json.loads(run("export", "--pure", "ses_baseline11831"))
    (args.out / "official-export.json").write_text(json.dumps(exported, indent=2) + "\n")
    connection = sqlite3.connect(home / ".local/share/opencode/opencode.db")
    lines = ["-- Native OpenCode 1.18.31 import output; unrelated application tables omitted.",
             "BEGIN TRANSACTION;"]
    for table in ("project", "session", "message", "part"):
        schema = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                                    (table,)).fetchone()[0]
        lines.append(schema + ";")
        for row in connection.execute(f"SELECT * FROM {table} ORDER BY id"):
            values = [connection.execute("SELECT quote(?)", (value,)).fetchone()[0] for value in row]
            lines.append(f"INSERT INTO {table} VALUES(" + ",".join(values) + ");")
    lines.append("COMMIT;")
    (args.out / "native-store.sql").write_text("\n".join(lines) + "\n")
    print(json.dumps({"version": version, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                      "messages": len(exported["messages"])}, indent=2))
