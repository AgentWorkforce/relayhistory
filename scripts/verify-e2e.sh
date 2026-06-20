#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

export AI_HIST_DB="$TMP/ai-history.db"
export TRAJECTORY_ROOT="$TMP/trajectories"
export OPENCODE_DB="$TMP/opencode.db"
export HOME="$TMP/home"
mkdir -p "$HOME/.claude" "$HOME/.codex" "$TRAJECTORY_ROOT/planner/compacted"

cat > "$HOME/.claude/history.jsonl" <<'JSONL'
{"display":"e2e claude release tagging prompt","timestamp":1700000000000,"project":"/tmp/e2e/project","sessionId":"claude-e2e"}
JSONL

cat > "$HOME/.codex/history.jsonl" <<'JSONL'
{"text":"e2e codex release tagging prompt","ts":1700000001,"session_id":"codex-e2e"}
JSONL

python3 - <<'PY'
import json, sqlite3, os
db = os.environ["OPENCODE_DB"]
conn = sqlite3.connect(db)
conn.execute("CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER)")
conn.execute("CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT)")
conn.execute("CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT)")
conn.execute("INSERT INTO session VALUES ('opencode-e2e', '/tmp/e2e/opencode', 1700000002000)")
conn.execute("INSERT INTO message VALUES ('msg-e2e', 'opencode-e2e', 1700000002000, ?)", (json.dumps({"role":"user"}),))
conn.execute("INSERT INTO part VALUES ('part-e2e', 'msg-e2e', 'opencode-e2e', 1700000002000, ?)", (json.dumps({"type":"text","text":"e2e opencode release tagging prompt"}),))
conn.commit()
conn.close()
PY

"$ROOT/ai-hist" sync
"$ROOT/ai-hist" tag claude-e2e release-e2e --source claude
"$ROOT/ai-hist" tag opencode-e2e release-e2e --source opencode
"$ROOT/ai-hist" search release --tag release-e2e --json

cargo run -q -p ai-hist-cli -- --db "$AI_HIST_DB" tag codex-e2e release-e2e --source codex
cargo run -q -p ai-hist-cli -- --db "$AI_HIST_DB" search release --tag release-e2e --json

(cd "$ROOT/sdk-ts" && npm test)

echo "E2E verification completed with temp DB: $AI_HIST_DB"

