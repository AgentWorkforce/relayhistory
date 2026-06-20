#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

export AI_HIST_DB="$TMP/ai-history.db"
export TRAJECTORY_ROOT="$TMP/trajectories"
export OPENCODE_DB="$TMP/opencode.db"
export HOME="$TMP/home"
mkdir -p "$HOME/.claude/projects/e2e-project" "$HOME/.codex/sessions/2026/06/20" "$TRAJECTORY_ROOT/planner/compacted"
mkdir -p "$HOME/.cursor/projects/tmp-e2e-cursor/agent-transcripts/cursor-e2e"

rust_ai_hist() {
  if [[ -n "${AI_HIST_RUST_BIN:-}" ]]; then
    "$AI_HIST_RUST_BIN" "$@"
  else
    cargo run -q -p ai-hist-cli --manifest-path "$ROOT/Cargo.toml" -- "$@"
  fi
}

cat > "$HOME/.claude/history.jsonl" <<'JSONL'
{"display":"e2e claude release tagging prompt","timestamp":1700000000000,"project":"/tmp/e2e/project","sessionId":"claude-e2e"}
JSONL

cat > "$HOME/.codex/history.jsonl" <<'JSONL'
{"text":"e2e codex release tagging prompt","ts":1700000001,"session_id":"codex-e2e"}
JSONL

cat > "$HOME/.codex/sessions/2026/06/20/rollout-codex-e2e.jsonl" <<'JSONL'
{"type":"session_meta","payload":{"id":"codex-e2e","cwd":"/tmp/e2e/codex","git":{"branch":"main"}}}
JSONL

cat > "$HOME/.claude/projects/e2e-project/claude-e2e.jsonl" <<'JSONL'
{"sessionId":"claude-e2e","cwd":"/tmp/e2e/project","gitBranch":"main","timestamp":"2026-06-20T10:00:00.000Z"}
{"sessionId":"claude-e2e","type":"assistant","message":{"content":[{"type":"text","text":"assistant summary"}]},"timestamp":"2026-06-20T10:01:00.000Z"}
JSONL

cat > "$HOME/.cursor/projects/tmp-e2e-cursor/agent-transcripts/cursor-e2e/cursor-e2e.jsonl" <<'JSONL'
{"role":"user","message":{"content":[{"type":"text","text":"<user_query>\ne2e cursor release tagging prompt\n</user_query>"}]}}
{"role":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}
JSONL

cat > "$TRAJECTORY_ROOT/planner/compacted/trajectory-e2e.json" <<'JSON'
{
  "id": "trajectory-e2e",
  "version": 1,
  "personaId": "planner",
  "projectId": "agent-workforce",
  "task": {
    "title": "e2e trajectory release tagging task",
    "description": "Choose release test coverage."
  },
  "status": "completed",
  "startedAt": "2026-06-06T10:00:00.000Z",
  "completedAt": "2026-06-06T10:05:00.000Z",
  "decisions": [{
    "question": "What should be tested?",
    "chosen": "full Rust parity E2E",
    "reasoning": "Fallback is no longer sufficient.",
    "alternatives": ["scoped wrapper"]
  }],
  "retrospective": {
    "summary": "Parity test selected.",
    "approach": "Exercise every source.",
    "learnings": ["Installer and sync both matter."],
    "confidence": 0.9
  }
}
JSON

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
"$ROOT/ai-hist" tag cursor-e2e release-e2e --source cursor
"$ROOT/ai-hist" tag trajectory-e2e release-e2e --source trajectory
"$ROOT/ai-hist" search release --tag release-e2e --json
"$ROOT/ai-hist" session claude-e2e --full >/dev/null
"$ROOT/ai-hist" show 1 --json >/dev/null
"$ROOT/ai-hist" context 1 >/dev/null
"$ROOT/ai-hist" pack release --json >/dev/null
"$ROOT/ai-hist" stats --json >/dev/null
"$ROOT/ai-hist" tags --sessions --json >/dev/null

python3 - <<'PY'
import os, sqlite3
conn = sqlite3.connect(os.environ["AI_HIST_DB"])
sources = {row[0] for row in conn.execute("SELECT DISTINCT source FROM history")}
expected = {"claude", "codex", "cursor", "opencode", "trajectory"}
missing = expected - sources
if missing:
    raise SystemExit(f"missing sources from Rust sync: {sorted(missing)}")
codex_project = conn.execute("SELECT project FROM history WHERE source='codex' AND session_id='codex-e2e'").fetchone()[0]
if codex_project != "/tmp/e2e/codex":
    raise SystemExit(f"codex project metadata not backfilled: {codex_project!r}")
claude_session = conn.execute("SELECT cwd, git_branch, last_assistant_text FROM sessions WHERE source='claude' AND session_id='claude-e2e'").fetchone()
if not claude_session or claude_session[0] != "/tmp/e2e/project" or claude_session[1] != "main" or "assistant summary" not in (claude_session[2] or ""):
    raise SystemExit(f"claude session metadata missing: {claude_session!r}")
conn.close()
PY

rust_ai_hist --db "$AI_HIST_DB" tag codex-e2e release-e2e --source codex
rust_ai_hist --db "$AI_HIST_DB" search release --tag release-e2e --json

(cd "$ROOT/sdk-ts" && npm ci && npm test)

echo "E2E verification completed with temp DB: $AI_HIST_DB"
