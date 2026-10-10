#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

command -v sqlite3 >/dev/null || { echo "verify-e2e: sqlite3 is required" >&2; exit 1; }
command -v node >/dev/null || { echo "verify-e2e: node is required" >&2; exit 1; }
unset AI_HIST_CLI

assert_eq() {
  local label="$1" actual="$2" expected="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "FAIL $label: got '$actual', want '$expected'" >&2
    exit 1
  fi
}

# Print one field of a --json payload piped on stdin. The argument is a JS
# expression over `d`, the parsed document -- enough to name a single field
# without pulling a JSON query tool into the script's dependencies.
json_field() {
  node -e '
    const d = JSON.parse(require("node:fs").readFileSync(0, "utf8"));
    process.stdout.write(String(eval(process.argv[1])));
  ' "$1"
}

# Print one field of a JSONL stream piped on stdin (the `sessions discover`
# shape). The argument is a JS expression over `d`, the array of parsed lines.
jsonl_field() {
  node -e '
    const d = require("node:fs").readFileSync(0, "utf8")
      .split("\n").filter(Boolean).map((line) => JSON.parse(line));
    process.stdout.write(String(eval(process.argv[1])));
  ' "$1"
}

export AI_HIST_DB="$TMP/ai-history.db"
export OPENCODE_DB="$TMP/opencode.db"
export HOME="$TMP/home"
mkdir -p "$HOME/.claude/projects/e2e-project" "$HOME/.codex/sessions/2026/06/20"
mkdir -p "$HOME/.grok/sessions/%2Ftmp%2Fe2e%2Fgrok/grok-e2e"
mkdir -p "$HOME/.cursor/projects/tmp-e2e-cursor/agent-transcripts/cursor-e2e"

cat > "$HOME/.claude/history.jsonl" <<'JSONL'
{"display":"e2e claude release tagging prompt","timestamp":1700000000000,"project":"/tmp/e2e/project","sessionId":"claude-e2e"}
JSONL

cat > "$HOME/.codex/history.jsonl" <<'JSONL'
{"text":"e2e codex release tagging prompt","ts":1700000001,"session_id":"codex-e2e"}
JSONL

cat > "$HOME/.codex/sessions/2026/06/20/rollout-codex-e2e.jsonl" <<'JSONL'
{"type":"session_meta","payload":{"id":"codex-e2e","cwd":"/tmp/e2e/codex","git":{"branch":"main"}}}
JSONL

cat > "$HOME/.grok/sessions/%2Ftmp%2Fe2e%2Fgrok/grok-e2e/summary.json" <<'JSON'
{"info":{"id":"grok-e2e","cwd":"/tmp/e2e/grok"},"created_at":"2026-06-20T10:02:00.000Z","updated_at":"2026-06-20T10:03:00.000Z","head_branch":"main"}
JSON
cat > "$HOME/.grok/sessions/%2Ftmp%2Fe2e%2Fgrok/grok-e2e/chat_history.jsonl" <<'JSONL'
{"type":"user","content":[{"type":"text","text":"e2e grok release tagging prompt"}]}
{"type":"user","synthetic_reason":"system_reminder","content":[{"type":"text","text":"do not import this synthetic prompt"}]}
{"type":"assistant","content":[{"type":"text","text":"grok assistant summary"}]}
JSONL

cat > "$HOME/.claude/projects/e2e-project/claude-e2e.jsonl" <<'JSONL'
{"sessionId":"claude-e2e","cwd":"/tmp/e2e/project","gitBranch":"main","timestamp":"2026-06-20T10:00:00.000Z"}
{"sessionId":"claude-e2e","type":"assistant","message":{"content":[{"type":"text","text":"assistant summary"}]},"timestamp":"2026-06-20T10:01:00.000Z"}
JSONL

cat > "$HOME/.cursor/projects/tmp-e2e-cursor/agent-transcripts/cursor-e2e/cursor-e2e.jsonl" <<'JSONL'
{"role":"user","message":{"content":[{"type":"text","text":"<user_query>\ne2e cursor release tagging prompt\n</user_query>"}]}}
{"role":"assistant","message":{"content":[{"type":"text","text":"ok"}]}}
JSONL

sqlite3 -bail "$OPENCODE_DB" <<'SQL'
CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER);
CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
INSERT INTO session VALUES ('opencode-e2e', '/tmp/e2e/opencode', 1700000002000);
INSERT INTO message VALUES ('msg-e2e', 'opencode-e2e', 1700000002000, '{"role":"user"}');
INSERT INTO part VALUES ('part-e2e', 'msg-e2e', 'opencode-e2e', 1700000002000, '{"type":"text","text":"e2e opencode release tagging prompt"}');
SQL

"$ROOT/ai-hist" sync
"$ROOT/ai-hist" tag claude-e2e release-e2e --source claude
"$ROOT/ai-hist" tag opencode-e2e release-e2e --source opencode
"$ROOT/ai-hist" tag grok-e2e release-e2e --source grok
"$ROOT/ai-hist" tag cursor-e2e release-e2e --source cursor
"$ROOT/ai-hist" search release --tag release-e2e --json
"$ROOT/ai-hist" search --tag release-e2e >/dev/null
session_prompt=$("$ROOT/ai-hist" session claude-e2e --full --json | json_field 'd.map((e) => e.prompt).join("|")')
assert_eq "session prompts" "$session_prompt" "e2e claude release tagging prompt"

show_resume=$("$ROOT/ai-hist" show 1 --json | json_field 'd.resume_cmd')
assert_eq "show resume command" "$show_resume" "cd /tmp/e2e/project && claude --resume claude-e2e"

# `context` has no --json; assert it marks exactly the requested entry.
context_out=$("$ROOT/ai-hist" context 1)
context_focus=$(printf '%s\n' "$context_out" | grep -c '>>>' || true)
assert_eq "context focus rows" "$context_focus" "1"

pack_sources=$("$ROOT/ai-hist" pack release --json | json_field 'd.entries.map((e) => e.source).sort().join(",")')
assert_eq "pack sources" "$pack_sources" "claude,codex,cursor,grok,opencode"

stats_total=$("$ROOT/ai-hist" stats --json | json_field 'd.total')
assert_eq "stats total" "$stats_total" "5"

tagged_sessions=$("$ROOT/ai-hist" tags --sessions --json | json_field 'd.find((t) => t.name === "release-e2e").session_count')
assert_eq "tagged session count" "$tagged_sessions" "4"

sources=$(sqlite3 "$AI_HIST_DB" "SELECT DISTINCT source FROM history" | sort)
expected_sources=$(printf '%s\n' claude codex cursor grok opencode)
assert_eq "sources" "$sources" "$expected_sources"

codex_project=$(sqlite3 "$AI_HIST_DB" "SELECT project FROM history WHERE source='codex' AND session_id='codex-e2e'")
assert_eq "codex project" "$codex_project" "/tmp/e2e/codex"

claude_session=$(sqlite3 -separator '|' "$AI_HIST_DB" "SELECT coalesce(cwd, '<NULL>'), coalesce(git_branch, '<NULL>'), coalesce(last_assistant_text, '<NULL>') FROM sessions WHERE source='claude' AND session_id='claude-e2e'")
assert_eq "claude session metadata" "$claude_session" "/tmp/e2e/project|main|assistant summary"

grok_session=$(sqlite3 -separator '|' "$AI_HIST_DB" "SELECT coalesce(cwd, '<NULL>'), coalesce(git_branch, '<NULL>'), coalesce(last_assistant_text, '<NULL>') FROM sessions WHERE source='grok' AND session_id='grok-e2e'")
assert_eq "grok session metadata" "$grok_session" "/tmp/e2e/grok|main|grok assistant summary"

synthetic=$(sqlite3 "$AI_HIST_DB" "SELECT COUNT(*) FROM history WHERE source='grok' AND prompt LIKE '%synthetic prompt%'")
assert_eq "grok synthetic prompt count" "$synthetic" "0"

# --- session catalog -------------------------------------------------------
# Shallow discovery over the same multi-provider fake HOME, then the cache-only
# listing a desktop app paints from. Both carry the catalog contract version.
discover_json=$("$ROOT/ai-hist" sessions discover --json)

discovered_sources=$(printf '%s\n' "$discover_json" | jsonl_field \
  'd.filter((l) => l.type === "session").map((l) => l.source).sort().join(",")')
assert_eq "discovered sources" "$discovered_sources" "claude,codex,cursor,grok,opencode"

discover_contract=$(printf '%s\n' "$discover_json" | jsonl_field \
  'd.find((l) => l.type === "summary").contract_version')
assert_eq "discover contract version" "$discover_contract" "3"

discover_trailer=$(printf '%s\n' "$discover_json" | jsonl_field 'd[d.length - 1].type')
assert_eq "discover trailer" "$discover_trailer" "summary"

discover_exempt=$(printf '%s\n' "$discover_json" | jsonl_field \
  'd.find((l) => l.type === "summary").exempt_sources.map((e) => e.source).join(",")')
assert_eq "discover exemptions" "$discover_exempt" ""

# Codex session_meta provenance is observed, not invented.
codex_branch=$(printf '%s\n' "$discover_json" | jsonl_field \
  'String(d.find((l) => l.type === "session" && l.source === "codex").git_branch)')
assert_eq "codex discovered branch" "$codex_branch" "main"
codex_repo=$(printf '%s\n' "$discover_json" | jsonl_field \
  'String(d.find((l) => l.type === "session" && l.source === "codex").repo_url)')
assert_eq "codex discovered repo url" "$codex_repo" "null"

# Cursor records no per-message timestamps: mtime is reported as last activity
# and first activity stays null rather than being fabricated.
cursor_first=$(printf '%s\n' "$discover_json" | jsonl_field \
  'String(d.find((l) => l.type === "session" && l.source === "cursor").first_activity_ms)')
assert_eq "cursor first activity" "$cursor_first" "null"
cursor_last=$(printf '%s\n' "$discover_json" | jsonl_field \
  'String(d.find((l) => l.type === "session" && l.source === "cursor").last_activity_ms !== null)')
assert_eq "cursor last activity present" "$cursor_last" "true"

# A rescan re-reads nothing: unchanged stamps are served from the catalog.
rescan_reads=$("$ROOT/ai-hist" sessions discover --json | jsonl_field \
  'd.find((l) => l.type === "summary").counters.shallow_reads')
assert_eq "rescan shallow reads" "$rescan_reads" "0"
rescan_skipped=$("$ROOT/ai-hist" sessions discover --json | jsonl_field \
  'd.find((l) => l.type === "summary").skipped_unchanged')
assert_eq "rescan skipped unchanged" "$rescan_skipped" "5"

# The global limit is shared across providers, not applied per provider.
limited=$("$ROOT/ai-hist" sessions discover --json --limit 2 | jsonl_field \
  'd.filter((l) => l.type === "session").length')
assert_eq "global discover limit" "$limited" "2"

list_json=$("$ROOT/ai-hist" sessions list --json)
list_contract=$(printf '%s\n' "$list_json" | json_field 'd.contract_version')
assert_eq "sessions list contract version" "$list_contract" "3"

list_sessions=$(printf '%s\n' "$list_json" | json_field \
  'd.sessions.map((s) => `${s.source}:${s.session_id}`).sort().join(",")')
assert_eq "sessions list rows" "$list_sessions" \
  "claude:claude-e2e,codex:codex-e2e,cursor:cursor-e2e,grok:grok-e2e,opencode:opencode-e2e"

# The catalog lists newest first.
list_ordered=$(printf '%s\n' "$list_json" | json_field \
  'JSON.stringify(d.sessions.map((s) => s.last_activity_ms)) === JSON.stringify(d.sessions.map((s) => s.last_activity_ms).slice().sort((a, b) => b - a))')
assert_eq "sessions list ordering" "$list_ordered" "true"

# Sessions the full sync already ingested stay marked `full`; the ones only
# shallow discovery knows about are `shallow`.
claude_state=$(printf '%s\n' "$list_json" | json_field \
  'd.sessions.find((s) => s.source === "claude").discovery_state')
assert_eq "claude discovery state" "$claude_state" "full"
cursor_state=$(printf '%s\n' "$list_json" | json_field \
  'd.sessions.find((s) => s.source === "cursor").discovery_state')
assert_eq "cursor discovery state" "$cursor_state" "shallow"

# The derived first-prompt excerpt skips synthetic/control turns.
grok_prompt=$(printf '%s\n' "$list_json" | json_field \
  'd.sessions.find((s) => s.source === "grok").first_prompt')
assert_eq "grok first prompt" "$grok_prompt" "e2e grok release tagging prompt"

list_filtered=$("$ROOT/ai-hist" sessions list --json --source codex --limit 5 | json_field \
  'd.sessions.map((s) => s.source).join(",")')
assert_eq "sessions list source filter" "$list_filtered" "codex"

"$ROOT/ai-hist" sessions list >/dev/null

"$ROOT/ai-hist" --db "$AI_HIST_DB" tag codex-e2e release-e2e --source codex
"$ROOT/ai-hist" --db "$AI_HIST_DB" search release --tag release-e2e --json

echo "E2E verification completed with temp DB: $AI_HIST_DB"
