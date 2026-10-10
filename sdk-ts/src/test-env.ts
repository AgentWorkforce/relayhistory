// Test-only helper (not in the published `files` list): isolate a test from
// the operator's real history.

/**
 * Every environment variable that points RelayHistory at a history root or a
 * store: `HOME`/`USERPROFILE` and the per-provider overrides the Rust
 * `crates/ai-hist/src/paths.rs` reads (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`,
 * `GROK_HOME`, `XDG_DATA_HOME`, `OPENCODE_DB`, `OPENCODE_STORAGE_DIR`),
 * plus the SDK's own `AI_HIST_*` and `RELAYHISTORY_*`. A variable left set would let a sync ingest a developer's
 * real sessions into a test database.
 */
export const HISTORY_ENV_PATTERN =
  /^(HOME|USERPROFILE|CLAUDE_CONFIG_DIR|CODEX_HOME|GROK_HOME|XDG_|OPENCODE_|AI_HIST_|RELAYHISTORY_)/;

/**
 * Delete every history-root variable from `process.env` and return a function
 * that restores the environment exactly as it was, removing anything the test
 * set in between.
 */
export function scrubHistoryEnv(): () => void {
  const saved = { ...process.env };
  for (const key of Object.keys(process.env)) {
    if (HISTORY_ENV_PATTERN.test(key)) delete process.env[key];
  }
  return () => {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
  };
}
