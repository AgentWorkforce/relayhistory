# Pair In-Session Warnings

Pair asks relayhistory-cloud for advisory warnings before a prompt or tool action. The
client sends minimal current context to `POST /v1/pair/check`; it does not upload raw
transcripts. The server returns scrubbed, cited convergence-event warnings.

## Authentication And CLI Primitive

Pair hook and MCP wrappers shell out to the Rust CLI primitive:

```bash
ai-hist pair check --json --task "refactor auth middleware" --file src/auth/middleware.ts
```

That primitive owns relayhistory-cloud auth and reads the same auth file as cloud sync:

```bash
RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-dev" ai-hist admin-mint ...
RELAYHISTORY_HOME="$HOME/.agentworkforce/relayhistory-dev" ai-hist push --json
```

Set `AI_HIST_PAIR_CHECK_BIN=/path/to/ai-hist` if `ai-hist` is not on `PATH`.

## MCP Tool

Install the `pair_check` MCP tool and advisory hooks with one command from the project you
want Pair scoped to:

```bash
npx -y ai-hist-mcp setup
```

The installer:

- writes `.mcp.json` with an `ai-hist` MCP server;
- writes Claude Code and Codex advisory hook config when requested;
- scopes the MCP server to the current project path;
- is safe to re-run (entries are merged, not duplicated);
- writes no tokens or secrets to MCP/hook config.

Options:

```bash
npx -y ai-hist-mcp setup --agents claude       # only Claude Code hooks
npx -y ai-hist-mcp setup --agents codex        # only Codex hooks
npx -y ai-hist-mcp setup --mcp-only            # only .mcp.json
npx -y ai-hist-mcp setup --hooks-only          # only hooks
npx -y ai-hist-mcp setup --project /path/to/repo
npx -y ai-hist-mcp setup --dry-run
```

After install, restart your agent session, ask your client to list MCP tools (`/mcp` in
Claude Code/Codex), and confirm `pair_check` is available.

The `ai-hist-mcp` server shells to `ai-hist pair check --json` and returns formatted
warnings:

```json
{
  "projectId": "github.com/org/repo",
  "repoPath": "/work/repo",
  "cwd": "/work/repo",
  "task": "refactor auth middleware token check",
  "files": ["src/auth/middleware.ts"],
  "tool": "Edit",
  "target": "src/auth/middleware.ts",
  "action": "edit",
  "limit": 5
}
```

`projectId` is optional. Local hooks should send `cwd` or `repoPath` when no canonical
project id is known.

## Advanced: Claude Code Hooks

The example command hook supports `UserPromptSubmit` and `PreToolUse`. It returns
`hookSpecificOutput.additionalContext` only, so Pair warnings are advisory and do not
approve, deny, or block tool calls.

The setup command above is the recommended path. The JSON below is only for auditing what
the installer writes or for unusual client setups.

`.claude/settings.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "npx -y ai-hist-mcp hook",
            "timeout": 10
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Edit|Write|Bash",
        "hooks": [
          {
            "type": "command",
            "command": "npx -y ai-hist-mcp hook",
            "timeout": 10
          }
        ]
      }
    ]
  }
}
```

## Advanced: Codex Hooks

Codex discovers hooks in `.codex/hooks.json` or inline config. The same command works for
Codex `UserPromptSubmit` and `PreToolUse`; Codex adds the returned
`additionalContext` as developer context.

`.codex/hooks.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "npx -y ai-hist-mcp hook",
            "timeout": 10,
            "statusMessage": "Checking Pair warnings"
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Edit|Write|apply_patch|Bash",
        "hooks": [
          {
            "type": "command",
            "command": "npx -y ai-hist-mcp hook",
            "timeout": 10,
            "statusMessage": "Checking Pair warnings"
          }
        ]
      }
    ]
  }
}
```

After changing Codex hooks, use `/hooks` in Codex to review and trust the command.

## Request Contract

```json
{
  "context": {
    "projectId": "proj-auth-svc",
    "repoPath": "/work/auth-svc",
    "cwd": "/work/auth-svc",
    "gitRemote": "git@github.com:org/auth-svc.git",
    "task": "refactor auth middleware token check",
    "files": ["src/auth/middleware.ts"],
    "tool": "Edit",
    "target": "src/auth/middleware.ts",
    "action": "edit",
    "recentPrompt": "short prompt summary"
  },
  "limit": 5
}
```

Expected response:

```json
{
  "decision": "warn",
  "warnings": [
    {
      "text": "Prior work found permissions config must be updated when auth middleware changes.",
      "kind": "reflection",
      "lens": "trajectories",
      "score": 0.91,
      "evidence": [
        {
          "machineId": "m_...",
          "source": "trajectory",
          "sessionId": "tA",
          "eventId": "reflection:tA:suggestion:0",
          "ts": "2026-06-21T20:00:00Z",
          "snippet": "update permissions config when editing auth middleware"
        }
      ]
    }
  ],
  "correlationId": "pair_..."
}
```

No-warning responses should be valid as:

```json
{ "decision": "allow", "warnings": [], "correlationId": "pair_..." }
```
