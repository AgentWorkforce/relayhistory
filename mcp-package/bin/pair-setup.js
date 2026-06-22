#!/usr/bin/env node
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const DEFAULT_AGENTS = "all";
const MCP_SERVER_NAME = "ai-hist";

function usage() {
  return `Usage: npx -y ai-hist-mcp setup [options]

Install Pair MCP + advisory hooks for the current project.

Options:
  --agents <all|claude|codex>   Which agent configs to update (default: all)
  --project <path>              Project path passed to ai-hist-mcp (default: cwd)
  --ai-hist-bin <path>          ai-hist binary used by the Pair hook (default: ai-hist)
  --mcp-only                    Only write .mcp.json, skip hooks
  --hooks-only                  Only write hook configs, skip .mcp.json
  --dry-run                     Print planned writes without changing files
  -h, --help                    Show this help
`;
}

function parseArgs(argv) {
  const out = {
    agents: DEFAULT_AGENTS,
    project: process.cwd(),
    aiHistBin: "ai-hist",
    hooks: true,
    mcp: true,
    dryRun: false,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "-h" || arg === "--help") {
      process.stdout.write(usage());
      process.exit(0);
    }
    if (arg === "--agents") out.agents = requireValue(argv, ++i, arg);
    else if (arg === "--project") out.project = requireValue(argv, ++i, arg);
    else if (arg === "--ai-hist-bin") out.aiHistBin = requireValue(argv, ++i, arg);
    else if (arg === "--mcp-only") out.hooks = false;
    else if (arg === "--hooks-only") out.mcp = false;
    else if (arg === "--dry-run") out.dryRun = true;
    else throw new Error(`unknown option: ${arg}`);
  }

  if (!["all", "claude", "codex"].includes(out.agents)) {
    throw new Error("--agents must be one of: all, claude, codex");
  }
  return out;
}

function requireValue(argv, index, flag) {
  const value = argv[index];
  if (!value || value.startsWith("-")) throw new Error(`${flag} requires a value`);
  return value;
}

async function readJson(path, fallback) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (err) {
    if (err?.code === "ENOENT") return fallback;
    throw err;
  }
}

async function writeJson(path, value, dryRun) {
  const body = `${JSON.stringify(value, null, 2)}\n`;
  if (dryRun) {
    process.stdout.write(`[dry-run] would write ${path}\n`);
    return;
  }
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, body, { mode: 0o600 });
  process.stdout.write(`wrote ${path}\n`);
}

function shellQuote(value) {
  if (/^[A-Za-z0-9_./:=@+-]+$/.test(value)) return value;
  return `'${value.replace(/'/g, "'\\''")}'`;
}

function hookCommand(aiHistBin) {
  const command = "npx -y ai-hist-mcp hook";
  if (!aiHistBin || aiHistBin === "ai-hist") return command;
  return `AI_HIST_PAIR_CHECK_BIN=${shellQuote(aiHistBin)} ${command}`;
}

function hook(command, statusMessage) {
  return {
    type: "command",
    command,
    timeout: 10,
    ...(statusMessage ? { statusMessage } : {}),
  };
}

function hasHook(hooks, command) {
  return Array.isArray(hooks) && hooks.some((entry) =>
    Array.isArray(entry?.hooks) && entry.hooks.some((candidate) => candidate?.command === command),
  );
}

function ensureHook(settings, eventName, entry) {
  settings.hooks ??= {};
  settings.hooks[eventName] ??= [];
  const command = entry.hooks?.[0]?.command;
  if (!command || hasHook(settings.hooks[eventName], command)) return;
  settings.hooks[eventName].push(entry);
}

async function installMcp(project, dryRun) {
  const path = resolve(process.cwd(), ".mcp.json");
  const config = await readJson(path, {});
  config.mcpServers ??= {};
  config.mcpServers[MCP_SERVER_NAME] = {
    command: "npx",
    args: ["-y", "ai-hist-mcp", "--project", project],
  };
  await writeJson(path, config, dryRun);
}

async function installClaudeHooks(command, dryRun) {
  const path = resolve(process.cwd(), ".claude", "settings.json");
  const settings = await readJson(path, {});
  ensureHook(settings, "UserPromptSubmit", {
    hooks: [hook(command)],
  });
  ensureHook(settings, "PreToolUse", {
    matcher: "Edit|Write|Bash",
    hooks: [hook(command)],
  });
  await writeJson(path, settings, dryRun);
}

async function installCodexHooks(command, dryRun) {
  const path = resolve(process.cwd(), ".codex", "hooks.json");
  const settings = await readJson(path, {});
  ensureHook(settings, "UserPromptSubmit", {
    hooks: [hook(command, "Checking Pair warnings")],
  });
  ensureHook(settings, "PreToolUse", {
    matcher: "Edit|Write|apply_patch|Bash",
    hooks: [hook(command, "Checking Pair warnings")],
  });
  await writeJson(path, settings, dryRun);
}

try {
  const args = parseArgs(process.argv.slice(2));
  const project = resolve(args.project);
  const command = hookCommand(args.aiHistBin);

  if (args.mcp) await installMcp(project, args.dryRun);
  if (args.hooks && (args.agents === "all" || args.agents === "claude")) {
    await installClaudeHooks(command, args.dryRun);
  }
  if (args.hooks && (args.agents === "all" || args.agents === "codex")) {
    await installCodexHooks(command, args.dryRun);
  }

  process.stdout.write("Pair setup complete. Restart your agent session so it reloads MCP/hooks.\n");
} catch (err) {
  process.stderr.write(`ai-hist pair setup failed: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
}
