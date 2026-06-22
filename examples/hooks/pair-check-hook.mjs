#!/usr/bin/env node
import { execFile } from "node:child_process";
import { isAbsolute, relative } from "node:path";
import { promisify } from "node:util";

const MAX_TASK_CHARS = 800;
const DEFAULT_LIMIT = 3;
const TIMEOUT_MS = Number(process.env.PAIR_CHECK_TIMEOUT_MS || 8000);
const execFileAsync = promisify(execFile);
const SECRET_PATTERNS = [
  /\brth_[a-z]+_[A-Za-z0-9._-]+/g,
  /\bsk-[A-Za-z0-9._-]+/g,
  /\bgh[pousr]_[A-Za-z0-9_]{20,}/g,
  /\bAKIA[0-9A-Z]{16}\b/g,
  /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g,
];

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

function compact(value, limit = MAX_TASK_CHARS) {
  if (typeof value !== "string") return undefined;
  let text = value.replace(/\s+/g, " ").trim();
  for (const pattern of SECRET_PATTERNS) text = text.replace(pattern, "[REDACTED]");
  if (!text) return undefined;
  return text.length > limit ? `${text.slice(0, limit)}...` : text;
}

function normalizeFile(cwd, file) {
  if (typeof file !== "string" || !file.trim()) return undefined;
  if (!cwd || !isAbsolute(file)) return file;
  return relative(cwd, file) || file;
}

function collectFiles(cwd, input) {
  const files = new Set();
  if (!input || typeof input !== "object") return [];
  for (const key of ["file_path", "filePath", "path", "target"]) {
    const normalized = normalizeFile(cwd, input[key]);
    if (normalized) files.add(normalized);
  }
  return [...files].slice(0, 20);
}

function buildContext(event) {
  const toolInput = event.tool_input && typeof event.tool_input === "object" ? event.tool_input : {};
  const cwd = typeof event.cwd === "string" ? event.cwd : process.cwd();
  const files = collectFiles(cwd, toolInput);
  const tool = event.tool_name || event.tool || undefined;
  const target = files[0] || compact(toolInput.command, 300) || compact(toolInput.pattern, 300);
  const prompt = compact(event.prompt);
  const description = compact(toolInput.description, 300);
  const command = compact(toolInput.command, 500);
  const action = event.hook_event_name === "UserPromptSubmit" ? "prompt" : "tool";

  return {
    cwd,
    repoPath: cwd,
    task: prompt || description || command,
    files,
    tool,
    target,
    action,
    recentPrompt: prompt,
  };
}

function pushArg(args, name, value) {
  if (typeof value === "string" && value.trim()) args.push(name, value.trim());
}

async function pairCheck(context) {
  const bin = process.env.AI_HIST_PAIR_CHECK_BIN || "ai-hist";
  const args = ["pair", "check", "--json"];
  pushArg(args, "--repo-path", context.repoPath);
  pushArg(args, "--cwd", context.cwd);
  pushArg(args, "--git-remote", context.gitRemote);
  pushArg(args, "--task", context.task);
  pushArg(args, "--tool", context.tool);
  pushArg(args, "--target", context.target);
  pushArg(args, "--action", context.action);
  pushArg(args, "--recent-prompt", context.recentPrompt);
  for (const file of context.files ?? []) pushArg(args, "--file", file);
  args.push("--limit", String(process.env.PAIR_CHECK_LIMIT || DEFAULT_LIMIT));

  const { stdout } = await execFileAsync(bin, args, {
    timeout: TIMEOUT_MS,
    maxBuffer: 1024 * 1024,
    env: process.env,
  });
  const parsed = stdout.trim() ? JSON.parse(stdout) : {};
  return Array.isArray(parsed.warnings) ? { ...parsed, warnings: parsed.warnings } : { ...parsed, warnings: [] };
}

function formatWarnings(result) {
  if (!result.warnings?.length) return "";
  const lines = ["Pair advisory warnings from prior convergence events:"];
  for (const warning of result.warnings) {
    const label = [warning.kind, warning.lens].filter(Boolean).join("/");
    const score = typeof warning.score === "number" ? ` score=${warning.score.toFixed(2)}` : "";
    lines.push(`- ${label ? `[${label}${score}] ` : ""}${warning.text}`);
    for (const ev of warning.evidence ?? []) {
      const id = [ev.machineId, ev.source, ev.sessionId, ev.eventId].filter(Boolean).join(":");
      const when = ev.ts ? ` ${ev.ts}` : "";
      const snippet = ev.snippet ? ` - ${ev.snippet}` : "";
      lines.push(`  evidence: ${id || ev.eventId || "(unknown)"}${when}${snippet}`);
    }
  }
  if (result.correlationId) lines.push(`correlationId: ${result.correlationId}`);
  return lines.join("\n");
}

function outputFor(eventName, additionalContext) {
  return {
    hookSpecificOutput: {
      hookEventName: eventName,
      additionalContext,
    },
  };
}

try {
  const input = await readStdin();
  const event = input.trim() ? JSON.parse(input) : {};
  const eventName = event.hook_event_name || "UserPromptSubmit";
  const result = await pairCheck(buildContext(event));
  const warningText = formatWarnings(result);
  if (warningText) {
    process.stdout.write(`${JSON.stringify(outputFor(eventName, warningText))}\n`);
  }
} catch (err) {
  if (process.env.PAIR_CHECK_DEBUG) {
    process.stderr.write(`pair-check-hook skipped: ${err instanceof Error ? err.message : String(err)}\n`);
  }
  process.exit(0);
}
