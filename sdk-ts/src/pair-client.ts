import { execFile } from "node:child_process";
import { promisify } from "node:util";

export type PairContext = {
  projectId?: string;
  repoPath?: string;
  cwd?: string;
  gitRemote?: string;
  task?: string;
  files?: string[];
  tool?: string;
  target?: string;
  action?: string;
  recentPrompt?: string;
};

export type PairCheckRequest = {
  context: PairContext;
  limit?: number;
};

export type PairEvidence = {
  eventId?: string;
  machineId?: string;
  source?: string;
  sessionId?: string;
  kind?: string;
  lens?: string;
  ts?: string;
  snippet?: string;
};

export type PairWarning = {
  text: string;
  kind?: string;
  lens?: string;
  score?: number;
  evidence?: PairEvidence[];
};

export type PairCheckResponse = {
  decision?: "allow" | "warn";
  warnings: PairWarning[];
  correlationId?: string;
};

const execFileAsync = promisify(execFile);

function pushArg(args: string[], name: string, value: string | undefined): void {
  const trimmed = value?.trim();
  if (trimmed) args.push(name, trimmed);
}

function cliArgs(req: PairCheckRequest): string[] {
  const c = req.context;
  const args = ["pair", "check", "--json"];
  pushArg(args, "--project-id", c.projectId);
  pushArg(args, "--repo-path", c.repoPath);
  pushArg(args, "--cwd", c.cwd);
  pushArg(args, "--git-remote", c.gitRemote);
  pushArg(args, "--task", c.task);
  pushArg(args, "--tool", c.tool);
  pushArg(args, "--target", c.target);
  pushArg(args, "--action", c.action);
  pushArg(args, "--recent-prompt", c.recentPrompt);
  for (const file of c.files ?? []) pushArg(args, "--file", file);
  if (req.limit != null) args.push("--limit", String(req.limit));
  return args;
}

export async function pairCheck(req: PairCheckRequest, timeoutMs = 10_000): Promise<PairCheckResponse> {
  const bin = process.env.AI_HIST_PAIR_CHECK_BIN || "ai-hist";
  const { stdout } = await execFileAsync(bin, cliArgs(req), {
    timeout: timeoutMs,
    maxBuffer: 1024 * 1024,
    env: process.env,
  });
  const parsed = (stdout.trim() ? JSON.parse(stdout) : {}) as Partial<PairCheckResponse>;
  return {
    decision: parsed.decision,
    warnings: Array.isArray(parsed.warnings) ? parsed.warnings : [],
    correlationId: parsed.correlationId,
  };
}

export function formatPairWarnings(result: PairCheckResponse): string {
  if (result.warnings.length === 0) return "";
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
