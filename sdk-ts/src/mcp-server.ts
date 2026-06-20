#!/usr/bin/env node
/**
 * ai-hist MCP server — exposes AI coding agent history as MCP tools.
 *
 * Covers Claude Code, Codex, Cursor, and Agent Relay in a single index.
 * Uses the ai-hist SQLite database when present ($AI_HIST_DB or the
 * default ~/.local/share/ai-hist/ai-history.db), falling back to
 * scanning local JSONL source files directly so the server works without
 * running `ai-hist sync` first.
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { resolve } from "node:path";
import { z } from "zod";
import { openAiHist, resumeCommand, type AiHist, type TrajectoryEntry, type HandoffCandidate } from "./index.js";

// ---------------------------------------------------------------------------
// Lazy singleton — opens on first tool call and reuses across all calls.
// ---------------------------------------------------------------------------

let _histPromise: Promise<AiHist> | null = null;

function normalizeProjectArg(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  if (!trimmed) return undefined;
  return resolve(trimmed);
}

function configuredProjectScope(): string | undefined {
  for (let i = 2; i < process.argv.length; i++) {
    const arg = process.argv[i];
    if (arg === "--project" || arg === "--project-path") {
      const next = process.argv[i + 1];
      if (next && !next.startsWith("-")) {
        const value = normalizeProjectArg(next);
        if (value) return value;
        i++;
      }
      continue;
    }
    for (const prefix of ["--project=", "--project-path="]) {
      if (arg.startsWith(prefix)) {
        const value = normalizeProjectArg(arg.slice(prefix.length));
        if (value) return value;
      }
    }
  }
  return undefined;
}

function getHist(): Promise<AiHist> {
  if (!_histPromise) {
    _histPromise = openAiHist({ fallback: "jsonl", projectScope: configuredProjectScope() }).catch((err) => {
      _histPromise = null;
      throw err;
    });
  }
  return _histPromise;
}

function fmtEntry(
  e: { id: number; source: string; sessionId: string | null; project: string | null; prompt: string; timestampMs: number },
  maxChars = 300,
): string {
  const dt = new Date(e.timestampMs).toISOString().slice(0, 16).replace("T", " ");
  const proj = e.project ? `  [${e.project}]` : "";
  const text =
    e.prompt.length > maxChars
      ? e.prompt.slice(0, maxChars).replace(/\n/g, " ") + "..."
      : e.prompt.replace(/\n/g, " ");
  return `#${e.id}  ${dt}  (${e.source})${proj}\n  session_id: ${e.sessionId ?? "(none)"}\n  ${text}`;
}

function fmtTrajectory(t: TrajectoryEntry, maxChars = 500): string {
  const dt = new Date(t.timestampMs).toISOString().slice(0, 16).replace("T", " ");
  const title = t.task.title ?? "(untitled task)";
  const project = t.projectId ? `  [${t.projectId}]` : "";
  const persona = t.personaId ? `  persona: ${t.personaId}` : "";
  const decisions = t.decisions
    .map((d, index) => {
      const alternatives = d.alternatives.length > 0 ? ` alternatives: ${d.alternatives.join(", ")}` : "";
      return `${index + 1}. ${d.question}\n   chosen: ${d.chosen}\n   reasoning: ${d.reasoning}${alternatives}`;
    })
    .join("\n");
  const retroParts = [
    t.retrospective.summary ? `summary: ${t.retrospective.summary}` : "",
    t.retrospective.approach ? `approach: ${t.retrospective.approach}` : "",
    t.retrospective.learnings.length > 0 ? `learnings: ${t.retrospective.learnings.join("; ")}` : "",
    t.retrospective.confidence != null ? `confidence: ${String(t.retrospective.confidence)}` : "",
  ].filter(Boolean);
  const body = [
    `trajectory_id: ${t.id}`,
    `${dt}${project}${persona}`,
    `task: ${title}`,
    t.task.description ? `description: ${t.task.description}` : "",
    decisions ? `decisions:\n${decisions}` : "",
    retroParts.length > 0 ? `retrospective:\n${retroParts.join("\n")}` : "",
  ].filter(Boolean).join("\n");
  return body.length > maxChars ? body.slice(0, maxChars).replace(/\n/g, " ") + "..." : body;
}

const READ_ONLY = { readOnlyHint: true, idempotentHint: true, openWorldHint: false } as const;

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

const server = new McpServer(
  { name: "ai-hist", version: "1.0.0" },
  { capabilities: { tools: {}, prompts: {} } },
);

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

server.prompt(
  "agent-history-guide",
  "How to search and use your AI coding agent history via ai-hist",
  () => ({
    messages: [
      {
        role: "user" as const,
        content: {
          type: "text" as const,
          text: `You are connected to the ai-hist MCP server, which gives you access to a
unified history of AI coding agent sessions across Claude Code, Codex, Cursor,
and Agent Relay — all searchable in a single index.

## Available tools

- **search_history** — Full-text search across all prompts. Start here when looking
  for past work on a specific topic, bug, feature, or keyword. Supports boolean
  operators (AND, OR, NOT) and prefix wildcards (*).

- **get_session** — Read the full conversation thread for a session_id. Use after
  search_history or recent_history to see the complete context of a session, and
  to get the exact CLI command to resume it.

- **get_context** — Reconstruct what you were working on at a specific moment in
  time. Shows the full session the entry belongs to, plus nearby entries from other
  sessions within a configurable time window.

- **recent_history** — Browse what was worked on recently across all agents, or
  filter by source and project.

- **pack_evidence** — Assemble a concise, token-budget-aware summary of the most
  relevant past sessions before starting a new task. Each entry includes a resume
  command so you can continue where you left off.

- **history_stats** — Get a quick overview of what is indexed: total counts by
  source, date range, and top projects.

- **search_trajectories** — Search compacted per-run decision trajectories: the
  WHY behind persona work, including decisions and retrospectives.

- **why_for_task** — Return the best matching trajectory's decisions and
  retrospective for a task query.

- **get_handoff** — Find sessions for a repo/branch and generate a warm-start
  handoff brief so you can continue work in a different CLI. Use this when you
  need to switch from Claude Code to Codex (or vice versa) because one is down
  or quota-exhausted. Ask: "where did codex leave off on branch feat/auth in
  /my-repo?" and this tool returns ranked candidates with resume commands and
  a ready-to-paste warm-start command for the target CLI.

## Tips

- If a tool returns "database not found", the user should run \`ai-hist sync\` to
  populate the database from their local history files.
- session_id values from search results can be passed directly to get_session.
- Use pack_evidence before starting a complex task to pull in relevant prior context.`,
        },
      },
    ],
  }),
);

// ---------------------------------------------------------------------------
// Tools
// ---------------------------------------------------------------------------

server.tool(
  "search_history",
  "Search your AI coding agent history using full-text search across all prompts " +
    "from Claude Code, Codex, Cursor, and Agent Relay. Returns matching entries with " +
    "source, project path, session ID, and timestamp ordered by most recent first. " +
    "Supports FTS5 boolean operators (AND, OR, NOT), leading - to exclude a term, and " +
    "trailing * for prefix matching. Use get_session with a returned session_id to read " +
    "the full conversation.",
  {
    query: z
      .string()
      .describe(
        "Search query. Plain terms are matched literally. Use AND/OR/NOT (uppercase) for " +
          'boolean, leading - to exclude, trailing * for prefix. ' +
          'Examples: "authentication", "auth AND login", "deploy*", "refactor -test"',
      ),
    source: z
      .enum(["claude", "codex", "cursor", "relay"])
      .optional()
      .describe("Filter to a single agent source. Omit to search all sources."),
    project: z
      .string()
      .optional()
      .describe(
        'Filter by project directory path. Substring match — use a partial path like "/myproject" or "src/api".',
      ),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .optional()
      .default(20)
      .describe("Maximum number of results to return. Default: 20."),
  },
  READ_ONLY,
  async ({ query, source, project, limit }) => {
    try {
      const hist = await getHist();
      const results = hist.search(query, { source, project, limit });
      if (results.length === 0) return { content: [{ type: "text", text: "No results found." }] };
      const lines = [`Found ${results.length} result(s):\n`];
      for (const e of results) lines.push(fmtEntry(e));
      return { content: [{ type: "text", text: lines.join("\n\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "search_trajectories",
  "Search compacted per-run decision trajectories: task title/description, decisions, " +
    "alternatives, reasoning, and retrospective summaries/learnings. Use this for the WHY " +
    "behind prior persona work.",
  {
    query: z.string().describe("Search query for decisions, task text, personaId, projectId, or retrospectives."),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .optional()
      .default(10)
      .describe("Maximum number of trajectory results to return. Default: 10."),
  },
  READ_ONLY,
  async ({ query, limit }) => {
    try {
      const hist = await getHist();
      const results = hist.searchTrajectories(query, { limit });
      if (results.length === 0) {
        return { content: [{ type: "text", text: "No trajectories found." }] };
      }
      const lines = [`Found ${results.length} trajectory result(s):\n`];
      for (const t of results) lines.push(fmtTrajectory(t));
      return { content: [{ type: "text", text: lines.join("\n\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "why_for_task",
  "Return the decisions and retrospective for the best-matching compacted trajectory. " +
    "Use this when starting a task and you need the prior WHY: question, chosen option, " +
    "reasoning, alternatives, summary, approach, learnings, and confidence.",
  {
    query: z.string().describe("Task or decision query to match against compacted trajectories."),
  },
  READ_ONLY,
  async ({ query }) => {
    try {
      const hist = await getHist();
      const trajectory = hist.whyForTask(query);
      if (!trajectory) {
        return { content: [{ type: "text", text: "No matching trajectory found." }] };
      }
      return { content: [{ type: "text", text: JSON.stringify(trajectory, null, 2) }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "get_session",
  "Retrieve all prompts from a specific session ordered oldest to newest. " +
    "Use this after search_history or recent_history to read the full conversation " +
    "thread for a session_id. Returns all prompts with timestamps and includes the " +
    "exact CLI command (claude --resume, codex resume, etc.) to continue that session.",
  {
    session_id: z
      .string()
      .describe(
        "Session ID to retrieve. Obtain from the session_id field in search_history or recent_history results.",
      ),
  },
  READ_ONLY,
  async ({ session_id }) => {
    try {
      const hist = await getHist();
      const entries = hist.getSession(session_id);
      if (entries.length === 0) {
        return { content: [{ type: "text", text: `No entries found for session ${session_id}.` }] };
      }
      const first = entries[0];
      const resume = resumeCommand(first);
      const lines: string[] = [
        `Session ${session_id} — ${entries.length} entries  (${first.source}${first.project ? ", " + first.project : ""})`,
      ];
      if (resume) lines.push(`Resume: ${resume}`);
      lines.push("");
      for (const e of entries) {
        const dt = new Date(e.timestampMs).toISOString().slice(0, 16).replace("T", " ");
        lines.push(`[${dt}] #${e.id}\n${e.prompt}`);
      }
      return { content: [{ type: "text", text: lines.join("\n\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "get_context",
  "Get temporal context around a specific history entry: all prompts from the same " +
    "session, plus prompts from other sessions within a configurable time window. " +
    "Use this when you remember roughly when you worked on something but not the exact " +
    "search terms — useful for reconstructing the full picture of a working session. " +
    "Obtain an entry ID from search_history or recent_history.",
  {
    id: z
      .number()
      .int()
      .describe("History entry ID. Obtain from the id field in search_history or recent_history results."),
    window_minutes: z
      .number()
      .int()
      .min(0)
      .max(60)
      .optional()
      .default(5)
      .describe("Minutes before and after the entry to include from other sessions. Default: 5."),
  },
  READ_ONLY,
  async ({ id, window_minutes }) => {
    try {
      const hist = await getHist();
      const entry = hist.getEntry(id);
      if (!entry) {
        return { content: [{ type: "text", text: `No entry with id ${id}.` }] };
      }
      const lines: string[] = [];
      if (entry.sessionId) {
        const sessionEntries = hist.getSession(entry.sessionId);
        lines.push(`=== Session ${entry.sessionId} (${sessionEntries.length} entries) ===`);
        for (const e of sessionEntries) {
          const dt = new Date(e.timestampMs).toISOString().slice(0, 16).replace("T", " ");
          const marker = e.id === id ? ">>>" : "   ";
          lines.push(`${marker} [${dt}] #${e.id} (${e.source})  ${e.prompt.slice(0, 150).replace(/\n/g, " ")}`);
        }
      }
      const windowMs = window_minutes * 60 * 1000;
      const nearby = hist
        .getInTimeWindow(entry.timestampMs, windowMs)
        .filter((e) => e.sessionId !== entry.sessionId && e.id !== id);
      if (nearby.length > 0) {
        lines.push(`\n=== Nearby (±${window_minutes}min, other sessions) ===`);
        for (const e of nearby) {
          const dt = new Date(e.timestampMs).toISOString().slice(0, 16).replace("T", " ");
          lines.push(`  [${dt}] #${e.id} (${e.source})  ${e.prompt.slice(0, 150).replace(/\n/g, " ")}`);
        }
      }
      return {
        content: [{ type: "text", text: lines.length > 0 ? lines.join("\n") : "No context found." }],
      };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "recent_history",
  "List the most recent AI coding agent history entries across all sources, newest first. " +
    "Use this to see what was worked on recently, to get a history entry ID for use with " +
    "get_context, or to browse history without a specific keyword in mind. " +
    "Optionally filter by agent source or project path.",
  {
    n: z
      .number()
      .int()
      .min(1)
      .max(100)
      .optional()
      .default(20)
      .describe("Number of entries to return. Default: 20."),
    source: z
      .enum(["claude", "codex", "cursor", "relay"])
      .optional()
      .describe("Filter to a specific agent source. Omit for all sources."),
    project: z
      .string()
      .optional()
      .describe('Filter by project directory path (substring match). Example: "/my-app" or "work/api".'),
  },
  READ_ONLY,
  async ({ n, source, project }) => {
    try {
      const hist = await getHist();
      const entries = hist.recent({ limit: n, source, project });
      if (entries.length === 0) return { content: [{ type: "text", text: "No history entries found." }] };
      const lines = [`Most recent ${entries.length} entries:\n`];
      for (const e of entries) lines.push(fmtEntry(e, 150));
      return { content: [{ type: "text", text: lines.join("\n\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "recent_entries",
  "List the most recent AI coding agent history entries across all sources, newest first. " +
    "This is the contract alias for recent_history.",
  {
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .optional()
      .default(20)
      .describe("Number of entries to return. Default: 20."),
    project: z
      .string()
      .optional()
      .describe('Filter by project directory/path or projectId (exact match in the SDK).'),
  },
  READ_ONLY,
  async ({ limit, project }) => {
    try {
      const hist = await getHist();
      const entries = hist.recent({ limit, project });
      if (entries.length === 0) return { content: [{ type: "text", text: "No history entries found." }] };
      const lines = [`Most recent ${entries.length} entries:\n`];
      for (const e of entries) lines.push(fmtEntry(e, 150));
      return { content: [{ type: "text", text: lines.join("\n\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "pack_evidence",
  "Search history and assemble a concise evidence bundle ready to paste into a new " +
    "LLM context window. Returns numbered entries with timestamps, source, project path, " +
    "truncated prompt text, and the exact resume command for each session. " +
    "Use this before starting work on a task to pull in relevant context from past sessions " +
    "without exceeding token limits. Adjust tokens_per_entry to control how much of each " +
    "prompt is included.",
  {
    query: z.string().describe("Search query to find relevant past sessions."),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .optional()
      .default(10)
      .describe("Maximum number of entries in the bundle. Default: 10."),
    tokens_per_entry: z
      .number()
      .int()
      .min(50)
      .max(2000)
      .optional()
      .default(200)
      .describe("Approximate token budget per entry's prompt text (~4 chars/token). Default: 200."),
  },
  READ_ONLY,
  async ({ query, limit, tokens_per_entry }) => {
    try {
      const hist = await getHist();
      const entries = hist.search(query, { limit });
      if (entries.length === 0) return { content: [{ type: "text", text: `No results for "${query}".` }] };
      const charsPerEntry = tokens_per_entry * 4;
      const now = new Date().toISOString().slice(0, 16).replace("T", " ");
      const lines = [`=== ai-hist pack: "${query}" | ${now} | ${entries.length} entries ===\n`];
      for (let i = 0; i < entries.length; i++) {
        const e = entries[i];
        const dt = new Date(e.timestampMs).toISOString().slice(0, 16).replace("T", " ");
        const proj = e.project ? `  ${e.project}` : "";
        const text =
          e.prompt.length > charsPerEntry
            ? e.prompt.slice(0, charsPerEntry).replace(/\n/g, " ") + "..."
            : e.prompt.replace(/\n/g, " ");
        const resume = resumeCommand(e);
        let entry = `[${i + 1}/${entries.length}] #${e.id}  ${dt}  ${e.source}${proj}\n      ${text}`;
        if (resume) entry += `\n      Resume: ${resume}`;
        lines.push(entry);
      }
      return { content: [{ type: "text", text: lines.join("\n\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "history_stats",
  "Return a summary of your AI coding agent history: total entry count broken down by " +
    "source, the date range of recorded sessions, and the ten most-active projects. " +
    "Use this to confirm that ai-hist sync has run successfully, to see which agents have " +
    "the most history, or to get an overview before deciding where to search.",
  READ_ONLY,
  async () => {
    try {
      const hist = await getHist();
      const s = hist.stats();
      const lines = [`Total entries: ${s.total.toLocaleString()}\n`];
      lines.push("By source:");
      for (const [src, count] of Object.entries(s.bySource)) {
        lines.push(`  ${src}: ${(count ?? 0).toLocaleString()}`);
      }
      if (s.firstTimestampMs && s.lastTimestampMs) {
        const first = new Date(s.firstTimestampMs).toISOString().slice(0, 10);
        const last = new Date(s.lastTimestampMs).toISOString().slice(0, 10);
        lines.push(`\nDate range: ${first} → ${last}`);
      }
      if (s.byProject.length > 0) {
        lines.push("\nTop projects:");
        for (const p of s.byProject) {
          lines.push(`  ${String(p.count).padStart(6)}  ${p.project}`);
        }
      }
      lines.push(`\nData source: ${hist.sourceKind} (${hist.dbPath})`);
      if (hist.projectScope) lines.push(`Project scope: ${hist.projectScope}`);
      return { content: [{ type: "text", text: lines.join("\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "stats",
  "Contract alias for history_stats. Return total counts by source, date range, top projects, and data source.",
  READ_ONLY,
  async () => {
    try {
      const hist = await getHist();
      const s = hist.stats();
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(
              {
                total: s.total,
                bySource: s.bySource,
                byProject: s.byProject,
                firstTimestampMs: s.firstTimestampMs,
                lastTimestampMs: s.lastTimestampMs,
                sourceKind: hist.sourceKind,
                dbPath: hist.dbPath,
                projectScope: hist.projectScope,
              },
              null,
              2,
            ),
          },
        ],
      };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

server.tool(
  "get_handoff",
  "Find active sessions for a repo/branch and generate a warm-start handoff brief " +
    "so you can continue work in a different CLI (e.g. pick up a Claude session in Codex " +
    "or vice versa). Returns ranked candidate sessions with: the original goal, files " +
    "touched (extracted from prompts), last user prompt, last assistant state, the resume " +
    "command for the original CLI, and a ready-to-paste warm-start command for the target CLI. " +
    "Use this when switching between Claude Code and Codex mid-task due to quota or outage.",
  {
    repo: z
      .string()
      .optional()
      .describe(
        "Project directory path to filter by (substring match against session cwd). " +
          'Example: "/my-app", "work/api". Omit to search across all projects.',
      ),
    branch: z
      .string()
      .optional()
      .describe(
        "Git branch name to filter by (substring match). " +
          'Example: "feat/auth", "main". Requires ai-hist sync to have captured gitBranch.',
      ),
    source: z
      .enum(["claude", "codex"])
      .optional()
      .describe(
        "Filter to sessions from a specific CLI. Omit to search both Claude Code and Codex.",
      ),
    limit: z
      .number()
      .int()
      .min(1)
      .max(10)
      .optional()
      .default(3)
      .describe("Maximum number of handoff candidates to return. Default: 3."),
  },
  READ_ONLY,
  async ({ repo, branch, source, limit }) => {
    try {
      const hist = await getHist();
      const candidates = hist.getHandoff({ repo, branch, source, limit });
      if (candidates.length === 0) {
        return {
          content: [
            {
              type: "text",
              text:
                "No sessions found matching the given filters. " +
                "Run `ai-hist sync` to populate session metadata, then try again.",
            },
          ],
        };
      }
      const lines: string[] = [`Found ${candidates.length} handoff candidate(s):\n`];
      for (let i = 0; i < candidates.length; i++) {
        const c = candidates[i];
        let dt = "unknown";
        if (c.lastActivityMs != null && Number.isFinite(c.lastActivityMs)) {
          dt = new Date(c.lastActivityMs).toISOString().slice(0, 16).replace("T", " ");
        }
        lines.push(
          `[${i + 1}] ${c.source.toUpperCase()} session  confidence: ${(c.confidence * 100).toFixed(0)}%`,
        );
        lines.push(`  session_id : ${c.sessionId}`);
        lines.push(`  cwd        : ${c.cwd ?? "(unknown)"}`);
        lines.push(`  branch     : ${c.gitBranch ?? "(unknown)"}`);
        lines.push(`  last seen  : ${dt}  (${c.promptCount} prompts)`);
        lines.push(`  goal       : ${c.goal.slice(0, 150)}`);
        lines.push(`  last state : ${c.lastState.slice(0, 150)}`);
        if (c.filesTouched.length > 0) {
          lines.push(`  files      : ${c.filesTouched.slice(0, 8).join(", ")}`);
        }
        if (c.lastAssistantText) {
          lines.push(`  assistant  : ${c.lastAssistantText.slice(0, 200).replace(/\n/g, " ")}`);
        }
        if (c.resumeCommand) {
          lines.push(`  resume     : ${c.resumeCommand}`);
        }
        lines.push(`  warm-start : ${c.warmStartCommand}`);
      }
      return { content: [{ type: "text", text: lines.join("\n") }] };
    } catch (err) {
      return { content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true };
    }
  },
);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const transport = new StdioServerTransport();
await server.connect(transport);
