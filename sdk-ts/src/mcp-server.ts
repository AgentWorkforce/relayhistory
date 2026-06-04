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
import { z } from "zod";
import { openAiHist, resumeCommand, type AiHist } from "./index.js";

// ---------------------------------------------------------------------------
// Lazy singleton — opens on first tool call and reuses across all calls.
// ---------------------------------------------------------------------------

let _histPromise: Promise<AiHist> | null = null;

function getHist(): Promise<AiHist> {
  if (!_histPromise) {
    _histPromise = openAiHist({ fallback: "jsonl" }).catch((err) => {
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
