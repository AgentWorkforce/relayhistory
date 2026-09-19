// Fabricate a deterministic multi-provider agent-history store for the
// sync/hydration benchmark.
//
//   node scripts/gen-synthetic-history.mjs --out /tmp/bench-home \
//     --target-bytes 104857600 --large-session-bytes 52428800 --seed 176
//
// The store is a fake HOME: `.claude/projects/…`, `.codex/sessions/…`,
// `.cursor/projects/…`, `.grok/sessions/…`, and — only when `node:sqlite` is
// available (Node 22.13+) — `.local/share/opencode/opencode.db`. Nothing real
// is read or written; every byte comes from the seeded generator in
// `benchmark-sync-lib.mjs`, so the same seed and target reproduce the same
// store on any machine. Generated stores are scratch data and must never be
// committed.
//
// A `manifest.json` beside the store records the exact byte and record counts
// the benchmark reports throughput against, plus the session the hydration
// phases target.

import { mkdir, rm, writeFile, stat, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  FILE_SOURCES,
  SOURCES,
  claudeHistoryLine,
  claudeTranscript,
  codexHistoryLine,
  codexRollout,
  createRng,
  cursorTranscript,
  grokSession,
  planStore,
} from "./benchmark-sync-lib.mjs";

const BASE_MS = Date.UTC(2026, 8, 1, 9, 0, 0);

function option(argv, name, fallback) {
  const prefix = `--${name}=`;
  const inline = argv.find((argument) => argument.startsWith(prefix));
  if (inline) return inline.slice(prefix.length);
  const index = argv.indexOf(`--${name}`);
  if (index >= 0 && argv[index + 1]) return argv[index + 1];
  return fallback;
}

function integer(argv, name, fallback) {
  const raw = option(argv, name, undefined);
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`--${name} must be a non-negative integer, got ${raw}`);
  }
  return value;
}

async function write(path, contents) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, contents, "utf8");
  return Buffer.byteLength(contents, "utf8");
}

/** Total bytes of every regular file under `root`. */
export async function storeBytes(root) {
  let total = 0;
  let files = 0;
  const walk = async (directory) => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile()) {
        total += (await stat(path)).size;
        files += 1;
      }
    }
  };
  await walk(root);
  return { bytes: total, files };
}

/** Whether this Node can write the OpenCode SQLite fixture. */
export async function opencodeAvailable() {
  try {
    await import("node:sqlite");
    return true;
  } catch {
    return false;
  }
}

async function writeOpencodeStore(root, sessions, rng) {
  const { DatabaseSync } = await import("node:sqlite");
  const path = join(root, ".local/share/opencode/opencode.db");
  await mkdir(dirname(path), { recursive: true });
  const db = new DatabaseSync(path);
  db.exec(`
    CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
    CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
    CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
    CREATE INDEX session_time_updated_id_idx ON session(time_updated DESC, id);
    CREATE INDEX message_session_time_created_id_idx ON message(session_id, time_created, id);
    CREATE INDEX part_session_idx ON part(session_id);
    CREATE INDEX part_message_id_id_idx ON part(message_id, id);
  `);
  const insertSession = db.prepare("INSERT INTO session VALUES (?, ?, ?, ?)");
  const insertMessage = db.prepare("INSERT INTO message VALUES (?, ?, ?, ?)");
  const insertPart = db.prepare("INSERT INTO part VALUES (?, ?, ?, ?, ?)");
  db.exec("BEGIN");
  for (let index = 0; index < sessions; index += 1) {
    const id = `oc-${index.toString().padStart(6, "0")}`;
    const created = BASE_MS + index;
    insertSession.run(id, "/work/relayhistory-bench", created, created + 1000);
    const message = `msg-${index}`;
    insertMessage.run(message, id, created, JSON.stringify({ role: "user", modelID: "claude-sonnet" }));
    insertPart.run(
      `prt-${index}`, message, id, created,
      JSON.stringify({ type: "text", text: `opencode prompt ${index} ${Math.floor(rng() * 1e6)}` }),
    );
  }
  db.exec("COMMIT");
  db.close();
  return sessions;
}

/**
 * Grow `root` to roughly `plan.targetBytes` by adding whole sessions round
 * robin across the requested file-backed sources. Session count is an outcome,
 * not an input, so a size target reproduces exactly.
 */
export async function generateStore(plan, root) {
  await rm(root, { recursive: true, force: true });
  await mkdir(root, { recursive: true });
  const rng = createRng(plan.seed);
  const fileSources = plan.sources.filter((source) => FILE_SOURCES.includes(source));
  if (fileSources.length === 0) throw new Error("at least one file-backed source is required");
  const sessions = [];
  let written = 0;
  let claudeHistory = "";
  let codexHistory = "";

  // One oversized Claude transcript first: the hydration phases need a single
  // session large enough for per-transcript cost to dominate.
  if (plan.largeSessionBytes > 0) {
    const id = "bench-large-0000";
    // The per-turn byte cost is stable for a given plan, so measure one turn
    // and scale, then trim nothing: the manifest reports what landed.
    const probe = claudeTranscript(plan, id, createRng(plan.seed), { turns: 4, baseMs: BASE_MS });
    const perTurn = Buffer.byteLength(probe, "utf8") / 4;
    const turns = Math.max(1, Math.round(plan.largeSessionBytes / perTurn));
    const path = join(root, `.claude/projects/${plan.project}/${id}.jsonl`);
    const bytes = await write(path, claudeTranscript(plan, id, createRng(plan.seed + 1), { turns, baseMs: BASE_MS }));
    claudeHistory += claudeHistoryLine(plan, id, rng, BASE_MS);
    sessions.push({ source: "claude", sessionId: id, path, bytes, turns });
    written += bytes;
  }

  let index = 0;
  while (written < plan.targetBytes) {
    const source = fileSources[index % fileSources.length];
    const ordinal = Math.floor(index / fileSources.length);
    const id = `bench-${source}-${ordinal.toString().padStart(6, "0")}`;
    const baseMs = BASE_MS + index * 60_000;
    if (source === "claude") {
      const path = join(root, `.claude/projects/${plan.project}/${id}.jsonl`);
      const bytes = await write(path, claudeTranscript(plan, id, rng, { baseMs }));
      claudeHistory += claudeHistoryLine(plan, id, rng, baseMs);
      sessions.push({ source, sessionId: id, path, bytes, turns: plan.turns });
      written += bytes;
    } else if (source === "codex") {
      const day = new Date(baseMs);
      const path = join(
        root,
        `.codex/sessions/${day.getUTCFullYear()}/${String(day.getUTCMonth() + 1).padStart(2, "0")}/` +
        `${String(day.getUTCDate()).padStart(2, "0")}/rollout-${id}.jsonl`,
      );
      const bytes = await write(path, codexRollout(plan, id, rng, { baseMs }));
      codexHistory += codexHistoryLine(plan, id, rng, baseMs);
      sessions.push({ source, sessionId: id, path, bytes, turns: plan.turns });
      written += bytes;
    } else if (source === "cursor") {
      const path = join(root, `.cursor/projects/${plan.project}/agent-transcripts/${id}/${id}.jsonl`);
      const bytes = await write(path, cursorTranscript(plan, id, rng, { baseMs }));
      sessions.push({ source, sessionId: id, path, bytes, turns: plan.turns });
      written += bytes;
    } else {
      const { summary, chat } = grokSession(plan, id, rng, { baseMs });
      const directory = join(root, `.grok/sessions/%2Fwork%2Frelayhistory-bench/${id}`);
      const bytes = (await write(join(directory, "summary.json"), summary))
        + (await write(join(directory, "chat_history.jsonl"), chat));
      sessions.push({ source, sessionId: id, path: join(directory, "chat_history.jsonl"), bytes, turns: plan.turns });
      written += bytes;
    }
    index += 1;
  }

  if (claudeHistory) written += await write(join(root, ".claude/history.jsonl"), claudeHistory);
  if (codexHistory) written += await write(join(root, ".codex/history.jsonl"), codexHistory);

  let opencodeSessions = 0;
  if (plan.sources.includes("opencode")) {
    if (await opencodeAvailable()) {
      opencodeSessions = await writeOpencodeStore(root, Math.max(10, sessions.length), rng);
    } else {
      throw new Error(
        "opencode was requested but node:sqlite is unavailable (needs Node 22.13+); " +
        "drop opencode from --sources or run on a newer Node",
      );
    }
  }

  const actual = await storeBytes(root);
  // The hydration target is the largest Claude transcript, which is the
  // oversized one when the plan asked for one.
  const hydrationTarget = sessions
    .filter((session) => session.source === "claude")
    .sort((left, right) => right.bytes - left.bytes)[0] ?? sessions[0];
  const manifest = {
    generator: "scripts/gen-synthetic-history.mjs",
    plan,
    root,
    storeBytes: actual.bytes,
    storeFiles: actual.files,
    sessionCount: sessions.length,
    opencodeSessions,
    hydrationTarget: hydrationTarget
      ? {
        source: hydrationTarget.source,
        sessionId: hydrationTarget.sessionId,
        path: hydrationTarget.path,
        bytes: hydrationTarget.bytes,
      }
      : null,
    // The transcript an incremental sync appends to: the smallest Claude
    // transcript, so re-parsing it is not what the measurement is dominated by.
    incrementalTarget: sessions
      .filter((session) => session.source === "claude")
      .sort((left, right) => left.bytes - right.bytes)[0] ?? null,
    sessions: sessions.map(({ source, sessionId, path, bytes }) => ({ source, sessionId, path, bytes })),
  };
  return manifest;
}

async function main(argv) {
  const out = resolve(option(argv, "out", ""));
  if (!out || out === resolve("")) throw new Error("--out <directory> is required");
  const sources = (option(argv, "sources", FILE_SOURCES.join(","))).split(",").filter(Boolean);
  for (const source of sources) {
    if (!SOURCES.includes(source)) throw new Error(`unknown --sources entry ${source}`);
  }
  const plan = planStore({
    seed: integer(argv, "seed", 176),
    sources,
    targetBytes: integer(argv, "target-bytes", 8 * 1024 * 1024),
    turns: integer(argv, "turns", 12),
    toolResults: integer(argv, "tool-results", 2),
    toolResultBytes: integer(argv, "tool-result-bytes", 1024),
    largeSessionBytes: integer(argv, "large-session-bytes", 0),
  });
  const manifest = await generateStore(plan, out);
  const manifestPath = option(argv, "manifest", join(dirname(out), `${out.split(/[\\/]/).pop()}.manifest.json`));
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  process.stdout.write(`${JSON.stringify({ manifestPath, storeBytes: manifest.storeBytes, storeFiles: manifest.storeFiles, sessionCount: manifest.sessionCount })}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url))) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
