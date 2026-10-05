#!/usr/bin/env node
// End-to-end: two machines' real local stores -> relayhistory-upload -> the real
// self-hosted server -> recall.
//
//   node scripts/e2e.mjs --admin-url postgres://postgres@127.0.0.1:5432/postgres
//
// Requires `npm run build` here and in ../server (which needs ../engine built). Local
// stores are built through the public ai-hist SDK from synthetic Claude transcripts;
// no Agent Relay account, desktop app or network beyond loopback is involved. Fails on
// the first broken check and never prints a token.
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import {
  appendFile,
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import http from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";
import pg from "pg";
import { stopChild } from "./child-process.mjs";
import { sync } from "ai-hist";

const run = promisify(execFile);
const here = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const uploaderCli = join(here, "dist/cli.js");
const serverCli = resolve(here, "../server/dist/cli.js");
const { values } = parseArgs({
  options: {
    "admin-url": { type: "string" },
    keep: { type: "boolean", default: false },
  },
});
const adminUrl = values["admin-url"] ?? process.env.E2E_ADMIN_URL;
assert.ok(adminUrl, "--admin-url is required");

const step = (message) => process.stdout.write(`ok - ${message}\n`);
const secrets = [];
const transcripts = [];

async function freePort() {
  const server = createServer().listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address();
  await new Promise((done) => server.close(done));
  return port;
}

// --- local stores --------------------------------------------------------------
// Everything the run creates is released in the final block, setup failures included.
let root;
let admin;
let dbName;
let dbCreated = false;
let server;
let serverLog = "";
try {
  root = await mkdtemp(join(tmpdir(), "rh-upload-e2e-"));
  const tag = randomBytes(3).toString("hex");

  function transcriptLines(sessionId, texts, startMs) {
    let parent = null;
    return texts.map((text, index) => {
      const uuid = `${sessionId}-${index}`;
      const role = index % 2 === 0 ? "user" : "assistant";
      const line = {
        type: role,
        uuid,
        ...(parent ? { parentUuid: parent } : {}),
        sessionId,
        cwd: "/work/selfhost",
        timestamp: new Date(startMs + index * 1_000).toISOString(),
        message:
          role === "user"
            ? { role, content: text }
            : { role, model: "claude-test", content: [{ type: "text", text }] },
      };
      parent = uuid;
      return JSON.stringify(line);
    });
  }

  async function machine(name, sessions) {
    const home = join(root, name, "home");
    const project = join(home, ".claude", "projects", "work-selfhost");
    await mkdir(project, { recursive: true });
    const files = {};
    for (const [sessionId, texts, startMs] of sessions) {
      files[sessionId] = join(project, `${sessionId}.jsonl`);
      await writeFile(
        files[sessionId],
        `${transcriptLines(sessionId, texts, startMs).join("\n")}\n`,
      );
    }
    const dbPath = join(root, name, "ai-history.db");
    const capture = async () => {
      const saved = process.env.HOME;
      process.env.HOME = home;
      try {
        await sync({ dbPath });
      } finally {
        process.env.HOME = saved;
      }
    };
    await capture();
    return { name, home, dbPath, files, capture };
  }

  const word = `quokka${tag}`;
  const A = {
    shared: `a-shared-${tag}`,
    private: `a-private-${tag}`,
  };
  const B = {
    feature: `b-feature-${tag}`,
    secret: `b-secret-${tag}`,
  };
  const laptop = await machine("laptop", [
    [
      A.shared,
      ["Outline the self-host guide", "Outlined the guide", "Add backups"],
      Date.UTC(2026, 9, 1, 9),
    ],
    [
      A.private,
      ["Personal notes that stay local", "Understood"],
      Date.UTC(2026, 9, 1, 10),
    ],
  ]);
  const desktop = await machine("desktop", [
    [
      B.feature,
      [
        `Why does the ${word} job stall`,
        `The ${word} job waits on a lock`,
        "Release it",
      ],
      Date.UTC(2026, 9, 2, 9),
    ],
    [
      B.secret,
      ["Credentials rotation notes", "Noted"],
      Date.UTC(2026, 9, 2, 10),
    ],
  ]);
  step(
    "two machines captured sessions into their own local stores through the SDK",
  );

  // --- server ----------------------------------------------------------------------
  dbName = `rh_upload_e2e_${tag}`;
  admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${dbName}`);
  dbCreated = true;
  const databaseUrl = new URL(adminUrl);
  databaseUrl.pathname = `/${dbName}`;
  const serverEnv = {
    ...process.env,
    DATABASE_URL: databaseUrl.toString(),
    HOST: "127.0.0.1",
  };
  // One port for the whole run: the uploader's cursor is keyed by its endpoint, so the
  // server must come back on the same URL after the restart.
  const port = await freePort();
  const endpoint = `http://127.0.0.1:${port}`;
  async function startServer() {
    server = spawn(process.execPath, [serverCli, "serve"], {
      env: { ...serverEnv, PORT: String(port) },
      stdio: ["ignore", "pipe", "pipe"],
    });
    server.stdout.on("data", (chunk) => (serverLog += chunk));
    server.stderr.on("data", (chunk) => (serverLog += chunk));
    for (let i = 0; i < 240; i += 1) {
      try {
        if ((await fetch(`${endpoint}/ready`)).ok) return;
      } catch {}
      await new Promise((done) => setTimeout(done, 250));
    }
    throw new Error("server did not become ready");
  }
  async function stopServer() {
    const exited = once(server, "exit");
    server.kill("SIGTERM");
    const [code] = await exited;
    assert.equal(code, 0);
  }

  async function mint(org, label, scopes) {
    const out = join(root, `${label}-token.json`);
    await run(
      process.execPath,
      [
        serverCli,
        "token",
        "create",
        "--org",
        org,
        "--workspace",
        "main",
        "--label",
        label,
        "--scopes",
        scopes,
        "--out",
        out,
      ],
      { env: serverEnv },
    );
    const file = JSON.parse(await readFile(out, "utf8"));
    secrets.push(file.token);
    return { path: out, ...file };
  }

  async function api(path, token) {
    const response = await fetch(`${endpoint}${path}`, {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(response.status, 200, `${path} -> ${response.status}`);
    return response.json();
  }

  // --- uploader ----------------------------------------------------------------------
  const ALL_KINDS = [
    "session",
    "session_event",
    "tool_call",
    "file_edit",
    "session_marker",
    "relationship",
    "history",
    "presence",
    "commit_link",
    "trajectory",
    "source_observation",
    "observation_evidence",
  ];

  async function writeConfig(name, body) {
    const path = join(root, `${name}.json`);
    await writeFile(path, JSON.stringify(body, null, 2));
    return path;
  }

  async function upload(configPath, ...flags) {
    // `--home DIR` runs the uploader as if on that machine (for `--sync`).
    const homeAt = flags.indexOf("--home");
    const home = homeAt >= 0 ? flags.splice(homeAt, 2)[1] : undefined;
    const child = spawn(
      process.execPath,
      [uploaderCli, "run", "--config", configPath, ...flags],
      {
        stdio: ["ignore", "pipe", "pipe"],
        ...(home
          ? { env: { ...process.env, HOME: home, USERPROFILE: home } }
          : {}),
      },
    );
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stderr += chunk));
    // `close` fires after the output streams are drained; `exit` may precede them.
    const [code] = await once(child, "close");
    transcripts.push(stdout, stderr);
    const summary = stdout.trim()
      ? JSON.parse(stdout.trim().split("\n").at(-1))
      : null;
    return { code, summary, stderr };
  }

  await startServer();
  const laptopToken = await mint("acme", `laptop-${tag}`, "rth:sync");
  const desktopToken = await mint("acme", `desktop-${tag}`, "rth:sync");
  const reader = await mint("acme", `reader-${tag}`, "rth:read");
  const otherSync = await mint("globex", `globex-laptop-${tag}`, "rth:sync");
  const otherReader = await mint("globex", `globex-reader-${tag}`, "rth:read");
  step(
    "the server started on an empty database and the operator minted per-machine tokens",
  );

  const laptopSelection = {
    all_sources: false,
    sources: [],
    sessions: [{ source: "claude", session_id: A.shared }],
    kinds: ALL_KINDS,
    excluded_sessions: [],
  };
  const laptopConfig = await writeConfig("laptop", {
    endpoint,
    tokenFile: laptopToken.path,
    dbPath: laptop.dbPath,
    instanceId: "laptop",
    selection: laptopSelection,
  });
  const desktopConfig = await writeConfig("desktop", {
    endpoint,
    tokenFile: desktopToken.path,
    dbPath: desktop.dbPath,
    instanceId: "desktop",
    selection: {
      all_sources: false,
      sources: ["claude"],
      sessions: [],
      kinds: ALL_KINDS,
      excluded_sessions: [{ source: "claude", session_id: B.secret }],
    },
  });

  const checked = await run(process.execPath, [
    uploaderCli,
    "check",
    "--config",
    laptopConfig,
  ]);
  transcripts.push(checked.stdout, checked.stderr);
  const check = JSON.parse(checked.stdout);
  assert.equal(check.accountId, laptopToken.accountId);
  assert.match(check.consumer, /^relayhistory-upload:[0-9a-f]{32}$/);
  step("check confirms the token, account and server limits");

  const dry = await upload(laptopConfig, "--dry-run");
  assert.equal(dry.code, 0, dry.stderr);
  assert.deepEqual(Object.keys(dry.summary.sessions), [`claude/${A.shared}`]);
  assert.equal(
    (await api("/v1/sessions?limit=50", reader.token)).sessions.length,
    0,
  );
  step("a dry run selects only the named session and sends nothing");

  const first = await upload(laptopConfig);
  assert.equal(first.code, 0, first.stderr);
  assert.ok(
    first.summary.accepted > 0 &&
      first.summary.accepted === first.summary.selected,
  );
  const second = await upload(desktopConfig);
  assert.equal(second.code, 0, second.stderr);
  assert.ok(second.summary.accepted > 0);
  step(
    `both machines uploaded their selections (${first.summary.accepted} + ${second.summary.accepted} records)`,
  );

  async function verifyRecall(label, { privateVisible = false } = {}) {
    const listed = (await api("/v1/sessions?limit=50", reader.token)).sessions;
    const ids = listed.map((s) => s.sessionId);
    assert.ok(
      ids.includes(A.shared) && ids.includes(B.feature),
      `${label}: selected sessions listed`,
    );
    assert.equal(
      ids.includes(A.private),
      privateVisible,
      `${label}: unselected laptop session`,
    );
    assert.ok(
      !ids.includes(B.secret),
      `${label}: excluded desktop session never uploaded`,
    );
    const machines = new Set(
      listed
        .filter((s) => [A.shared, B.feature].includes(s.sessionId))
        .flatMap((s) => s.machineIds),
    );
    assert.equal(machines.size, 2, `${label}: two machine origins`);
    const found = (await api(`/v1/events?q=${word}`, reader.token)).events;
    assert.ok(
      found.length >= 2 && found.every((e) => e.sessionId === B.feature),
      `${label}: search`,
    );
    const transcript = (
      await api(`/v1/sessions/${A.shared}/events?source=claude`, reader.token)
    ).events
      .filter((e) => e.kind === "session_event")
      .map((e) => e.content);
    assert.deepEqual(
      transcript.slice(0, 3),
      ["Outline the self-host guide", "Outlined the guide", "Add backups"],
      `${label}: ordered transcript`,
    );
    const catalog = await fetch(
      `${endpoint}/v1/sessions/${A.shared}/catalog?source=claude`,
      { headers: { authorization: `Bearer ${reader.token}` } },
    );
    assert.equal(catalog.status, 200, `${label}: catalog`);
    return transcript;
  }
  await verifyRecall("after first upload");
  step(
    "recall lists, searches and replays both machines; unselected and excluded sessions are absent",
  );

  const idle = await upload(laptopConfig);
  assert.equal(idle.code, 0);
  assert.equal(idle.summary.scanned, 0);
  step(
    "a rerun finds nothing new: the cursor advanced only after durable receipts",
  );

  await appendFile(
    laptop.files[A.shared],
    `${transcriptLines(
      A.shared,
      ["", "", "", "Document restore drills"],
      Date.UTC(2026, 9, 1, 9),
    )
      .slice(3)
      .join("\n")}\n`,
  );
  await laptop.capture();
  const incremental = await upload(laptopConfig);
  assert.equal(incremental.code, 0, incremental.stderr);
  assert.ok(
    incremental.summary.accepted > 0 &&
      incremental.summary.accepted < first.summary.accepted,
    "only the new evidence is sent",
  );
  assert.ok(
    (await verifyRecall("after append")).includes("Document restore drills"),
  );
  step("new local evidence uploads incrementally");

  await appendFile(
    laptop.files[A.shared],
    `${transcriptLines(
      A.shared,
      ["", "", "", "", "", "Captured by --sync"],
      Date.UTC(2026, 9, 1, 9),
    )
      .slice(5)
      .join("\n")}\n`,
  );
  const synced = await upload(laptopConfig, "--sync", "--home", laptop.home);
  assert.equal(synced.code, 0, synced.stderr);
  assert.equal(
    synced.summary.syncCompleted,
    true,
    "the real capture completed",
  );
  assert.ok(
    synced.summary.accepted > 0,
    "--sync captured the new message before uploading",
  );
  assert.ok(
    (await verifyRecall("after --sync")).includes("Captured by --sync"),
  );
  step("--sync captures in a child process, then uploads what it captured");

  // A response lost after the server committed: the uploader resends the identical
  // batch and receives the identical receipt.
  let dropped = null;
  const replays = [];
  const proxy = http.createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", async () => {
      const body = Buffer.concat(chunks);
      const upstream = await fetch(`${endpoint}${req.url}`, {
        method: req.method,
        headers: {
          authorization: req.headers.authorization,
          "content-type": req.headers["content-type"] ?? "application/json",
        },
        ...(body.length ? { body } : {}),
      });
      const text = await upstream.text();
      if (req.method === "POST") {
        const batchId = JSON.parse(body).batch.batch_id;
        replays.push({ batchId, receiptId: JSON.parse(text).receiptId });
        if (!dropped) {
          dropped = batchId;
          req.socket.destroy();
          return;
        }
      }
      res
        .writeHead(upstream.status, { "content-type": "application/json" })
        .end(text);
    });
  });
  proxy.listen(0, "127.0.0.1");
  await once(proxy, "listening");
  const proxyPort = proxy.address().port;
  await appendFile(
    laptop.files[A.shared],
    `${transcriptLines(
      A.shared,
      ["", "", "", "", "Schedule a restore drill"],
      Date.UTC(2026, 9, 1, 9),
    )
      .slice(4)
      .join("\n")}\n`,
  );
  await laptop.capture();
  const viaProxy = await writeConfig("laptop-proxy", {
    endpoint: `http://127.0.0.1:${proxyPort}`,
    tokenFile: laptopToken.path,
    dbPath: laptop.dbPath,
    instanceId: "laptop",
    selection: laptopSelection,
  });
  // Same endpoint identity matters for the cursor; the proxy is a different URL, so
  // this config has its own cursor and replays the laptop's whole selection.
  const lost = await upload(viaProxy);
  proxy.close();
  assert.equal(lost.code, 0, lost.stderr);
  const firstSend = replays.filter((r) => r.batchId === dropped);
  assert.equal(firstSend.length, 2, "the dropped batch was resent once");
  assert.equal(
    firstSend[0].receiptId,
    firstSend[1].receiptId,
    "the resend got the identical receipt",
  );
  assert.match(lost.stderr, /"message":"retrying"/);
  step(
    "a lost response is retried with the same batch and answered with the same receipt",
  );

  const mismatched = join(root, "mismatched-token.json");
  await writeFile(
    mismatched,
    JSON.stringify({ ...laptopToken, token: otherSync.token }),
    { mode: 0o600 },
  );
  await chmod(mismatched, 0o600);
  const mismatchConfig = await writeConfig("mismatch", {
    endpoint,
    tokenFile: mismatched,
    dbPath: laptop.dbPath,
    selection: {
      ...laptopSelection,
      sessions: [{ source: "claude", session_id: A.private }],
    },
  });
  const refused = await upload(mismatchConfig);
  assert.equal(refused.code, 2);
  assert.match(refused.stderr, /permission_denied/);
  const retry = await upload(mismatchConfig, "--dry-run");
  assert.ok(retry.summary.selected > 0, "the refused page was not committed");
  step("an account mismatch stops the upload without moving the cursor");

  const caughtUp = await upload(laptopConfig);
  assert.equal(caughtUp.code, 0, caughtUp.stderr);
  assert.ok(
    caughtUp.summary.scanned > 0,
    "the direct endpoint's cursor is independent of the proxy URL's",
  );
  const globexConfig = await writeConfig("globex", {
    endpoint,
    tokenFile: otherSync.path,
    dbPath: laptop.dbPath,
    selection: laptopSelection,
  });
  const globex = await upload(globexConfig);
  assert.equal(globex.code, 0, globex.stderr);
  assert.ok(
    globex.summary.accepted > 0,
    "a second account has its own cursor from the start",
  );
  assert.equal(
    (await upload(laptopConfig)).summary.scanned,
    0,
    "the first account's cursor did not move",
  );
  const globexIds = (
    await api("/v1/sessions?limit=50", otherReader.token)
  ).sessions.map((s) => s.sessionId);
  assert.deepEqual(globexIds, [A.shared]);
  assert.ok(
    !(await api("/v1/sessions?limit=50", reader.token)).sessions.some(
      (s) => s.sessionId === B.secret,
    ),
  );
  step("each endpoint and account keeps its own cursor; tenants stay isolated");

  await stopServer();
  await startServer();
  await verifyRecall("after restart");
  assert.equal((await upload(laptopConfig)).summary.scanned, 0);
  step(
    "server restart preserved evidence; the uploader resumes from its cursor",
  );

  const widened = await writeConfig("laptop", {
    endpoint,
    tokenFile: laptopToken.path,
    dbPath: laptop.dbPath,
    instanceId: "laptop",
    selection: {
      ...laptopSelection,
      sessions: [
        ...laptopSelection.sessions,
        { source: "claude", session_id: A.private },
      ],
    },
  });
  const backfill = await upload(widened);
  assert.equal(backfill.code, 0, backfill.stderr);
  assert.ok(
    Object.keys(backfill.summary.sessions).includes(`claude/${A.private}`),
  );
  await verifyRecall("after widening", { privateVisible: true });
  step(
    "adding a session to the selection backfills it; already-sent records replay idempotently",
  );

  for (const text of [...transcripts, serverLog])
    for (const secret of secrets)
      assert.ok(!text.includes(secret), "a token appeared in output");
  step("no token appeared in uploader or server output");
  process.stdout.write("uploader e2e passed\n");
} finally {
  // Each step is attempted and none replaces the error that ended the run.
  const attempt = async (step, work) => {
    try {
      await work();
    } catch {
      process.stderr.write(`cleanup: ${step} failed\n`);
    }
  };
  if (server) await attempt("stop server", () => stopChild(server));
  if (!values.keep) {
    if (dbCreated)
      await attempt("drop database", () =>
        admin.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`),
      );
    if (root)
      await attempt("remove files", () =>
        rm(root, { recursive: true, force: true }),
      );
  }
  if (admin) await attempt("close admin connection", () => admin.end());
}
