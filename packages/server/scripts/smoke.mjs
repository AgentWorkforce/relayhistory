#!/usr/bin/env node
// Clean-start smoke test for a self-hosted RelayHistory server.
//
// Local (spawns the built server against a fresh database it creates):
//   node scripts/smoke.mjs --admin-url postgres://postgres@127.0.0.1:5432/postgres
// Compose (drives a running `docker compose` deployment):
//   node scripts/smoke.mjs --compose compose.yaml --base-url http://127.0.0.1:8080
//
// Proves: tokens bootstrapped without any external identity, uploads from two machine
// origins into one workspace, list/search/transcript reads, replay idempotency, scope
// and account enforcement, tenant isolation, tombstones, and persistence across a
// graceful restart. Exits nonzero on the first failed check. Never prints tokens.
import { spawn, execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { parseArgs } from "node:util";
import assert from "node:assert/strict";
import pg from "pg";
import { batch, eventTombstone, sessionRecords } from "./fixtures.mjs";

const run = promisify(execFile);
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cli = resolve(root, "dist/cli.js");

const { values } = parseArgs({
  options: {
    "admin-url": { type: "string" },
    compose: { type: "string" },
    "base-url": { type: "string" },
    keep: { type: "boolean", default: false },
  },
});

function step(message) {
  process.stdout.write(`ok - ${message}\n`);
}

async function waitReady(baseUrl, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${baseUrl}/ready`, {
        signal: AbortSignal.timeout(
          Math.max(1, Math.min(5_000, deadline - Date.now())),
        ),
      });
      if (response.ok) return;
    } catch {
      // Not listening yet, or this probe timed out.
    }
    await new Promise((done) => setTimeout(done, 250));
  }
  throw new Error(`server at ${baseUrl} did not become ready`);
}

/** Spawns `relayhistory-server serve` against a database this run creates. */
async function localRuntime(adminUrl) {
  const name = `rh_smoke_${randomBytes(4).toString("hex")}`;
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${name}`);
  const url = new URL(adminUrl);
  url.pathname = `/${name}`;
  const env = { ...process.env, DATABASE_URL: url.toString() };
  // The server binds an OS-assigned port; its `listening` log line names it.
  let child;
  const rt = {
    baseUrl: "",
    async start() {
      child = spawn(process.execPath, [cli, "serve"], {
        env: { ...env, HOST: "127.0.0.1", PORT: "0" },
        stdio: ["ignore", "inherit", "pipe"],
      });
      const port = await new Promise((resolve, reject) => {
        let buffered = "";
        child.stderr.on("data", (chunk) => {
          process.stderr.write(chunk);
          buffered += chunk;
          const line = buffered
            .split("\n")
            .map((text) => {
              try {
                return JSON.parse(text);
              } catch {
                return null;
              }
            })
            .find((entry) => entry?.message === "listening");
          if (line) resolve(line.port);
        });
        child.once("exit", (code) =>
          reject(new Error(`server exited ${code} before listening`)),
        );
      });
      rt.baseUrl = `http://127.0.0.1:${port}`;
      await waitReady(rt.baseUrl);
    },
    async stop() {
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      const [code, signal] = await exited;
      assert.equal(code, 0, `server exited ${code ?? signal} after SIGTERM`);
    },
    async mint(args) {
      const { stdout } = await run(
        process.execPath,
        [cli, "token", "create", ...args, "--out", "-"],
        { env },
      );
      return JSON.parse(stdout);
    },
    async cleanup() {
      if (child && child.exitCode === null) {
        const exited = once(child, "exit");
        child.kill("SIGTERM");
        await exited;
      }
      if (!values.keep)
        await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
      await admin.end();
    },
  };
  return rt;
}

/** Drives an already running Compose deployment; restart is `docker compose restart`. */
function composeRuntime(file, baseUrl) {
  const compose = (...args) =>
    run("docker", ["compose", "-f", file, ...args], { maxBuffer: 1 << 20 });
  return {
    baseUrl,
    async start() {
      await waitReady(baseUrl, 180_000);
    },
    async stop() {
      await compose("restart", "server");
    },
    async mint(args) {
      const { stdout } = await compose(
        "exec",
        "-T",
        "server",
        "relayhistory-server",
        "token",
        "create",
        ...args,
        "--out",
        "-",
      );
      return JSON.parse(stdout);
    },
    async cleanup() {},
  };
}

const runtime = values.compose
  ? composeRuntime(
      values.compose,
      values["base-url"] ?? "http://127.0.0.1:8080",
    )
  : localRuntime(
      values["admin-url"] ??
        process.env.SMOKE_ADMIN_URL ??
        assert.fail("--admin-url or --compose is required"),
    );

const rt = await runtime;
const http = async (method, path, token, body, headers = {}) => {
  const response = await fetch(`${rt.baseUrl}${path}`, {
    method,
    headers: {
      ...(token ? { authorization: `Bearer ${token}` } : {}),
      ...(body ? { "content-type": "application/json" } : {}),
      ...headers,
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : null };
};

try {
  await rt.start();
  step("server started on an empty database and reports ready");

  const run = randomBytes(3).toString("hex");
  const laptop = await rt.mint([
    "--org",
    "acme",
    "--workspace",
    "main",
    "--label",
    `laptop-${run}`,
    "--scopes",
    "rth:sync,rth:read",
  ]);
  const desktop = await rt.mint([
    "--org",
    "acme",
    "--workspace",
    "main",
    "--label",
    `desktop-${run}`,
    "--scopes",
    "rth:sync",
  ]);
  const reader = await rt.mint([
    "--org",
    "acme",
    "--workspace",
    "main",
    "--label",
    `reader-${run}`,
    "--scopes",
    "rth:read",
  ]);
  const other = await rt.mint([
    "--org",
    "globex",
    "--workspace",
    "main",
    "--label",
    `globex-${run}`,
  ]);
  assert.equal(laptop.accountId, desktop.accountId);
  assert.notEqual(laptop.accountId, other.accountId);
  assert.match(laptop.token, /^rth_st_/);
  step("operator bootstrapped scoped tokens for two tenants from the database");

  const unauthenticated = await http("GET", "/v1/sessions");
  assert.equal(unauthenticated.status, 401);
  step("unauthenticated reads are refused");

  const laptopOrigin = randomBytes(8).toString("hex");
  const desktopOrigin = randomBytes(8).toString("hex");
  const laptopSession = `laptop-session-${run}`;
  const desktopSession = `desktop-session-${run}`;
  const word = `zephyr${run}`;
  const laptopRecords = sessionRecords({
    originId: laptopOrigin,
    sessionId: laptopSession,
    lines: ["Plan the release checklist", "Drafted the checklist", "Ship it"],
    startMs: Date.UTC(2026, 9, 1, 9),
    project: "/work/selfhost",
  });
  const desktopRecords = sessionRecords({
    originId: desktopOrigin,
    sessionId: desktopSession,
    lines: [
      `Investigate the ${word} cache miss`,
      `The ${word} cache key omitted the branch`,
      "Fix it",
    ],
    startMs: Date.UTC(2026, 9, 2, 9),
    project: "/work/selfhost",
  });
  const laptopBatch = batch({
    originId: laptopOrigin,
    accountId: laptop.accountId,
    batchId: `laptop-1-${run}`,
    records: laptopRecords,
    instanceId: "laptop",
  });
  const desktopBatch = batch({
    originId: desktopOrigin,
    accountId: desktop.accountId,
    batchId: `desktop-1-${run}`,
    records: desktopRecords,
    instanceId: "desktop",
  });

  const laptopReceipt = await http(
    "POST",
    "/v1/delivery/batches",
    laptop.token,
    laptopBatch,
  );
  assert.equal(laptopReceipt.status, 200, JSON.stringify(laptopReceipt.body));
  assert.equal(laptopReceipt.body.acceptanceLevel, "durable");
  assert.deepEqual(
    laptopReceipt.body.acceptedRevisionIds,
    laptopRecords.map((r) => r.revision_id),
  );
  const desktopReceipt = await http(
    "POST",
    "/v1/delivery/batches",
    desktop.token,
    desktopBatch,
  );
  assert.equal(desktopReceipt.status, 200, JSON.stringify(desktopReceipt.body));
  step("two machine origins delivered durable batches into one workspace");

  const replay = await http(
    "POST",
    "/v1/delivery/batches",
    laptop.token,
    laptopBatch,
  );
  assert.equal(replay.status, 200);
  assert.equal(replay.body.receiptId, laptopReceipt.body.receiptId);
  step("a replayed batch returns the identical receipt");

  const changed = structuredClone(laptopBatch);
  changed.batch.batch_id = `laptop-conflict-${run}`;
  changed.batch.records = [
    {
      ...changed.batch.records[2],
      payload: { ...changed.batch.records[2].payload, text: "rewritten" },
    },
  ];
  const conflict = await http(
    "POST",
    "/v1/delivery/batches",
    laptop.token,
    changed,
  );
  assert.equal(conflict.status, 409);
  assert.equal(conflict.body.error.code, "delivery_conflict");
  assert.equal(conflict.body.error.conflict.type, "record_revision");
  step("a reused revision with different content is a typed 409 conflict");

  const readOnly = await http(
    "POST",
    "/v1/delivery/batches",
    reader.token,
    laptopBatch,
  );
  assert.equal(readOnly.status, 403);
  const wrongAccount = await http(
    "POST",
    "/v1/delivery/batches",
    other.token,
    laptopBatch,
  );
  assert.equal(wrongAccount.status, 403);
  assert.equal(wrongAccount.body.error.code, "delivery_account_mismatch");
  const desktopRead = await http("GET", "/v1/sessions", desktop.token);
  assert.equal(desktopRead.status, 403);
  step("scopes and the delivery account are enforced server-side");

  async function verifyReads(label) {
    const sessions = await http("GET", "/v1/sessions?limit=50", reader.token);
    assert.equal(sessions.status, 200);
    const ids = sessions.body.sessions.map((s) => s.sessionId);
    assert.ok(
      ids.includes(laptopSession) && ids.includes(desktopSession),
      `${label}: both sessions listed`,
    );
    const machines = new Set(
      sessions.body.sessions
        .filter((s) => [laptopSession, desktopSession].includes(s.sessionId))
        .flatMap((s) => s.machineIds),
    );
    assert.equal(machines.size, 2, `${label}: two machine origins`);

    const search = await http("GET", `/v1/events?q=${word}`, reader.token);
    assert.equal(search.status, 200);
    assert.ok(
      search.body.events.length >= 2,
      `${label}: search finds the desktop transcript`,
    );
    assert.ok(search.body.events.every((e) => e.sessionId === desktopSession));

    const transcript = await http(
      "GET",
      `/v1/sessions/${laptopSession}/events?source=claude`,
      reader.token,
    );
    assert.equal(transcript.status, 200);
    const texts = transcript.body.events
      .filter((e) => e.kind === "session_event")
      .map((e) => e.content);
    assert.deepEqual(
      texts,
      ["Plan the release checklist", "Drafted the checklist"],
      `${label}: ordered transcript without the tombstoned message`,
    );

    const catalog = await http(
      "GET",
      `/v1/sessions/${desktopSession}/catalog?source=claude`,
      reader.token,
    );
    assert.equal(catalog.status, 200, `${label}: delivered catalog`);
    const hidden = await http(
      "GET",
      `/v1/sessions/${desktopSession}/catalog?source=claude`,
      other.token,
    );
    assert.equal(hidden.status, 404, `${label}: other tenant has no catalog`);

    const isolated = await http("GET", "/v1/sessions?limit=50", other.token);
    assert.equal(isolated.status, 200);
    assert.ok(
      isolated.body.sessions.every(
        (s) => ![laptopSession, desktopSession].includes(s.sessionId),
      ),
      `${label}: other tenant sees none`,
    );
    const isolatedSearch = await http(
      "GET",
      `/v1/events?q=${word}`,
      other.token,
    );
    assert.equal(isolatedSearch.body.events.length, 0);
    const isolatedTranscript = await http(
      "GET",
      `/v1/sessions/${laptopSession}/events`,
      other.token,
    );
    assert.equal(isolatedTranscript.body.events?.length ?? 0, 0);
  }

  const tombstone = batch({
    originId: laptopOrigin,
    accountId: laptop.accountId,
    batchId: `laptop-2-${run}`,
    records: [
      eventTombstone({
        originId: laptopOrigin,
        sessionId: laptopSession,
        index: 2,
        revision: laptopRecords.length + 1,
      }),
    ],
    instanceId: "laptop",
  });
  const deleted = await http(
    "POST",
    "/v1/delivery/batches",
    laptop.token,
    tombstone,
  );
  assert.equal(deleted.status, 200, JSON.stringify(deleted.body));
  step("a tombstone at a higher revision is accepted");

  await verifyReads("before restart");
  step(
    "list, search, transcript and catalog read back both machines; the other tenant sees nothing",
  );

  await rt.stop();
  await rt.start();
  step("server drained on SIGTERM and restarted");

  await verifyReads("after restart");
  const replayAfter = await http(
    "POST",
    "/v1/delivery/batches",
    desktop.token,
    desktopBatch,
  );
  assert.equal(replayAfter.status, 200);
  assert.equal(replayAfter.body.receiptId, desktopReceipt.body.receiptId);
  step("evidence and receipts persisted across the restart");
  process.stdout.write("smoke passed\n");
} finally {
  await rt.cleanup();
}
