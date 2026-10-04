// Installs the packed tarball into an empty project from the public registry and runs
// it end to end against DATABASE_URL: migrate, bootstrap a token, upload one delivery
// batch, read it back. Proves the published artifact works without this checkout.
//
//   DATABASE_URL=postgres://... node scripts/pack-smoke.mjs
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = resolve(fileURLToPath(import.meta.url), "../..");
const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is required");

const work = mkdtempSync(join(tmpdir(), "relayhistory-engine-smoke-"));
const run = (cmd, args, cwd) =>
  execFileSync(cmd, args, { cwd, stdio: ["ignore", "pipe", "inherit"] })
    .toString()
    .trim();
try {
  const tarball = run(
    "npm",
    ["pack", "--silent", "--pack-destination", work],
    packageDir,
  )
    .split("\n")
    .pop();
  const consumer = join(work, "consumer");
  run("mkdir", ["-p", consumer]);
  writeFileSync(
    join(consumer, "package.json"),
    JSON.stringify({ name: "engine-smoke", private: true, type: "module" }),
  );
  run(
    "npm",
    [
      "install",
      "--no-audit",
      "--no-fund",
      join(work, tarball),
      "hono@^4.7.0",
      "drizzle-orm@^0.38.3",
      "pg@^8",
    ],
    consumer,
  );
  const database = `rh_engine_smoke_${process.pid}`;
  writeFileSync(
    join(consumer, "smoke.mjs"),
    `
import pg from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import {
  bootstrapServiceToken, createHistoryEngine, deliveryAccount, schema,
} from "@relayhistory/engine";
import { applyMigrations } from "@relayhistory/engine/migrations";

const admin = new pg.Client({ connectionString: process.env.DATABASE_URL });
await admin.connect();
await admin.query("CREATE DATABASE ${database}");
const target = new URL(process.env.DATABASE_URL);
target.pathname = "/${database}";
const pool = new pg.Pool({ connectionString: target.toString() });
try {
  const client = await pool.connect();
  await client.query("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public");
  const migrated = await applyMigrations(client);
  client.release();
  const db = drizzle(pool, { schema });
  const app = createHistoryEngine({ database: () => db });
  const issued = await bootstrapServiceToken(db, {
    orgId: "smoke-org", workspaceId: "smoke", label: "pack smoke",
  });
  const headers = { authorization: "Bearer " + issued.token, "content-type": "application/json" };
  const account = await deliveryAccount({ orgId: "smoke-org", workspaceId: "smoke" });
  const batch = {
    schema_version: 1, origin_id: "smoke-machine", batch_id: "b1", job_id: "j1",
    generation: 1, destination_id: "self", instance_id: "i1", account_id: account,
    mapping_version: "relayhistory-delivery-v1",
    records: [{
      schema_version: 1, origin_id: "smoke-machine", record_id: "r1",
      revision_id: "r1-1", revision: 1, kind: "history", source: "claude",
      session_id: "s1", operation: "upsert",
      payload: { source: "claude", session_id: "s1", prompt: "pack smoke prompt", timestamp_ms: 1700000000000 },
    }],
  };
  const upload = await app.request("/v1/delivery/batches", {
    method: "POST", headers, body: JSON.stringify({ protocolVersion: 1, batch }),
  });
  const receipt = await upload.json();
  if (upload.status !== 200 || receipt.acceptedRevisionIds?.[0] !== "r1-1")
    throw new Error("upload failed: " + upload.status + " " + JSON.stringify(receipt));
  const sessions = await (await app.request("/v1/sessions", { headers })).json();
  if (!sessions.sessions?.some((s) => s.sessionId === "s1"))
    throw new Error("uploaded session not listed: " + JSON.stringify(sessions));
  console.log(JSON.stringify({
    migrations: migrated.applied.length, receipt: receipt.receiptId,
    sessions: sessions.sessions.length,
  }));
} finally {
  await pool.end();
  await admin.query("DROP DATABASE IF EXISTS ${database} WITH (FORCE)");
  await admin.end();
}
`,
  );
  console.log(run("node", ["smoke.mjs"], consumer));
} finally {
  rmSync(work, { recursive: true, force: true });
}
