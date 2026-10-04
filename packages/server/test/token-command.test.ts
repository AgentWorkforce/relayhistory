// The token command prepares the schema like serve does, runtime-role grants included.
// Needs TEST_ADMIN_DATABASE_URL with a role that may create databases and roles.
import { randomBytes } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { silentLogger } from "../src/log.js";
import { tokenCommand } from "../src/token-command.js";

const adminUrl = process.env.TEST_ADMIN_DATABASE_URL;

const EVERY_PRIVILEGE = {
  schema_usage: true,
  select: true,
  insert: true,
  update: true,
  delete: true,
};

/**
 * The role's schema usage, and each table privilege held on every `sessions` table
 * (the migration ledger aside), each checked on its own.
 */
async function privileges(client: pg.Client, role: string) {
  const { rows } = await client.query(
    `SELECT has_schema_privilege($1, 'sessions', 'USAGE') AS schema_usage,
            bool_and(has_table_privilege($1, t.oid, 'SELECT')) AS select,
            bool_and(has_table_privilege($1, t.oid, 'INSERT')) AS insert,
            bool_and(has_table_privilege($1, t.oid, 'UPDATE')) AS update,
            bool_and(has_table_privilege($1, t.oid, 'DELETE')) AS delete
       FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace
      WHERE n.nspname = 'sessions' AND t.relkind IN ('r', 'p') AND t.relname <> '__migrations'`,
    [role],
  );
  return rows[0];
}

describe.skipIf(!adminUrl)("token command on a fresh database", () => {
  const suffix = randomBytes(4).toString("hex");
  const name = `rh_token_cmd_${suffix}`;
  const role = `rh_runtime_${suffix}`;
  let admin: pg.Client;
  let url: string;
  let dir: string;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: adminUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE ${name}`);
    await admin.query(`CREATE ROLE ${role} NOLOGIN`);
    const target = new URL(adminUrl!);
    target.pathname = `/${name}`;
    url = target.toString();
    dir = await mkdtemp(join(tmpdir(), "rh-token-cmd-"));
  });

  afterAll(async () => {
    await admin?.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin?.query(`DROP ROLE IF EXISTS ${role}`);
    await admin?.end();
    if (dir) await rm(dir, { recursive: true, force: true });
  });

  it("grants the runtime role on the tables its migrations create", async () => {
    await tokenCommand(
      [
        "create",
        "--org",
        "acme",
        "--workspace",
        "main",
        "--label",
        "first",
        "--out",
        join(dir, "first.json"),
      ],
      silentLogger,
      { DATABASE_URL: url, RELAYHISTORY_RUNTIME_ROLE: role },
    );
    const check = new pg.Client({ connectionString: url });
    await check.connect();
    try {
      expect(await privileges(check, role)).toEqual(EVERY_PRIVILEGE);
      // has_table_privilege with a comma list is true if ANY is held, so each privilege
      // is checked alone; prove that by revoking one and seeing exactly it go missing.
      await check.query(`REVOKE DELETE ON sessions.auth_sessions FROM ${role}`);
      expect(await privileges(check, role)).toEqual({
        ...EVERY_PRIVILEGE,
        delete: false,
      });
    } finally {
      await check.end();
    }
  });

  it("refuses an invalid runtime role before touching the database", async () => {
    await expect(
      tokenCommand(["list", "--org", "acme"], silentLogger, {
        DATABASE_URL: "postgres://nobody@127.0.0.1:1/none",
        RELAYHISTORY_RUNTIME_ROLE: "x; DROP ROLE y",
      }),
    ).rejects.toThrow(/RELAYHISTORY_RUNTIME_ROLE/);
  });
});
