// Token bootstrap against real PostgreSQL. Set TEST_ADMIN_DATABASE_URL to a server
// where the role may create databases (CI provides one); skipped otherwise.
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import { resolveAccessToken } from "@relayhistory/engine";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  openDatabase,
  prepareDatabase,
  type Database,
} from "../src/database.js";
import { silentLogger } from "../src/log.js";
import {
  createToken,
  listTokens,
  revokeToken,
  createTokenFile,
  UndeliveredTokenError,
} from "../src/tokens.js";

const adminUrl = process.env.TEST_ADMIN_DATABASE_URL;

describe.skipIf(!adminUrl)("token bootstrap", () => {
  const name = `rh_server_${randomBytes(4).toString("hex")}`;
  let admin: pg.Client;
  let database: Database;
  let dir: string;

  beforeAll(async () => {
    admin = new pg.Client({ connectionString: adminUrl });
    await admin.connect();
    await admin.query(`CREATE DATABASE ${name}`);
    const url = new URL(adminUrl!);
    url.pathname = `/${name}`;
    await prepareDatabase(url.toString(), { log: silentLogger });
    // A second run is a no-op under the ledger.
    await prepareDatabase(url.toString(), { log: silentLogger });
    database = openDatabase(url.toString(), { max: 2, log: silentLogger });
    dir = await mkdtemp(join(tmpdir(), "rh-server-tokens-"));
  });

  afterAll(async () => {
    await database?.close();
    await admin?.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin?.end();
    if (dir) await rm(dir, { recursive: true, force: true });
  });

  it("mints a tenant-bound token that resolves through service-local auth", async () => {
    const file = await createToken(database.db, {
      orgId: "acme",
      workspaceId: "main",
      label: "laptop",
      scopes: ["rth:sync"],
      expiresInDays: 7,
    });
    expect(file.token).toMatch(/^rth_st_/);
    expect(file.scopes).toEqual(["rth:sync"]);
    expect(file.accountId).toMatch(/^relayhistory:[0-9a-f]{64}$/);
    const session = await resolveAccessToken(database.db, file.token);
    expect(session).toMatchObject({
      orgId: "acme",
      workspaceId: "main",
      scopes: ["rth:sync"],
    });

    const listed = await listTokens(database.db, "acme");
    expect(listed.map((token) => token.id)).toContain(file.id);
    expect(JSON.stringify(listed)).not.toContain(file.token);
    expect(await listTokens(database.db, "globex")).toEqual([]);

    expect(await revokeToken(database.db, "globex", file.id)).toBe(false);
    expect(await revokeToken(database.db, "acme", file.id)).toBe(true);
    expect(await resolveAccessToken(database.db, file.token)).toBeNull();
  });

  it("writes an owner-only token file and never replaces one", async () => {
    const path = join(dir, "desktop.json");
    const options = { orgId: "files", workspaceId: "main", label: "desktop" };
    const file = await createTokenFile(database.db, options, { path });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual(file);
    expect(await resolveAccessToken(database.db, file.token)).not.toBeNull();

    // The existing file is refused before anything is minted.
    await expect(
      createTokenFile(database.db, options, { path }),
    ).rejects.toMatchObject({ code: "EEXIST" });
    expect((await listTokens(database.db, "files")).length).toBe(1);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual(file);
  });

  it("revokes a token whose file could not be delivered", async () => {
    let minted: string | undefined;
    await expect(
      createTokenFile(
        database.db,
        { orgId: "pipe", workspaceId: "main", label: "broken-pipe" },
        {
          async write(text) {
            minted = JSON.parse(text).token;
            throw Object.assign(new Error("write EPIPE"), { code: "EPIPE" });
          },
        },
      ),
    ).rejects.toMatchObject({ code: "EPIPE" });
    expect(minted).toMatch(/^rth_st_/);
    expect(await resolveAccessToken(database.db, minted!)).toBeNull();
    const [row] = await listTokens(database.db, "pipe");
    expect(row.revokedAt).not.toBeNull();
  });

  it("revokes an undelivered token even when removing the partial file fails", async () => {
    let minted: string | undefined;
    const failing = {
      open: async () => ({
        writeFile: async (text: string | Uint8Array) => {
          minted = JSON.parse(String(text)).token;
          throw Object.assign(new Error("write ENOSPC"), { code: "ENOSPC" });
        },
        close: async () => {
          throw Object.assign(new Error("close EIO"), { code: "EIO" });
        },
      }),
      remove: async () => {
        throw Object.assign(new Error("rm EACCES"), { code: "EACCES" });
      },
    };
    await expect(
      createTokenFile(
        database.db,
        { orgId: "cleanup", workspaceId: "main", label: "rm-fails" },
        { path: join(dir, "never.json") },
        failing,
      ),
    ).rejects.toMatchObject({ code: "ENOSPC" });
    expect(minted).toMatch(/^rth_st_/);
    expect(await resolveAccessToken(database.db, minted!)).toBeNull();
  });

  it("names a token it could neither deliver nor revoke", async () => {
    let minted: string | undefined;
    // The database accepts the mint but refuses the revoking update.
    const noRevoke = new Proxy(database.db, {
      get(target, prop, receiver) {
        if (prop === "update")
          return () => {
            throw new Error("connection lost");
          };
        return Reflect.get(target, prop, receiver);
      },
    });
    const failure = createTokenFile(
      noRevoke,
      { orgId: "cleanup", workspaceId: "main", label: "revoke-fails" },
      {
        async write(text) {
          minted = JSON.parse(text).token;
          throw Object.assign(new Error("write EPIPE"), { code: "EPIPE" });
        },
      },
    );
    const error = (await failure.catch(
      (e: unknown) => e,
    )) as UndeliveredTokenError;
    expect(error).toBeInstanceOf(UndeliveredTokenError);
    expect(error.message).toContain(error.tokenId);
    expect(error.message).not.toContain(minted!);
    // It really is still live: the operator must revoke it by id.
    expect(await resolveAccessToken(database.db, minted!)).not.toBeNull();
    expect(await revokeToken(database.db, "cleanup", error.tokenId)).toBe(true);
  });

  it("refuses scopes beyond sync and read", async () => {
    await expect(
      createToken(database.db, {
        orgId: "acme",
        workspaceId: "main",
        label: "admin",
        scopes: ["rth:admin"],
      }),
    ).rejects.toThrow(/unknown scopes/);
  });
});
