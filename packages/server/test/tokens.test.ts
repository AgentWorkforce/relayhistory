// Token bootstrap against real PostgreSQL. Set TEST_ADMIN_DATABASE_URL to a server
// where the role may create databases (CI provides one); skipped otherwise.
import { chmod, mkdtemp, open, readFile, rm, stat } from "node:fs/promises";
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
  LeftoverTokenFileError,
  shellArgument,
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
    const removed: string[] = [];
    const path = join(dir, "never.json");
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
      remove: async (target: string) => {
        removed.push(target);
        throw Object.assign(new Error("rm EACCES"), { code: "EACCES" });
      },
    };
    await expect(
      createTokenFile(
        database.db,
        { orgId: "cleanup", workspaceId: "main", label: "rm-fails" },
        { path },
        failing,
      ),
    ).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof LeftoverTokenFileError &&
        error.path === path &&
        (error.cause as { code?: string }).code === "ENOSPC",
    );
    expect(minted).toMatch(/^rth_st_/);
    expect(await resolveAccessToken(database.db, minted!)).toBeNull();
    // The partial file's removal was attempted, after the revoke, and its failure ignored.
    expect(removed).toEqual([path]);
  });

  describe("a write that succeeds but whose close fails", () => {
    // close() can report a deferred write error (EIO, ENOSPC on NFS): the secret may
    // not be on disk, so delivery has failed and the token must not stay live.
    const closeFails = (blockRemove: boolean) => ({
      open: async (target: string) => {
        const handle = await open(target, "wx", 0o600);
        return {
          writeFile: handle.writeFile.bind(handle),
          close: async () => {
            await handle.close();
            throw Object.assign(new Error("close EIO"), { code: "EIO" });
          },
        };
      },
      remove: async (target: string) => {
        if (!blockRemove) return rm(target, { force: true });
        // A directory the process may no longer write: the real remove fails.
        await chmod(dir, 0o500);
        try {
          await rm(target, { force: true });
        } finally {
          await chmod(dir, 0o700);
        }
      },
    });

    it("revokes the token and removes the file", async () => {
      const path = join(dir, "close-eio.json");
      const error = await createTokenFile(
        database.db,
        { orgId: "closefail", workspaceId: "main", label: "close-eio" },
        { path },
        closeFails(false),
      ).catch((e: unknown) => e);
      expect(error).toMatchObject({ code: "EIO" });
      await expect(stat(path)).rejects.toMatchObject({ code: "ENOENT" });
      const [row] = await listTokens(database.db, "closefail");
      expect(row.revokedAt).not.toBeNull();
    });

    it.skipIf(process.platform === "win32" || process.getuid?.() === 0)(
      "names a file it could not remove, whose token is already revoked",
      async () => {
        const path = join(dir, "close-eio-left.json");
        const error = (await createTokenFile(
          database.db,
          { orgId: "closeleft", workspaceId: "main", label: "close-left" },
          { path },
          closeFails(true),
        ).catch((e: unknown) => e)) as LeftoverTokenFileError;
        expect(error).toBeInstanceOf(LeftoverTokenFileError);
        expect(error.message).toContain(path);
        expect(error.message).toContain("revoked");
        expect((error.cause as { code?: string }).code).toBe("EIO");
        // The file really is left, complete, and its token is dead.
        const left = JSON.parse(await readFile(path, "utf8"));
        expect(left.id).toBe(error.tokenId);
        expect(await resolveAccessToken(database.db, left.token)).toBeNull();
        // Reusing the path is refused before anything is minted.
        const before = (await listTokens(database.db, "closeleft")).length;
        await expect(
          createTokenFile(
            database.db,
            { orgId: "closeleft", workspaceId: "main", label: "again" },
            { path },
          ),
        ).rejects.toMatchObject({ code: "EEXIST" });
        expect((await listTokens(database.db, "closeleft")).length).toBe(
          before,
        );
      },
    );
  });

  it("an empty path is refused before anything is minted", async () => {
    const before = (await listTokens(database.db, "emptypath")).length;
    await expect(
      createTokenFile(
        database.db,
        { orgId: "emptypath", workspaceId: "main", label: "empty" },
        { path: "" },
      ),
    ).rejects.toMatchObject({ code: "ENOENT" });
    expect((await listTokens(database.db, "emptypath")).length).toBe(before);
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
    expect(error.revokeCommand).toBe(
      `relayhistory-server token revoke --org cleanup --id ${error.tokenId}`,
    );
    expect(error.message).toContain(error.revokeCommand);
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

describe("shellArgument", () => {
  it("leaves safe identifiers alone and single-quotes anything else", () => {
    expect(shellArgument("acme")).toBe("acme");
    expect(shellArgument("org:acme@eu-1")).toBe("org:acme@eu-1");
    expect(shellArgument("acme corp")).toBe("'acme corp'");
    expect(shellArgument("it's; rm -rf ~")).toBe(`'it'\\''s; rm -rf ~'`);
    expect(shellArgument("$(whoami)")).toBe("'$(whoami)'");
  });
});
