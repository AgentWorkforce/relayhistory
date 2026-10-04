import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  ConfigError,
  endpointBase,
  loadConfig,
  parseEndpoint,
  parseSelection,
} from "../src/config.js";

const selection = {
  all_sources: false,
  sources: [],
  sessions: [{ source: "claude", session_id: "s1" }],
  kinds: ["session", "session_event"],
  excluded_sessions: [],
};
const tokenFile = {
  version: 1,
  token: "rth_st_abcdef",
  accountId: `relayhistory:${"0".repeat(64)}`,
  orgId: "acme",
  workspaceId: "main",
};

describe("config", () => {
  let dir: string;
  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), "rh-upload-config-"));
  });
  afterEach(() => rm(dir, { recursive: true, force: true }));

  async function write(config: unknown, mode = 0o600) {
    await writeFile(join(dir, "token.json"), JSON.stringify(tokenFile));
    await chmod(join(dir, "token.json"), mode);
    await writeFile(join(dir, "upload.json"), JSON.stringify(config));
    return join(dir, "upload.json");
  }

  it("resolves paths against the config file and reads the token file", async () => {
    const path = await write({
      endpoint: "https://h.example.com/",
      tokenFile: "token.json",
      dbPath: "history.db",
      selection,
    });
    const config = await loadConfig(path);
    expect(config.token).toBe(tokenFile.token);
    expect(config.accountId).toBe(tokenFile.accountId);
    expect(config.dbPath).toBe(join(dir, "history.db"));
    expect(config.endpoint.href).toBe("https://h.example.com/");
    expect(config.instanceId).toBe("default");
    expect(config.limits).toEqual({ maxRecords: 100, maxBytes: 1_048_576 });
  });

  it.skipIf(process.platform === "win32")(
    "refuses a token file others can read",
    async () => {
      const path = await write(
        {
          endpoint: "https://h.example.com",
          tokenFile: "token.json",
          selection,
        },
        0o644,
      );
      await expect(loadConfig(path)).rejects.toThrow(/chmod 600/);
    },
  );

  it("refuses unknown fields and implicit selections", async () => {
    await expect(
      loadConfig(
        await write({
          endpoint: "https://h.example.com",
          tokenFile: "token.json",
          selection,
          extra: 1,
        }),
      ),
    ).rejects.toThrow(ConfigError);
    expect(() => parseSelection({ ...selection, sessions: [] })).toThrow(
      /all_sources or name/,
    );
    expect(() => parseSelection({ ...selection, kinds: [] })).toThrow(/kinds/);
    expect(() => parseSelection({ ...selection, kinds: ["usage"] })).toThrow(
      /kinds/,
    );
  });

  it("requires https except on loopback or by explicit opt-in", () => {
    expect(() => parseEndpoint("http://history.example.com")).toThrow(/https/);
    expect(parseEndpoint("http://127.0.0.1:8080").port).toBe("8080");
    for (const host of [
      "http://localhost",
      "http://127.0.0.2",
      "http://[::1]:8080",
      "http://[::ffff:127.0.0.1]:8080",
    ])
      expect(parseEndpoint(host).protocol, host).toBe("http:");
    expect(() => parseEndpoint("http://[::ffff:10.0.0.1]")).toThrow(/https/);
    expect(() => parseEndpoint("http://127.0.0.1.example.com")).toThrow(
      /https/,
    );
    expect(parseEndpoint("http://history.lan", true).protocol).toBe("http:");
    expect(() => parseEndpoint("https://user:pw@h.example.com")).toThrow(
      /credentials/,
    );
    expect(endpointBase(parseEndpoint("https://h.example.com/history/"))).toBe(
      "https://h.example.com/history",
    );
    expect(endpointBase(parseEndpoint("https://h.example.com"))).toBe(
      "https://h.example.com",
    );
  });
});
