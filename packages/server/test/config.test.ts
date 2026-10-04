import { describe, expect, it } from "vitest";
import { ConfigError, loadConfig } from "../src/config.js";

const base = { DATABASE_URL: "postgres://history:secret@db:5432/history" };

describe("loadConfig", () => {
  it("applies defaults", () => {
    const config = loadConfig(base);
    expect(config).toMatchObject({
      host: "127.0.0.1",
      port: 8080,
      poolMax: 10,
      shutdownTimeoutMs: 15_000,
      retentionIntervalMs: 60_000,
    });
    expect(config.runtimeRole).toBeUndefined();
  });

  it("requires a postgres DATABASE_URL without echoing it", () => {
    expect(() => loadConfig({})).toThrow(ConfigError);
    try {
      loadConfig({ DATABASE_URL: "mysql://user:hunter2@db/x" });
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(ConfigError);
      expect((error as Error).message).not.toContain("hunter2");
    }
  });

  it("rejects out-of-range numbers", () => {
    expect(() => loadConfig({ ...base, PORT: "70000" })).toThrow(/PORT/);
    expect(() =>
      loadConfig({ ...base, RELAYHISTORY_DB_POOL_MAX: "0" }),
    ).toThrow(/POOL_MAX/);
    expect(() => loadConfig({ ...base, PORT: "80a" })).toThrow(/PORT/);
  });

  it("accepts a runtime role name only", () => {
    expect(
      loadConfig({ ...base, RELAYHISTORY_RUNTIME_ROLE: "history_app" })
        .runtimeRole,
    ).toBe("history_app");
    expect(() =>
      loadConfig({ ...base, RELAYHISTORY_RUNTIME_ROLE: "x; DROP ROLE y" }),
    ).toThrow(ConfigError);
  });

  it("passes embedding settings through untouched", () => {
    expect(
      loadConfig({ ...base, EMBEDDING_API_KEY: "k", EMBEDDING_MODEL: "m" })
        .embeddings,
    ).toEqual({
      EMBEDDING_API_KEY: "k",
      OPENAI_API_KEY: undefined,
      EMBEDDING_API_URL: undefined,
      EMBEDDING_MODEL: "m",
    });
  });
});
