/**
 * Server configuration from the environment. Values that carry credentials
 * (`DATABASE_URL`, provider keys) are never logged or echoed in errors.
 */
import type { EmbeddingEnv } from "@relayhistory/engine";

export interface ServerConfig {
  databaseUrl: string;
  host: string;
  port: number;
  /** Connections the HTTP pool may hold. */
  poolMax: number;
  /** How long a shutdown waits for in-flight requests before closing them. */
  shutdownTimeoutMs: number;
  /** Interval of the retention job that clears expired Babysitter evidence. */
  retentionIntervalMs: number;
  /** Granted runtime access to the `sessions` schema after migrations. */
  runtimeRole?: string;
  /** Optional embedding provider settings; absent keys mean no embeddings. */
  embeddings: EmbeddingEnv;
}

export class ConfigError extends Error {
  override name = "ConfigError";
}

type Env = Record<string, string | undefined>;

function integer(
  env: Env,
  name: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = env[name]?.trim();
  if (!raw) return fallback;
  const value = /^\d+$/.test(raw) ? Number(raw) : NaN;
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new ConfigError(`${name} must be an integer from ${min} to ${max}`);
  }
  return value;
}

/** The database URL alone, for commands that need nothing else. */
export function databaseUrl(env: Env = process.env): string {
  const url = env.DATABASE_URL?.trim();
  if (!url) throw new ConfigError("DATABASE_URL is required");
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new ConfigError("DATABASE_URL must be a postgres:// URL");
  }
  if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
    throw new ConfigError("DATABASE_URL must be a postgres:// URL");
  }
  return url;
}

/**
 * The role granted access to the `sessions` schema after migrations. Every command that
 * prepares the schema (`serve`, `migrate`, `token`) passes it, so tables a run creates
 * are granted at once.
 */
export function runtimeRole(env: Env = process.env): string | undefined {
  const role = env.RELAYHISTORY_RUNTIME_ROLE?.trim();
  if (role && !/^[a-z_][a-z0-9_$]{0,62}$/.test(role))
    throw new ConfigError(
      "RELAYHISTORY_RUNTIME_ROLE must be a lowercase PostgreSQL role name",
    );
  return role || undefined;
}

export function loadConfig(env: Env = process.env): ServerConfig {
  const role = runtimeRole(env);
  return {
    databaseUrl: databaseUrl(env),
    host: env.HOST?.trim() || "127.0.0.1",
    port: integer(env, "PORT", 8080, 0, 65_535),
    poolMax: integer(env, "RELAYHISTORY_DB_POOL_MAX", 10, 1, 200),
    shutdownTimeoutMs: integer(
      env,
      "RELAYHISTORY_SHUTDOWN_TIMEOUT_MS",
      15_000,
      0,
      600_000,
    ),
    retentionIntervalMs: integer(
      env,
      "RELAYHISTORY_RETENTION_INTERVAL_MS",
      60_000,
      1_000,
      86_400_000,
    ),
    ...(role ? { runtimeRole: role } : {}),
    embeddings: {
      EMBEDDING_API_KEY: env.EMBEDDING_API_KEY,
      OPENAI_API_KEY: env.OPENAI_API_KEY,
      EMBEDDING_API_URL: env.EMBEDDING_API_URL,
      EMBEDDING_MODEL: env.EMBEDDING_MODEL,
    },
  };
}
