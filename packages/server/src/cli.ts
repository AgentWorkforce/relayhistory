#!/usr/bin/env node
/**
 * relayhistory-server serve
 * relayhistory-server migrate
 * relayhistory-server token create --org ORG --workspace WS --label LABEL --out FILE
 *                                  [--scopes rth:sync,rth:read] [--expires-days N]
 * relayhistory-server token list --org ORG
 * relayhistory-server token revoke --org ORG --id ID
 */
import { parseArgs } from "node:util";
import { ServiceTokenError } from "@relayhistory/engine";
import { ConfigError, databaseUrl, loadConfig } from "./config.js";
import { openDatabase, prepareDatabase } from "./database.js";
import { createLogger, type Logger } from "./log.js";
import { startServer } from "./server.js";
import { createTokenFile, listTokens, revokeToken } from "./tokens.js";

const USAGE = `Usage:
  relayhistory-server serve
  relayhistory-server migrate
  relayhistory-server token create --org ORG --workspace WS --label LABEL --out FILE
                                   [--scopes rth:sync,rth:read] [--expires-days N]
  relayhistory-server token list --org ORG
  relayhistory-server token revoke --org ORG --id ID

Environment: DATABASE_URL (required), HOST, PORT, RELAYHISTORY_DB_POOL_MAX,
RELAYHISTORY_SHUTDOWN_TIMEOUT_MS, RELAYHISTORY_RETENTION_INTERVAL_MS,
RELAYHISTORY_RUNTIME_ROLE, EMBEDDING_API_KEY|OPENAI_API_KEY, EMBEDDING_API_URL,
EMBEDDING_MODEL.
`;

class UsageError extends Error {}

async function serve(log: Logger) {
  const server = await startServer(loadConfig(), log);
  const drained = await new Promise<boolean>((resolve) => {
    let signals = 0;
    const stop = (signal: NodeJS.Signals) => {
      signals += 1;
      if (signals > 1) {
        log.warn("forced exit", { signal });
        process.exit(1);
      }
      log.info("shutdown requested", { signal });
      server.close().then(resolve, (error: unknown) => {
        log.error("shutdown failed", {
          code: (error as { code?: unknown })?.code ?? "unknown",
        });
        resolve(false);
      });
    };
    process.on("SIGTERM", stop);
    process.on("SIGINT", stop);
  });
  // Exit even if a stuck database connection still holds the event loop.
  process.exit(drained ? 0 : 1);
}

async function migrate(log: Logger) {
  const config = loadConfig();
  const prepared = await prepareDatabase(config.databaseUrl, {
    ...(config.runtimeRole ? { runtimeRole: config.runtimeRole } : {}),
    log,
  });
  log.info("database ready", {
    migrations: prepared.migrations,
    rolledUpSessions: prepared.rolledUpSessions,
  });
}

function required(values: Record<string, unknown>, name: string): string {
  const value = values[name];
  if (typeof value !== "string" || !value.trim())
    throw new UsageError(`--${name} is required`);
  return value.trim();
}

function wholeDays(value: string): number {
  if (!/^\d+$/.test(value))
    throw new UsageError("--expires-days must be a whole number of days");
  return Number(value);
}

async function token(args: string[], log: Logger) {
  const [action, ...rest] = args;
  const { values } = parseArgs({
    args: rest,
    options: {
      org: { type: "string" },
      workspace: { type: "string" },
      label: { type: "string" },
      scopes: { type: "string" },
      "expires-days": { type: "string" },
      out: { type: "string" },
      id: { type: "string" },
    },
    strict: true,
  });
  if (action !== "create" && action !== "list" && action !== "revoke")
    throw new UsageError("token needs create, list or revoke");
  // Every argument is checked before the database is touched.
  const org = required(values, "org");
  const create =
    action === "create"
      ? {
          out: required(values, "out"),
          options: {
            orgId: org,
            workspaceId: required(values, "workspace"),
            label: required(values, "label"),
            ...(values.scopes
              ? {
                  scopes: values.scopes.split(",").map((scope) => scope.trim()),
                }
              : {}),
            ...(values["expires-days"] !== undefined
              ? { expiresInDays: wholeDays(values["expires-days"]) }
              : {}),
          },
        }
      : undefined;
  const id = action === "revoke" ? required(values, "id") : undefined;
  // Token commands may run before the first `serve`; the schema they write must exist.
  const url = databaseUrl();
  await prepareDatabase(url, { log });
  const database = openDatabase(url, { max: 1, log });
  try {
    if (create) {
      const { out } = create;
      const file = await createTokenFile(
        database.db,
        create.options,
        // `--out -` hands the token file to a pipe; otherwise the secret only goes to disk.
        out === "-"
          ? {
              write: (text) =>
                new Promise<void>((resolve, reject) =>
                  process.stdout.write(text, (error) =>
                    error ? reject(error) : resolve(),
                  ),
                ),
            }
          : { path: out },
      );
      log.info("token created", {
        id: file.id,
        label: file.label,
        scopes: file.scopes,
        expiresAt: file.expiresAt,
        orgId: file.orgId,
        workspaceId: file.workspaceId,
        accountId: file.accountId,
        ...(out === "-" ? {} : { out }),
      });
    } else if (action === "list") {
      const tokens = await listTokens(database.db, org);
      process.stdout.write(`${JSON.stringify(tokens, null, 2)}\n`);
    } else {
      if (!(await revokeToken(database.db, org, id!)))
        throw new UsageError("no active service token with that id");
      log.info("token revoked", { id });
    }
  } finally {
    await database.close();
  }
}

async function main(argv: string[]) {
  const log = createLogger();
  const [command, ...rest] = argv;
  try {
    if (command === "serve") await serve(log);
    else if (command === "migrate") await migrate(log);
    else if (command === "token") await token(rest, log);
    else if (command === "help" || command === "--help" || command === "-h")
      process.stdout.write(USAGE);
    else
      throw new UsageError(
        command ? `unknown command ${command}` : "a command is required",
      );
  } catch (error) {
    if (
      error instanceof UsageError ||
      String((error as { code?: unknown })?.code).startsWith("ERR_PARSE_ARGS_")
    ) {
      process.stderr.write(`${(error as Error).message}\n\n${USAGE}`);
      process.exitCode = 2;
    } else if (
      error instanceof ConfigError ||
      error instanceof ServiceTokenError
    ) {
      log.error((error as Error).message);
      process.exitCode = 2;
    } else if ((error as { code?: string })?.code === "EEXIST") {
      log.error("token file already exists; choose another --out path");
      process.exitCode = 2;
    } else {
      // Driver errors can echo SQL or connection details; report the class and code only.
      log.error("command failed", {
        error: (error as Error)?.name ?? "Error",
        code: (error as { code?: unknown })?.code ?? "unknown",
      });
      process.exitCode = 1;
    }
  }
}

await main(process.argv.slice(2));
