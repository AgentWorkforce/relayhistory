#!/usr/bin/env node
/**
 * relayhistory-server serve
 * relayhistory-server migrate
 * relayhistory-server token create --org ORG --workspace WS --label LABEL --out FILE
 *                                  [--scopes rth:sync,rth:read] [--expires-days N]
 * relayhistory-server token list --org ORG
 * relayhistory-server token revoke --org ORG --id ID
 */
import { ServiceTokenError } from "@relayhistory/engine";
import { ConfigError, loadConfig } from "./config.js";
import { prepareDatabase } from "./database.js";
import { createLogger, type Logger } from "./log.js";
import { startServer } from "./server.js";
import { UndeliveredTokenError } from "./tokens.js";
import { tokenCommand, UsageError } from "./token-command.js";

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
  // A clean drain leaves nothing holding the event loop, so the process exits on its own
  // once stderr has flushed. Only a stuck database connection past the deadline forces it.
  // The flush gets a bounded moment: a stderr pipe nobody reads must not outlive the
  // deadline either.
  if (!drained) {
    process.exitCode = 1;
    setTimeout(() => process.exit(1), FORCED_EXIT_FLUSH_MS).unref();
    process.stderr.write("", () => process.exit(1));
  }
}

const FORCED_EXIT_FLUSH_MS = 1_000;

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

async function main(argv: string[]) {
  const log = createLogger();
  const [command, ...rest] = argv;
  try {
    if (command === "serve") await serve(log);
    else if (command === "migrate") await migrate(log);
    else if (command === "token") await tokenCommand(rest, log);
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
      error instanceof ServiceTokenError ||
      error instanceof UndeliveredTokenError
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
