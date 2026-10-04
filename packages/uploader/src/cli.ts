#!/usr/bin/env node
/**
 * relayhistory-upload check --config FILE
 * relayhistory-upload run --config FILE [--dry-run] [--sync] [--watch] [--interval SECONDS]
 */
import { parseArgs } from "node:util";
import { sync } from "ai-hist";
import { HistoryClient, UploadError } from "./client.js";
import {
  ConfigError,
  endpointBase,
  loadConfig,
  type UploaderConfig,
} from "./config.js";
import { createLogger, type Logger } from "./log.js";
import { consumerName, upload } from "./uploader.js";

const USAGE = `Usage:
  relayhistory-upload check --config FILE
  relayhistory-upload run --config FILE [--dry-run] [--sync] [--watch] [--interval SECONDS]

check     Validate the config and token file and confirm the server accepts the token.
run       Upload selected changes until the local feed is drained, then print a summary.
          --dry-run   read and select only: nothing is sent and the cursor does not move
          --sync      capture local sessions (ai-hist sync) before each upload
          --watch     keep running; upload again every --interval seconds (default 60)
`;

class UsageError extends Error {}

/** Exit codes: 0 ok, 1 retryable failure, 2 needs the operator (config, auth, refused data). */
function exitCode(error: unknown): number {
  if (error instanceof UploadError) return error.retryable ? 1 : 2;
  if (error instanceof ConfigError || error instanceof UsageError) return 2;
  return 1;
}

async function check(config: UploaderConfig, log: Logger) {
  const limits = await new HistoryClient({
    endpoint: config.endpoint,
    token: config.token,
  }).limits();
  process.stdout.write(
    `${JSON.stringify(
      {
        endpoint: endpointBase(config.endpoint),
        accountId: config.accountId,
        consumer: consumerName(config),
        serverLimits: limits,
      },
      null,
      2,
    )}\n`,
  );
  log.info("check passed");
}

async function runOnce(
  config: UploaderConfig,
  log: Logger,
  flags: { dryRun: boolean; sync: boolean },
  signal: AbortSignal,
) {
  if (flags.sync) await sync(config.dbPath ? { dbPath: config.dbPath } : {});
  const summary = await upload({ config, log, signal, dryRun: flags.dryRun });
  process.stdout.write(
    `${JSON.stringify({ dryRun: flags.dryRun, ...summary })}\n`,
  );
}

function describe(error: unknown) {
  if (error instanceof UploadError)
    return { failure: error.failure, detail: error.message };
  if (error instanceof ConfigError)
    return { failure: "config", detail: error.message };
  // SDK and driver errors may carry paths but never the token; report class and code.
  return {
    failure: "error",
    error: (error as Error)?.name ?? "Error",
    code: (error as { code?: unknown })?.code ?? "unknown",
  };
}

async function main(argv: string[]) {
  const log = createLogger();
  const [command, ...rest] = argv;
  if (
    !command ||
    command === "help" ||
    command === "--help" ||
    command === "-h"
  ) {
    process.stdout.write(USAGE);
    return;
  }
  const controller = new AbortController();
  const stop = () => controller.abort(new Error("stopped"));
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  try {
    const { values } = parseArgs({
      args: rest,
      options: {
        config: { type: "string" },
        "dry-run": { type: "boolean", default: false },
        sync: { type: "boolean", default: false },
        watch: { type: "boolean", default: false },
        interval: { type: "string" },
      },
      strict: true,
    });
    if (!values.config) throw new UsageError("--config is required");
    const config = await loadConfig(values.config);
    if (command === "check") return await check(config, log);
    if (command !== "run") throw new UsageError(`unknown command ${command}`);
    const interval =
      values.interval === undefined
        ? 60
        : /^\d+$/.test(values.interval)
          ? Number(values.interval)
          : NaN;
    if (!Number.isSafeInteger(interval) || interval < 1)
      throw new UsageError("--interval must be a whole number of seconds");
    const flags = { dryRun: values["dry-run"], sync: values.sync };
    if (!values.watch)
      return await runOnce(config, log, flags, controller.signal);
    while (!controller.signal.aborted) {
      try {
        await runOnce(config, log, flags, controller.signal);
      } catch (error) {
        if (controller.signal.aborted) break;
        // Retryable failures wait for the next round; anything else needs the operator.
        if (exitCode(error) !== 1) throw error;
        log.warn(
          "upload round failed; retrying next interval",
          describe(error),
        );
      }
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, interval * 1_000);
        controller.signal.addEventListener(
          "abort",
          () => {
            clearTimeout(timer);
            resolve();
          },
          { once: true },
        );
      });
    }
    log.info("stopped");
  } catch (error) {
    if (controller.signal.aborted) {
      log.info(
        "stopped before the upload finished; the cursor stays at the last durable page",
      );
      process.exitCode = 1;
      return;
    }
    if (
      error instanceof UsageError ||
      (error as { code?: string })?.code?.startsWith?.("ERR_PARSE_ARGS")
    ) {
      process.stderr.write(`${(error as Error).message}\n\n${USAGE}`);
      process.exitCode = 2;
      return;
    }
    log.error("upload failed", describe(error));
    process.exitCode = exitCode(error);
  }
}

await main(process.argv.slice(2));
