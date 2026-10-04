/**
 * Process lifecycle: prepare the schema, listen, run background jobs, and drain on
 * shutdown.
 */
import type { AddressInfo } from "node:net";
import type { Server } from "node:http";
import { serve } from "@hono/node-server";
import {
  embeddingProviderFromEnv,
  expireBabysitterEvidence,
} from "@relayhistory/engine";
import { createServerApp } from "./app.js";
import type { ServerConfig } from "./config.js";
import { openDatabase, prepareDatabase } from "./database.js";
import { startJob, type RunningJob } from "./jobs.js";
import { databaseReadiness } from "./readiness.js";
import type { Logger } from "./log.js";

/** Minimum time to stop jobs and close the pool once requests are done. */
export const CLEANUP_GRACE_MS = 5_000;

/** Time left to stop jobs and close the pool: the rest of the deadline, at least the grace. */
export function cleanupBudgetMs(deadline: number, now: number): number {
  return Math.max(CLEANUP_GRACE_MS, deadline - now);
}

/**
 * Bind and serve. A failure to bind (the port in use, say) rejects. Once listening, the
 * startup listener is replaced by one that stays: a later server error, such as an
 * accept failure when file descriptors run out, is logged by code and the server keeps
 * listening, rather than being dropped or crashing the process as an unheard event.
 */
export function listen(
  fetch: (request: Request) => Response | Promise<Response>,
  host: string,
  port: number,
  log: Logger,
): Promise<Server> {
  return new Promise<Server>((resolve, reject) => {
    const server = serve({ fetch, hostname: host, port }, () => {
      server.removeListener("error", reject);
      server.on("error", (error: Error & { code?: string; syscall?: string }) =>
        log.error("server error", {
          code: error.code ?? "unknown",
          ...(error.syscall ? { syscall: error.syscall } : {}),
        }),
      );
      resolve(server);
    }) as Server;
    server.once("error", reject);
  });
}

export interface RunningServer {
  /** The bound port (useful when `PORT=0`). */
  port: number;
  /**
   * Stop accepting, finish in-flight requests, stop jobs and close the pool, within the
   * shutdown timeout. Resolves false when the deadline passed first. Idempotent.
   */
  close(): Promise<boolean>;
}

export async function startServer(
  config: ServerConfig,
  log: Logger,
): Promise<RunningServer> {
  const prepared = await prepareDatabase(config.databaseUrl, {
    ...(config.runtimeRole ? { runtimeRole: config.runtimeRole } : {}),
    log,
  });
  log.info("database ready", {
    migrations: prepared.migrations,
    rolledUpSessions: prepared.rolledUpSessions,
  });

  const database = openDatabase(config.databaseUrl, {
    max: config.poolMax,
    log,
  });
  const embeddings = embeddingProviderFromEnv(config.embeddings);
  const readiness = databaseReadiness(config.databaseUrl);
  let accepting = false;
  const app = createServerApp({
    db: database.db,
    databaseReady: readiness,
    embeddings,
    log,
    accepting: () => accepting,
  });

  let server: Server;
  try {
    server = await listen(app.fetch, config.host, config.port, log);
  } catch (error) {
    await database.close();
    throw error;
  }

  const jobs: RunningJob[] = [
    startJob(
      {
        name: "retention",
        intervalMs: config.retentionIntervalMs,
        run: () => expireBabysitterEvidence(database.db),
      },
      log,
    ),
  ];
  accepting = true;
  const port = (server.address() as AddressInfo).port;
  log.info("listening", {
    host: config.host,
    port,
    embeddings: embeddings ? embeddings.model : "off",
  });

  let closing: Promise<boolean> | undefined;
  const close = () =>
    (closing ??= (async () => {
      accepting = false;
      log.info("draining", { timeoutMs: config.shutdownTimeoutMs });
      const deadline = Date.now() + config.shutdownTimeoutMs;
      const closed = new Promise<void>((resolve) =>
        server.close(() => resolve()),
      );
      server.closeIdleConnections();
      const cutoff = setTimeout(
        () => server.closeAllConnections(),
        config.shutdownTimeoutMs,
      );
      await closed;
      clearTimeout(cutoff);
      // Requests get the shutdown timeout; stopping jobs and closing the pool then get
      // whatever is left of it, but never less than a short grace of their own, so
      // connections forced closed at the deadline do not turn a prompt cleanup into a
      // reported stall. A job or connection stuck on the database still cannot hold the
      // process: its work is bounded and resumes from PostgreSQL on the next start.
      const cleanupMs = cleanupBudgetMs(deadline, Date.now());
      let expire: NodeJS.Timeout | undefined;
      const drained = await Promise.race([
        Promise.all([...jobs.map((job) => job.stop()), readiness.close()])
          .then(() => database.close())
          .then(() => true),
        new Promise<boolean>((resolve) => {
          expire = setTimeout(() => resolve(false), cleanupMs);
        }),
      ]);
      clearTimeout(expire);
      if (drained) log.info("stopped");
      else
        log.warn("shutdown cleanup timed out with database work in progress");
      return drained;
    })());

  return { port, close };
}
