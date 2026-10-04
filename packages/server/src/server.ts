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
import type { Logger } from "./log.js";

export interface RunningServer {
  /** The bound port (useful when `PORT=0`). */
  port: number;
  /** Stop accepting, finish in-flight requests, stop jobs and close the pool. Idempotent. */
  close(): Promise<void>;
}

export async function startServer(
  config: ServerConfig,
  log: Logger,
): Promise<RunningServer> {
  const prepared = await prepareDatabase(config.databaseUrl, {
    ...(config.runtimeRole ? { runtimeRole: config.runtimeRole } : {}),
    log,
  });
  log.info("database ready", { migrations: prepared.migrations });

  const database = openDatabase(config.databaseUrl, {
    max: config.poolMax,
    log,
  });
  const embeddings = embeddingProviderFromEnv(config.embeddings);
  let accepting = false;
  const app = createServerApp({
    db: database.db,
    pool: database.pool,
    embeddings,
    accepting: () => accepting,
  });

  let server: Server;
  try {
    server = await new Promise<Server>((resolve, reject) => {
      const listening = serve(
        { fetch: app.fetch, hostname: config.host, port: config.port },
        () => resolve(listening as Server),
      ) as Server;
      listening.once("error", reject);
    });
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

  let closing: Promise<void> | undefined;
  const close = () =>
    (closing ??= (async () => {
      accepting = false;
      log.info("draining", { timeoutMs: config.shutdownTimeoutMs });
      const closed = new Promise<void>((resolve) =>
        server.close(() => resolve()),
      );
      server.closeIdleConnections();
      const deadline = setTimeout(
        () => server.closeAllConnections(),
        config.shutdownTimeoutMs,
      );
      await closed;
      clearTimeout(deadline);
      await Promise.all(jobs.map((job) => job.stop()));
      await database.close();
      log.info("stopped");
    })());

  return { port, close };
}
