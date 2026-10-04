/**
 * The database check behind `/ready`.
 *
 * It keeps one connection of its own, opened on first use and reused by every probe, so
 * readiness neither waits behind a saturated request pool nor needs a free server slot
 * per probe. Every check has a hard deadline: when it passes, the check destroys the
 * connection's socket, which settles anything still waiting on it, and answers not
 * ready; the next probe reconnects. A database that accepts connections but stops
 * answering therefore costs at most one deadline per probe, never a probe that hangs.
 * Concurrent probes share the check in flight.
 */
import { Socket } from "node:net";
import pg from "pg";

export const READINESS_TIMEOUT_MS = 2_000;

export interface DatabaseReadiness {
  (): Promise<boolean>;
  /** Close the readiness connection, within the same deadline. */
  close(): Promise<void>;
}

export function databaseReadiness(
  url: string,
  timeoutMs = READINESS_TIMEOUT_MS,
): DatabaseReadiness {
  let client: pg.Client | undefined;
  let socket: Socket | undefined;
  let inFlight: Promise<boolean> | undefined;

  const drop = () => {
    socket?.destroy();
    socket = undefined;
    client = undefined;
  };

  const query = async () => {
    if (!client) {
      const own = new Socket();
      const fresh = new pg.Client({
        connectionString: url,
        application_name: "relayhistory-ready",
        statement_timeout: timeoutMs,
        stream: () => own,
      });
      // Errors after a check settles (a dropped or destroyed socket) are expected.
      fresh.on("error", () => {});
      socket = own;
      client = fresh;
      await fresh.connect();
    }
    await client.query("SELECT 1");
    return true;
  };

  const check = async () => {
    let timer: NodeJS.Timeout | undefined;
    const deadline = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => {
        drop();
        resolve(false);
      }, timeoutMs);
    });
    const attempt = query().catch(() => {
      drop();
      return false;
    });
    try {
      return await Promise.race([attempt, deadline]);
    } finally {
      clearTimeout(timer);
    }
  };

  const ready = (() =>
    (inFlight ??= check().finally(() => {
      inFlight = undefined;
    }))) as DatabaseReadiness;

  ready.close = async () => {
    const closing = client;
    if (!closing) return;
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      closing.end().catch(() => {}),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      }),
    ]);
    clearTimeout(timer);
    drop();
  };

  return ready;
}
