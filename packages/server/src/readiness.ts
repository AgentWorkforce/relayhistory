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
  /** The readiness connection: its client, the socket it owns, and its connect. */
  interface Connection {
    client: pg.Client;
    socket: Socket;
    connected: Promise<unknown>;
  }
  let current: Connection | undefined;
  let inFlight: Promise<boolean> | undefined;

  // Tear down a connection only if it is still the current one: a timed-out check's
  // late failure must never destroy the connection a newer probe has opened since.
  const drop = (connection: Connection) => {
    connection.socket.destroy();
    if (current === connection) current = undefined;
  };

  const open = (): Connection => {
    const socket = new Socket();
    const client = new pg.Client({
      connectionString: url,
      application_name: "relayhistory-ready",
      statement_timeout: timeoutMs,
      stream: () => socket,
    });
    // Errors after a check settles (a dropped or destroyed socket) are expected.
    client.on("error", () => {});
    const connected = client.connect();
    connected.catch(() => {});
    return { client, socket, connected };
  };

  const check = async () => {
    const connection = (current ??= open());
    let timer: NodeJS.Timeout | undefined;
    // One deadline covers connecting and the query.
    const deadline = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => {
        drop(connection);
        resolve(false);
      }, timeoutMs);
    });
    const attempt = (async () => {
      await connection.connected;
      await connection.client.query("SELECT 1");
      return true;
    })().catch(() => {
      drop(connection);
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
    const closing = current;
    if (!closing) return;
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      closing.client.end().catch(() => {}),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, timeoutMs);
      }),
    ]);
    clearTimeout(timer);
    drop(closing);
  };

  return ready;
}
