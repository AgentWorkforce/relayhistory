/**
 * The database check behind `/ready`. Each check uses its own short-lived connection
 * with a connect timeout and a server-side statement timeout, so a saturated request
 * pool or a stuck database answers "not ready" within a bound instead of leaving
 * probes waiting. Concurrent probes share the check in flight, so they never pile up.
 */
import pg from "pg";

export const READINESS_TIMEOUT_MS = 2_000;

export function databaseReadiness(
  url: string,
  timeoutMs = READINESS_TIMEOUT_MS,
): () => Promise<boolean> {
  let inFlight: Promise<boolean> | undefined;
  const check = async () => {
    const client = new pg.Client({
      connectionString: url,
      application_name: "relayhistory-ready",
      connectionTimeoutMillis: timeoutMs,
      statement_timeout: timeoutMs,
    });
    // A dropped connection after the check must not become an unhandled error.
    client.on("error", () => {});
    try {
      await client.connect();
      await client.query("SELECT 1");
      return true;
    } catch {
      return false;
    } finally {
      await client.end().catch(() => {});
    }
  };
  return () =>
    (inFlight ??= check().finally(() => {
      inFlight = undefined;
    }));
}
