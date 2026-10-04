/**
 * `relayhistory-upload run --sync` runs local capture here, in its own process, so a
 * signal can stop it promptly: the native sync does not observe an AbortSignal. Ending
 * this process mid-capture is a crash as far as the store is concerned, which ai-hist
 * survives: its locks are released by the OS and SQLite rolls back the open
 * transaction.
 */
import { sync } from "ai-hist";

const dbPath = process.argv[2];
try {
  await sync(dbPath ? { dbPath } : {});
} catch (error) {
  // Capture errors can carry paths; report the class and code only.
  process.stderr.write(
    `${JSON.stringify({
      time: new Date().toISOString(),
      level: "error",
      message: "local sync failed",
      error: (error as Error)?.name ?? "Error",
      code: (error as { code?: unknown })?.code ?? "unknown",
    })}\n`,
  );
  process.exitCode = 1;
}
