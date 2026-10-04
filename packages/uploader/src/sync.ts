/** Local capture before an upload, stoppable by a signal. */
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export class SyncError extends Error {
  override name = "SyncError";
}

/**
 * The capture child's exit code when sync ran but did not complete: another process
 * held the sync lock (`ai-hist watch`, say), or a source reported diagnostics.
 */
export const SYNC_INCOMPLETE_EXIT = 75;

/** How long a stopped capture may take to exit before it is killed outright. */
export const SYNC_KILL_GRACE_MS = 5_000;

export async function runSync(options: {
  dbPath?: string;
  signal?: AbortSignal;
  /** The capture script; tests substitute their own. */
  childPath?: string;
}): Promise<{ completed: boolean }> {
  options.signal?.throwIfAborted();
  const child = spawn(
    process.execPath,
    [
      options.childPath ??
        fileURLToPath(new URL("./sync-child.js", import.meta.url)),
      options.dbPath ?? "",
    ],
    { stdio: ["ignore", "ignore", "inherit"] },
  );
  let kill: NodeJS.Timeout | undefined;
  const stop = () => {
    child.kill("SIGTERM");
    kill = setTimeout(() => child.kill("SIGKILL"), SYNC_KILL_GRACE_MS);
  };
  options.signal?.addEventListener("abort", stop, { once: true });
  try {
    const [code, ended] = await new Promise<
      [number | null, NodeJS.Signals | null]
    >((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (exit, signal) => resolve([exit, signal]));
    });
    options.signal?.throwIfAborted();
    if (code === 0) return { completed: true };
    if (code === SYNC_INCOMPLETE_EXIT) return { completed: false };
    throw new SyncError(
      ended ? `local sync was ended by ${ended}` : `local sync exited ${code}`,
    );
  } finally {
    options.signal?.removeEventListener("abort", stop);
    clearTimeout(kill);
  }
}
