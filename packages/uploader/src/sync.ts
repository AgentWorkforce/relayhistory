/** Local capture before an upload, stoppable by a signal. */
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export class SyncError extends Error {
  override name = "SyncError";
}

/** How long a stopped capture may take to exit before it is killed outright. */
export const SYNC_KILL_GRACE_MS = 5_000;

export async function runSync(options: {
  dbPath?: string;
  signal?: AbortSignal;
  /** The capture script; tests substitute their own. */
  childPath?: string;
}): Promise<void> {
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
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (exit) => resolve(exit));
    });
    options.signal?.throwIfAborted();
    if (code !== 0) throw new SyncError(`local sync exited ${code}`);
  } finally {
    options.signal?.removeEventListener("abort", stop);
    clearTimeout(kill);
  }
}
