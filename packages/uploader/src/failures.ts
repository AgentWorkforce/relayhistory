/** How the CLI reports and exits on a failure. Nothing here prints a secret. */
import { UploadError } from "./client.js";
import { ConfigError } from "./config.js";
import { SyncError } from "./sync.js";

export class UsageError extends Error {}

/** Exit codes: 0 ok (including --watch stopped between rounds), 1 retryable failure or
 * a signal during an upload, 2 needs the operator (config, auth, refused data). */
export function exitCode(error: unknown): number {
  if (error instanceof UploadError) return error.retryable ? 1 : 2;
  if (error instanceof ConfigError || error instanceof UsageError) return 2;
  return 1;
}

/** Log fields for a failure. Messages the uploader wrote itself are kept; anything
 * else (SDK, driver) is reduced to class and code, since it may carry paths. */
export function describe(error: unknown): Record<string, unknown> {
  if (error instanceof UploadError)
    return { failure: error.failure, detail: error.message };
  if (error instanceof ConfigError)
    return { failure: "config", detail: error.message };
  // Fixed wording naming only the capture's exit code or signal.
  if (error instanceof SyncError)
    return { failure: "sync", detail: error.message };
  return {
    failure: "error",
    error: (error as Error)?.name ?? "Error",
    code: (error as { code?: unknown })?.code ?? "unknown",
  };
}
