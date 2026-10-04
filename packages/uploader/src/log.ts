/**
 * One JSON object per line on stderr. Callers pass fixed messages and non-secret
 * fields only: never the token, payloads, or server response bodies.
 */
export interface Logger {
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

export function createLogger(
  write: (line: string) => void = (line) => process.stderr.write(line),
): Logger {
  const emit =
    (level: string) =>
    (message: string, fields: Record<string, unknown> = {}) =>
      write(
        `${JSON.stringify({ time: new Date().toISOString(), level, message, ...fields })}\n`,
      );
  return { info: emit("info"), warn: emit("warn"), error: emit("error") };
}

export const silentLogger: Logger = { info() {}, warn() {}, error() {} };
