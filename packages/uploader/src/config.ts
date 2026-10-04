/**
 * Uploader configuration: the endpoint, the token file, the local store and an
 * explicit selection. Nothing is uploaded that the selection does not name.
 */
import { readFile, stat } from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import {
  CHANGE_KINDS,
  type ChangeKind,
  type HistoryExportSelection,
} from "ai-hist";

export interface UploaderConfigFile {
  /** Base URL of the History service, e.g. `https://history.example.com`. */
  endpoint: string;
  /** Path to the token file `relayhistory-server token create` wrote. Relative to the config file. */
  tokenFile: string;
  /** The local `ai-history.db`. Defaults to the SDK's default path. Relative to the config file. */
  dbPath?: string;
  /** Which sessions and evidence kinds to upload. */
  selection: HistoryExportSelection;
  /** A non-secret label for this machine's uploads. Defaults to `default`. */
  instanceId?: string;
  /** Upper bounds per batch; the server's advertised limits also apply. */
  limits?: { maxRecords?: number; maxBytes?: number };
  /** Permit plain `http://` to a non-loopback host. Tokens then cross the network in clear text. */
  allowInsecureHttp?: boolean;
}

export interface UploaderConfig {
  endpoint: URL;
  token: string;
  accountId: string;
  dbPath?: string;
  selection: HistoryExportSelection;
  instanceId: string;
  limits: { maxRecords: number; maxBytes: number };
}

export class ConfigError extends Error {
  override name = "ConfigError";
}

const CONFIG_FIELDS = [
  "endpoint",
  "tokenFile",
  "dbPath",
  "selection",
  "instanceId",
  "limits",
  "allowInsecureHttp",
];
const SELECTION_FIELDS = [
  "all_sources",
  "sources",
  "sessions",
  "kinds",
  "excluded_sessions",
];
const LABEL = /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$/;
const DEFAULT_MAX_RECORDS = 100;
const DEFAULT_MAX_BYTES = 1_048_576;

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function exactFields(
  value: Record<string, unknown>,
  fields: string[],
  what: string,
) {
  const unknown = Object.keys(value).filter((key) => !fields.includes(key));
  if (unknown.length)
    throw new ConfigError(`${what} has unknown fields: ${unknown.join(", ")}`);
}

function sessionList(value: unknown, field: string) {
  if (
    !Array.isArray(value) ||
    !value.every(
      (item) =>
        object(item) &&
        typeof item.source === "string" &&
        item.source.length > 0 &&
        typeof item.session_id === "string" &&
        item.session_id.length > 0 &&
        Object.keys(item).length === 2,
    )
  )
    throw new ConfigError(
      `selection.${field} must be a list of { source, session_id }`,
    );
  return value.map((item) => ({
    source: item.source as string,
    session_id: item.session_id as string,
  }));
}

export function parseSelection(value: unknown): HistoryExportSelection {
  if (!object(value)) throw new ConfigError("selection must be an object");
  exactFields(value, SELECTION_FIELDS, "selection");
  if (typeof value.all_sources !== "boolean")
    throw new ConfigError("selection.all_sources must be true or false");
  if (
    !Array.isArray(value.sources) ||
    !value.sources.every((s) => typeof s === "string" && s.length > 0)
  )
    throw new ConfigError("selection.sources must be a list of source names");
  const sessions = sessionList(value.sessions, "sessions");
  const excluded = sessionList(value.excluded_sessions, "excluded_sessions");
  if (
    !Array.isArray(value.kinds) ||
    value.kinds.length === 0 ||
    !value.kinds.every((kind) =>
      (CHANGE_KINDS as readonly string[]).includes(kind as string),
    ) ||
    new Set(value.kinds).size !== value.kinds.length
  )
    throw new ConfigError(
      `selection.kinds must name evidence kinds explicitly: ${CHANGE_KINDS.join(", ")}`,
    );
  if (!value.all_sources && value.sources.length === 0 && sessions.length === 0)
    throw new ConfigError(
      "selection must set all_sources or name sources or sessions",
    );
  return {
    all_sources: value.all_sources,
    sources: value.sources as string[],
    sessions,
    kinds: value.kinds as ChangeKind[],
    excluded_sessions: excluded,
  };
}

function loopback(url: URL) {
  return ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
}

function bound(
  value: unknown,
  fallback: number,
  max: number,
  field: string,
): number {
  if (value === undefined) return fallback;
  if (
    !Number.isSafeInteger(value) ||
    (value as number) < 1 ||
    (value as number) > max
  )
    throw new ConfigError(
      `limits.${field} must be an integer from 1 to ${max}`,
    );
  return value as number;
}

/** The secret and the account it is bound to, from a server-written token file. */
export async function readTokenFile(
  path: string,
): Promise<{ token: string; accountId: string }> {
  let info;
  try {
    info = await stat(path);
  } catch {
    throw new ConfigError("token file could not be read");
  }
  // Like ssh with a private key: a token others can read is refused, not warned about.
  if (process.platform !== "win32" && (info.mode & 0o077) !== 0)
    throw new ConfigError(
      "token file must be readable only by its owner (chmod 600)",
    );
  let parsed: unknown;
  try {
    parsed = JSON.parse(await readFile(path, "utf8"));
  } catch {
    throw new ConfigError(
      "token file must be the JSON relayhistory-server token create wrote",
    );
  }
  if (
    !object(parsed) ||
    typeof parsed.token !== "string" ||
    !/^rth_(st|at)_[A-Za-z0-9_-]+$/.test(parsed.token) ||
    typeof parsed.accountId !== "string" ||
    !/^relayhistory:[0-9a-f]{64}$/.test(parsed.accountId)
  )
    throw new ConfigError("token file must carry a token and an accountId");
  return { token: parsed.token, accountId: parsed.accountId };
}

export function parseConfigFile(value: unknown): UploaderConfigFile {
  if (!object(value)) throw new ConfigError("config must be a JSON object");
  exactFields(value, CONFIG_FIELDS, "config");
  if (typeof value.endpoint !== "string")
    throw new ConfigError("endpoint is required");
  if (typeof value.tokenFile !== "string" || !value.tokenFile)
    throw new ConfigError("tokenFile is required");
  if (
    value.dbPath !== undefined &&
    (typeof value.dbPath !== "string" || !value.dbPath)
  )
    throw new ConfigError("dbPath must be a path");
  if (
    value.instanceId !== undefined &&
    (typeof value.instanceId !== "string" || !LABEL.test(value.instanceId))
  )
    throw new ConfigError(
      "instanceId must be a short label of letters, digits and . _ : @ -",
    );
  if (value.limits !== undefined) {
    if (!object(value.limits))
      throw new ConfigError("limits must be an object");
    exactFields(value.limits, ["maxRecords", "maxBytes"], "limits");
  }
  if (
    value.allowInsecureHttp !== undefined &&
    typeof value.allowInsecureHttp !== "boolean"
  )
    throw new ConfigError("allowInsecureHttp must be true or false");
  return {
    endpoint: value.endpoint,
    tokenFile: value.tokenFile,
    ...(value.dbPath !== undefined ? { dbPath: value.dbPath as string } : {}),
    selection: parseSelection(value.selection),
    ...(value.instanceId !== undefined
      ? { instanceId: value.instanceId as string }
      : {}),
    ...(value.limits !== undefined
      ? { limits: value.limits as UploaderConfigFile["limits"] }
      : {}),
    ...(value.allowInsecureHttp !== undefined
      ? { allowInsecureHttp: value.allowInsecureHttp as boolean }
      : {}),
  };
}

export function parseEndpoint(raw: string, allowInsecureHttp = false): URL {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new ConfigError("endpoint must be an http(s) URL");
  }
  if (url.protocol !== "https:" && url.protocol !== "http:")
    throw new ConfigError("endpoint must be an http(s) URL");
  if (url.username || url.password || url.search || url.hash)
    throw new ConfigError(
      "endpoint must not carry credentials, a query or a fragment",
    );
  if (url.protocol === "http:" && !loopback(url) && !allowInsecureHttp)
    throw new ConfigError(
      "endpoint must use https:// (set allowInsecureHttp for a trusted network)",
    );
  return url;
}

/** The endpoint as one string without a trailing slash: `https://host[/base]`. */
export function endpointBase(url: URL): string {
  return `${url.origin}${url.pathname.replace(/\/+$/, "")}`;
}

/** Load and validate a config file; relative paths resolve against its directory. */
export async function loadConfig(path: string): Promise<UploaderConfig> {
  const absolute = resolve(path);
  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(absolute, "utf8"));
  } catch {
    throw new ConfigError("config could not be read as JSON");
  }
  const file = parseConfigFile(raw);
  const base = dirname(absolute);
  const at = (p: string) => (isAbsolute(p) ? p : resolve(base, p));
  const { token, accountId } = await readTokenFile(at(file.tokenFile));
  return {
    endpoint: parseEndpoint(file.endpoint, file.allowInsecureHttp),
    token,
    accountId,
    ...(file.dbPath ? { dbPath: at(file.dbPath) } : {}),
    selection: file.selection,
    instanceId: file.instanceId ?? "default",
    limits: {
      maxRecords: bound(
        file.limits?.maxRecords,
        DEFAULT_MAX_RECORDS,
        10_000,
        "maxRecords",
      ),
      maxBytes: bound(
        file.limits?.maxBytes,
        DEFAULT_MAX_BYTES,
        16_777_216,
        "maxBytes",
      ),
    },
  };
}
