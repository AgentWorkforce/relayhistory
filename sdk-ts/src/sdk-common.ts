/** Shared SDK errors, provider vocabulary, and database path defaults. */
import { homedir } from 'node:os';
import { join } from 'node:path';

export const SESSION_CATALOG_CONTRACT_VERSION = 3;
export const SESSION_HYDRATION_CONTRACT_VERSION = 2;
export const SESSION_RELATIONSHIP_CONTRACT_VERSION = 1;
export const SESSION_EVIDENCE_CONTRACT_VERSION = 1;
export const SESSION_USAGE_CONTRACT_VERSION = 1;

/**
 * The provider ids this build knows, as a value. `Source` is derived from it
 * so the runtime checks below and the compile-time type cannot drift apart,
 * and it is the same list the MCP server's `SOURCE` enum publishes.
 */
export const SOURCES = Object.freeze([
  'claude',
  'codex',
  'cursor',
  'grok',
  'relay',
  'trajectory',
  'opencode',
] as const);

export type Source = (typeof SOURCES)[number];
export type CatalogSource = Exclude<Source, 'trajectory'>;
export const CATALOG_SOURCES: readonly CatalogSource[] = Object.freeze(
  SOURCES.filter((source): source is CatalogSource => source !== 'trajectory'),
);

const SOURCE_SET: ReadonlySet<string> = new Set(SOURCES);
const CATALOG_SOURCE_SET: ReadonlySet<string> = new Set(CATALOG_SOURCES);

/** Return whether an unknown value is a source recognized by this SDK build. */
export function isSource(value: unknown): value is Source {
  return typeof value === 'string' && SOURCE_SET.has(value);
}

/** Return whether an unknown value can identify a session catalog entry. */
export function isCatalogSource(value: unknown): value is CatalogSource {
  return typeof value === 'string' && CATALOG_SOURCE_SET.has(value);
}

export type SessionScope = 'local' | 'remote' | 'all';
export type SessionLocation = Exclude<SessionScope, 'all'>;

export class RelayHistoryError extends Error {
  constructor(message: string, readonly code: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
  }
}

export class UnsupportedPlatformError extends RelayHistoryError {}
export class NativePackageMissingError extends RelayHistoryError {}
export class NativeLoadError extends RelayHistoryError {}
export class NativeContractMismatchError extends RelayHistoryError {}
export class DatabaseOpenError extends RelayHistoryError {}
export class InvalidArgumentError extends RelayHistoryError {}
export class UnsupportedOperationError extends RelayHistoryError {}
export class SessionNotFoundError extends RelayHistoryError {}
export class SessionSourceUnavailableError extends RelayHistoryError {}
export class SessionSourceMismatchError extends RelayHistoryError {}
export class HydrationUnsupportedError extends RelayHistoryError {}
export class HydrationFailedError extends RelayHistoryError {}
export class ConnectorNotConfiguredError extends RelayHistoryError {}
export class AuthenticationExpiredError extends RelayHistoryError {}
export class EvidencePartialError extends RelayHistoryError {}
export class ConnectorFailureError extends RelayHistoryError {}

export function defaultDbPath(): string {
  if (process.env.AI_HIST_DB !== undefined) return process.env.AI_HIST_DB;
  if (process.env.XDG_DATA_HOME !== undefined) {
    return join(process.env.XDG_DATA_HOME, 'ai-hist', 'ai-history.db');
  }
  return join(homedir(), '.local', 'share', 'ai-hist', 'ai-history.db');
}
