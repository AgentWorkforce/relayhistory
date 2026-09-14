import { nativeCall } from './native.js';
import {
  RelayHistoryError,
  AuthenticationExpiredError,
  SessionNotFoundError,
  SessionSourceUnavailableError,
  HydrationUnsupportedError,
  InvalidArgumentError,
  ConnectorFailureError,
  ConnectorNotConfiguredError,
  type CatalogSource,
} from './sdk-common.js';
import type {
  HistorySource,
  SourceObservationKey,
  SourceObservationState,
  ShallowSourceSession,
} from './source-contracts.js';
import type { HistoryPluginRegistry } from './delivery-plugins.js';
export interface SourcePluginOptions {
  dbPath?: string;
  sourceConnectors?: string[];
  sources?: CatalogSource[];
  sessionId?: string;
  limit?: number;
  signal?: AbortSignal;
  acquisitionTimeoutMs?: number;
  onUnavailable?: (source: string, error: RelayHistoryError) => void;
}
// Reconstruct public errors with safe messages. Plugin messages/causes may
// contain credentials; only these acquisition codes cross the boundary.
export function sourceAcquisitionError(error: unknown): RelayHistoryError {
  const code = error instanceof RelayHistoryError ? error.code : '';
  switch (code) {
    case 'AUTHENTICATION_EXPIRED': return new AuthenticationExpiredError('Source authentication expired', code);
    case 'SESSION_NOT_FOUND': return new SessionNotFoundError('Source session was not found', code);
    case 'SESSION_SOURCE_UNAVAILABLE': return new SessionSourceUnavailableError('Source session is unavailable', code);
    case 'CONNECTOR_NOT_CONFIGURED': return new ConnectorNotConfiguredError('Source connector is not configured', code);
    case 'HYDRATION_UNSUPPORTED': return new HydrationUnsupportedError('Source hydration is unsupported', code);
    case 'SOURCE_ACQUISITION_TIMEOUT':
    case 'HISTORY_PLUGIN_TIMEOUT': return new ConnectorFailureError('Source acquisition timed out', 'SOURCE_ACQUISITION_TIMEOUT');
    case 'SOURCE_ACQUISITION_CANCELLED':
    case 'HISTORY_PLUGIN_CANCELLED': return new ConnectorFailureError('Source acquisition cancelled', 'SOURCE_ACQUISITION_CANCELLED');
    default: return new ConnectorFailureError('Source plugin acquisition failed', 'CONNECTOR_FAILURE');
  }
}
export function sourceAcquisitionTimeout(value?: number): number {
  if (value !== undefined && (!Number.isInteger(value) || value < 1 || value > 3_600_000))
    throw new InvalidArgumentError('acquisitionTimeoutMs must be an integer from 1 to 3600000', 'INVALID_ARGUMENT');
  return value ?? 300_000;
}
export function throwIfSourceAborted(signal?: AbortSignal): void {
  if (signal?.aborted)
    throw new ConnectorFailureError('Source acquisition cancelled', 'SOURCE_ACQUISITION_CANCELLED');
}
async function acquire<T>(
  run: (signal: AbortSignal, timeoutMs: number) => Promise<T>,
  options: { signal?: AbortSignal; acquisitionTimeoutMs?: number },
): Promise<T> {
  const timeoutMs = sourceAcquisitionTimeout(options.acquisitionTimeoutMs);
  const { signal } = options;
  throwIfSourceAborted(signal);
  const controller = new AbortController();
  let rejectAborted!: (reason: unknown) => void;
  const aborted = new Promise<never>((_resolve, reject) => { rejectAborted = reject; });
  const stop = (code: string) => {
    // Settle the boundary before notifying helper listeners so cancellation and
    // timeout keep their distinct codes regardless of helper rejection order.
    rejectAborted(new ConnectorFailureError(
      code === 'SOURCE_ACQUISITION_TIMEOUT' ? 'Source acquisition timed out' : 'Source acquisition cancelled', code));
    controller.abort();
  };
  const cancel = () => stop('SOURCE_ACQUISITION_CANCELLED');
  signal?.addEventListener('abort', cancel, { once: true });
  const timer = setTimeout(() => stop('SOURCE_ACQUISITION_TIMEOUT'), timeoutMs);
  try {
    return await Promise.race([run(controller.signal, timeoutMs), aborted]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', cancel);
  }
}
export function getSourceObservation(
  key: SourceObservationKey,
  options: { dbPath?: string } = {},
): Promise<SourceObservationState> {
  return nativeCall(
    async (native) =>
      JSON.parse(
        await native.getSourceObservation(JSON.stringify({ ...key, db_path: options.dbPath })),
      ) as SourceObservationState,
  );
}
export async function discoverSourcePlugins(
  registry: HistoryPluginRegistry,
  options: SourcePluginOptions = {},
) {
  throwIfSourceAborted(options.signal);
  sourceAcquisitionTimeout(options.acquisitionTimeoutMs);
  const selected = registry
    .sourceConnectors(options.sourceConnectors)
    .filter(
      (source) =>
        !options.sources ||
        source.supportedSources.some((value) => options.sources!.includes(value)),
    );
  if (!selected.length)
    throw new ConnectorNotConfiguredError(
      'No selected source plugin is configured',
      'CONNECTOR_NOT_CONFIGURED',
    );
  const discovered: Array<{
    connector: HistorySource;
    observations: ShallowSourceSession[];
    summary: Record<string, unknown>;
  }> = [];
  let firstFailure: RelayHistoryError | undefined;
  for (const connector of selected) {
    throwIfSourceAborted(options.signal);
    let result: { observations: ShallowSourceSession[] };
    try {
      result = await acquire(
        (signal, acquisitionTimeoutMs) => connector.discover({ ...options, signal, acquisitionTimeoutMs }),
        options,
      );
    } catch (error) {
      throwIfSourceAborted(options.signal);
      const failure = sourceAcquisitionError(error);
      firstFailure ??= failure;
      options.onUnavailable?.(`${connector.id}:${connector.instanceId}`, failure);
      continue;
    }
    if (
      !result ||
      !Array.isArray(result.observations) ||
      result.observations.length > 10_000 ||
      result.observations.some(
        (row) =>
          !row ||
          !connector.supportedSources.includes(row.source) ||
          typeof row.session_id !== 'string' ||
          !row.session_id,
      )
    ) {
      const failure = sourceAcquisitionError(null);
      firstFailure ??= failure;
      options.onUnavailable?.(`${connector.id}:${connector.instanceId}`, failure);
      continue;
    }
    const observations = result.observations.filter(
      (row) =>
        (!options.sessionId || row.session_id === options.sessionId) &&
        (!options.sources || options.sources.includes(row.source)),
    );
    throwIfSourceAborted(options.signal);
    const summary = await nativeCall(
      async (native) =>
        JSON.parse(
          await native.applySourceObservations(
            JSON.stringify({
              db_path: options.dbPath,
              connector_id: connector.id,
              connector_instance: connector.instanceId,
              location: connector.location,
              observations,
            }),
          ),
        ) as Record<string, unknown>,
    );
    discovered.push({ connector, observations, summary });
  }
  if (!discovered.length && !options.onUnavailable)
    throw firstFailure ?? new ConnectorNotConfiguredError(
      'No selected source plugin is available',
      'CONNECTOR_NOT_CONFIGURED',
    );
  return discovered;
}
export async function hydrateSourcePlugin(
  connector: HistorySource,
  identity: { source: CatalogSource; sessionId: string },
  options: { dbPath?: string; signal?: AbortSignal; acquisitionTimeoutMs?: number } = {},
) {
  sourceAcquisitionTimeout(options.acquisitionTimeoutMs);
  throwIfSourceAborted(options.signal);
  const key: SourceObservationKey = {
    source: identity.source,
    session_id: identity.sessionId,
    location: connector.location,
    connector_id: connector.id,
    connector_instance: connector.instanceId,
  };
  const state = await getSourceObservation(key, options);
  if (!state.observation || state.revision === null)
    throw new ConnectorNotConfiguredError(
      'Discover this source observation before hydration',
      'CONNECTOR_NOT_CONFIGURED',
    );
  throwIfSourceAborted(options.signal);
  let snapshot;
  try {
    snapshot = await acquire(
      (signal, acquisitionTimeoutMs) => connector.hydrate(state.observation!, { signal, acquisitionTimeoutMs }),
      options,
    );
  } catch (error) {
    throw sourceAcquisitionError(error);
  }
  throwIfSourceAborted(options.signal);
  if (
    Array.isArray(snapshot.covered_kinds) &&
    snapshot.covered_kinds.length === 0 &&
    Array.isArray(snapshot.records) &&
    snapshot.records.length === 0
  )
    return {
      contract_version: 2,
      source: identity.source,
      session_id: identity.sessionId,
      status: 'capability_limited',
      capability: 'shallow_only',
      discovery_state: state.observation.discovery_state,
      presence: connector.location,
      indexed_through: { source_stamp: null, last_event_at_ms: null },
      evidence: { prompts: 0, events: 0, tool_calls: 0, file_edits: 0, related_sessions: 0 },
      related_session_ids: [],
      diagnostics: [
        {
          code: 'SOURCE_CAPABILITY_LIMITED',
          message: 'Selected source exposes no supported normalized evidence',
          duration_ms: null,
          source_bytes: snapshot.source_bytes,
          records_parsed: 0,
        },
      ],
    };
  return nativeCall(
    async (native) =>
      JSON.parse(
        await native.applySourceEvidence(
          JSON.stringify({
            ...key,
            db_path: options.dbPath,
            expected_revision: state.revision,
            source_stamp: snapshot.source_stamp,
            source_bytes: snapshot.source_bytes,
            covered_kinds: snapshot.covered_kinds,
            records: snapshot.records,
          }),
        ),
      ) as Record<string, unknown>,
  );
}

/** Convert only the typed result envelope; evidence payloads never pass here. */
export function camelSourceResult(value: Record<string, unknown>): Record<string, unknown> {
  const convert = (item: unknown): unknown =>
    Array.isArray(item)
      ? item.map(convert)
      : item && typeof item === 'object'
        ? Object.fromEntries(
            Object.entries(item).map(([key, val]) => [
              key.replace(/_([a-z])/g, (_match, letter: string) => letter.toUpperCase()),
              convert(val),
            ]),
          )
        : item;
  return convert(value) as Record<string, unknown>;
}
