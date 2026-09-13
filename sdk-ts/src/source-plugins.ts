import { nativeCall } from './native.js';
import {
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
  onUnavailable?: (source: string) => void;
}
async function acquire<T>(
  run: (signal: AbortSignal) => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  signal?.throwIfAborted();
  const controller = new AbortController();
  let rejectAborted!: (reason: unknown) => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    rejectAborted = reject;
  });
  const stop = () => {
    controller.abort();
    rejectAborted(
      new ConnectorFailureError('Source acquisition cancelled or timed out', 'CONNECTOR_FAILURE'),
    );
  };
  signal?.addEventListener('abort', stop, { once: true });
  const timer = setTimeout(stop, 30_000);
  try {
    return await Promise.race([run(controller.signal), aborted]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', stop);
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
  options.signal?.throwIfAborted();
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
  for (const connector of selected) {
    options.signal?.throwIfAborted();
    let result: { observations: ShallowSourceSession[] };
    try {
      result = await acquire(
        (signal) => connector.discover({ ...options, signal }),
        options.signal,
      );
    } catch {
      options.signal?.throwIfAborted();
      options.onUnavailable?.(`${connector.id}:${connector.instanceId}`);
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
      options.onUnavailable?.(`${connector.id}:${connector.instanceId}`);
      continue;
    }
    const observations = result.observations.filter(
      (row) =>
        (!options.sessionId || row.session_id === options.sessionId) &&
        (!options.sources || options.sources.includes(row.source)),
    );
    options.signal?.throwIfAborted();
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
    throw new ConnectorNotConfiguredError(
      'No selected source plugin is available',
      'CONNECTOR_NOT_CONFIGURED',
    );
  return discovered;
}
export async function hydrateSourcePlugin(
  connector: HistorySource,
  identity: { source: CatalogSource; sessionId: string },
  options: { dbPath?: string; signal?: AbortSignal } = {},
) {
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
  options.signal?.throwIfAborted();
  let snapshot;
  try {
    snapshot = await acquire(
      (signal) => connector.hydrate(state.observation!, { signal }),
      options.signal,
    );
  } catch {
    throw new ConnectorFailureError('Source plugin acquisition failed', 'CONNECTOR_FAILURE');
  }
  options.signal?.throwIfAborted();
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
