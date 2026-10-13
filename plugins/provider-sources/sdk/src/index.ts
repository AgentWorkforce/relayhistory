import {
  InvalidArgumentError,
  type CatalogSource,
  type HistoryPlugin,
  type HistorySource,
  type SourceEvidenceSnapshot,
  type ShallowSourceSession,
} from 'ai-hist';
import { helperRequest, type HelperOptions } from './helper.js';
const CONNECTOR_SOURCES = {
  'claude-web': 'claude',
  'codex-cloud': 'codex',
  'devin-cloud': 'devin',
} as const;
type ProviderConnectorId = keyof typeof CONNECTOR_SOURCES;
export interface ProviderSourceOptions extends HelperOptions {
  connectors?: ProviderConnectorId[];
  instanceId?: string;
}
/** Registration is inert; provider credentials are read only by selected acquisitions. */
export function createHistoryPlugin(options: ProviderSourceOptions = {}): HistoryPlugin {
  const connectors = options.connectors ?? (Object.keys(CONNECTOR_SOURCES) as ProviderConnectorId[]);
  if (
    !Array.isArray(connectors) ||
    connectors.some((id) => !(id in CONNECTOR_SOURCES)) ||
    new Set(connectors).size !== connectors.length
  )
    throw new InvalidArgumentError(
      'Provider connectors must be unique claude-web, codex-cloud or devin-cloud IDs',
      'INVALID_ARGUMENT',
    );
  const sources: HistorySource[] = connectors.map((connectorId) => {
    const source: CatalogSource = CONNECTOR_SOURCES[connectorId];
    const connectorInstance = options.instanceId ?? 'default';
    return {
      id: connectorId,
      instanceId: connectorInstance,
      location: 'remote',
      supportedSources: [source],
      discover: async (query) =>
        helperRequest<{ observations: ShallowSourceSession[] }>(
          'discover',
          { connectorId, connectorInstance, source, limit: query.limit },
          { ...options, signal: query.signal, timeoutMs: query.acquisitionTimeoutMs ?? options.timeoutMs },
        ),
      hydrate: async (observation, context) =>
        helperRequest<SourceEvidenceSnapshot>(
          'hydrate',
          // The helper derives relationship evidence from the transcript it
          // already fetched, so honouring this is a matter of not reporting or
          // returning it rather than of skipping a separate request.
          { connectorId, connectorInstance, observation, includeRelated: context.includeRelated },
          { ...options, signal: context.signal, timeoutMs: context.acquisitionTimeoutMs ?? options.timeoutMs },
        ),
    };
  });
  return { sources };
}
export type { HistorySource } from 'ai-hist';
