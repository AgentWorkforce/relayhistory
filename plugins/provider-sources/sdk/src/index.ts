import {
  InvalidArgumentError,
  type CatalogSource,
  type HistoryPlugin,
  type HistorySource,
  type SourceEvidenceSnapshot,
  type ShallowSourceSession,
} from 'ai-hist';
import { helperRequest, type HelperOptions } from './helper.js';
export interface ProviderSourceOptions extends HelperOptions {
  connectors?: Array<'claude-web' | 'codex-cloud'>;
  instanceId?: string;
}
/** Registration is inert; provider credentials are read only by selected acquisitions. */
export function createHistoryPlugin(options: ProviderSourceOptions = {}): HistoryPlugin {
  const connectors = options.connectors ?? ['claude-web', 'codex-cloud'];
  if (
    !Array.isArray(connectors) ||
    connectors.some((id) => id !== 'claude-web' && id !== 'codex-cloud') ||
    new Set(connectors).size !== connectors.length
  )
    throw new InvalidArgumentError(
      'Provider connectors must be unique claude-web or codex-cloud IDs',
      'INVALID_ARGUMENT',
    );
  const sources: HistorySource[] = connectors.map((connectorId) => {
    const source: CatalogSource = connectorId === 'claude-web' ? 'claude' : 'codex';
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
          { ...options, signal: query.signal },
        ),
      hydrate: async (observation, context) =>
        helperRequest<SourceEvidenceSnapshot>(
          'hydrate',
          { connectorId, connectorInstance, observation },
          { ...options, signal: context.signal },
        ),
    };
  });
  return { sources };
}
export type { HistorySource } from 'ai-hist';
