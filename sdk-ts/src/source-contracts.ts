import type { CatalogSource, SessionLocation } from './sdk-common.js';
/** Native JSON intake uses stored snake_case fields; no SQL crosses this API. */
export interface SourceObservationKey {
  source: string;
  session_id: string;
  location: SessionLocation;
  connector_id: string;
  connector_instance: string;
}
export interface SourceObservation {
  key: SourceObservationKey;
  raw_locator: string | null;
  source_stamp: string | null;
  discovery_state: 'shallow' | 'full';
  access_state: 'available' | 'unavailable' | 'withdrawn';
  updated_ms: number;
}
export interface SourceObservationState {
  observation: SourceObservation | null;
  checkpoint: unknown;
  revision: string | null;
}
export interface ShallowSourceSession {
  source: CatalogSource;
  session_id: string;
  raw_path?: string | null;
  raw_locator?: string | null;
  source_stamp?: string | null;
  [field: string]: unknown;
}
export type AcquiredEvidenceKind =
  | 'history'
  | 'session_event'
  | 'tool_call'
  | 'file_edit'
  | 'relationship'
  | 'commit_link';
export interface SourceEvidenceSnapshot {
  source_stamp: string;
  source_bytes: number;
  covered_kinds: AcquiredEvidenceKind[];
  records: Array<{
    kind: AcquiredEvidenceKind;
    payload: Record<string, unknown>;
    record_id?: string;
    revision_id?: string;
  }>;
}
export interface HistorySource {
  id: string;
  instanceId: string;
  location: 'remote';
  supportedSources: readonly CatalogSource[];
  discover(options: {
    sources?: CatalogSource[];
    sessionId?: string;
    limit?: number;
    signal?: AbortSignal;
  }): Promise<{ observations: ShallowSourceSession[] }>;
  /** Complete snapshot for every declared covered kind; omit unsupported kinds. */
  hydrate(
    observation: Readonly<SourceObservation>,
    options: { signal?: AbortSignal },
  ): Promise<SourceEvidenceSnapshot>;
}
