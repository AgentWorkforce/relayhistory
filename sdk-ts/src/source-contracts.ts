import type { CatalogSource, EvidenceKind, SessionLocation } from './sdk-common.js';
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
/** The same closed set as {@link EvidenceKind}; kept as its acquisition-side name. */
export type AcquiredEvidenceKind = EvidenceKind;
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
  /**
   * `remote` sources run only for `remote` and `all` scope requests. `local`
   * sources read this machine's own files and run for `local` and `all`
   * scope, beside the built-in parsers, with their sessions presented as
   * local. Default and `local` scope never invoke a remote source.
   */
  location: SessionLocation;
  /**
   * Required for a `local` source, ignored for a `remote` one: the absolute
   * directories this source reads. Every `raw_path` it reports must lie inside
   * one of them, or its discovery is rejected, so a local plugin cannot point
   * the catalog at files it did not declare.
   */
  roots?: readonly string[];
  supportedSources: readonly CatalogSource[];
  discover(options: {
    sources?: CatalogSource[];
    sessionId?: string;
    limit?: number;
    signal?: AbortSignal;
    acquisitionTimeoutMs?: number;
  }): Promise<{ observations: ShallowSourceSession[] }>;
  /**
   * Complete snapshot for every declared covered kind; omit unsupported kinds.
   *
   * `covered_kinds` declares what the acquisition **examined**, not what it
   * happened to find. A complete export of a session with no file edits
   * examined `file_edit` and found none, so it still covers it: a covered kind
   * with no records means "this session has none", while an absent kind means
   * "nothing looked". Filtering `covered_kinds` down to the kinds with rows
   * turns every sparse session into a `partial` snapshot and costs it priority
   * in a merge. Keep `records` to the rows that exist.
   *
   * When `options.includeRelated` is `false` the caller asked for the selected
   * thread alone: do not acquire delegation evidence, and omit `relationship`
   * from `covered_kinds` (and any relationship rows from `records`). Reporting
   * it covered would let the merge reinstate a kind the local side deliberately
   * dropped, so `scope: 'all'` could report `full` despite the opt-out.
   */
  hydrate(
    observation: Readonly<SourceObservation>,
    options: { signal?: AbortSignal; acquisitionTimeoutMs?: number; includeRelated?: boolean },
  ): Promise<SourceEvidenceSnapshot>;
}
