import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import {
  createHistoryDelivery,
  DEFAULT_DELIVERY_LIMITS,
  HistoryDeliveryError,
  InvalidArgumentError,
  RelayHistoryError,
  type DeliveryAcknowledgment,
  type DeliveryFailure,
  type HistoryDestination,
  type HistoryExportRecord,
  type HistoryExportSelection,
  type HistoryPlugin,
  type PreparedHistoryPayload,
  type HistorySource,
  isCatalogSource,
  CATALOG_SOURCES,
} from 'ai-hist';
import { helperRequest, type HelperOptions } from './helper.js';
import {
  accessToken,
  login,
  replay,
  resolveCloudSession,
  getSessionThread as getLegacySessionThread,
  type SessionThreadQuery,
  type SessionThreadOptions,
  type CloudSessionResolution,
} from './cloud-client.js';
const ordinal = (left: string, right: string) => (left < right ? -1 : left > right ? 1 : 0);
const MAPPING = 'relayhistory-delivery-v1';
export interface RelayHistoryPluginOptions extends HelperOptions {
  baseUrl?: string;
  instanceId?: string;
  expectedAccount?: string;
  acknowledgeUninspectedLegacySchedules?: boolean;
}
export interface DeliveryReadOptions {
  expectedAccount: string;
  kind?: string;
  source?: string;
  sessionId?: string;
  cursor?: string;
  includeDeleted?: boolean;
  limit?: number;
}
export interface DeliveryReadPage {
  protocolVersion: 1;
  listing: 'live';
  records: HistoryExportRecord[];
  nextCursor: string | null;
}
export function deliveryAccount(options: RelayHistoryPluginOptions = {}): Promise<string> {
  return helperRequest('deliveryAccount', { baseUrl: options.baseUrl }, options);
}
export function readDeliveredHistory(
  query: DeliveryReadOptions,
  options: RelayHistoryPluginOptions = {},
): Promise<DeliveryReadPage> {
  return helperRequest('deliveryRead', { baseUrl: options.baseUrl, readOptions: query }, options);
}
/** Restart each traversal from the beginning: the live listing is not a change feed. */
export async function* deliveredHistory(
  query: Omit<DeliveryReadOptions, 'cursor'>,
  options: RelayHistoryPluginOptions = {},
): AsyncGenerator<HistoryExportRecord> {
  let cursor: string | undefined;
  const seen = new Set<string>();
  do {
    const page = await readDeliveredHistory({ ...query, cursor }, options);
    if (page.protocolVersion !== 1 || page.listing !== 'live' || !Array.isArray(page.records))
      throw new RelayHistoryError(
        'Invalid delivery read response',
        'HISTORY_PLUGIN_PROTOCOL_FAILED',
      );
    for (const record of page.records) yield record;
    cursor = page.nextCursor ?? undefined;
    if (cursor && seen.has(cursor))
      throw new RelayHistoryError(
        'Delivery listing cursor repeated',
        'HISTORY_PLUGIN_PROTOCOL_FAILED',
      );
    if (cursor) seen.add(cursor);
  } while (cursor);
}
export function projectDeliveredTranscript(records: readonly HistoryExportRecord[]) {
  const available = canonicalDeliveredEvidence(records).filter(
    (record) =>
      record.operation === 'upsert' && record.payload && typeof record.payload === 'object',
  );
  const events = available.filter(
    (record) =>
      record.kind === 'session_event' &&
      typeof (record.payload as Record<string, unknown>).text === 'string',
  );
  const selected = events.length ? events : available.filter((record) => record.kind === 'history');
  const seen = new Set<string>();
  return selected
    .map((record) => {
      const payload = record.payload as Record<string, unknown>;
      return {
        originId: record.origin_id,
        recordId: record.record_id,
        revisionId: record.revision_id,
        eventId: typeof payload.event_uid === 'string' ? payload.event_uid : null,
        timestampMs:
          typeof payload.ts_ms === 'number'
            ? payload.ts_ms
            : typeof payload.timestamp_ms === 'number'
              ? payload.timestamp_ms
              : null,
        role: typeof payload.role === 'string' ? payload.role : 'user',
        text: String(record.kind === 'history' ? (payload.prompt ?? '') : (payload.text ?? '')),
        representation: record.kind,
      };
    })
    .sort(
      (a, b) =>
        (a.timestampMs ?? Number.MAX_SAFE_INTEGER) - (b.timestampMs ?? Number.MAX_SAFE_INTEGER) ||
        ordinal(a.originId, b.originId) ||
        ordinal(a.recordId, b.recordId),
    )
    .filter((row) => {
      const key = row.eventId ?? JSON.stringify([row.originId, row.recordId]);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}
export async function getDeliveredSession(
  query: { source: string; sessionId: string },
  options: RelayHistoryPluginOptions = {},
) {
  const expectedAccount = options.expectedAccount ?? (await deliveryAccount(options));
  const records: HistoryExportRecord[] = [];
  for await (const record of deliveredHistory({ ...query, expectedAccount }, options))
    records.push(record);
  return { records, transcript: projectDeliveredTranscript(records), listing: 'live' as const };
}
/** Both components use one pinned account. Legacy reads cannot rotate credentials. */
export async function getSessionThreadWithHistory(
  query: SessionThreadQuery,
  options: RelayHistoryPluginOptions & SessionThreadOptions = {},
) {
  const resolved = options.resolveSession
    ? await options.resolveSession(options.baseUrl)
    : await helperRequest<CloudSessionResolution>(
        'cloudResolveSession',
        { baseUrl: options.baseUrl, now: Date.now() },
        options,
      );
  if (!resolved.auth?.orgId) throw new HistoryDeliveryError('authentication_required');
  const auth = resolved.auth;
  const expectedAccount =
    'relayhistory:' +
    createHash('sha256')
      .update(JSON.stringify([auth.orgId, auth.workspaceId ?? '']))
      .digest('hex');
  if (options.expectedAccount && options.expectedAccount !== expectedAccount)
    throw new HistoryDeliveryError('permission_denied');
  const history = await getDeliveredSession(query, { ...options, expectedAccount });
  let legacyStatus: { available: boolean; code?: string } = { available: true };
  let thread;
  try {
    thread = await getLegacySessionThread(query, {
      ...options,
      resolveSession: async () => ({ auth }),
    });
  } catch (error) {
    if (!(error instanceof RelayHistoryError)) throw error;
    legacyStatus = { available: false, code: error.code };
    thread = { session: null, outcomes: [], links: [], nextCursor: null };
  }
  return {
    ...thread,
    legacyStatus,
    deliveredHistory: history.records,
    transcript: history.transcript,
    historyListing: history.listing,
  };
}
/** Read-only migration check. Loading the module never inspects or changes services. */
export interface LegacyScheduleStatus {
  state: 'clear' | 'active' | 'unknown';
  jobs: string[];
}
export function legacySchedules(options: HelperOptions = {}): Promise<LegacyScheduleStatus> {
  return helperRequest('deliveryMigrationStatus', {}, options);
}
/** Explicit command pre-check. Delivery itself rechecks this in the helper,
 * per dispatch, where a later install cannot race the decision. */
async function requireMigration(options: RelayHistoryPluginOptions): Promise<void> {
  const result = await legacySchedules(options);
  if (
    result.state === 'active' ||
    (result.state === 'unknown' && options.acknowledgeUninspectedLegacySchedules !== true)
  )
    throw new HistoryDeliveryError('permission_denied');
}
function classified(error: unknown): never {
  if (error instanceof HistoryDeliveryError) throw error;
  // A deterministic helper envelope/input rejection cannot recover on retry.
  if (error instanceof InvalidArgumentError) throw new HistoryDeliveryError('invalid_payload');
  if (error instanceof RelayHistoryError && error.code.startsWith('DELIVERY_')) {
    const failure = error.code.slice(9).toLowerCase();
    if (
      [
        'transient',
        'rate_limited',
        'authentication_required',
        'permission_denied',
        'invalid_payload',
        'unsupported_evidence',
        'mapping_version_mismatch',
      ].includes(failure)
    )
      throw new HistoryDeliveryError(failure as DeliveryFailure);
  }
  throw new HistoryDeliveryError(
    error instanceof RelayHistoryError && error.code === 'CLOUD_MIGRATION_REQUIRED'
      ? 'permission_denied'
      : 'transient',
  );
}
export function relayHistoryInstance(options: RelayHistoryPluginOptions = {}): string {
  const endpoint = new URL(options.baseUrl ?? 'https://history.agentrelay.com');
  endpoint.hash = '';
  endpoint.search = '';
  const canonical = endpoint.toString().replace(/\/+$/, '');
  return `${options.instanceId ?? 'default'}:${createHash('sha256').update(canonical).digest('hex').slice(0, 32)}`;
}
/** What this configuration asserts about the generation it delivers to. The
 * helper enforces them; an omitted assertion is not checked there either. */
function guards(options: RelayHistoryPluginOptions) {
  return {
    expectedAccount: options.expectedAccount,
    instanceId: relayHistoryInstance(options),
    acknowledgeUninspectedSchedules: options.acknowledgeUninspectedLegacySchedules === true,
  };
}
export function relayHistoryDestination(
  options: RelayHistoryPluginOptions = {},
): HistoryDestination {
  options = { ...options, baseUrl: options.baseUrl ?? 'https://history.agentrelay.com' };
  return {
    id: 'relayhistory',
    mappingVersion: MAPPING,
    idempotency: 'revision',
    orderedRevisions: true,
    supportsTombstones: true,
    supportedKinds: [
      'history',
      'session_event',
      'tool_call',
      'file_edit',
      'session',
      'presence',
      'relationship',
      'commit_link',
      'trajectory',
      'source_observation',
      'observation_evidence',
    ],
    // The legacy-scheduler guard and the account/instance assertions live in
    // the helper's receiver, so this plugin and the probe recheck them
    // identically before mapping and again before transport.
    prepare: async (batch, { signal }) => {
      try {
        const value = await helperRequest<PreparedHistoryPayload>(
          'deliveryPrepare',
          { batch, ...guards(options) },
          { ...options, signal },
        );
        return { body: value.body, content_type: value.content_type };
      } catch (error) {
        return classified(error);
      }
    },
    send: async (prepared, { signal, batch }) => {
      try {
        return await helperRequest<DeliveryAcknowledgment>(
          'deliverySend',
          { baseUrl: options.baseUrl, prepared, batch, ...guards(options) },
          { ...options, signal },
        );
      } catch (error) {
        return classified(error);
      }
    },
  };
}
/** Cross-origin revisions are incomparable. Pick a stable origin/record winner
 * for each canonical identity; readback still exposes every original record. */
export function canonicalDeliveredEvidence(
  rows: readonly HistoryExportRecord[],
): HistoryExportRecord[] {
  const keys: Partial<Record<HistoryExportRecord['kind'], string[]>> = {
    history: ['source', 'timestamp_ms', 'prompt'],
    session_event: ['source', 'session_id', 'event_uid'],
    tool_call: ['source', 'session_id', 'tool_use_id'],
    file_edit: ['source', 'session_id', 'tool_use_id'],
    relationship: ['source', 'parent_session_id', 'relationship_uid'],
    commit_link: ['source', 'session_id', 'commit_sha', 'match_method'],
  };
  const selected = new Map<string, HistoryExportRecord>();
  for (const row of [...rows].sort(
    (a, b) => ordinal(a.origin_id, b.origin_id) || ordinal(a.record_id, b.record_id),
  )) {
    if (
      row.operation !== 'upsert' ||
      !keys[row.kind] ||
      !row.payload ||
      typeof row.payload !== 'object'
    )
      continue;
    const payload = row.payload as Record<string, unknown>;
    const key = JSON.stringify([
      row.kind,
      ...keys[row.kind]!.map((field) => payload[field] ?? null),
    ]);
    if (!selected.has(key)) selected.set(key, row);
  }
  return [...selected.values()];
}
export function relayHistorySource(options: RelayHistoryPluginOptions = {}): HistorySource {
  const pinned = () => {
    if (!options.expectedAccount)
      throw new InvalidArgumentError(
        'Configure expectedAccount before acquiring cloud history',
        'INVALID_ARGUMENT',
      );
    return options.expectedAccount;
  };
  const instanceId =
    relayHistoryInstance(options) +
    (options.expectedAccount
      ? ':' + createHash('sha256').update(options.expectedAccount).digest('hex').slice(0, 16)
      : '');
  return {
    id: 'cloud',
    instanceId,
    location: 'remote',
    supportedSources: CATALOG_SOURCES,
    discover: async (query) => {
      const expectedAccount = pinned();
      const sessions = new Map<
        string,
        {
          source: (typeof CATALOG_SOURCES)[number];
          session_id: string;
          first_prompt: string | null;
          first_activity_ms: number | null;
          last_activity_ms: number | null;
          source_stamp: string;
          revisions: string[];
        }
      >();
      const sources = query.sources ?? [undefined];
      for (const source of sources)
        for await (const record of deliveredHistory(
          { expectedAccount, source, sessionId: query.sessionId },
          { ...options, signal: query.signal, timeoutMs: query.acquisitionTimeoutMs ?? options.timeoutMs },
        )) {
          if (!record.session_id || !isCatalogSource(record.source)) continue;
          const key = JSON.stringify([record.source, record.session_id]);
          const row = sessions.get(key) ?? {
            source: record.source,
            session_id: record.session_id,
            first_prompt: null,
            first_activity_ms: null,
            last_activity_ms: null,
            source_stamp: '',
            revisions: [],
          };
          const payload = record.payload as Record<string, unknown> | null;
          const timestamps = [
            payload?.ts_ms,
            payload?.timestamp_ms,
            payload?.first_activity_ms,
            payload?.last_activity_ms,
          ].filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
          if (timestamps.length) {
            row.first_activity_ms = Math.min(row.first_activity_ms ?? Infinity, ...timestamps);
            row.last_activity_ms = Math.max(row.last_activity_ms ?? -Infinity, ...timestamps);
          }
          row.first_prompt ??=
            typeof payload?.prompt === 'string'
              ? payload.prompt
              : typeof payload?.first_prompt === 'string'
                ? payload.first_prompt
                : null;
          row.revisions.push(
            JSON.stringify([
              record.origin_id,
              record.record_id,
              record.revision_id,
              record.operation,
            ]),
          );
          sessions.set(key, row);
        }
      return {
        observations: [...sessions.values()]
          .sort(
            (a, b) =>
              (b.last_activity_ms ?? -Infinity) - (a.last_activity_ms ?? -Infinity) ||
              ordinal(a.source, b.source) ||
              ordinal(a.session_id, b.session_id),
          )
          .slice(0, query.limit)
          .map(({ revisions, ...row }) => ({
            ...row,
            source_stamp: createHash('sha256').update(revisions.sort().join('\n')).digest('hex'),
          })),
      };
    },
    hydrate: async (observation, context) => {
      const rows: HistoryExportRecord[] = [];
      for await (const row of deliveredHistory(
        {
          expectedAccount: pinned(),
          source: observation.key.source,
          sessionId: observation.key.session_id,
          includeDeleted: true,
        },
        { ...options, signal: context.signal, timeoutMs: context.acquisitionTimeoutMs ?? options.timeoutMs },
      ))
        rows.push(row);
      const allowed = [
        'history',
        'session_event',
        'tool_call',
        'file_edit',
        'relationship',
        'commit_link',
      ] as const;
      const covered = allowed.filter((kind) => rows.some((row) => row.kind === kind));
      const records = canonicalDeliveredEvidence(rows)
        .filter(
          (row) =>
            row.operation === 'upsert' && covered.includes(row.kind as (typeof allowed)[number]),
        )
        .map((row) => ({
          kind: row.kind as (typeof allowed)[number],
          payload: Object.fromEntries(
            Object.entries(row.payload as Record<string, unknown>).filter(([key]) => key !== 'id'),
          ),
          record_id: row.record_id,
          revision_id: row.revision_id,
        }));
      return {
        source_stamp: createHash('sha256')
          .update(
            rows
              .map((row) =>
                JSON.stringify([row.origin_id, row.record_id, row.revision_id, row.operation]),
              )
              .sort()
              .join('\n'),
          )
          .digest('hex'),
        source_bytes: Buffer.byteLength(JSON.stringify(records)),
        covered_kinds: covered,
        records,
      };
    },
  };
}
function flags(args: readonly string[]): Record<string, string | true> {
  const result: Record<string, string | true> = {};
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (!arg.startsWith('--'))
      throw new InvalidArgumentError('Plugin commands require named options', 'INVALID_ARGUMENT');
    const [key, inline] = arg.slice(2).split(/=(.*)/s);
    if (!['base-url', 'token', 'label', 'session', 'limit', 'selection', 'db'].includes(key))
      throw new InvalidArgumentError(`Unknown plugin option --${key}`, 'INVALID_ARGUMENT');
    const value = inline ?? args[++index];
    if (value === undefined)
      throw new InvalidArgumentError(`--${key} requires a value`, 'INVALID_ARGUMENT');
    result[key] = value;
  }
  return result;
}
/** Inert registration. Network/auth/service checks occur only in invoked operations. */
export function createHistoryPlugin(options: RelayHistoryPluginOptions = {}): HistoryPlugin {
  options = { ...options, baseUrl: options.baseUrl ?? 'https://history.agentrelay.com' };
  return {
    sources: [relayHistorySource(options)],
    destinations: [
      { instanceId: relayHistoryInstance(options), destination: relayHistoryDestination(options) },
    ],
    commands: [
      {
        name: 'login',
        run: async (args) => {
          const f = flags(args);
          const baseUrl = String(
            f['base-url'] ?? options.baseUrl ?? 'https://history.agentrelay.com',
          );
          const relayAccessToken =
            typeof f.token === 'string'
              ? f.token
              : ((await (
                  await import('./cloud-preflight.js')
                ).prepareCloudSessionForEnableCloud('login', args)) ?? undefined);
          const auth = await login({
            baseUrl,
            relayAccessToken,
            label: typeof f.label === 'string' ? f.label : undefined,
          });
          return { ok: true, baseUrl: auth.baseUrl };
        },
      },
      {
        name: 'token',
        run: async (args) => {
          const f = flags(args);
          return accessToken({
            baseUrl: typeof f['base-url'] === 'string' ? f['base-url'] : options.baseUrl,
          });
        },
      },
      {
        name: 'replay',
        run: async (args) => {
          const f = flags(args);
          if (typeof f.session !== 'string')
            throw new InvalidArgumentError('--session is required', 'INVALID_ARGUMENT');
          return replay(f.session, {
            baseUrl: typeof f['base-url'] === 'string' ? f['base-url'] : options.baseUrl,
          });
        },
      },
      {
        name: 'relayhistory-migration-status',
        run: async () => ({
          legacySchedules: await legacySchedules(options),
          action:
            'Stop old managed push jobs explicitly before enabling a new durable generation. Existing auth and legacy cursors remain unchanged.',
        }),
      },
      {
        name: 'relayhistory-enable',
        run: async (args) => {
          const f = flags(args);
          await requireMigration(options);
          if (typeof f.selection !== 'string')
            throw new InvalidArgumentError(
              '--selection file is required for a new delivery generation',
              'INVALID_ARGUMENT',
            );
          const selection = JSON.parse(
            await readFile(f.selection, 'utf8'),
          ) as HistoryExportSelection;
          const selected = {
            ...options,
            baseUrl: typeof f['base-url'] === 'string' ? f['base-url'] : options.baseUrl,
          };
          return createHistoryDelivery(
            {
              destination_id: 'relayhistory',
              instance_id: relayHistoryInstance(selected),
              account_id: await deliveryAccount(selected),
              mapping_version: MAPPING,
              selection,
              limits: DEFAULT_DELIVERY_LIMITS,
            },
            { dbPath: typeof f.db === 'string' ? f.db : undefined },
          );
        },
      },
    ],
    tools: [
      {
        name: 'get_session_thread',
        description:
          'Current delivered session evidence plus legacy lifecycle links. Fresh live scan; no incremental cursor.',
        run: async (input) =>
          getSessionThreadWithHistory(
            { source: String(input.source ?? ''), sessionId: String(input.session_id ?? '') },
            options,
          ),
      },
      {
        name: 'read_delivered_history',
        description:
          'Explicit live readback of retained history evidence; cursors are not change-feed checkpoints.',
        run: async (input) =>
          readDeliveredHistory(
            {
              expectedAccount: await deliveryAccount(options),
              source: typeof input.source === 'string' ? input.source : undefined,
              sessionId: typeof input.session_id === 'string' ? input.session_id : undefined,
              cursor: typeof input.cursor === 'string' ? input.cursor : undefined,
            },
            options,
          ),
      },
    ],
  };
}
