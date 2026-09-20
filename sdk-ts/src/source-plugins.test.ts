import assert from 'node:assert/strict';
import { mkdtemp, rm, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { normalizeHydration } from './normalization.js';
import {
  HistoryPluginRegistry,
  sync,
  discoverSessions,
  hydrateSession,
  getSessionEventsPage,
  getSourceObservation,
  hydrateSourcePlugin,
  discoverSourcePlugins,
  SESSION_HYDRATION_CONTRACT_VERSION,
  __testing,
  type HydrateSessionResult,
  RelayHistoryError,
  AuthenticationExpiredError,
  SessionNotFoundError,
  type HistorySource,
  type SourceEvidenceSnapshot,
} from './index.js';
function fixture(instanceId: string): HistorySource & { snapshot: SourceEvidenceSnapshot } {
  const source: HistorySource & { snapshot: SourceEvidenceSnapshot } = {
    id: 'fixture-source',
    instanceId,
    location: 'remote',
    supportedSources: ['claude'],
    snapshot: {
      source_stamp: 'full1',
      source_bytes: 20,
      covered_kinds: ['session_event'],
      records: [
        {
          kind: 'session_event',
          payload: {
            source: 'claude',
            session_id: 'fixture-session',
            event_uid: 'event-' + instanceId,
            role: 'assistant',
            kind: 'text',
            ts_ms: 1,
            text: instanceId,
          },
        },
      ],
    },
    discover: async () => ({
      observations: [
        {
          source: 'claude',
          session_id: 'fixture-session',
          raw_path: 'opaque:' + instanceId,
          source_stamp: 'listing1',
        },
      ],
    }),
    hydrate: async () => source.snapshot,
  };
  return source;
}
test('installed source modules are selected explicitly and persisted through the native contract', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-sdk-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const one = fixture('one'),
    two = fixture('two');
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [one, two] });
  const discovery = await discoverSessions({
    scope: 'remote',
    plugins: registry,
    sourceConnectors: ['fixture-source:one'],
    dbPath,
  });
  assert.equal(discovery.discovered, 1);
  const hydrated = await hydrateSession({
    source: 'claude',
    sessionId: 'fixture-session',
    scope: 'remote',
    plugins: registry,
    sourceConnectors: ['fixture-source:one'],
    dbPath,
  });
  assert.equal(hydrated.evidence.events, 1);
  await hydrateSession({
    source: 'claude',
    sessionId: 'fixture-session',
    scope: 'remote',
    plugins: registry,
    sourceConnectors: ['fixture-source:two'],
    dbPath,
  });
  assert.deepEqual(
    (await getSessionEventsPage('fixture-session', { source: 'claude', dbPath })).events
      .map((event) => event.text)
      .sort(),
    ['one', 'two'],
  );
  one.snapshot = { ...one.snapshot, source_stamp: 'full2', records: [] };
  await hydrateSourcePlugin(one, { source: 'claude', sessionId: 'fixture-session' }, { dbPath });
  assert.deepEqual(
    (await getSessionEventsPage('fixture-session', { source: 'claude', dbPath })).events.map(
      (event) => event.text,
    ),
    ['two'],
  );
});
test('source acquisition rejects stale completion while another snapshot commits', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-race-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const source = fixture('one');
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [source] });
  await discoverSourcePlugins(registry, { dbPath });
  let finish!: () => void;
  let started!: () => void;
  const ready = new Promise<void>((resolve) => (started = resolve));
  const release = new Promise<void>((resolve) => (finish = resolve));
  const delayed = {
    ...source,
    hydrate: async () => {
      started();
      await release;
      return source.snapshot;
    },
  };
  const delayedRegistry = new HistoryPluginRegistry();
  delayedRegistry.register({ sources: [delayed] });
  const pending = hydrateSession({
    source: 'claude',
    sessionId: 'fixture-session',
    scope: 'remote',
    plugins: delayedRegistry,
    dbPath,
  });
  await ready;
  await hydrateSourcePlugin(source, { source: 'claude', sessionId: 'fixture-session' }, { dbPath });
  finish();
  await assert.rejects(
    pending,
    (error: unknown) =>
      error instanceof RelayHistoryError && error.code === 'SOURCE_REVISION_CONFLICT',
  );
  const state = await getSourceObservation(
    {
      source: 'claude',
      session_id: 'fixture-session',
      location: 'remote',
      connector_id: source.id,
      connector_instance: source.instanceId,
    },
    { dbPath },
  );
  assert.ok(state.revision);
});
test('local reads and explicit empty selection do not invoke installed source callbacks', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-local-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const saved = new Map(
    ['HOME', 'USERPROFILE', 'XDG_DATA_HOME', 'OPENCODE_DB', 'TRAJECTORY_ROOT'].map((name) => [
      name,
      process.env[name],
    ]),
  );
  for (const name of saved.keys()) delete process.env[name];
  process.env.HOME = dir;
  process.env.USERPROFILE = dir;
  process.env.XDG_DATA_HOME = join(dir, 'share');
  t.after(() => {
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });
  const source = fixture('one');
  let calls = 0;
  source.discover = async () => {
    calls++;
    throw new Error('credential-store-must-not-be-read');
  };
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [source] });
  await discoverSessions({ scope: 'local', plugins: registry, dbPath, sources: [] });
  await discoverSessions({
    scope: 'all',
    plugins: registry,
    sourceConnectors: [],
    dbPath,
    sources: [],
  });
  assert.equal(calls, 0);
});

test('unknown source selection fails before local IO for all acquisition APIs', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-invalid-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'never-create.db');
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [fixture('one')] });
  const options = {
    scope: 'all' as const,
    plugins: registry,
    sourceConnectors: ['not-registered'],
    dbPath,
  };
  await assert.rejects(discoverSessions(options), /unconfigured source connector/);
  await assert.rejects(sync(options), /unconfigured source connector/);
  await assert.rejects(
    hydrateSession({ ...options, source: 'claude', sessionId: 'fixture-session' }),
    /unconfigured source connector/,
  );
  await assert.rejects(access(dbPath));
});
test('failed source instances do not block healthy remote or local acquisition', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-isolation-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const saved = new Map(
    ['HOME', 'USERPROFILE', 'XDG_DATA_HOME', 'OPENCODE_DB', 'TRAJECTORY_ROOT'].map((name) => [
      name,
      process.env[name],
    ]),
  );
  for (const name of saved.keys()) delete process.env[name];
  process.env.HOME = dir;
  process.env.USERPROFILE = dir;
  process.env.XDG_DATA_HOME = join(dir, 'share');
  t.after(() => {
    for (const [name, value] of saved) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });
  const broken = fixture('broken');
  broken.discover = async () => {
    throw new Error('private credential details');
  };
  const healthy = fixture('healthy');
  const registry = new HistoryPluginRegistry();
  registry.register({ sources: [broken, healthy] });
  const discovery = await discoverSessions({ scope: 'remote', plugins: registry, dbPath });
  assert.equal(discovery.discovered, 1);
  assert.match(discovery.diagnostics[0].source, /fixture-source:broken/);
  const hydration = await hydrateSession({
    scope: 'remote',
    source: 'claude',
    sessionId: 'fixture-session',
    plugins: registry,
    dbPath,
  });
  assert.equal(hydration.evidence.events, 1);
  assert.match(hydration.diagnostics.at(-1)!.message, /fixture-source:broken/);
  const synced = await sync({ scope: 'all', plugins: registry, dbPath });
  assert.equal(synced.completed, false);
  assert.match(synced.diagnostics![0].source, /fixture-source:broken/);
  const onlyBroken = new HistoryPluginRegistry();
  onlyBroken.register({ sources: [broken] });
  const missing = join(dir, 'no-remote.db');
  await assert.rejects(
    discoverSessions({ scope: 'remote', plugins: onlyBroken, dbPath: missing }),
    /Source plugin acquisition failed/,
  );
  await assert.rejects(access(missing));
  assert.equal(
    (await discoverSessions({ scope: 'all', plugins: onlyBroken, dbPath })).scope,
    'all',
  );
});

test('invalid runtime options are rejected before source callbacks or database creation', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-validation-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'never.db');
  const source = fixture('one');
  let calls = 0;
  source.discover = async () => {
    calls++;
    return { observations: [] };
  };
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [source] });
  for (const options of [
    { scope: 'bogus' },
    { scope: 'remote', limit: -1 },
    { scope: 'remote', sources: ['unknown'] },
    { scope: 'remote', acquisitionTimeoutMs: 0 },
    { scope: 'remote', acquisitionTimeoutMs: 3_600_001 },
  ])
    await assert.rejects(
      discoverSessions({ ...options, plugins, dbPath } as never),
      (error: unknown) => error instanceof RelayHistoryError && error.code === 'INVALID_ARGUMENT',
    );
  await assert.rejects(sync({ scope: 'bogus', plugins, dbPath } as never), /scope/);
  assert.equal(calls, 0);
  await assert.rejects(access(dbPath));
});
test('malformed observation response is isolated and shallow results cannot downgrade richer evidence', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-capability-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  for (const reverse of [false, true]) {
    const dbPath = join(dir, reverse ? 'reverse.db' : 'forward.db');
    const broken = fixture('malformed');
    broken.discover = async () =>
      ({ observations: [{ source: 'claude', session_id: null }] }) as never;
    const shallow = fixture('shallow');
    shallow.snapshot = {
      source_stamp: 'metadata-only',
      source_bytes: 1,
      covered_kinds: [],
      records: [],
    };
    const rich = fixture('rich');
    const plugins = new HistoryPluginRegistry();
    plugins.register({ sources: [broken, ...(reverse ? [shallow, rich] : [rich, shallow])] });
    const discovered = await discoverSessions({ scope: 'remote', plugins, dbPath });
    assert.equal(discovered.diagnostics[0].source, 'fixture-source:malformed');
    const result = await hydrateSession({
      source: 'claude',
      sessionId: 'fixture-session',
      scope: 'remote',
      plugins,
      dbPath,
    });
    assert.equal(result.capability, 'partial');
    assert.equal(result.evidence.events, 1);
    assert.ok(result.diagnostics.some((row) => row.code === 'SOURCE_CAPABILITY_LIMITED'));
  }
});

test('typed acquisition failures survive public APIs without plugin secrets', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-errors-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const source = fixture('errors');
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [source] });
  const options = { scope: 'remote' as const, plugins, dbPath };
  await discoverSessions(options);
  for (const [ErrorType, code] of [
    [AuthenticationExpiredError, 'AUTHENTICATION_EXPIRED'],
    [SessionNotFoundError, 'SESSION_NOT_FOUND'],
  ] as const) {
    const fail = async (): Promise<never> => { throw new ErrorType('private-token', code, { cause: new Error('secret cause') }); };
    source.hydrate = fail;
    for (const operation of [
      () => hydrateSourcePlugin(source, { source: 'claude', sessionId: 'fixture-session' }, { dbPath }),
      () => hydrateSession({ ...options, source: 'claude', sessionId: 'fixture-session' }),
    ]) await assert.rejects(operation(), (error: unknown) => {
      assert.ok(error instanceof ErrorType);
      assert.equal(error.code, code);
      assert.doesNotMatch(error.message, /private-token/);
      assert.equal(error.cause, undefined);
      return true;
    });
    source.discover = fail;
    for (const operation of [() => discoverSessions(options), () => sync(options)])
      await assert.rejects(operation(), (error: unknown) => error instanceof ErrorType && error.code === code && !error.message.includes('private-token'));
  }
  source.hydrate = async () => { throw new Error('private-token'); };
  await assert.rejects(hydrateSourcePlugin(source, { source: 'claude', sessionId: 'fixture-session' }, { dbPath }),
    (error: unknown) => error instanceof RelayHistoryError && error.code === 'CONNECTOR_FAILURE' && !error.message.includes('private-token'));
});

test('complete source snapshots can exceed thirty seconds and receive the configured budget', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-budget-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const source = fixture('budget');
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [source] });
  await discoverSessions({ scope: 'remote', plugins, dbPath });
  let ready!: () => void;
  const started = new Promise<void>(resolve => { ready = resolve; });
  source.hydrate = async (_observation, context) => {
    assert.equal(context.acquisitionTimeoutMs, 120_000);
    ready();
    await new Promise(resolve => setTimeout(resolve, 45_000));
    return source.snapshot;
  };
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const pending = hydrateSourcePlugin(source, { source: 'claude', sessionId: 'fixture-session' }, { dbPath, acquisitionTimeoutMs: 120_000 });
  await started;
  t.mock.timers.tick(45_000);
  await pending;
  t.mock.timers.reset();
  assert.equal((await getSessionEventsPage('fixture-session', { source: 'claude', dbPath })).events.length, 1);
});

test('source deadlines and explicit cancellation have distinct safe errors and do not commit', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-cancel-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const source = fixture('cancel');
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [source] });
  await discoverSessions({ scope: 'remote', plugins, dbPath });
  for (const cancel of [false, true]) {
    let ready!: () => void;
    const started = new Promise<void>(resolve => { ready = resolve; });
    let signal: AbortSignal | undefined;
    let finish!: (snapshot: SourceEvidenceSnapshot) => void;
    source.hydrate = async (_observation, context) => {
      signal = context.signal;
      ready();
      return new Promise(resolve => { finish = resolve; });
    };
    const controller = new AbortController();
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const pending = hydrateSourcePlugin(source, { source: 'claude', sessionId: 'fixture-session' }, { dbPath, acquisitionTimeoutMs: 100, signal: controller.signal });
    const checked = assert.rejects(pending, (error: unknown) => error instanceof RelayHistoryError && error.code === (cancel ? 'SOURCE_ACQUISITION_CANCELLED' : 'SOURCE_ACQUISITION_TIMEOUT') && !error.message.includes('secret'));
    await started;
    if (cancel) controller.abort(new Error('secret cancel reason'));
    else t.mock.timers.tick(100);
    await checked;
    assert.equal(signal?.aborted, true);
    finish(source.snapshot);
    t.mock.timers.reset();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal((await getSessionEventsPage('fixture-session', { source: 'claude', dbPath })).events.length, 0);
  }
});

test('a capability-limited plugin result satisfies the hydration contract it declares', async (t) => {
  // `hydrateSourcePlugin` is public and hands this object straight back, so
  // every field the declared contract version requires has to be on it. The
  // empty-snapshot branch builds its result by hand rather than through the
  // native normalizer, and its return type is inferred from a snake_case
  // literal rather than checked against `HydrateSessionResult`, so a field
  // added to the contract goes missing here without the compiler noticing.
  const dir = await mkdtemp(join(tmpdir(), 'rh-source-contract-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const dbPath = join(dir, 'history.db');
  const empty = fixture('empty');
  empty.snapshot = {
    source_stamp: 'metadata-only',
    source_bytes: 7,
    covered_kinds: [],
    records: [],
  };
  const plugins = new HistoryPluginRegistry();
  plugins.register({ sources: [empty] });
  await discoverSessions({ scope: 'remote', plugins, dbPath });

  const result = await hydrateSourcePlugin(
    empty,
    { source: 'claude', sessionId: 'fixture-session' },
    { dbPath },
  );
  assert.equal(result.capability, 'shallow_only');
  assert.equal(result.contract_version, SESSION_HYDRATION_CONTRACT_VERSION);
  for (const field of [
    'contract_version', 'source', 'session_id', 'status', 'capability',
    'discovery_state', 'presence', 'indexed_through', 'evidence', 'bytes_read',
    'related_session_ids', 'diagnostics',
  ]) {
    assert.ok(field in result, `plugin hydration result is missing ${field}`);
  }
  // Nothing was read from a provider file, so this is a real zero rather than
  // an absent field that happens to read as one.
  assert.equal(result.bytes_read, 0);
});

test('a hydration drawing on several sources reports the bytes all of them read', async () => {
  // Evidence counts are the same rows seen twice, so the larger wins. Bytes
  // are disjoint work each source actually did, so they add. Spreading only
  // the result that won the capability rank let a real read be reported as
  // the other one's zero.
  const base: Omit<HydrateSessionResult, 'capability' | 'evidence' | 'bytesRead'> = {
    contractVersion: SESSION_HYDRATION_CONTRACT_VERSION,
    source: 'claude',
    sessionId: 'fixture-session',
    status: 'hydrated',
    discoveryState: 'full',
    presence: 'local',
    indexedThrough: { sourceStamp: null, lastEventAtMs: null },
    relatedSessionIds: [],
    diagnostics: [],
  };
  const local: HydrateSessionResult = {
    ...base,
    capability: 'full',
    evidence: { prompts: 2, events: 9, toolCalls: 1, fileEdits: 0, relatedSessions: 0 },
    bytesRead: 4096,
  };
  const connector: HydrateSessionResult = {
    ...base,
    capability: 'partial',
    evidence: { prompts: 0, events: 3, toolCalls: 0, fileEdits: 0, relatedSessions: 0 },
    bytesRead: 512,
  };

  const merged = __testing.combineHydration(local, connector);
  assert.equal(merged.bytesRead, 4608);
  // Order must not change the total, and the richer result still wins the rank.
  assert.equal(__testing.combineHydration(connector, local).bytesRead, 4608);
  assert.equal(merged.capability, 'full');
  assert.equal(merged.evidence.events, 9);
  // Neither input's own figure can pass for the total.
  assert.notEqual(merged.bytesRead, local.bytesRead);
  assert.notEqual(merged.bytesRead, connector.bytesRead);
  // A single result is returned unchanged rather than doubled.
  assert.equal(__testing.combineHydration(undefined, local).bytesRead, 4096);
});

test('a hydration result missing bytesRead is a broken contract, not a zero read', () => {
  // Zero is the one value a caller cannot tell apart from "this hydration read
  // nothing", so defaulting to it turns an addon that has fallen behind the
  // contract into a watch loop that sees no activity and reports none. Version
  // 3 requires the field; a response without it is not version 3.
  const complete = {
    contractVersion: SESSION_HYDRATION_CONTRACT_VERSION,
    source: 'claude',
    sessionId: 's',
    status: 'hydrated',
    capability: 'full',
    discoveryState: 'full',
    presence: 'local',
    indexedThrough: { sourceStamp: null, lastEventAtMs: null },
    evidence: { prompts: 0, events: 0, toolCalls: 0, fileEdits: 0, relatedSessions: 0 },
    bytesRead: 4096,
    relatedSessionIds: [],
    diagnostics: [],
  };
  // Positive control: the same shape normalizes cleanly when the field is
  // there, so the throw below is about the missing field and not the fixture.
  assert.equal(normalizeHydration(complete).bytesRead, 4096);
  // A real zero still passes. Only absence is a contract failure.
  assert.equal(normalizeHydration({ ...complete, bytesRead: 0 }).bytesRead, 0);

  for (const broken of [undefined, null, '4096', Number.NaN, Number.POSITIVE_INFINITY]) {
    const { bytesRead: _dropped, ...rest } = complete;
    const value = broken === undefined ? rest : { ...rest, bytesRead: broken };
    assert.throws(
      () => normalizeHydration(value as never),
      (error: unknown) => (error as { code?: string }).code === 'NATIVE_CONTRACT_MISMATCH',
      `bytesRead ${String(broken)} must be rejected`,
    );
  }
});
