import assert from 'node:assert/strict';
import { mkdtemp, rm, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  HistoryPluginRegistry,
  sync,
  discoverSessions,
  hydrateSession,
  getSessionEventsPage,
  getSourceObservation,
  hydrateSourcePlugin,
  discoverSourcePlugins,
  RelayHistoryError,
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
    /No selected source plugin is available/,
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
