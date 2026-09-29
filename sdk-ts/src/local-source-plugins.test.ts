import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test, { type TestContext } from 'node:test';
import {
  HistoryPluginRegistry,
  loadHistoryPlugins,
  discoverSessions,
  hydrateSession,
  listSessionCatalog,
  getSessionEventsPage,
  sync,
  type HistorySource,
} from './index.js';

const SDK_ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

/** An empty provider HOME, so the built-in parsers find nothing of their own. */
async function isolatedHome(t: TestContext, prefix: string): Promise<{ dir: string; dbPath: string; root: string }> {
  const dir = await mkdtemp(join(tmpdir(), prefix));
  t.after(() => rm(dir, { recursive: true, force: true }));
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
  const root = join(dir, 'host-app', 'sessions');
  await mkdir(root, { recursive: true });
  return { dir, dbPath: join(dir, 'history.db'), root };
}

async function writeSession(root: string, id: string): Promise<void> {
  await writeFile(
    join(root, `${id}.jsonl`),
    [
      { role: 'user', text: 'add a retry to the client', ts: 1_789_000_000_000 },
      { role: 'assistant', text: 'added exponential backoff', ts: 1_789_000_001_000 },
    ]
      .map((line) => JSON.stringify(line))
      .join('\n') + '\n',
  );
}

function remoteSpy(): HistorySource & { calls: number } {
  const source: HistorySource & { calls: number } = {
    id: 'fixture-remote',
    instanceId: 'one',
    location: 'remote',
    supportedSources: ['claude'],
    calls: 0,
    discover: async () => {
      source.calls++;
      throw new Error('a remote connector must not run for a local request');
    },
    hydrate: async () => {
      source.calls++;
      throw new Error('a remote connector must not run for a local request');
    },
  };
  return source;
}

test('a local source plugin discovers and hydrates a session presented as local', async (t) => {
  const { dbPath, root } = await isolatedHome(t, 'rh-local-plugin-');
  await writeSession(root, 'host-session-1');
  const plugins = await loadHistoryPlugins(
    [{ module: './fixtures/local-source-plugin/index.mjs', options: { root } }],
    { baseDirectory: SDK_ROOT },
  );
  const remote = remoteSpy();
  plugins.register({ sources: [remote] });

  const discovery = await discoverSessions({ scope: 'local', plugins, dbPath });
  assert.equal(discovery.scope, 'local');
  assert.deepEqual(discovery.locationsRun, ['local']);
  assert.equal(discovery.discovered, 1);
  assert.ok(discovery.providers.some((provider) => provider.source === 'fixture-local'));

  const hydrated = await hydrateSession({
    source: 'claude',
    sessionId: 'host-session-1',
    scope: 'local',
    plugins,
    dbPath,
  });
  assert.equal(hydrated.evidence.events, 2);

  const catalog = await listSessionCatalog({ scope: 'local', dbPath });
  const row = catalog.find((session) => session.sessionId === 'host-session-1');
  assert.ok(row, JSON.stringify(catalog));
  assert.equal(row.source, 'claude');
  assert.deepEqual(row.locations, ['local']);

  const events = await getSessionEventsPage('host-session-1', { source: 'claude', dbPath });
  assert.deepEqual(
    events.events.map((event) => event.text),
    ['add a retry to the client', 'added exponential backoff'],
  );
  assert.equal(remote.calls, 0, 'local scope never invokes a remote connector');
});

test('the default scope runs registered local plugins, and remote scope does not', async (t) => {
  const { dbPath, root } = await isolatedHome(t, 'rh-local-plugin-scope-');
  await writeSession(root, 'host-session-2');
  const plugins = await loadHistoryPlugins(
    [{ module: './fixtures/local-source-plugin/index.mjs', options: { root } }],
    { baseDirectory: SDK_ROOT },
  );
  const [local] = plugins.sourceConnectors();
  let calls = 0;
  const discover = local.discover.bind(local);
  local.discover = async (options) => {
    calls++;
    return discover(options);
  };

  await assert.rejects(
    discoverSessions({ scope: 'remote', plugins, dbPath }),
    /No selected source plugin/,
  );
  assert.equal(calls, 0, 'remote scope never invokes a local connector');

  const result = await sync({ plugins, dbPath });
  assert.equal(result.scope, 'local');
  assert.equal(result.completed, true, JSON.stringify(result.diagnostics));
  assert.equal(calls, 1);
  const catalog = await listSessionCatalog({ scope: 'local', dbPath });
  assert.ok(catalog.some((session) => session.sessionId === 'host-session-2'));

  // An explicit empty selection is how a caller opts out, as for remotes.
  await discoverSessions({ scope: 'local', plugins, sourceConnectors: [], dbPath });
  assert.equal(calls, 1);
});

test('all scope runs local and remote connectors together', async (t) => {
  const { dbPath, root } = await isolatedHome(t, 'rh-local-plugin-all-');
  await writeSession(root, 'host-session-3');
  const plugins = await loadHistoryPlugins(
    [{ module: './fixtures/local-source-plugin/index.mjs', options: { root } }],
    { baseDirectory: SDK_ROOT },
  );
  const remote = remoteSpy();
  plugins.register({ sources: [remote] });
  const discovery = await discoverSessions({ scope: 'all', plugins, dbPath });
  assert.equal(remote.calls, 1);
  assert.equal(discovery.discovered, 1);
  assert.ok(discovery.diagnostics.some((item) => item.source === 'fixture-remote:one'));
});

test('a local source must declare absolute roots and stay inside them', async (t) => {
  const { dbPath, dir, root } = await isolatedHome(t, 'rh-local-plugin-roots-');
  const base = {
    id: 'bad-local',
    instanceId: 'one',
    location: 'local' as const,
    supportedSources: ['claude'] as const,
    discover: async () => ({ observations: [] }),
    hydrate: async () => ({ source_stamp: 's', source_bytes: 0, covered_kinds: [], records: [] }),
  };
  for (const roots of [undefined, [], ['relative/dir'], [''], 'not-an-array']) {
    assert.throws(
      () => new HistoryPluginRegistry().register({
        sources: [{ ...base, roots } as unknown as HistorySource],
      }),
      /declare the absolute directories it reads/,
      `roots ${JSON.stringify(roots)} must be refused`,
    );
  }

  const outside = join(dir, 'elsewhere', 'stolen.jsonl');
  const escaping = join(root, '..', '..', 'elsewhere', 'stolen.jsonl');
  for (const raw_path of [outside, escaping, 'relative.jsonl']) {
    const registry = new HistoryPluginRegistry();
    registry.register({
      sources: [{
        ...base,
        roots: [root],
        discover: async () => ({
          observations: [{ source: 'claude', session_id: 'escaped', raw_path, source_stamp: '1' }],
        }),
      }],
    });
    const discovery = await discoverSessions({ scope: 'local', plugins: registry, dbPath });
    assert.equal(discovery.discovered, 0, raw_path);
    assert.ok(
      discovery.diagnostics.some((item) => item.source === 'bad-local:one'),
      JSON.stringify(discovery.diagnostics),
    );
  }
  const catalog = await listSessionCatalog({ scope: 'local', dbPath });
  assert.ok(!catalog.some((session) => session.sessionId === 'escaped'));
});

test('a source filter the local plugin does not cover leaves the native pass answering', async (t) => {
  const { dbPath, root } = await isolatedHome(t, 'rh-local-plugin-filter-');
  await writeSession(root, 'host-session-4');
  const plugins = await loadHistoryPlugins(
    [{ module: './fixtures/local-source-plugin/index.mjs', options: { root } }],
    { baseDirectory: SDK_ROOT },
  );
  const [local] = plugins.sourceConnectors();
  let calls = 0;
  const discover = local.discover.bind(local);
  local.discover = async (options) => {
    calls++;
    return discover(options);
  };
  // The fixture reads only Claude sessions; a Codex-only request is not its.
  const discovery = await discoverSessions({ sources: ['codex'], plugins, dbPath });
  assert.equal(discovery.scope, 'local');
  const synced = await discoverSessions({ scope: 'all', sources: ['codex'], plugins, dbPath });
  assert.equal(synced.scope, 'all');
  assert.equal(calls, 0, 'a plugin that covers none of the requested sources is not run');

  await discoverSessions({ sources: ['claude'], plugins, dbPath });
  assert.equal(calls, 1);
});

test('a relay session a local plugin holds hydrates even after the built-in adapter catalogues it', async (t) => {
  const { dbPath, root } = await isolatedHome(t, 'rh-local-plugin-relay-');
  const registry = new HistoryPluginRegistry();
  let hydrations = 0;
  registry.register({
    sources: [{
      id: 'relay-local',
      instanceId: 'one',
      location: 'local',
      roots: [root],
      supportedSources: ['relay'],
      discover: async () => ({
        observations: [{ source: 'relay', session_id: 'host-1', raw_locator: 'host-1', source_stamp: '1' }],
      }),
      hydrate: async () => {
        hydrations++;
        return {
          source_stamp: '1',
          source_bytes: 10,
          covered_kinds: ['history'],
          records: [{
            kind: 'history',
            payload: { source: 'relay', session_id: 'host-1', prompt: 'deploy it', timestamp_ms: 1_789_000_000_000 },
          }],
        };
      },
    }],
  });
  await discoverSessions({ plugins: registry, dbPath });
  const first = await hydrateSession({ source: 'relay', sessionId: 'host-1', plugins: registry, dbPath });
  assert.equal(first.evidence.prompts, 1);
  // The stored relay history is now what the built-in relay adapter
  // enumerates, so it observes the session too and refuses to hydrate it.
  await discoverSessions({ plugins: registry, dbPath });
  const again = await hydrateSession({ source: 'relay', sessionId: 'host-1', plugins: registry, dbPath });
  assert.equal(again.evidence.prompts, 1);
  assert.equal(hydrations, 2);
});

test('the roots check rejects only real escapes, for raw_path and an absolute raw_locator', async (t) => {
  const { dbPath, dir, root } = await isolatedHome(t, 'rh-local-plugin-dotted-');
  let next = 0;
  const accept = async (row: Record<string, unknown>, expected: number) => {
    const sessionId = `dotted-${next++}`;
    const registry = new HistoryPluginRegistry();
    registry.register({
      sources: [{
        id: 'dotted-local',
        instanceId: 'one',
        location: 'local',
        roots: [root],
        supportedSources: ['claude'],
        discover: async () => ({
          observations: [{ source: 'claude', session_id: sessionId, source_stamp: '1', ...row }],
        }),
        hydrate: async () => ({ source_stamp: 's', source_bytes: 0, covered_kinds: [], records: [] }),
      }],
    });
    const discovery = await discoverSessions({ scope: 'local', plugins: registry, dbPath });
    assert.equal(discovery.discovered, expected, JSON.stringify({ row, diagnostics: discovery.diagnostics }));
  };
  // Children whose names merely begin with `..` are inside the root.
  await accept({ raw_path: join(root, '..archive', 'old.jsonl') }, 1);
  await accept({ raw_path: join(root, '...') }, 1);
  // An opaque, relative locator names no file.
  await accept({ raw_locator: 'handle-42' }, 1);
  await accept({ raw_locator: join(root, 'inside.jsonl') }, 1);
  // An absolute locator is presented as the session's path: held to the roots.
  await accept({ raw_locator: join(dir, 'elsewhere', 'stolen.jsonl') }, 0);
  await accept({ raw_path: join(root, '..', 'sibling.jsonl') }, 0);
});
