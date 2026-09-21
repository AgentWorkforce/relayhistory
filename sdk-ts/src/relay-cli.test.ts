import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { promisify } from 'node:util';
import { gunzipSync } from 'node:zlib';

import { assertSurfaceConforms, walkCommands, type RelayCliIo } from '@agent-relay/cli-surface';

import { BOOLEAN_FLAGS, COMMANDS, FLAG_SPECS, HOST_OWNED_FLAGS, VALUE_FLAGS } from './cli.js';
import { createRelayCliSurface, __testing } from './relay-cli.js';
import type { RelayhistoryCloudClient } from './cloud-contract.js';

/**
 * Collects what a run wrote, so nothing in a test reaches the real streams.
 *
 * `RelayCliIo` carries `string | Uint8Array`, and a streamed command such as
 * `export` writes bytes, so the sink decodes incrementally rather than
 * stringifying each chunk: a multi-byte sequence split across two chunks is
 * only recoverable if the decoder sees them in order. `bytes` counts the
 * chunks that arrived as bytes, which is how a test tells a pass-through from
 * a decode round trip.
 */
function capture(): RelayCliIo & { out: string; err: string; bytes: number } {
  const decoder = new TextDecoder();
  const sink = {
    out: '', err: '', bytes: 0,
    stdout(chunk: string | Uint8Array) {
      if (typeof chunk === 'string') { sink.out += chunk; return; }
      sink.bytes += 1;
      sink.out += decoder.decode(chunk, { stream: true });
    },
    stderr(chunk: string | Uint8Array) {
      sink.err += typeof chunk === 'string' ? chunk : new TextDecoder().decode(chunk);
    },
  };
  return sink;
}

/** The offline fixture store the delivery tests index against, in a temp dir. */
async function historyFixture(): Promise<{ root: string; db: string }> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-surface-delivery-'));
  const db = join(root, 'history.db');
  await writeFile(db, gunzipSync(await readFile(new URL('../fixtures/offline-history.db.gz', import.meta.url))));
  return { root, db };
}

/** Every routable path, as argv, from the dispatch side of the tables. */
function routablePaths(): string[] {
  return [...__testing.buildRoutes().keys()].sort();
}

/** Every declared path, as argv, from the tree a host renders help from. */
function declaredPaths(surface: ReturnType<typeof createRelayCliSurface>): string[] {
  const paths: string[] = [];
  for (const { path, command } of walkCommands(surface.commands)) {
    // Group nodes are headings, not commands: they carry subcommands and
    // nothing to run.
    if (command.subcommands?.length) continue;
    paths.push(path.join(' '));
    for (const alias of command.aliases ?? []) {
      paths.push([...path.slice(0, -1), alias].join(' '));
    }
  }
  return paths.sort();
}

// ---------------------------------------------------------------------------
// Drift: `commands` and `run` must describe the same tree
// ---------------------------------------------------------------------------

test('every declared command is routable and every routable command is declared', () => {
  const cloud = stubCloud();
  const surface = createRelayCliSurface({ cloud });
  const declared = declaredPaths(surface);
  const routable = routablePaths();

  const undeclared = routable.filter((path) => !declared.includes(path));
  const unroutable = declared.filter((path) => !routable.includes(path));

  assert.deepEqual(undeclared, [],
    `routable but not declared, so no help describes them: ${undeclared.join(', ')}`);
  assert.deepEqual(unroutable, [],
    `declared but not routable, so help advertises commands that cannot run: ${unroutable.join(', ')}`);
});

test('the surface conforms to the contract, with and without a cloud client', () => {
  assertSurfaceConforms(createRelayCliSurface());
  assertSurfaceConforms(createRelayCliSurface({ cloud: stubCloud() }));
});

test('every declared path resolves to the ai-hist command that implements it', () => {
  const routes = __testing.buildRoutes();
  for (const [key, spec] of COMMANDS) {
    if (!spec.surface) continue;
    const route = routes.get(spec.surface.join(' '));
    assert.ok(route, `${spec.name} declares surface '${spec.surface.join(' ')}' but nothing routes it`);
    assert.equal(route.kind, 'local');
    assert.deepEqual(
      route.kind === 'local' ? [...route.words] : [],
      key.split(' '),
      `${spec.surface.join(' ')} must run '${key}'`,
    );
  }
});

// ---------------------------------------------------------------------------
// Drift: the option tables must match the parser the commands actually use
// ---------------------------------------------------------------------------

test('every allowed flag is described, and described with the arity parse enforces', () => {
  const allowed = new Set<string>();
  for (const spec of COMMANDS.values()) {
    for (const name of spec.allowed) {
      if (!HOST_OWNED_FLAGS.has(name)) allowed.add(name);
    }
  }

  const undescribed = [...allowed].filter((name) => !FLAG_SPECS[name]).sort();
  assert.deepEqual(undescribed, [],
    `allowed by a command but absent from FLAG_SPECS: ${undescribed.join(', ')}`);

  const unused = Object.keys(FLAG_SPECS).filter((name) => !allowed.has(name)).sort();
  assert.deepEqual(unused, [],
    `described in FLAG_SPECS but allowed by no command: ${unused.join(', ')}`);

  for (const [name, spec] of Object.entries(FLAG_SPECS)) {
    // A flag `parse` does not know is rejected as an unknown option, so a
    // command allowing it could never actually be given it.
    assert.ok(BOOLEAN_FLAGS.has(name) || VALUE_FLAGS.has(name),
      `--${name} is allowed and described but parse() knows no such option`);
    const takesValue = spec.flags.includes('<');
    assert.equal(takesValue, VALUE_FLAGS.has(name),
      `--${name} is described as ${takesValue ? 'taking' : 'not taking'} a value, which parse() disagrees with`);
  }
});

test('declared positionals agree with the counts runCli enforces', () => {
  const surface = createRelayCliSurface({ cloud: stubCloud() });
  for (const { path, command } of walkCommands(surface.commands)) {
    if (command.subcommands?.length) continue;
    const args = command.args ?? [];
    args.forEach((arg, index) => {
      if (index < args.length - 1) {
        assert.ok(!arg.variadic, `${path.join(' ')}: only the last argument may be variadic`);
      }
      if (index > 0) {
        assert.ok(!(arg.required && !args[index - 1]!.required),
          `${path.join(' ')}: a required argument may not follow an optional one`);
      }
    });
  }

  for (const spec of COMMANDS.values()) {
    if (!spec.surface) continue;
    const args = spec.args ?? [];
    const [least, most] = spec.positionals;
    assert.equal(args.filter((arg) => arg.required).length, least,
      `${spec.name}: declares ${args.filter((a) => a.required).length} required args but requires ${least}`);
    if (most !== null) {
      assert.equal(args.length, most, `${spec.name}: declares ${args.length} args but accepts at most ${most}`);
    } else {
      assert.ok(args.some((arg) => arg.variadic),
        `${spec.name}: accepts unbounded positionals but declares none as variadic`);
    }
  }
});

test('cloud options are described and no cloud flag is left undocumented', () => {
  const used = new Set(__testing.CLOUD_COMMANDS.flatMap((spec) => [...spec.options]));
  const undescribed = [...used].filter((name) => !__testing.CLOUD_FLAG_SPECS[name]).sort();
  assert.deepEqual(undescribed, [], `cloud flags without a description: ${undescribed.join(', ')}`);
  const unused = Object.keys(__testing.CLOUD_FLAG_SPECS).filter((name) => !used.has(name)).sort();
  assert.deepEqual(unused, [], `described cloud flags no command accepts: ${unused.join(', ')}`);
});

// ---------------------------------------------------------------------------
// Composition: the cloud half appears only when a client is supplied
// ---------------------------------------------------------------------------

function stubCloud(calls: string[] = []): RelayhistoryCloudClient {
  const record = <T>(name: string, value: T) => { calls.push(name); return Promise.resolve(value); };
  async function* empty(): AsyncGenerator<unknown> { /* no rows */ }
  return {
    baseUrl: 'https://example.invalid/v1',
    listSessions: () => record('listSessions', { sessions: [{ sessionId: 's1' }], nextCursor: null }),
    getSessionEvents: (id) => record('getSessionEvents', { sessionId: id, events: [], nextCursor: null }),
    searchEvents: () => record('searchEvents', { events: [], nextCursor: null }),
    getSessionThread: () => record('getSessionThread', { links: [] }),
    iterateSessions: () => { calls.push('iterateSessions'); return empty(); },
    iterateSessionEvents: () => { calls.push('iterateSessionEvents'); return empty(); },
    iterateEvents: () => { calls.push('iterateEvents'); return empty(); },
    listTurns: (id) => record('listTurns', { sessionId: id, turns: [] }),
    getSessionMetadata: () => record('getSessionMetadata', null),
    getDailyDigest: () => record('getDailyDigest', { entries: [] }),
    listMachines: () => record('listMachines', { machines: [] }),
  };
}

test('cloud commands are absent from help without a client, and named with one', () => {
  const withoutCloud = declaredPaths(createRelayCliSurface());
  assert.equal(withoutCloud.some((path) => path.startsWith('cloud ')), false,
    'help must not advertise cloud commands that have no client to run against');

  const withCloud = declaredPaths(createRelayCliSurface({ cloud: stubCloud() }));
  assert.ok(withCloud.includes('cloud list'));
  assert.ok(withCloud.includes('cloud digest'));
  // The local tree is unchanged by composition.
  assert.deepEqual(withoutCloud, withCloud.filter((path) => !path.startsWith('cloud ')));
});

test('a cloud command without a client names the remedy rather than failing as unknown', async () => {
  const io = capture();
  const code = await createRelayCliSurface().run(['cloud', 'list'], io);
  assert.equal(code, 2);
  assert.match(io.err, /requires cloud access: run `agent-relay login`/);
  assert.doesNotMatch(io.err, /unknown command/);
});

test('a cloud command routes through the supplied client', async () => {
  const calls: string[] = [];
  const io = capture();
  const code = await createRelayCliSurface({ cloud: stubCloud(calls) }).run(['cloud', 'list', '--json'], io);
  assert.equal(code, 0);
  assert.deepEqual(calls, ['listSessions']);
  assert.equal((JSON.parse(io.out.trim()) as { sessions: unknown[] }).sessions.length, 1);
});

test('--all walks every page rather than printing a prefix', async () => {
  const calls: string[] = [];
  const io = capture();
  const code = await createRelayCliSurface({ cloud: stubCloud(calls) }).run(['cloud', 'list', '--all'], io);
  assert.equal(code, 0);
  assert.deepEqual(calls, ['iterateSessions'], '--all must use the iterator, not one page');
});

test('a cloud 401 is answered with the login remedy', async () => {
  const cloud = stubCloud();
  const failing: RelayhistoryCloudClient = {
    ...cloud,
    listSessions: () => Promise.reject(Object.assign(new Error('unauthorized'), { status: 401 })),
  };
  const io = capture();
  assert.equal(await createRelayCliSurface({ cloud: failing }).run(['cloud', 'list'], io), 2);
  assert.match(io.err, /cloud access was refused: run `agent-relay login`/);
});

// ---------------------------------------------------------------------------
// Usage errors: one tree, one answer to a mistyped command line
// ---------------------------------------------------------------------------

test('an undeclared option exits 2 on every command, local and cloud alike', async () => {
  const calls: string[] = [];
  const surface = createRelayCliSurface({ cloud: stubCloud(calls) });
  for (const path of declaredPaths(surface)) {
    const io = capture();
    const code = await surface.run([...path.split(' '), '--not-a-real-flag'], io);
    assert.equal(code, 2,
      `${path} --not-a-real-flag exited ${code}, not the 2 a usage error owes: ${io.out}${io.err}`);
    assert.equal(io.out, '', `${path} produced output for a command line it refused: ${io.out}`);
    assert.match(io.err, /not-a-real-flag/);
  }
  assert.deepEqual(calls, [], 'a refused command line must never reach the cloud client');
});

test('cloud commands reject a surplus argument rather than discarding it', async () => {
  const calls: string[] = [];
  const surface = createRelayCliSurface({ cloud: stubCloud(calls) });
  const fixed = __testing.CLOUD_COMMANDS.filter((spec) => !(spec.args ?? []).some((arg) => arg.variadic));
  assert.ok(fixed.length > 0, 'there must be fixed-arity cloud commands for this to mean anything');
  for (const spec of fixed) {
    const declared = spec.args ?? [];
    const io = capture();
    const code = await surface.run(
      [...spec.path, ...declared.map((_, index) => `arg${index}`), 'surplus'], io);
    assert.equal(code, 2,
      `${spec.path.join(' ')} took a surplus argument instead of rejecting it: ${io.out}${io.err}`);
    assert.match(io.err, /does not accept positional argument 'surplus'/);
  }
  assert.deepEqual(calls, [], 'a surplus argument must be refused before the client is called');
});

test('a variadic cloud command still takes every argument it is given', async () => {
  const calls: string[] = [];
  const io = capture();
  const code = await createRelayCliSurface({ cloud: stubCloud(calls) })
    .run(['cloud', 'search', 'one', 'two', 'three'], io);
  assert.equal(code, 0, io.err);
  assert.deepEqual(calls, ['searchEvents'], 'the surplus check must not clip a variadic argument');
});

test('the same mistake exits 2 whichever half of the tree it is typed against', async () => {
  // The halves are parsed by different code — `runCli` for the local tree,
  // `parseCloud` for the cloud one — so the codes they answer with can drift
  // apart silently. Pair them here: a mistake listed on one side must be
  // listed on the other, and both must be 2.
  const pairs = [
    { mistake: 'an option the command does not accept',
      local: ['list', '--config', 'x'], cloud: ['cloud', 'list', '--refresh'] },
    { mistake: 'a missing required argument',
      local: ['hydrate'], cloud: ['cloud', 'events'] },
    { mistake: 'an argument the command does not take',
      local: ['list', 'surplus'], cloud: ['cloud', 'digest', 'surplus'] },
    { mistake: 'an option given no value',
      local: ['list', '--limit'], cloud: ['cloud', 'list', '--limit'] },
  ] as const;

  const surface = createRelayCliSurface({ cloud: stubCloud() });
  for (const { mistake, local, cloud } of pairs) {
    const localIo = capture();
    const cloudIo = capture();
    const localCode = await surface.run([...local], localIo);
    const cloudCode = await surface.run([...cloud], cloudIo);
    assert.equal(localCode, 2, `${mistake}: '${local.join(' ')}' exited ${localCode}: ${localIo.err}`);
    assert.equal(cloudCode, 2, `${mistake}: '${cloud.join(' ')}' exited ${cloudCode}: ${cloudIo.err}`);
    assert.equal(localCode, cloudCode,
      `${mistake} exits ${localCode} as '${local.join(' ')}' and ${cloudCode} as '${cloud.join(' ')}'`);
    assert.notEqual(cloudIo.err, '', `${mistake}: the cloud half must say what it refused`);
  }
});

test('a failure the cloud reported still exits 1, not the usage code', async () => {
  // The consistency fix must not flatten every cloud error into 2: a request
  // that reached the cloud and failed is not a mistyped command line.
  const cloud = stubCloud();
  const failing: RelayhistoryCloudClient = {
    ...cloud,
    listSessions: () => Promise.reject(Object.assign(new Error('upstream exploded'), { status: 500 })),
  };
  const io = capture();
  assert.equal(await createRelayCliSurface({ cloud: failing }).run(['cloud', 'list'], io), 1);
  assert.match(io.err, /upstream exploded/);
});

// ---------------------------------------------------------------------------
// Contract behaviour
// ---------------------------------------------------------------------------

test('an unknown command exits 2 with a message on stderr', async () => {
  const io = capture();
  assert.equal(await createRelayCliSurface().run(['nonesuch'], io), 2);
  assert.equal(io.out, '');
  assert.match(io.err, /unknown command 'nonesuch'/);
});

test('the surface identifies itself as contract v1 relayhistory', () => {
  const surface = createRelayCliSurface();
  assert.equal(surface.id, 'relayhistory');
  assert.equal(surface.contract, 1);
  assert.match(surface.version, /^\d+\.\d+\.\d+/);
});

// ---------------------------------------------------------------------------
// E2E: a real local store, written and read back through surface.run()
// ---------------------------------------------------------------------------

test('a real local history store is written and read back through surface.run()', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-surface-'));
  t.after(() => rm(root, { recursive: true, force: true }));

  const home = join(root, 'home');
  const projects = join(home, '.claude', 'projects', 'project');
  await mkdir(projects, { recursive: true });
  await writeFile(join(home, '.claude', 'history.jsonl'), `${JSON.stringify({
    display: 'surface probe prompt', sessionId: 'surface-1', project: '/work/surface', timestamp: 1,
  })}\n`);
  await writeFile(join(projects, 'surface-1.jsonl'), `${JSON.stringify({
    sessionId: 'surface-1', cwd: '/work/surface', type: 'user',
    message: { role: 'user', content: 'surface probe prompt' },
    timestamp: '2026-09-01T10:00:00.000Z',
  })}\n`);

  const db = join(root, 'history.db');
  const previousHome = process.env.HOME;
  const previousProfile = process.env.USERPROFILE;
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  t.after(() => {
    if (previousHome === undefined) delete process.env.HOME; else process.env.HOME = previousHome;
    if (previousProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = previousProfile;
  });

  const surface = createRelayCliSurface();

  // Write: the real native engine indexes the fixture into a real SQLite store.
  const synced = capture();
  assert.equal(await surface.run(['sync', '--db', db, '--json', '--no-warning'], synced), 0, synced.err);
  assert.equal(synced.err, '');

  // Read back: `agent-relay sessions list` is `ai-hist sessions list`.
  const listed = capture();
  assert.equal(await surface.run(['list', '--db', db, '--json', '--no-warning'], listed), 0, listed.err);
  const page = JSON.parse(listed.out.trim()) as { sessions: Array<{ session_id: string }> };
  assert.ok(page.sessions.some((session) => session.session_id === 'surface-1'),
    `the indexed session must come back through the surface: ${listed.out}`);

  // Read back through a different verb, against the same store.
  const searched = capture();
  assert.equal(await surface.run(['search', 'surface probe', '--db', db, '--json', '--no-warning'], searched), 0);
  const hits = JSON.parse(searched.out.trim()) as unknown;
  assert.ok(Array.isArray(hits) && hits.length > 0, `search must find the indexed prompt: ${searched.out}`);

  // The renamed verb reaches `ai-hist session`, and its alias reaches the same.
  for (const verb of ['show', 'session']) {
    const shown = capture();
    assert.equal(await surface.run([verb, 'surface-1', '--db', db, '--json', '--no-warning'], shown), 0, shown.err);
    assert.match(shown.out, /surface-1/);
  }
});

test('a usage error from the local tree surfaces as exit 2 on stderr, not a thrown error', async () => {
  const io = capture();
  const code = await createRelayCliSurface().run(['hydrate'], io);
  assert.equal(code, 2);
  assert.equal(io.out, '');
  assert.match(io.err, /sessions hydrate requires SOURCE and SESSION_ID/);
  assert.match(io.err, /Usage:/);
});

test('run never writes to the real streams', async () => {
  // Asserted in a child process rather than by patching the global streams:
  // this runner writes its own reporter output to stdout, so patching here
  // would capture that instead of anything `run` did.
  const script = `
    const { createRelayCliSurface } = await import(${JSON.stringify(new URL('./relay-cli.js', import.meta.url).href)});
    const io = { stdout: () => {}, stderr: () => {} };
    const surface = createRelayCliSurface();
    await surface.run(['stats', '--db', ${JSON.stringify(join(tmpdir(), 'relayhistory-absent.db'))}, '--no-bootstrap'], io);
    await surface.run(['nonesuch'], io);
    await surface.run(['hydrate'], io);
    await surface.run(['cloud', 'list'], io);
  `;
  const result = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script]);
  assert.equal(result.stdout, '', `run() wrote to the real stdout: ${result.stdout}`);
  assert.equal(result.stderr, '', `run() wrote to the real stderr: ${result.stderr}`);
});

test('importing the surface does not run the bin', async () => {
  // `relay-cli.ts` imports `cli.ts` for `runCli`; without the entrypoint guard
  // that import would execute a command line belonging to the host.
  const script = `
    await import(${JSON.stringify(new URL('./relay-cli.js', import.meta.url).href)});
    await import(${JSON.stringify(new URL('./cli.js', import.meta.url).href)});
  `;
  const result = await promisify(execFile)(process.execPath,
    ['--input-type=module', '-e', script, 'search', 'should-not-run']);
  assert.equal(result.stdout, '');
  assert.equal(result.stderr, '');
});

// ---------------------------------------------------------------------------
// The mount must carry everything `runCli` needs, not just argv
// ---------------------------------------------------------------------------

test('mounted export with no --out streams NDJSON to the host, as bytes', async (t) => {
  const { root, db } = await historyFixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  const selectionPath = join(root, 'selection.json');
  await writeFile(selectionPath, JSON.stringify({
    all_sources: false, sources: ['claude'], sessions: [], kinds: ['history'], excluded_sessions: [],
  }));

  // Help describes --out as "Write to this file instead of standard output",
  // so a mounted run without it must reach standard output — which under a
  // mount is the host's sink — rather than be rejected for want of one.
  const io = capture();
  const code = await createRelayCliSurface().run(['export', '--selection', selectionPath, '--db', db], io);

  assert.equal(code, 0, io.err);
  assert.equal(io.err, '');
  const records = io.out.trim().split('\n').map((line) => JSON.parse(line) as { revision_id: string });
  assert.equal(records.length, 3, `every fixture record must reach the host: ${io.out}`);
  assert.ok(records.every((record) => record.revision_id));
  // The export reached the sink as the bytes it produced. Decoding to a string
  // inside the surface would corrupt any chunk boundary falling mid-character,
  // which is the whole reason `RelayCliIo` carries `string | Uint8Array`.
  assert.ok(io.bytes > 0, 'streamed output must reach the host as Uint8Array chunks, not a decoded string');
});

test('mounted legacy delivery command reports the probe migration', async () => {
  const io = capture();
  const code = await createRelayCliSurface().run(['delivery', 'run'], io);
  assert.equal(code, 1);
  assert.match(io.err, /HISTORY_DELIVERY_MOVED/);
});
