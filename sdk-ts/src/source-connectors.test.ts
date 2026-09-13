import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { access, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import {
  InvalidArgumentError, NativeContractMismatchError, RelayHistoryError,
  discoverSessions, hydrateSession, sync, validateNativeContract,
} from './index.js';

const run = promisify(execFile);
const cli = fileURLToPath(new URL('./cli.js', import.meta.url));
const mcp = fileURLToPath(new URL('./mcp-server.js', import.meta.url));

async function isolated(body: (dbPath: string) => Promise<void>): Promise<void> {
  const home = await mkdtemp(join(tmpdir(), 'relayhistory-connectors-'));
  const keys = Object.keys(process.env).filter((key) => /^(HOME|USERPROFILE|XDG_|OPENCODE_|TRAJECTORY_|AI_HIST_|RELAYHISTORY_|RELAYCAST_)/.test(key));
  const saved = { ...process.env };
  for (const key of keys) delete process.env[key];
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  process.env.RELAYHISTORY_HOME = join(home, 'commercial');
  process.env.RELAYHISTORY_NO_UPDATE_CHECK = '1';
  process.env.AI_HIST_DB = join(home, 'history.db');
  try {
    // An installed commercial sign-in must never affect an empty selection.
    await mkdir(join(process.env.RELAYHISTORY_HOME, 'stages'), { recursive: true });
    await writeFile(join(process.env.RELAYHISTORY_HOME, 'stages', 'broken.auth.json'), '{not-json', { mode: 0o600 });
    await body(process.env.AI_HIST_DB);
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
    await rm(home, { recursive: true, force: true });
  }
}

test('SDK rejects malformed connector selection before native acquisition', async () => {
  await isolated(async (dbPath) => {
    for (const sourceConnectors of [null, 'cloud', [null], [''], [' cloud'], ['cloud', 'cloud'], new Array(1)]) {
      const selection = { dbPath, scope: 'remote' as const, sourceConnectors: sourceConnectors as string[] };
      for (const operation of [
        () => discoverSessions(selection),
        () => sync(selection),
        () => hydrateSession({ ...selection, source: 'claude', sessionId: 'missing' }),
      ]) {
        await assert.rejects(operation, (error: unknown) => error instanceof InvalidArgumentError
          && error.code === 'INVALID_ARGUMENT' && error.message.includes('sourceConnectors'));
      }
    }
    await assert.rejects(access(dbPath), { code: 'ENOENT' });
  });
});

test('native contract 11 is rejected because it silently ignores connector selection', () => {
  assert.throws(() => validateNativeContract(11), (error: unknown) => error instanceof NativeContractMismatchError
    && error.code === 'NATIVE_CONTRACT_MISMATCH');
});

test('local scope ignores selected commercial connectors and all scope can disable remotes', async () => {
  await isolated(async (dbPath) => {
    const local = await discoverSessions({ dbPath, scope: 'local', sourceConnectors: ['cloud', 'relaycast'] });
    assert.equal(local.scope, 'local');
    assert.deepEqual(local.locationsRun, ['local']);
    assert.equal((await sync({ dbPath, scope: 'local', sourceConnectors: ['cloud', 'relaycast'] })).completed, true);
    const all = await discoverSessions({ dbPath, scope: 'all', sourceConnectors: [] });
    assert.equal(all.scope, 'all');
    assert.deepEqual(all.locationsRun, ['local']);
    assert.equal((await sync({ dbPath, scope: 'all', sourceConnectors: [] })).completed, true);
  });
});

test('explicit empty or unknown selection fails remote acquisition before creating a database', async () => {
  await isolated(async (dbPath) => {
    for (const sourceConnectors of [[], ['not-a-connector']]) {
      const options = { dbPath, scope: 'remote' as const, sourceConnectors };
      for (const [operation, unavailableCode] of [
        [() => discoverSessions(options), 'UNSUPPORTED_OPERATION'],
        [() => sync(options), 'UNSUPPORTED_OPERATION'],
        [() => hydrateSession({ ...options, source: 'claude', sessionId: 'missing' }), 'CONNECTOR_NOT_CONFIGURED'],
      ] as const) {
        const expectedCode = sourceConnectors.length ? 'INVALID_ARGUMENT' : unavailableCode;
        await assert.rejects(operation, (error: unknown) => error instanceof RelayHistoryError
          && error.code === expectedCode);
        await assert.rejects(access(dbPath), { code: 'ENOENT' });
      }
    }
  });
});

test('CLI passes explicit empty and named connector selections to acquisition', async () => {
  await isolated(async (dbPath) => {
    for (const command of [['sync'], ['sessions', 'discover'], ['sessions', 'hydrate', 'claude', 'missing']]) {
      for (const flags of [['--no-source-connectors'], ['--source-connector', 'not-a-connector']]) {
        const expectedCode = flags[0] === '--source-connector' ? 'INVALID_ARGUMENT'
          : command.includes('hydrate') ? 'CONNECTOR_NOT_CONFIGURED' : 'UNSUPPORTED_OPERATION';
        await assert.rejects(
          () => run(process.execPath, [cli, ...command, '--remote', '--db', dbPath, ...flags, '--no-warning'], { env: process.env }),
          (error: unknown) => typeof error === 'object' && error !== null && 'stderr' in error
            && String(error.stderr).includes(expectedCode),
        );
        await assert.rejects(access(dbPath), { code: 'ENOENT' });
      }
    }
    await assert.rejects(
      () => run(process.execPath, [cli, 'sync', '--source-connector', 'cloud', '--no-source-connectors'], { env: process.env }),
      (error: unknown) => typeof error === 'object' && error !== null && 'stderr' in error
        && String(error.stderr).includes('mutually exclusive'),
    );
  });
});

test('MCP acquisition exposes and forwards source connector selection', async () => {
  await isolated(async (dbPath) => {
    const env = Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined));
    const client = new Client({ name: 'source-selection-fixture', version: '1' });
    const transport = new StdioClientTransport({ command: process.execPath, args: [mcp], env, stderr: 'pipe' });
    try {
      await client.connect(transport);
      const { tools } = await client.listTools();
      for (const name of ['discover_sessions', 'hydrate_session', 'sync']) {
        const tool = tools.find((entry) => entry.name === name);
        assert.ok(tool?.inputSchema.properties?.source_connectors, `${name} exposes the selection`);
        assert.equal(tool.annotations?.openWorldHint, true);
        const result = await client.callTool({ name, arguments: {
          scope: 'remote', source_connectors: [],
          ...(name === 'hydrate_session' ? { source: 'claude', session_id: 'missing' } : {}),
        } });
        assert.equal(result.isError, true);
        assert.ok(JSON.stringify(result.content).includes(name === 'hydrate_session' ? 'CONNECTOR_NOT_CONFIGURED' : 'UNSUPPORTED_OPERATION'));
        await assert.rejects(access(dbPath), { code: 'ENOENT' });
      }
    } finally {
      await client.close();
      await transport.close();
    }
  });
});
