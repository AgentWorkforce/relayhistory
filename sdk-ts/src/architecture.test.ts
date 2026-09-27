import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const sourceDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'src');
const repositoryRoot = join(sourceDir, '..', '..');

test('production TypeScript has one native implementation', async () => {
  const files = ['index.ts', 'cli.ts', 'mcp-server.ts', 'operations.ts', 'normalization.ts', 'pagination.ts', 'native.ts', 'sdk-common.ts'];
  const source = (await Promise.all(files.map((file) => readFile(join(sourceDir, file), 'utf8')))).join('\n');
  for (const forbidden of ['sql.js', 'node:child_process', 'AI_HIST_RUST_BIN', "fallback: 'jsonl'", 'readFile(dbPath)']) {
    assert.equal(source.includes(forbidden), false, `production source contains ${forbidden}`);
  }
  const pkg = JSON.parse(await readFile(join(repositoryRoot, 'sdk-ts', 'package.json'), 'utf8')) as { dependencies: Record<string, string> };
  assert.equal(pkg.dependencies['sql.js'], undefined);
  assert.equal(typeof pkg.dependencies['ai-hist-native'], 'string');
});

test('CLI and MCP import only the public SDK for history operations', async () => {
  const [cli, mcp] = await Promise.all([
    readFile(join(sourceDir, 'cli.ts'), 'utf8'),
    readFile(join(sourceDir, 'mcp-server.ts'), 'utf8'),
  ]);
  assert.match(cli, /from '\.\/index\.js'/);
  assert.match(mcp, /from '\.\/index\.js'/);
  assert.doesNotMatch(cli + mcp, /ai-hist-native|node:sqlite|sql\.js|child_process/);
});

test('MCP session operations expose scope and acquisition is declared open-world', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  assert.match(mcp, /const SESSION_SCOPE = z\.enum\(\['local', 'remote', 'all'\]\)/);
  assert.match(mcp, /const ACQUIRE = \{ readOnlyHint: false, idempotentHint: true, openWorldHint: true \}/);
  for (const tool of ['search_history', 'recent_history', 'list_sessions', 'discover_sessions', 'hydrate_session', 'history_stats', 'sync']) {
    const start = mcp.indexOf(`server.tool('${tool}'`);
    assert.notEqual(start, -1, `${tool} is registered`);
    const end = mcp.indexOf("server.tool('", start + 13);
    const registration = mcp.slice(start, end === -1 ? undefined : end);
    assert.match(registration, /scope: SESSION_SCOPE\.optional\(\)\.default\('local'\)/, `${tool} defaults scope to local`);
  }
  for (const tool of ['discover_sessions', 'sync']) {
    const start = mcp.indexOf(`server.tool('${tool}'`);
    const end = mcp.indexOf("server.tool('", start + 13);
    assert.match(mcp.slice(start, end === -1 ? undefined : end), /ACQUIRE/, `${tool} may reach remote provider connectors`);
  }
  {
    const start = mcp.indexOf("server.tool('hydrate_session'");
    const end = mcp.indexOf("server.tool('", start + 13);
    assert.match(mcp.slice(start, end === -1 ? undefined : end), /ACQUIRE/, 'hydrate_session can acquire explicitly selected remote evidence');
  }
});

test('identity-addressed MCP tools are read-only and take no scope', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  for (const tool of ['get_session', 'get_session_events', 'get_session_relationships', 'get_session_tree', 'get_session_usage', 'get_session_markers', 'get_source_capabilities']) {
    const start = mcp.indexOf(`server.tool('${tool}'`);
    assert.notEqual(start, -1, `${tool} is registered`);
    const end = mcp.indexOf("server.tool('", start + 13);
    const registration = mcp.slice(start, end === -1 ? undefined : end);
    assert.match(registration, /READ/, `${tool} is a read-only tool`);
    assert.doesNotMatch(registration, /SESSION_SCOPE/, `${tool} addresses a session by identity`);
  }
});

test('native topology enums are validated rather than cast', async () => {
  const source = await readFile(join(sourceDir, 'normalization.ts'), 'utf8');
  const start = source.indexOf('function relationship(value: UnknownRecord)');
  assert.notEqual(start, -1, 'the relationship normalizer exists');
  const body = source.slice(start, source.indexOf('\n}', start));
  // An out-of-contract value must reach a caller as a contract mismatch, not
  // as a lie about the shape of the typed API.
  assert.match(body, /source: catalogSource\(value\.source\)/);
  assert.match(body, /relationship: relationshipType\(value\.relationship\)/);
  assert.match(body, /identityStatus: identityStatus\(value\.identityStatus\)/);
  assert.doesNotMatch(body, /as CatalogSource|as RelationshipType|as IdentityStatus/);
});

test('MCP evidence tools require both halves of a session identity', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  for (const tool of ['get_session_tool_calls', 'get_session_file_edits', 'get_session_markers']) {
    const start = mcp.indexOf(`server.tool('${tool}'`);
    assert.notEqual(start, -1, `${tool} is registered`);
    const end = mcp.indexOf("server.tool('", start + 13);
    const registration = mcp.slice(start, end === -1 ? undefined : end);
    assert.match(registration, /source: SOURCE,/, `${tool} requires a source`);
    assert.match(registration, /session_id: z\.string\(\)\.min\(1\)/, `${tool} requires a session id`);
    assert.match(registration, /after: EVIDENCE_CURSOR\.optional\(\)/, `${tool} paginates`);
    assert.match(registration, /READ/, `${tool} is a cache-only read`);
  }
});


test('the native session-store dispatcher is named in one place', async () => {
  // `native.ts` owns the op vocabulary; every other production module reaches
  // the dispatcher through its typed helper, so a new facade read is one entry
  // there and one arm in Rust, never a string literal scattered across callers.
  const native = await readFile(join(sourceDir, 'native.ts'), 'utf8');
  assert.match(native, /export const SESSION_STORE_OPS = Object\.freeze\(\{/);
  for (const op of ['markers', 'requests', 'usage_summary', 'user_turns', 'capabilities']) {
    assert.match(native, new RegExp(`'${op}'`), `native.ts names the ${op} op`);
  }
  const files = ['index.ts', 'cli.ts', 'mcp-server.ts', 'operations.ts', 'normalization.ts', 'pagination.ts', 'sdk-common.ts'];
  const source = (await Promise.all(files.map((file) => readFile(join(sourceDir, file), 'utf8')))).join('\n');
  assert.doesNotMatch(source, /\.sessionStoreCall\(/, 'only native.ts calls the binding directly');
  assert.doesNotMatch(source, /sessionStoreCall\(\s*'/, 'op names are not spelled outside native.ts');
  // The usage reads the SDK exposes go through the dispatcher, not the older
  // typed functions, so the JSON boundary is what the usage tests exercise.
  const operations = await readFile(join(sourceDir, 'operations.ts'), 'utf8');
  for (const legacy of ['native.getSessionRequestsPage(', 'native.getSessionUsage(', 'native.getSessionUserTurnsPage(']) {
    assert.equal(operations.includes(legacy), false, `${legacy} is no longer called by the SDK`);
  }
});

test('MCP usage tools state that usage is provider-reported and cost is never computed', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  const start = mcp.indexOf("server.tool('get_session_usage'");
  assert.notEqual(start, -1);
  const end = mcp.indexOf("server.tool('", start + 13);
  const registration = mcp.slice(start, end === -1 ? undefined : end);
  assert.match(registration, /never an assumed zero/);
  assert.match(registration, /cost appears only when the source data carried one/);
});

test('local artifacts exclude cloud APIs and dependencies', async () => {
  const files=['index.ts','operations.ts','native.ts','sdk-common.ts','cli.ts','mcp-server.ts'];
  const source=(await Promise.all(files.map(file=>readFile(join(sourceDir,file),'utf8')))).join('\n');
  assert.doesNotMatch(source,/cloud-client|cloud-auth|@agent-relay\/cloud|cloudLoadAuth|pushCloud/);
  const pkg=JSON.parse(await readFile(join(sourceDir,'../package.json'),'utf8'));
  assert.equal(pkg.exports['./cloud'],undefined);
  assert.equal(pkg.devDependencies['@agent-relay/cloud'],undefined);
  assert.doesNotMatch(pkg.scripts.build,/cloud/);
});
