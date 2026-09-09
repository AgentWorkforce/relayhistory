import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const sourceDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'src');
const repositoryRoot = join(sourceDir, '..', '..');

test('production TypeScript has one native implementation', async () => {
  const files = ['index.ts', 'cli.ts', 'mcp-server.ts', 'cloud-client.ts'];
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
  assert.match(mcp, /const LOCAL_WRITE = \{ readOnlyHint: false, idempotentHint: true, openWorldHint: false \}/);
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
    assert.match(mcp.slice(start, end === -1 ? undefined : end), /LOCAL_WRITE/, 'hydrate_session indexes local provider evidence, closed-world');
  }
});

test('identity-addressed MCP tools are read-only and take no scope', async () => {
  const mcp = await readFile(join(sourceDir, 'mcp-server.ts'), 'utf8');
  for (const tool of ['get_session', 'get_session_events', 'get_session_relationships', 'get_session_tree']) {
    const start = mcp.indexOf(`server.tool('${tool}'`);
    assert.notEqual(start, -1, `${tool} is registered`);
    const end = mcp.indexOf("server.tool('", start + 13);
    const registration = mcp.slice(start, end === -1 ? undefined : end);
    assert.match(registration, /READ/, `${tool} is a read-only tool`);
    assert.doesNotMatch(registration, /SESSION_SCOPE/, `${tool} addresses a session by identity`);
  }
});

test('native topology enums are validated rather than cast', async () => {
  const source = await readFile(join(sourceDir, 'index.ts'), 'utf8');
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
  for (const tool of ['get_session_tool_calls', 'get_session_file_edits']) {
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


// cloud-client.ts still carries #112's getSessionThread recall client together
// with its own stage resolution, rotation and `~/.config/ai-hist/auth.json`
// handling, so it is not yet a pure delegation to the native SDK. It also still
// defines loginCloud and loadStoredRelayhistoryAuth alongside the Rust-backed
// versions in index.ts — a second credential implementation in the same package.
// Consolidating that is tracked in docs/ws12-validation.md and deliberately not
// attempted here. Until then, guard the narrower invariant this change did
// establish: the operations moved to Rust must have exactly one implementation.
test('cloud compatibility entrypoint does not reimplement the native cloud operations', async () => {
  const source = await readFile(join(sourceDir, 'cloud-client.ts'), 'utf8');
  assert.match(source, /from '\.\/index\.js'/);
  for (const operation of ['enableCloud', 'pushCloud', 'accessToken', 'replay', 'createShareableTrace']) {
    assert.equal(
      source.includes(`export async function ${operation}`), false,
      `${operation} has one native implementation; cloud-client must not add a second`,
    );
  }
});

test('token and replay SDK operations delegate to native code', async () => {
  const source = await readFile(join(sourceDir, 'index.ts'), 'utf8');
  const cloudCommands = source.slice(source.indexOf('export async function accessToken('), source.indexOf('export interface CloudOptions'));
  assert.match(cloudCommands, /native\.accessToken\(/);
  assert.match(cloudCommands, /native\.replay\(/);
  assert.doesNotMatch(cloudCommands, /fetch\(|readFile|writeFile|auth\.json|nextCursor|refreshToken/);
});
