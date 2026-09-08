import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { enableCloud, installGitHooks, loadStoredRelayhistoryAuth, pushCloud, sync } from './index.js';

async function run(bin: string, args: string[], env: NodeJS.ProcessEnv, cwd?: string) {
  const child = spawn(bin, args, { env, cwd, stdio: ['ignore', 'pipe', 'pipe'] });
  let stdout = '', stderr = '';
  child.stdout.on('data', (chunk) => { stdout += chunk; });
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const [code] = await once(child, 'close');
  assert.equal(code, 0, stderr);
  return stdout;
}

test('npm command: fresh auth to 525-record push, refresh, stage isolation, SDK auth and commit hook', { timeout: 120_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-cloud-'));
  const saved = { ...process.env };
  const bodies: any[] = [];
  let refreshes = 0;
  let rejectIngest = false;
  let acceptToken = 'rth_at_first';
  const server = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk;
    const data = body ? JSON.parse(body) : {};
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/v1/cli/login') {
      assert.equal(data.mode, 'sync');
      res.end(JSON.stringify({ accessToken: 'rth_at_first', refreshToken: 'rth_rt_first' }));
    } else if (req.url === '/v1/auth/token/refresh') {
      refreshes++;
      assert.equal(data.refreshToken, 'rth_rt_first');
      res.end(JSON.stringify({ accessToken: acceptToken, refreshToken: 'rth_rt_rotated' }));
    } else if (req.headers.authorization !== `Bearer ${acceptToken}`) {
      res.statusCode = 401; res.end('{}');
    } else if (req.url?.endsWith('/v1/ingest')) {
      if (rejectIngest) { res.statusCode = 500; res.end('{}'); return; }
      bodies.push(data);
      res.end(JSON.stringify({ batchId: data.batchId, received: data.records.length, accepted: data.records.length, cursors: data.cursors }));
    } else { res.end(JSON.stringify({ accepted: data.turns?.length ?? 0 })); }
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const address = server.address() as { port: number };
  const baseUrl = `http://127.0.0.1:${address.port}`;
  const dbPath = join(root, 'history.db');
  try {
    // The broker can inject a shared hooksPath using command-scope Git config.
    // Test repositories must not inherit or mutate that shared hook directory.
    for (const key of Object.keys(process.env)) {
      if (key.startsWith('GIT_CONFIG_') || ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR'].includes(key)) delete process.env[key];
    }
    process.env.GIT_CONFIG_NOSYSTEM = '1';
    process.env.HOME = root;
    process.env.USERPROFILE = root;
    process.env.RELAYHISTORY_HOME = join(root, 'auth');
    process.env.AI_HIST_CONFIG_DIR = join(root, 'old-sdk');
    process.env.CLAUDE_CONFIG_DIR = join(root, '.claude');
    delete process.env.CODEX_HOME;
    process.env.AI_HIST_DB = dbPath;
    process.env.RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL = '1';
    delete process.env.RELAYHISTORY_BASE_URL; delete process.env.AI_HIST_BASE_URL;
    delete process.env.CLOUD_API_ACCESS_TOKEN;
    const fixture = join(root, 'agent-relay');
    // Exercise the real Rust session -> device-login -> session subprocess path.
    await writeFile(fixture, `#!/bin/sh\nif [ "$2" = login ]; then touch '${root}/logged-in'; exit 0; fi\nif [ ! -f '${root}/logged-in' ]; then exit 1; fi\necho '{"accessToken":"fixture-cloud-token"}'\n`, { mode: 0o755 });
    process.env.AGENT_RELAY_BIN = fixture;
    const transcripts = join(root, '.claude', 'projects', 'fixture');
    await mkdir(transcripts, { recursive: true });
    await writeFile(join(transcripts, 'session-a.jsonl'), Array.from({ length: 525 }, (_, i) => JSON.stringify({ type: 'user', uuid: `u-${i}`, sessionId: 'session-a', timestamp: new Date(1_783_000_000_000 + i * 1000).toISOString(), message: { role: 'user', content: `synthetic cloud prompt ${i}` } })).join('\n'));
    const cli = new URL('./cli.js', import.meta.url).pathname;
    const stdout = await run(process.execPath, [cli, 'enable-cloud', '--base-url', baseUrl, '--db', dbPath, '--once', '--json'], process.env);
    assert.equal(JSON.parse(stdout).sent, 525);
    const prompts = bodies.flatMap((body) => body.records).map((row) => row.content);
    assert.equal(new Set(prompts).size, 525);
    for (let i = 0; i < 525; i++) assert(prompts.some((text) => text.includes(`synthetic cloud prompt ${i}`)));
    const auth = await loadStoredRelayhistoryAuth(baseUrl);
    assert.equal(auth?.accessToken, 'rth_at_first');
    assert.equal(bodies.length, 2);
    await mkdir(process.env.AI_HIST_CONFIG_DIR!, { recursive: true });
    await writeFile(join(process.env.AI_HIST_CONFIG_DIR!, 'auth.json'), JSON.stringify({ baseUrl, accessToken: 'stale-sdk-token' }));
    assert.equal((await loadStoredRelayhistoryAuth(baseUrl))?.accessToken, 'rth_at_first');

    const repo = join(root, 'repo'); await mkdir(repo);
    await run('git', ['init', '-q'], process.env, repo);
    await run('git', ['config', 'user.email', 'test@example.com'], process.env, repo);
    await run('git', ['config', 'user.name', 'SDK test'], process.env, repo);
    await writeFile(join(repo, '.git', 'hooks', 'post-commit'), '#!/bin/sh\necho old-hook > old-hook-ran\nexit 0\n', { mode: 0o755 });
    await run('git', ['config', 'ai-hist.pr-url', 'https://github.com/AgentWorkforce/relayhistory/pull/123'], process.env, repo);
    const hook = await installGitHooks({ repo, sessionId: 'session-a', source: 'claude', dbPath });
    assert.equal(hook.prUrl, 'https://github.com/AgentWorkforce/relayhistory/pull/123');
    assert.match(await readFile(hook.hookPath, 'utf8'), /post-commit.before-ai-hist/);
    assert.equal((await stat(hook.hookPath)).mode & 0o111, 0o111);
    await writeFile(join(repo, 'file.txt'), 'fixture');
    await run('git', ['add', 'file.txt'], process.env, repo);
    await run('git', ['commit', '-qm', 'fixture commit'], process.env, repo);
    assert.equal((await readFile(join(repo, 'old-hook-ran'), 'utf8')).trim(), 'old-hook');
    assert.match(await run('git', ['notes', '--ref=ai-hist', 'show', 'HEAD'], process.env, repo), /ai-hist:claude:session-a/);
    acceptToken = 'rth_at_rotated';
    await pushCloud({ dbPath, baseUrl });
    assert.equal(refreshes, 1);
    assert.equal((await loadStoredRelayhistoryAuth(baseUrl))?.accessToken, acceptToken);
    assert(bodies.flatMap((body) => body.records).some((row) => row.lens === 'github' && row.taskRef?.id === 'AgentWorkforce/relayhistory#123'));
    const files = await readdir(join(root, 'auth', 'stages'));
    const cursorFile = files.find((file) => file.endsWith('.cursor.json'))!;
    const before = await readFile(join(root, 'auth', 'stages', cursorFile), 'utf8');
    const failingBase = `${baseUrl}/second-stage`;
    // Seed a second stage through the legacy SDK migration; it must not borrow stage one's cursor.
    await writeFile(join(process.env.AI_HIST_CONFIG_DIR!, 'auth.json'), JSON.stringify({ baseUrl: failingBase, accessToken: acceptToken }));
    await loadStoredRelayhistoryAuth(failingBase);
    await assert.rejects(loadStoredRelayhistoryAuth(), /Refusing to guess/);
    rejectIngest = true;
    // Change the fixture server to reject any ingest path for the second destination.
    await assert.rejects(enableCloud({ dbPath, baseUrl: failingBase, watch: false }));
    assert.equal(await readFile(join(root, 'auth', 'stages', cursorFile), 'utf8'), before);
    const authFiles = (await readdir(join(root, 'auth', 'stages'))).filter((file) => file.endsWith('.auth.json'));
    for (const file of authFiles) assert.equal((await stat(join(root, 'auth', 'stages', file))).mode & 0o777, 0o600);
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
    server.closeAllConnections(); await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(root, { recursive: true, force: true });
  }
});
