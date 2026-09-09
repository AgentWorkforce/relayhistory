// Runs the built CLI, not the SDK: the defect these guard survived to release
// because bootstrap was wired into one entry point and asserted through the
// library, so no test ever typed the command a new user types first.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const cli = fileURLToPath(new URL('./cli.js', import.meta.url));

type Run = { code: number; stdout: string; stderr: string };

function run(args: string[], env: NodeJS.ProcessEnv): Promise<Run> {
  return new Promise((resolve) => {
    execFile(process.execPath, [cli, ...args], { env }, (error, stdout, stderr) => {
      const code = error ? (error as NodeJS.ErrnoException & { code?: number }).code ?? 1 : 0;
      resolve({ code: typeof code === 'number' ? code : 1, stdout, stderr });
    });
  });
}

async function emptyHome(): Promise<{ home: string; env: NodeJS.ProcessEnv }> {
  const home = await mkdtemp(join(tmpdir(), 'ai-hist-first-use-'));
  return {
    home,
    env: {
      ...process.env, HOME: home, USERPROFILE: home,
      XDG_DATA_HOME: join(home, '.local', 'share'), RELAYHISTORY_NO_UPDATE_CHECK: '1',
    },
  };
}

async function seedClaudeSession(home: string): Promise<void> {
  const folder = join(home, '.claude', 'projects', '-work-demo');
  await mkdir(folder, { recursive: true });
  await writeFile(join(folder, 'session.jsonl'), `${JSON.stringify({
    sessionId: '11111111-1111-1111-1111-555555555555', uuid: 'user-1', cwd: '/work/demo', type: 'user',
    message: { role: 'user', content: 'please fix the zzqqmarker parser bug' },
    timestamp: '2026-09-08T10:00:00Z',
  })}\n`);
}

// The reported defect verbatim: five searches, no database, `No results.` each time.
test('search is the first command typed and bootstraps like the bare invocation', async () => {
  const { home, env } = await emptyHome();
  try {
    await seedClaudeSession(home);
    for (let attempt = 0; attempt < 3; attempt++) {
      const found = await run(['search', 'zzqqmarker'], env);
      assert.equal(found.code, 0, found.stderr);
      assert.match(found.stdout, /zzqqmarker parser bug/);
    }
    assert.ok(existsSync(join(home, '.local', 'share', 'ai-hist', 'ai-history.db')));
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

test('every command that reads local history bootstraps, not only the bare one', async () => {
  for (const command of [['recent'], ['stats'], ['sessions', 'list'], ['pack', 'zzqqmarker'], ['resume', 'zzqqmarker']]) {
    const { home, env } = await emptyHome();
    try {
      await seedClaudeSession(home);
      const result = await run(command, env);
      assert.equal(result.code, 0, `${command.join(' ')}: ${result.stderr}`);
      assert.ok(existsSync(join(home, '.local', 'share', 'ai-hist', 'ai-history.db')), command.join(' '));
    } finally {
      await rm(home, { recursive: true, force: true });
    }
  }
});

// `No results.` is a claim about a store that was searched. A store that holds
// nothing at all has to say something else, and say it with a failing status.
test('an unindexed store and a store with no match are different answers', async () => {
  const { home, env } = await emptyHome();
  try {
    const bare = await run(['search', 'zzqqmarker'], env);
    assert.equal(bare.code, 1);
    assert.match(bare.stdout, /No searchable local sessions found/);
    assert.doesNotMatch(bare.stdout, /No results\./);

    const asJson = await run(['search', 'zzqqmarker', '--json'], env);
    assert.equal(asJson.code, 1);
    assert.deepEqual(JSON.parse(asJson.stdout).status, 'empty');

    await seedClaudeSession(home);
    const miss = await run(['search', 'nothingmatchesthis'], env);
    assert.equal(miss.code, 0, miss.stderr);
    assert.equal(miss.stdout, 'No results.\n');
    const hit = await run(['search', 'zzqqmarker'], env);
    assert.equal(hit.code, 0, hit.stderr);
    assert.match(hit.stdout, /zzqqmarker parser bug/);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

test('--no-bootstrap reports an unbuilt store instead of building or lying', async () => {
  const { home, env } = await emptyHome();
  try {
    await seedClaudeSession(home);
    const declined = await run(['search', 'zzqqmarker', '--no-bootstrap'], env);
    assert.equal(declined.code, 1);
    assert.match(declined.stdout, /No local index yet/);
    const built = await run(['search', 'zzqqmarker'], env);
    assert.equal(built.code, 0, built.stderr);
    // Once the store answers, --no-bootstrap answers from it unchanged.
    const reread = await run(['search', 'zzqqmarker', '--no-bootstrap'], env);
    assert.equal(reread.code, 0, reread.stderr);
    assert.match(reread.stdout, /zzqqmarker parser bug/);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

// The usage block is what tells a user bootstrap is the default. Whatever it
// documents as reading local history must actually accept the opt-out.
test('the built npm CLI ships login so token/replay errors name a real remedy', async () => {
  const shipped = await readFile(fileURLToPath(new URL('../dist/cli.js', import.meta.url)), 'utf8');
  assert.match(shipped, /['"]login['"]/);
  assert.doesNotMatch(shipped, /admin-mint/);
  const missingToken = await run(['login', '--token', 'fixture-token'], process.env);
  assert.equal(missingToken.code, 2);
  assert.match(missingToken.stderr, /login requires --base-url with --token/);
});

test('every documented local-history command accepts --no-bootstrap', async () => {
  const { home, env } = await emptyHome();
  try {
    await seedClaudeSession(home);
    await run(['sync'], env);
    for (const command of [[], ['search', 'zzqqmarker'], ['recent'], ['stats'], ['sessions', 'list'],
      ['session', '11111111-1111-1111-1111-555555555555'], ['pack', 'zzqqmarker'], ['resume', 'zzqqmarker']]) {
      const result = await run([...command, '--no-bootstrap'], env);
      assert.notEqual(result.code, 2, `${command.join(' ') || 'ai-hist'} rejected --no-bootstrap: ${result.stderr}`);
    }
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});
