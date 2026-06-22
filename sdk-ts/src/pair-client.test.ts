import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { formatPairWarnings, pairCheck } from './pair-client.js';

test('pairCheck shells to ai-hist pair check with locked v1 context args', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-pair-cli-'));
  const callsPath = join(root, 'calls.json');
  const binPath = join(root, 'ai-hist-mock.mjs');
  await writeFile(
    binPath,
    `#!/usr/bin/env node
import { writeFileSync } from 'node:fs';
writeFileSync(${JSON.stringify(callsPath)}, JSON.stringify(process.argv.slice(2)));
process.stdout.write(JSON.stringify({
  decision: 'warn',
  warnings: [{
    text: 'Update permissions config before editing auth middleware.',
    kind: 'reflection',
    lens: 'trajectories',
    score: 0.91,
    evidence: [{
      machineId: 'm_1',
      source: 'trajectory',
      sessionId: 'tA',
      kind: 'reflection',
      eventId: 'reflection:tA:suggestion:0',
      ts: '2026-06-21T20:00:00Z',
      snippet: 'update permissions config'
    }]
  }],
  correlationId: 'pair_test'
}));
`,
    { mode: 0o755 },
  );

  const previousBin = process.env.AI_HIST_PAIR_CHECK_BIN;
  process.env.AI_HIST_PAIR_CHECK_BIN = binPath;
  try {
    const result = await pairCheck({
      context: {
        projectId: 'proj-auth-svc',
        repoPath: '/work/auth',
        cwd: '/work/auth',
        gitRemote: 'git@github.com:org/auth.git',
        task: 'refactor auth middleware token check',
        files: ['src/auth/middleware.ts'],
        tool: 'Edit',
        target: 'src/auth/middleware.ts',
        action: 'edit',
        recentPrompt: 'short prompt summary',
      },
      limit: 5,
    });

    assert.equal(result.decision, 'warn');
    assert.equal(result.warnings[0].evidence?.[0].eventId, 'reflection:tA:suggestion:0');
    assert.deepEqual(JSON.parse(await readFile(callsPath, 'utf8')), [
      'pair',
      'check',
      '--json',
      '--project-id',
      'proj-auth-svc',
      '--repo-path',
      '/work/auth',
      '--cwd',
      '/work/auth',
      '--git-remote',
      'git@github.com:org/auth.git',
      '--task',
      'refactor auth middleware token check',
      '--tool',
      'Edit',
      '--target',
      'src/auth/middleware.ts',
      '--action',
      'edit',
      '--recent-prompt',
      'short prompt summary',
      '--file',
      'src/auth/middleware.ts',
      '--limit',
      '5',
    ]);
  } finally {
    if (previousBin === undefined) delete process.env.AI_HIST_PAIR_CHECK_BIN;
    else process.env.AI_HIST_PAIR_CHECK_BIN = previousBin;
  }
});

test('formatPairWarnings renders advisory evidence and no-ops on empty warnings', () => {
  assert.equal(formatPairWarnings({ decision: 'allow', warnings: [] }), '');
  const text = formatPairWarnings({
    decision: 'warn',
    warnings: [
      {
        text: 'Prior finding applies here.',
        kind: 'finding',
        lens: 'trajectories',
        score: 0.875,
        evidence: [
          {
            machineId: 'm_1',
            source: 'trajectory',
            sessionId: 'tA',
            eventId: 'finding:tA:learning:0',
            ts: '2026-06-21T20:00:00Z',
            snippet: 'rth_at hash lookup bypasses JWT verify',
          },
        ],
      },
    ],
    correlationId: 'pair_1',
  });
  assert.match(text, /Pair advisory warnings/);
  assert.match(text, /\[finding\/trajectories score=0\.88\]/);
  assert.match(text, /m_1:trajectory:tA:finding:tA:learning:0/);
  assert.match(text, /correlationId: pair_1/);
});
