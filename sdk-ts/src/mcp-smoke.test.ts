import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { openAiHist } from './index.js';

test('SDK fallback ingests compacted per-run trajectories from TRAJECTORY_ROOT', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-trajectory-'));
  const compacted = join(root, 'planner', 'compacted');
  await mkdir(compacted, { recursive: true });
  await writeFile(
    join(compacted, 'run-1.json'),
    JSON.stringify({
      id: 'run-1',
      version: 1,
      personaId: 'planner',
      projectId: 'agent-workforce',
      task: { title: 'Latency budget', description: 'Choose retry behavior for API calls.' },
      status: 'completed',
      startedAt: '2026-06-06T10:00:00.000Z',
      completedAt: '2026-06-06T10:05:00.000Z',
      decisions: [
        {
          question: 'How should retries behave?',
          chosen: 'Use capped exponential backoff',
          reasoning: 'It protects downstream services while preserving UX.',
          alternatives: ['fixed delay', 'no retry'],
        },
      ],
      retrospective: {
        summary: 'Retry policy selected.',
        approach: 'Compared failure modes and downstream pressure.',
        learnings: ['Bound retries by elapsed time.'],
        confidence: 0.82,
      },
    }),
  );

  const previousRoot = process.env.TRAJECTORY_ROOT;
  const previousDb = process.env.AI_HIST_DB;
  process.env.TRAJECTORY_ROOT = root;
  process.env.AI_HIST_DB = join(root, 'missing.db');
  const hist = await openAiHist({ dbPath: join(root, 'missing.db') });
  try {
    const results = hist.searchTrajectories('exponential backoff', { limit: 5 });
    assert.equal(results.length, 1);
    assert.equal(results[0].id, 'run-1');
    assert.equal(results[0].decisions[0].chosen, 'Use capped exponential backoff');
    assert.equal(hist.whyForTask('retry policy')?.retrospective.summary, 'Retry policy selected.');
    assert.ok(hist.search('Latency budget', { source: 'trajectory', limit: 5 }).length >= 1);
  } finally {
    hist.close();
    if (previousRoot === undefined) delete process.env.TRAJECTORY_ROOT;
    else process.env.TRAJECTORY_ROOT = previousRoot;
    if (previousDb === undefined) delete process.env.AI_HIST_DB;
    else process.env.AI_HIST_DB = previousDb;
  }
});

test('MCP server exposes history and trajectory tools over stdio', async () => {
  const root = await mkdtemp(join(tmpdir(), 'ai-hist-mcp-'));
  const child = spawn(process.execPath, [new URL('./mcp-server.js', import.meta.url).pathname], {
    env: {
      ...process.env,
      AI_HIST_DB: join(root, 'missing.db'),
      TRAJECTORY_ROOT: root,
    },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  const stderr: Buffer[] = [];
  child.stderr.on('data', (chunk: Buffer) => stderr.push(chunk));

  try {
    const tools = await new Promise<Set<string>>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timed out waiting for MCP tools/list')), 5000);
      let buffer = '';

      child.stdout.on('data', (chunk: Buffer) => {
        buffer += chunk.toString('utf8');
        while (true) {
          const marker = buffer.indexOf('\n');
          if (marker === -1) return;
          const body = buffer.slice(0, marker).trim();
          buffer = buffer.slice(marker + 1);
          if (!body) continue;
          const message = JSON.parse(body) as {
            id?: number;
            result?: { tools?: Array<{ name: string }> };
            error?: unknown;
          };
          if (message.error) {
            clearTimeout(timer);
            reject(new Error(`MCP error: ${JSON.stringify(message.error)}`));
            return;
          }
          if (message.id === 2) {
            clearTimeout(timer);
            resolve(new Set((message.result?.tools ?? []).map((tool) => tool.name)));
            return;
          }
        }
      });

      child.on('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
      child.on('exit', (code) => {
        if (code !== null && code !== 0) {
          clearTimeout(timer);
          reject(new Error(`MCP server exited ${code}: ${Buffer.concat(stderr).toString('utf8')}`));
        }
      });

      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2025-06-18',
          capabilities: {},
          clientInfo: { name: 'ai-hist-smoke', version: '0.0.0' },
        },
      });
      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        method: 'notifications/initialized',
        params: {},
      });
      writeJsonRpc(child.stdin, {
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/list',
        params: {},
      });
    });

    for (const name of [
      'search_history',
      'recent_entries',
      'get_session',
      'get_context',
      'stats',
      'search_trajectories',
      'why_for_task',
    ]) {
      assert.ok(tools.has(name), `missing MCP tool ${name}`);
    }
  } finally {
    child.kill();
  }
});

function writeJsonRpc(stdin: NodeJS.WritableStream, payload: unknown): void {
  stdin.write(`${JSON.stringify(payload)}\n`);
}
