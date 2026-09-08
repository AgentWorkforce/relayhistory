#!/usr/bin/env node
// Exercise an npm tarball, never the source CLI or a standalone ai-hist binary.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { resolveImage } from './first-search-image.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const evidenceRoot = resolve(process.argv[2] ?? join(root, 'tmp', 'first-search'));
const image = process.env.AI_HIST_TEST_IMAGE ?? 'node:22-trixie-slim';
const stage = mkdtempSync(join(tmpdir(), 'ai-hist-pack-'));
function run(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: 'utf8', ...options });
  if (result.status !== 0) throw new Error(`${command} failed: ${result.error ?? result.stderr ?? result.stdout}`);
  return result.stdout;
}
try {
  mkdirSync(evidenceRoot, { recursive: true });
  // Keep each attempt isolated, including failures and concurrent invocations.
  const evidence = mkdtempSync(join(evidenceRoot, 'run-'));
  process.stdout.write(`Evidence directory: ${evidence}\n`);
  const pkg = JSON.parse(readFileSync(join(root, 'sdk-ts/package.json'), 'utf8'));
  assert.equal(pkg.bin['ai-hist'], './dist/cli.js');
  pkg.dependencies['ai-hist-native'] = pkg.version;
  delete pkg.scripts.prepare;
  writeFileSync(join(stage, 'package.json'), JSON.stringify(pkg, null, 2));
  cpSync(join(root, 'sdk-ts/dist'), join(stage, 'dist'), { recursive: true });
  cpSync(join(root, 'sdk-ts/README.md'), join(stage, 'README.md'));
  const [packed] = JSON.parse(run('npm', ['pack', '--ignore-scripts', '--json'], { cwd: stage }));
  assert.ok(packed.files.some((file) => file.path === 'dist/cli.js'));
  assert.ok(readFileSync(join(stage, 'dist/cli.js'), 'utf8').startsWith('#!/usr/bin/env node'));
  cpSync(join(root, 'scripts/first-search-container.mjs'), join(stage, 'verify.mjs'));
  const digest = resolveImage(run, image);
  writeFileSync(join(evidence, 'artifact.json'), JSON.stringify({
    version: pkg.version, integrity: packed.integrity, image: digest,
    revision: run('git', ['rev-parse', 'HEAD'], { cwd: root }).trim(),
    timingScope: 'Fresh container with Node/npm; empty npm cache and DB. Includes npm dependencies download, bootstrap, first search. SDK tarball mounted locally; image pull and Node installation excluded.',
  }, null, 2) + '\n');
  for (const mode of ['before', 'after']) {
    process.stdout.write(run('docker', ['run', '--rm',
      '-v', `${stage}:/artifact:ro`, '-v', `${evidence}:/evidence`,
      '-e', `AI_HIST_TARBALL=/artifact/${packed.filename}`, image,
      'node', '/artifact/verify.mjs', mode], { timeout: 180_000 }));
  }
} finally {
  rmSync(stage, { recursive: true, force: true });
}
