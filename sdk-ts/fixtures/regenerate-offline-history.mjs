// Regenerate with Node >=22 after building the SDK/native addon:
// node sdk-ts/fixtures/regenerate-offline-history.mjs
// The checked-in gzip is readable by Node20; only regeneration uses node:sqlite.
import { DatabaseSync } from 'node:sqlite';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { sync } from '../dist/index.js';

const root = await mkdtemp(join(tmpdir(), 'offline-history-fixture-'));
const saved = { ...process.env };
// This generator must never discover the operator's history or credentials.
for (const key of Object.keys(process.env)) {
  if (/^(HOME|USERPROFILE|XDG_|OPENCODE_|TRAJECTORY_|AI_HIST_|RELAYHISTORY_|RELAYCAST_)/.test(key)) delete process.env[key];
}
process.env.HOME = root;
process.env.USERPROFILE = root;
process.env.RELAYHISTORY_HOME = join(root, 'commercial');
const dbPath = join(root, 'history.db');
try {
  const project = join(root, '.claude', 'projects', '-fixture-project');
  await mkdir(project, { recursive: true });
  for (const id of ['local-only', 'remote-only', 'both']) {
    await writeFile(join(project, `${id}.jsonl`), JSON.stringify({
      type: 'user', uuid: `${id}-message`, sessionId: id, cwd: '/fixture/project',
      timestamp: '2026-09-01T10:00:00.000Z',
      message: { role: 'user', content: `offlinefixture ${id}` },
    }) + '\n');
  }
  await sync({ dbPath, scope: 'local', sourceConnectors: [] });
  const db = new DatabaseSync(dbPath);
  try {
    db.exec(`
      UPDATE session_presences SET location='remote' WHERE session_id='remote-only';
      INSERT INTO session_presences (source, session_id, location, discovery_state)
        VALUES ('claude', 'both', 'remote', 'full');
    `);
    // Replace generated paths with synthetic stable paths, including ingestion
    // checkpoints. Ignore virtual FTS/shadow tables: their contents are prompts.
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND sql NOT LIKE 'CREATE VIRTUAL%' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%fts%'").all();
    for (const { name } of tables) {
      const quote = (value) => `"${value.replaceAll('"', '""')}"`;
      for (const column of db.prepare(`PRAGMA table_info(${quote(name)})`).all()) {
        if (!/TEXT/i.test(column.type)) continue;
        db.prepare(`UPDATE ${quote(name)} SET ${quote(column.name)}=replace(${quote(column.name)}, ?, ?) WHERE instr(${quote(column.name)}, ?) > 0`)
          .run(root, '/fixture/home', root);
      }
    }
    db.exec('PRAGMA wal_checkpoint(TRUNCATE); VACUUM;');
  } finally {
    db.close();
  }
  await writeFile(new URL('./offline-history.db.gz', import.meta.url), gzipSync(await readFile(dbPath), { level: 9 }));
} finally {
  for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
  Object.assign(process.env, saved);
  await rm(root, { recursive: true, force: true });
}
