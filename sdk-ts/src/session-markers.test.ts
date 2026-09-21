import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  CATALOG_SOURCES, FULL_SESSION_KINDS, InvalidArgumentError,
  SESSION_EVIDENCE_CONTRACT_VERSION, SESSION_HYDRATION_CONTRACT_VERSION,
  SESSION_RELATIONSHIP_CONTRACT_VERSION,
  getSessionMarkers, getSessionMarkersPage, getSessionRelationships, getSourceCapabilities,
  sessionMarkers, sync,
  type SessionMarker,
} from './index.js';

const SESSION = 'markers-1';

/**
 * A Claude transcript carrying the records the event model cannot hold: a
 * compaction boundary (dated, from its `system` row) and a `summary` rollup,
 * which Claude writes without a timestamp — the undated tail the marker
 * keyset exists for.
 */
const CLAUDE_TRANSCRIPT = [
  { type: 'summary', summary: 'What we did before', leafUuid: 'leaf-1' },
  { type: 'user', uuid: 'u1', sessionId: SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:00.000Z', message: { role: 'user', content: 'keep going' } },
  { type: 'system', subtype: 'compact_boundary', uuid: 's1', parentUuid: 'u1', sessionId: SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:01.000Z', compactMetadata: { trigger: 'auto', preTokens: 120000 } },
  { type: 'assistant', uuid: 'a1', parentUuid: 's1', sessionId: SESSION, cwd: '/work/app', timestamp: '2026-08-30T10:00:02.000Z', message: { role: 'assistant', model: 'claude-test', content: [{ type: 'text', text: 'Continuing.' }] } },
];

async function seededDatabase(): Promise<{ dbPath: string; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-markers-'));
  const home = join(root, 'home');
  const claude = join(home, '.claude', 'projects', 'work-app');
  await mkdir(claude, { recursive: true });
  await writeFile(join(claude, `${SESSION}.jsonl`), `${CLAUDE_TRANSCRIPT.map((line) => JSON.stringify(line)).join('\n')}\n`);
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = home;
  process.env.USERPROFILE = home;
  const dbPath = join(root, 'history.db');
  try {
    await sync({ dbPath });
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
  }
  return { dbPath, cleanup: () => rm(root, { recursive: true, force: true }) };
}

test('marker pages carry classified kinds, provider subkinds and bounded payloads', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const markers = await getSessionMarkers('claude', SESSION, { dbPath });
    const byKind = new Map(markers.map((marker) => [marker.kind, marker]));
    const compaction = byKind.get('compaction_boundary');
    assert.ok(compaction, `a compaction boundary is recorded: ${markers.map((m) => m.kind).join(', ')}`);
    assert.equal(compaction.source, 'claude');
    assert.equal(compaction.sessionId, SESSION);
    assert.equal(compaction.subkind, 'compact_boundary');
    assert.equal(compaction.tsMs, Date.parse('2026-08-30T10:00:01.000Z'));
    assert.equal(compaction.messageId, 's1');
    // The payload arrives parsed and as the stored string, like tool-call
    // arguments: the SDK owns parsing so one unreadable row cannot fail a page.
    assert.equal(typeof compaction.payloadJson, 'string');
    assert.deepEqual(
      compaction.payload && typeof compaction.payload === 'object' && !Array.isArray(compaction.payload)
        ? { trigger: compaction.payload.trigger, pre_tokens: compaction.payload.pre_tokens }
        : null,
      { trigger: 'auto', pre_tokens: 120000 },
    );

    const summary = byKind.get('summary');
    assert.ok(summary, 'a summary rollup is recorded');
    assert.equal(summary.tsMs, null, 'Claude writes summaries without a timestamp');
    assert.equal(summary.subkind, 'summary');
    assert.match(summary.payloadJson ?? '', /What we did before/);

    // Every marker is identified deterministically so a re-sync updates in place.
    assert.ok(markers.every((marker) => marker.markerUid.length > 0));
    // Dated markers page before undated ones.
    const undatedIndex = markers.findIndex((marker) => marker.tsMs === null);
    assert.ok(markers.slice(undatedIndex).every((marker) => marker.tsMs === null));
  } finally {
    await cleanup();
  }
});

test('paged, iterated and collected marker reads agree, through the undated tail', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const all = await getSessionMarkers('claude', SESSION, { dbPath });
    assert.ok(all.length >= 2, 'the transcript records at least two markers');

    const paged: SessionMarker[] = [];
    let page = await getSessionMarkersPage('claude', SESSION, { dbPath, limit: 1 });
    assert.equal(page.contractVersion, SESSION_EVIDENCE_CONTRACT_VERSION);
    assert.equal(page.source, 'claude');
    assert.equal(page.sessionId, SESSION);
    paged.push(...page.markers);
    while (page.nextCursor) {
      // The emitted cursor goes straight back in, including one whose `tsMs`
      // is null — the undated tail — which the JSON boundary carries intact.
      page = await getSessionMarkersPage('claude', SESSION, { dbPath, limit: 1, after: page.nextCursor });
      paged.push(...page.markers);
    }
    assert.deepEqual(paged.map((marker) => marker.id), all.map((marker) => marker.id));

    const iterated: number[] = [];
    for await (const marker of sessionMarkers('claude', SESSION, { dbPath, limit: 1 })) iterated.push(marker.id);
    assert.deepEqual(iterated, all.map((marker) => marker.id));
  } finally {
    await cleanup();
  }
});

test('marker pages read unknown sessions empty and require a valid identity', async () => {
  const { dbPath, cleanup } = await seededDatabase();
  try {
    const missing = await getSessionMarkersPage('codex', SESSION, { dbPath });
    assert.deepEqual(missing.markers, []);
    assert.equal(missing.nextCursor, null);
    // A database that does not exist is an empty page, not an error.
    const absent = await getSessionMarkersPage('claude', SESSION, { dbPath: join(dbPath, '..', 'absent.db') });
    assert.deepEqual(absent.markers, []);

    await assert.rejects(
      getSessionMarkersPage('claude', '', { dbPath }),
      (error: unknown) => error instanceof InvalidArgumentError,
    );
    await assert.rejects(
      getSessionMarkersPage('gemini' as never, SESSION, { dbPath }),
      (error: unknown) => error instanceof InvalidArgumentError,
    );
    await assert.rejects(
      getSessionMarkersPage('claude', ` ${SESSION} `, { dbPath }),
      (error: unknown) => error instanceof InvalidArgumentError && /padded/.test(error.message),
    );
    await assert.rejects(
      getSessionMarkersPage('claude', SESSION, { dbPath, limit: 0 }),
      (error: unknown) => error instanceof InvalidArgumentError,
    );
  } finally {
    await cleanup();
  }
});

test('source capabilities answer from the provider tables, before any database exists', async () => {
  const claude = await getSourceCapabilities('claude');
  assert.equal(claude.hydrationContractVersion, SESSION_HYDRATION_CONTRACT_VERSION);
  assert.equal(claude.relationshipContractVersion, SESSION_RELATIONSHIP_CONTRACT_VERSION);
  assert.equal(claude.source, 'claude');
  assert.equal(claude.fullCoverage, true);
  assert.deepEqual(claude.missingEvidenceKinds, []);
  assert.deepEqual([...claude.evidenceKinds].sort(), [...FULL_SESSION_KINDS].sort());
  // The same delegation table `getSessionRelationships` reports, so a consumer
  // can ask before it has a session to ask about.
  const relationships = await getSessionRelationships({ source: 'claude', sessionId: 'never-indexed', dbPath: '/nonexistent/relayhistory/history.db' });
  assert.deepEqual(claude.relationships, relationships.capabilities);

  const cursor = await getSourceCapabilities('cursor');
  assert.equal(cursor.fullCoverage, false);
  assert.ok(cursor.missingEvidenceKinds.length > 0);
  assert.equal(cursor.relationships.stableChildIdentity, 'never');
  for (const source of CATALOG_SOURCES) {
    const capabilities = await getSourceCapabilities(source);
    assert.equal(capabilities.fullCoverage, capabilities.missingEvidenceKinds.length === 0, source);
  }

  await assert.rejects(
    getSourceCapabilities('trajectory' as never),
    (error: unknown) => error instanceof InvalidArgumentError,
  );
});
