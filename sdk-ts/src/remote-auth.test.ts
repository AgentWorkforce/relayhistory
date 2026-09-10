import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  RelayHistoryError, discoverSessions, hydrateSession, listSessionCatalogPage,
  recent, search, stats, sync,
} from './index.js';

test('remote scope fails when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-remote-auth-'));
  const dbPath = join(root, 'history.db');
  // Point to an empty home to ensure no stored authentication
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;
  
  try {
    // Test functions that support remote scope
    const operations = [
      () => stats({ dbPath, scope: 'remote' }),
      () => search('test', { dbPath, scope: 'remote' }),
      () => recent({ dbPath, scope: 'remote' }),
      () => sync({ dbPath, scope: 'remote' }),
      () => listSessionCatalogPage({ dbPath, scope: 'remote' }),
      () => discoverSessions({ dbPath, scope: 'remote' }),
      () => hydrateSession({ source: 'claude', sessionId: 'test', dbPath, scope: 'remote' }),
    ];
    
    for (const operation of operations) {
      await assert.rejects(
        operation(),
        (error: unknown) => error instanceof RelayHistoryError
          && error.code === 'CLOUD_AUTH_FAILED'
          && error.message.includes('not authenticated for remote scope')
          && error.message.includes('ai-hist login'),
        `Operation should fail with authentication error: ${operation.name}`,
      );
    }
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('all scope fails when unauthenticated', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-all-auth-'));
  const dbPath = join(root, 'history.db');
  // Point to an empty home to ensure no stored authentication
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;
  
  try {
    // Test functions that support all scope (which includes remote)
    const operations = [
      () => stats({ dbPath, scope: 'all' }),
      () => search('test', { dbPath, scope: 'all' }),
      () => recent({ dbPath, scope: 'all' }),
      () => sync({ dbPath, scope: 'all' }),
      () => listSessionCatalogPage({ dbPath, scope: 'all' }),
      () => discoverSessions({ dbPath, scope: 'all' }),
      () => hydrateSession({ source: 'claude', sessionId: 'test', dbPath, scope: 'all' }),
    ];
    
    for (const operation of operations) {
      await assert.rejects(
        operation(),
        (error: unknown) => error instanceof RelayHistoryError
          && error.code === 'CLOUD_AUTH_FAILED'
          && error.message.includes('not authenticated for remote scope')
          && error.message.includes('ai-hist login'),
        `Operation should fail with authentication error: ${operation.name}`,
      );
    }
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('local scope works without authentication', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-local-auth-'));
  const dbPath = join(root, 'history.db');
  // Point to an empty home to ensure no stored authentication
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;
  
  try {
    // These should all work without authentication when using local scope (default or explicit)
    const localStats = await stats({ dbPath, scope: 'local' });
    assert.equal(localStats.scope, 'local');
    assert.equal(localStats.total, 0);
    
    const defaultStats = await stats({ dbPath }); // Should default to local
    assert.equal(defaultStats.scope, 'local');
    assert.equal(defaultStats.total, 0);
    
    const searchResults = await search('test', { dbPath, scope: 'local' });
    assert.deepEqual(searchResults, []);
    
    const recentResults = await recent({ dbPath, scope: 'local' });
    assert.deepEqual(recentResults, []);
    
    const listResults = await listSessionCatalogPage({ dbPath, scope: 'local' });
    assert.equal(listResults.scope, 'local');
    assert.deepEqual(listResults.sessions, []);
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});