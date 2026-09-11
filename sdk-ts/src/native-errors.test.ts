import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  DatabaseOpenError, InvalidArgumentError, NATIVE_CONTRACT_VERSION, NativeContractMismatchError,
  SessionNotFoundError,
  UnsupportedOperationError, discoverSessions, getSessionFileEditsPage, getSessionToolCallsPage,
  hydrateSession, listSessionCatalogPage, recent, stats, sync,
  validateNativeContract, validateNativeLocation, validateNativeScope,
} from './index.js';

test('missing database reads are explicit empty cache operations', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-missing-db-'));
  const dbPath = join(root, 'missing', 'history.db');
  try {
    assert.deepEqual(await listSessionCatalogPage({ dbPath, limit: 20 }), {
      contractVersion: 3, scope: 'local', sessions: [], nextCursor: null,
    });
    assert.deepEqual(await getSessionToolCallsPage('claude', 'missing', { dbPath }), {
      contractVersion: 1, source: 'claude', sessionId: 'missing', toolCalls: [], nextCursor: null,
    });
    assert.deepEqual(await getSessionFileEditsPage('claude', 'missing', { dbPath }), {
      contractVersion: 1, source: 'claude', sessionId: 'missing', fileEdits: [], nextCursor: null,
    });
    assert.deepEqual(await recent({ dbPath, limit: 20 }), []);
    assert.deepEqual(await stats({ dbPath }), {
      scope: 'local', total: 0, bySource: {}, byProject: [], firstTimestampMs: null, lastTimestampMs: null,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('targeted hydration requires an existing catalog row', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-hydrate-missing-'));
  try {
    await assert.rejects(
      hydrateSession({ source: 'claude', sessionId: 'missing', dbPath: join(root, 'history.db') }),
      (error: unknown) => error instanceof SessionNotFoundError
        && error.code === 'SESSION_NOT_FOUND'
        && /discoverSessions/.test(error.message),
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('SDK/native contract mismatch is actionable', () => {
  assert.throws(
    () => validateNativeContract(999),
    (error: unknown) => error instanceof NativeContractMismatchError
      && error.code === 'NATIVE_CONTRACT_MISMATCH'
      && error.message.includes(`requires native contract ${NATIVE_CONTRACT_VERSION}`),
  );
  assert.throws(
    () => validateNativeScope('cloud'),
    (error: unknown) => error instanceof NativeContractMismatchError
      && error.code === 'NATIVE_CONTRACT_MISMATCH'
      && /invalid session scope/.test(error.message),
  );
  assert.equal(validateNativeLocation('remote'), 'remote');
  assert.throws(
    () => validateNativeLocation('cloud'),
    (error: unknown) => error instanceof NativeContractMismatchError
      && error.code === 'NATIVE_CONTRACT_MISMATCH'
      && /invalid session location/.test(error.message),
  );
});

test('unconfigured remote acquisition has one stable SDK error', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-unsupported-remote-'));
  const dbPath = join(root, 'history.db');
  // Connector detection reads the provider CLIs' stored sign-ins under HOME,
  // so point it at an empty home rather than the machine running the tests.
  const saved = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  process.env.HOME = root;
  process.env.USERPROFILE = root;
  try {
    for (const operation of [
      () => discoverSessions({ dbPath, scope: 'remote' }),
      () => sync({ dbPath, scope: 'remote' }),
    ]) {
      await assert.rejects(
        operation(),
        (error: unknown) => error instanceof UnsupportedOperationError
          && error.code === 'UNSUPPORTED_OPERATION'
          && error.message.includes('no remote provider connectors are configured'),
      );
    }
  } finally {
    if (saved.HOME === undefined) delete process.env.HOME; else process.env.HOME = saved.HOME;
    if (saved.USERPROFILE === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = saved.USERPROFILE;
    await rm(root, { recursive: true, force: true });
  }
});

test('evidence pages reject out-of-range limits at the native boundary', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-evidence-limit-'));
  const dbPath = join(root, 'history.db');
  try {
    for (const operation of [
      () => getSessionToolCallsPage('claude', 'any', { dbPath, limit: 0 }),
      () => getSessionFileEditsPage('claude', 'any', { dbPath, limit: 5_000 }),
    ]) {
      await assert.rejects(
        operation(),
        (error: unknown) => error instanceof InvalidArgumentError
          && error.code === 'INVALID_ARGUMENT'
          && /limit must be between 1 and 1000/.test(error.message),
      );
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('evidence pages reject a padded identity instead of answering it empty', async () => {
  const root = await mkdtemp(join(tmpdir(), 'relayhistory-evidence-identity-'));
  const dbPath = join(root, 'history.db');
  try {
    // The page matches an identity exactly, so a padded one would silently
    // read as "no such session" rather than as the caller's mistake.
    for (const operation of [
      () => getSessionToolCallsPage('claude', ' sess-1 ', { dbPath }),
      () => getSessionFileEditsPage(' claude ' as never, 'sess-1', { dbPath }),
    ]) {
      await assert.rejects(
        operation(),
        (error: unknown) => error instanceof InvalidArgumentError
          && error.code === 'INVALID_ARGUMENT'
          && /must not be padded with whitespace/.test(error.message),
      );
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('database open failures use the stable SDK error', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'relayhistory-not-a-db-'));
  try {
    for (const operation of [
      () => recent({ dbPath: directory }),
      () => listSessionCatalogPage({ dbPath: directory }),
    ]) {
      await assert.rejects(
        operation(),
        (error: unknown) => error instanceof DatabaseOpenError
          && error.code === 'DATABASE_OPEN_FAILED'
          && error.message.includes(directory),
      );
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
