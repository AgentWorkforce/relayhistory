import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { DEFAULT_DELIVERY_LIMITS, controlHistoryDelivery as removedCoreControl } from 'ai-hist';
import { createHistoryDelivery, historyDeliveryStatus, controlHistoryDelivery, drainProbeDelivery, historyDeliveryRetention } from './delivery.js';

const binaryPath = process.env.RELAYHISTORY_PLUGIN_BIN;
test('probe helper owns persisted jobs and drains paused work without credentials', { skip: !binaryPath }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'probe-delivery-api-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const options = { binaryPath, dbPath: join(root, 'synthetic.db') };
  const job = await createHistoryDelivery({
    destination_id: 'relayhistory', instance_id: 'fixture', account_id: 'fixture-account', mapping_version: 'fixture-v1',
    selection: { all_sources: false, sources: ['claude'], sessions: [], kinds: ['history'], excluded_sessions: [] },
    limits: DEFAULT_DELIVERY_LIMITS,
  }, options);
  assert.equal(job.state, 'active');
  await controlHistoryDelivery(job.job_id, 'pause', options);
  await assert.rejects(removedCoreControl(job.job_id, 'resume', options), { code: 'HISTORY_DELIVERY_MOVED' });
  assert.equal((await historyDeliveryStatus(job.job_id, options))[0].state, 'paused');
  const drained = await drainProbeDelivery({ ...options, baseUrl: 'https://fixture.invalid',
    instanceId: 'fixture', expectedAccount: 'fixture-account', jobIds: [job.job_id] });
  assert.equal(drained.attempts, 0);
  assert.equal(drained.statuses[0].state, 'paused');
  assert.ok((await historyDeliveryRetention(options)).limitBytes > 0);
  await controlHistoryDelivery(job.job_id, 'cancel', options);
  assert.equal((await historyDeliveryStatus(job.job_id, options))[0].state, 'cancelled');
});
