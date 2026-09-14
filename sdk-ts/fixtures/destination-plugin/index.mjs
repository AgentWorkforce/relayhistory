// A fake receiver, using only the public SDK contract and its own output file.
// It deliberately has no SQLite/native/private-core import.
import { readFile, writeFile } from 'node:fs/promises';
import { HistoryDeliveryError } from 'ai-hist';

export function createHistoryPlugin(options) {
  return { destinations: [{ instanceId: options.instanceId, destination: {
    id: 'fixture', mappingVersion: '1', idempotency: 'revision', orderedRevisions: true,
    supportedKinds: ['history'], supportsTombstones: true,
    async prepare(batch) {
      return { content_type: 'application/json', body: JSON.stringify(batch.records) };
    },
    async send(payload, { batch }) {
      // Simulated authenticated account must match the durable job's tenant.
      if (options.accountId !== batch.account_id) throw new HistoryDeliveryError('permission_denied');
      let receiver = { records: {}, latest: {}, attempts: [] };
      try { receiver = JSON.parse(await readFile(options.receiverPath, 'utf8')); }
      catch (error) { if (error.code !== 'ENOENT') throw error; }
      receiver.attempts.push({ batchId: batch.batch_id, body: payload.body, sha256: payload.sha256 });
      for (const record of JSON.parse(payload.body)) {
        const previous = receiver.records[record.revision_id];
        if (previous && JSON.stringify(previous) !== JSON.stringify(record)) throw new HistoryDeliveryError('invalid_payload');
        receiver.records[record.revision_id] = record;
        const identity = JSON.stringify([record.origin_id, record.record_id]);
        const latest = receiver.latest[identity];
        if (!latest || latest.revision < record.revision) receiver.latest[identity] = record;
        else if (latest.revision === record.revision && latest.revision_id !== record.revision_id) throw new HistoryDeliveryError('invalid_payload');
      }
      await writeFile(options.receiverPath, JSON.stringify(receiver));
      // Simulate process termination after durable receiver write but before
      // returning an acknowledgment. A second process must redeliver safely.
      if (options.crashAfterAcceptance) process.exit(73);
      return { batch_id: batch.batch_id, accepted_revision_ids: batch.records.map((record) => record.revision_id),
        unsupported_revision_ids: [], acceptance_level: 'durable' };
    },
  } }] };
}
