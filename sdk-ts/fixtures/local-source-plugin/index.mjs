// A fake out-of-tree *local* harness reader, using only the public source
// plugin contract. It models a host application that keeps Claude sessions in
// its own directory: one `<session>.jsonl` per session, one
// `{"role","text","ts"}` object per line.
//
// It deliberately has no SQLite/native/private-core import: everything it
// knows reaches the catalog through the normalized evidence it returns.
import { readdir, readFile, stat } from 'node:fs/promises';
import { join } from 'node:path';

async function readSession(root, sessionId) {
  const path = join(root, `${sessionId}.jsonl`);
  const text = await readFile(path, 'utf8');
  const lines = text
    .split('\n')
    .filter((line) => line.trim())
    .map((line) => JSON.parse(line));
  return { path, text, lines };
}

export function createHistoryPlugin(options) {
  const root = options.root;
  return {
    sources: [
      {
        id: 'fixture-local',
        instanceId: options.instanceId ?? 'default',
        location: 'local',
        roots: [root],
        supportedSources: ['claude'],
        async discover({ sessionId } = {}) {
          const names = (await readdir(root)).filter((name) => name.endsWith('.jsonl'));
          const observations = [];
          for (const name of names) {
            const id = name.slice(0, -'.jsonl'.length);
            if (sessionId && id !== sessionId) continue;
            const path = join(root, name);
            const info = await stat(path);
            observations.push({
              source: 'claude',
              session_id: id,
              raw_path: path,
              source_stamp: `${info.size}:${Math.trunc(info.mtimeMs)}`,
            });
          }
          return { observations };
        },
        async hydrate(observation) {
          const sessionId = observation.key.session_id;
          const { text, lines } = await readSession(root, sessionId);
          const records = [];
          lines.forEach((line, index) => {
            records.push({
              kind: 'session_event',
              payload: {
                source: 'claude',
                session_id: sessionId,
                event_uid: `${sessionId}:${index}`,
                message_id: `${sessionId}:${index}`,
                role: line.role,
                kind: 'text',
                ts_ms: line.ts,
                text: line.text,
              },
            });
            if (line.role === 'user') {
              records.push({
                kind: 'history',
                payload: {
                  source: 'claude',
                  session_id: sessionId,
                  prompt: line.text,
                  timestamp_ms: line.ts,
                },
              });
            }
          });
          return {
            source_stamp: observation.source_stamp ?? 'unstamped',
            source_bytes: Buffer.byteLength(text),
            covered_kinds: ['history', 'session_event'],
            records,
          };
        },
      },
    ],
  };
}
