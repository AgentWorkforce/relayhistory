// Synthetic capture from the exact released global CLI transcript writer.
// Usage: node capture.mjs /path/to/unpacked/@qoder-ai/qodercli/package
import { createHash } from 'node:crypto';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { pathToFileURL } from 'node:url';

const packageDir = process.argv[2];
if (!packageDir) throw new Error('Pass the unpacked pinned package directory');
const bundle = join(packageDir, 'bundle/qodercli.js');
const source = await readFile(bundle, 'utf8');
const hash = createHash('sha256').update(source).digest('hex');
if (hash !== '1b5b17bb25bcd2e059a97e8bc1d4280e4c16fb4194b004463cd677c434536948') {
  throw new Error(`Unexpected Qoder CLI bundle hash: ${hash}`);
}
const entry = 'Hm(),FvA(async A=>{let{createCliFramework:e}=await Promise.resolve().then(()=>(vva(),Mva));return e(A)}),sGt();';
if (!source.endsWith(entry)) throw new Error('Pinned entrypoint changed');
// The published npm bundle has no library exports. Replace only its CLI launch
// statement in a temporary copy to expose the shipped writer and reader.
const probe = join(packageDir, 'bundle/qodercli-capture-probe.mjs');
await writeFile(probe, source.slice(0, -entry.length) + 'U7n();GG(); export const __Q={TranscriptStore:lre, Repository:kf, path:Ap};');
const { __Q } = await import(pathToFileURL(probe));
const destination = new URL('../../../..', import.meta.url);
const root = join(destination.pathname, 'crates/ai-hist/tests/fixtures/qoder/global-cli/.qoder');
const cwd = '/synthetic/qoder/project';
const cases = [
  { id: 'synthetic-ordinary', messages: [
    { type: 'user', uuid: 'ordinary-u1', message: { role: 'user', content: 'Explain the synthetic project.' } },
    { type: 'assistant', uuid: 'ordinary-a1', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'It contains sample files.' }] } },
  ] },
  { id: 'synthetic-tool', messages: [
    { type: 'user', uuid: 'tool-u1', message: { role: 'user', content: 'Read note.txt.' } },
    { type: 'assistant', uuid: 'tool-a1', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'Reading it.' }, { type: 'tool_use', id: 'call-1', name: 'Read', input: { file_path: 'note.txt' } }] } },
    { type: 'user', uuid: 'tool-r1', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'call-1', content: 'Synthetic note contents.' }] } },
    { type: 'assistant', uuid: 'tool-a2', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'The note is synthetic.' }] } },
  ] },
  { id: 'synthetic-same-ms', sameMs: true, messages: [
    { type: 'user', uuid: 'same-u1', message: { role: 'user', content: 'Read one.txt.' } },
    { type: 'assistant', uuid: 'same-a1', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'Reading it.' }, { type: 'tool_use', id: 'same-call', name: 'Read', input: { file_path: 'one.txt' } }] } },
    { type: 'user', uuid: 'same-r1', message: { role: 'user', content: [{ type: 'tool_result', tool_use_id: 'same-call', content: 'One synthetic line.' }] } },
    { type: 'assistant', uuid: 'same-a2', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'Found one line.' }] } },
  ] },
  { id: 'synthetic-resume', messages: [
    { type: 'user', uuid: 'resume-u1', message: { role: 'user', content: 'Start the sample.' } },
    { type: 'assistant', uuid: 'resume-a1', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'Started.' }] } },
  ], resumed: [
    { type: 'user', uuid: 'resume-u2', message: { role: 'user', content: 'Continue the sample.' } },
    { type: 'assistant', uuid: 'resume-a2', message: { role: 'assistant', model: 'synthetic-model', content: [{ type: 'text', text: 'Continued.' }] } },
  ] },
];
const store = new __Q.TranscriptStore();
const oracle = {};
async function record(writer, messages, sameMs, initialParent = null) {
  if (sameMs) return writer.recordMessages(messages, initialParent);
  let parent = initialParent;
  for (const message of messages) {
    await writer.recordMessages([message], parent);
    parent = message.uuid;
    await new Promise(resolve => setTimeout(resolve, 15));
  }
}
try {
  for (const example of cases) {
    const filePath = __Q.path(cwd, example.id, root);
    await mkdir(dirname(filePath), { recursive: true });
    const options = { filePath, sessionId: example.id, cwd, entrypoint: 'cli', version: '1.1.75' };
    let writer = await store.acquireSessionWriter(options);
    await record(writer, example.messages, example.sameMs);
    await writer.flush();
    await writer.close();
    if (example.resumed) {
      writer = await store.acquireSessionWriter(options);
      await record(writer, example.resumed, false, example.messages.at(-1).uuid);
      await writer.flush();
      await writer.close();
    }
    // Resolve with a fresh reader after writer close: independent of the
    // acquisition path implemented by RelayHistory.
    const reader = new __Q.Repository();
    const session = await reader.resolveSession(filePath, { sessionId: example.id, view: 'historical' });
    oracle[example.id] = session.messages.map(({ uuid, type, parentUuid, message }) => ({ uuid, type, parentUuid, message }));
  }
} finally {
  await store.close();
}
await writeFile(join(destination.pathname, 'plans/history-providers/fixtures/qoder/official-reader.json'), `${JSON.stringify(oracle, null, 2)}\n`);
console.log('Captured four synthetic global CLI sessions and restart-reader oracle.');
