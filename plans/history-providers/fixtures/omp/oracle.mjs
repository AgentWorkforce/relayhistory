// Offline research oracle: execute checksum-pinned upstream pure reader/writer helpers.
// Usage: bun oracle.mjs /path/to/pinned/oh-my-pi [--write]
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
const dir = import.meta.dirname;
const root = process.argv[2];
if (!root) throw new Error('Pass an upstream checkout; never reads operator history');
const manifest = JSON.parse(fs.readFileSync(path.join(dir, 'provenance.json')));
function source(relative) {
  const text = fs.readFileSync(path.join(root, relative), 'utf8');
  assert.equal(crypto.createHash('sha256').update(text).digest('hex'), manifest.upstream_files[relative]);
  return text;
}
const transpiler = new Bun.Transpiler({loader:'ts'});
function compile(text, names, context={}) {
  const js = transpiler.transformSync(text).replace(/export /g, '');
  return Function(...Object.keys(context), `${js}\nreturn {${names.join(',')}};`)(...Object.values(context));
}
const titleSource = source('packages/coding-agent/src/session/session-title-slot.ts').replace(/^import[\s\S]*?from "\.\/session-entries";\s*/, '');
const constants = source('packages/coding-agent/src/session/session-entries.ts');
const title = compile(constants.match(/export const SESSION_TITLE_SLOT_BYTES = .*;/)[0]+'\n'+constants.match(/export const SESSION_TITLE_SLOT_ENTRY_TYPE = .*;/)[0]+'\n'+titleSource, ['serializeTitleSlot','parseTitleSlotFromContent','titleUpdateFromSlot','overlayTitleSlotContent']);
const stream = source('packages/utils/src/stream.ts');
assert.equal(typeof Bun.JSONL?.parseChunk, 'function', 'Upstream reader requires supported Bun JSONL runtime');
const jsonl = compile(stream.slice(stream.indexOf('export function parseJsonlLenient<')), ['parseJsonlLenient']);
const loader = source('packages/coding-agent/src/session/session-loader.ts');
const reader = compile(loader.slice(loader.indexOf('function splitTitleSlot('), loader.indexOf('/** Upper bound on the bytes read')), ['parseSessionContent'], {...title,...jsonl});
const blobSource = source('packages/coding-agent/src/session/blob-store.ts');
const blob = compile(blobSource.match(/const BLOB_PREFIX = .*;/)[0]+'\n'+blobSource.match(/export const BLOB_HASH_RE = .*;/)[0]+'\n'+blobSource.slice(blobSource.indexOf('export function parseBlobRef('), blobSource.indexOf('/** Identify provider transport image')), ['parseBlobRef'], {logger:{warn(){}}});
const results={};
for(const name of ['ordinary','title-rewrite','fork','rewrite','partial-tail']) {
  const text=fs.readFileSync(path.join(dir,`${name}.jsonl`),'utf8');
  const parsed=reader.parseSessionContent(text);
  results[name]={id:parsed.entries[0]?.id,title:parsed.entries[0]?.title,ids:parsed.entries.slice(1).map(e=>e.id),malformedRecords:parsed.malformedRecords,invalidHeader:parsed.invalidHeader,sourceSize:parsed.sourceSize};
}
assert.equal(results.ordinary.title,'Synthetic initial title');
assert.equal(results['title-rewrite'].title,'Synthetic changed title');
assert.equal(results.ordinary.sourceSize,results['title-rewrite'].sourceSize);
assert.equal(results['partial-tail'].malformedRecords,1);
assert.deepEqual(results.fork.ids,results.ordinary.ids);
assert.notEqual(results.fork.id,results.ordinary.id);
assert.deepEqual(results.rewrite.ids,['00000001']);
assert.equal(blob.parseBlobRef('blob:sha256:../../outside'),null);
assert.equal(blob.parseBlobRef('blob:sha256:'+'A'.repeat(64)),null);
const hash=manifest.blob_sha256;
assert.equal(blob.parseBlobRef(`blob:sha256:${hash}`),hash);
assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(dir,'blobs',hash))).digest('hex'),hash);
if(process.argv.includes('--write')) fs.writeFileSync(path.join(dir,'oracle-results.json'),JSON.stringify(results,null,2)+'\n');
else assert.deepEqual(results,JSON.parse(fs.readFileSync(path.join(dir,'oracle-results.json'))));
console.log('5 synthetic sessions match pinned official parseSessionContent; title/blob probes pass');
