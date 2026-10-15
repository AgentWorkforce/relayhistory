// Offline structural checks; official parser results are separately pinned in oracle-results.json.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const dir=import.meta.dirname;
const provenance=JSON.parse(fs.readFileSync(path.join(dir,'provenance.json')));
function load(name){return fs.readFileSync(path.join(dir,`${name}.jsonl`),'utf8');}
function valid(text){
 const first=text.indexOf('\n')+1;
 assert.equal(Buffer.byteLength(text.slice(0,first)),256);
 const [slot,header,...entries]=text.trimEnd().split('\n').map(JSON.parse);
 assert.equal(slot.type,'title');assert.equal(header.type,'session');assert.equal(header.version,3);
 assert.equal(typeof header.id,'string');
 const ids=new Set();for(const entry of entries){assert.ok(!ids.has(entry.id));assert.ok(entry.parentId===null||ids.has(entry.parentId));ids.add(entry.id);}
 return {slot,header,entries};
}
test('fixed title slot is rewritten at equal file size without changing entries',()=>{
 const a=valid(load('ordinary')),b=valid(load('title-rewrite'));
 assert.notEqual(a.slot.title,b.slot.title);assert.equal(Buffer.byteLength(load('ordinary')),Buffer.byteLength(load('title-rewrite')));
 assert.deepEqual(a.entries,b.entries);
});
test('fork copies native entry IDs while changing session identity; rewrite retires descendants',()=>{
 const a=valid(load('ordinary')),fork=valid(load('fork')),rewrite=valid(load('rewrite'));
 assert.notEqual(a.header.id,fork.header.id);assert.equal(fork.header.parentSession,a.header.id);assert.deepEqual(a.entries,fork.entries);
 assert.deepEqual(rewrite.entries,[a.entries[0]]);
});
test('raw partial usage remains missing; native tool identity joins independently',()=>{
 const {entries}=valid(load('ordinary'));const assistant=entries[1].message,result=entries[2].message;
 assert.deepEqual(assistant.usage,{input:12});assert.ok(!Object.hasOwn(assistant.usage,'output'));
 assert.equal(assistant.content.find(b=>b.type==='toolCall').id,result.toolCallId);
});
test('image blob bytes match address; absent digest and traversal are distinct unavailable cases',()=>{
 const hash=provenance.blob_sha256;const bytes=fs.readFileSync(path.join(dir,'blobs',hash));
 assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),hash);
 const data=valid(load('ordinary')).entries[0].message.content[1].data;
 assert.equal(data,`blob:sha256:${hash}`);
 assert.equal(/^blob:sha256:[a-f0-9]{64}$/.test('blob:sha256:../../outside'),false);
 assert.equal(fs.existsSync(path.join(dir,'blobs','0'.repeat(64))),false);
});
test('partial final record is pinned as malformed by official parser without losing preceding entries',()=>{
 const expected=JSON.parse(fs.readFileSync(path.join(dir,'oracle-results.json')));
 assert.equal(expected['partial-tail'].malformedRecords,1);assert.deepEqual(expected['partial-tail'].ids,expected.ordinary.ids);
 assert.ok(load('partial-tail').startsWith(load('ordinary')));
});
test('negative controls reject damaged slot width, parent links and tool identity',()=>{
 assert.throws(()=>valid(load('ordinary').replace('"pad":" ','"pad":"')));
 const rows=load('ordinary').trimEnd().split('\n');const entry=JSON.parse(rows[2]);entry.parentId='absent';rows[2]=JSON.stringify(entry);
 assert.throws(()=>valid(rows.join('\n')+'\n'));
 const a=valid(load('ordinary'));a.entries[2].message.toolCallId='wrong';
 assert.throws(()=>assert.equal(a.entries[1].message.content[1].id,a.entries[2].message.toolCallId));
});
