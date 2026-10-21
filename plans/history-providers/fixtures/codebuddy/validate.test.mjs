import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { createHash } from 'node:crypto';
const root=import.meta.dirname;
const manifest=JSON.parse(fs.readFileSync(path.join(root,'manifest.json'),'utf8'));
const combined=JSON.parse(fs.readFileSync(path.resolve(root,'../../../../crates/ai-hist/tests/fixtures/codebuddy/official-reader.json'),'utf8'));
for(const item of manifest) test(`${item.name}: immutable native capture and reader oracle`,()=>{
 for(const file of item.files){
  const bytes=fs.readFileSync(path.join(root,'cases',item.name,file.path));
  assert.equal(createHash('sha256').update(bytes).digest('hex'),file.sha256);
  assert.deepEqual(fs.readFileSync(path.resolve(root,'../../../../crates/ai-hist/tests/fixtures/codebuddy',item.name,file.path)),bytes);
 }
 const saved=JSON.parse(fs.readFileSync(path.join(root,'oracle',`${item.name}.json`),'utf8'));
 const expected=saved.replay??saved.history;
 assert.deepEqual(combined[item.name],expected);
 const raw=fs.readFileSync(path.join(root,'cases',item.name,item.files[0].path),'utf8').trim().split('\n').map(JSON.parse);
 const ids=new Set(raw.flatMap(x=>[x.id,...(x.meta?.['codebuddy.ai/sessionTitle']??[]).map(f=>f.id)]).filter(Boolean));
 for(const row of expected) if(row.id) assert.ok(ids.has(row.id));
});
test('resume retains native IDs from the first process',()=>{
 const before=combined.ordinary.filter(x=>x.type==='message');
 const after=combined.resumed.filter(x=>x.type==='message');
 assert.equal(before.length,2); assert.equal(after.length,4);
 assert.deepEqual(after.slice(0,2).map(x=>x.id),before.map(x=>x.id));
 assert.deepEqual(after.slice(0,2).map(x=>[x.role,x.content]),before.map(x=>[x.role,x.content]));
});
test('native spill inline content equals independently captured sidecar',()=>{
 const result=combined.spill.find(x=>x.type==='function_call_result');
 assert.ok(result.output.text.startsWith('<persisted-output>'));
 const file=manifest.find(x=>x.name==='spill').files[1];
 assert.equal(result.providerData.toolResult.content,fs.readFileSync(path.join(root,'cases/spill',file.path),'utf8'));
});
test('native cold reader removes old branch and compact summaries from active prompts',()=>{
 assert.deepEqual(combined.fork.map(x=>x.id),['u1','a4']);
 assert.deepEqual(combined['pending-rewind'].map(x=>x.id),['u1']);
 assert.deepEqual(combined.compact.map(x=>x.id),['u1','a2','s3','u4','a5']);
});
test('high-level history retains precompaction messages omitted by live lane context',()=>{
 const oracle=JSON.parse(fs.readFileSync(path.join(root,'oracle/compact.json'),'utf8'));
 assert.deepEqual(oracle.lane.map(x=>x.payload.id),['s3','u4','a5']);
 assert.deepEqual(oracle.replay.map(x=>x.id),['u1','a2','s3','u4','a5']);
});
test('native prompt classifier distinguishes literal tag lookalikes from controls',()=>{
 const oracle=JSON.parse(fs.readFileSync(path.join(root,'oracle/string-content.json'),'utf8'));
 assert.ok(oracle.realUserMessageIds.includes('u3'));
 assert.ok(!oracle.realUserMessageIds.includes('u5'));
});
test('native title classifier gives custom title precedence over newer AI and metadata topic',()=>{
 const oracle=JSON.parse(fs.readFileSync(path.join(root,'oracle/metadata.json'),'utf8'));
 assert.equal(oracle.nativeTitle,'Synthetic chosen title.');
 assert.equal(oracle.session.meta['codebuddy.ai/sessionTitle'].at(-1).customTitle,'Synthetic chosen title.');
});
