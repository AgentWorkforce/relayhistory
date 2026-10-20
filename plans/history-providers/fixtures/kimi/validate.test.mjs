import assert from'node:assert/strict';import fs from'node:fs';import path from'node:path';import test from'node:test';import{createHash}from'node:crypto';import{fileURLToPath}from'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(base,'../../../..');const manifest=JSON.parse(fs.readFileSync(path.join(base,'manifest.json'))),fixtures=path.join(repo,'crates/ai-hist/tests/fixtures/kimi'),oracle=JSON.parse(fs.readFileSync(path.join(fixtures,'official-reader.json')));
const text=m=>m.content.filter(p=>p.type==='text').map(p=>p.text).join('\n');
for(const item of manifest)test(item.name+': pinned native journal matches independent official restart fold',()=>{
 for(const[file,hash]of Object.entries(item.files)){const bytes=fs.readFileSync(path.join(fixtures,item.name,item.root,file));assert.equal(createHash('sha256').update(bytes).digest('hex'),hash.sanitized_sha256);if(!file.endsWith('wire.jsonl'))continue;const agent=path.basename(path.dirname(file));const rows=bytes.toString().trim().split('\n').map(JSON.parse);assert.equal(rows[0].protocol_version,'1.5');const messages=(item.name==='rewound'?[]:rows).filter(r=>r.type==='agent.message.appended').map(r=>r.message.message);const restored=oracle[item.name][agent].replay.filter(r=>r.type==='message'&&r.message.origin?.kind!=='injection').map(r=>r.message);assert.deepEqual(messages.map(m=>[m.role,text(m)]),restored.map(m=>[m.role,text(m)]));const calls=messages.flatMap(m=>m.toolCalls??[]);assert.deepEqual(calls.map(c=>[c.id,c.name,JSON.parse(c.arguments)]),restored.flatMap(m=>m.toolCalls??[]).map(c=>[c.id,c.name,JSON.parse(c.arguments)]));}
});
test('restart continuation and separate child journal are captured',()=>{assert.equal(oracle.resumed.main.replay.filter(r=>r.type==='message'&&r.message.origin?.kind!=='injection').length,4);assert.ok(oracle.subagent['agent-0']);});

test('sanitized fixtures and reader oracle contain no capture temporary paths',()=>{
 for(const item of manifest)for(const file of Object.keys(item.files))assert.doesNotMatch(fs.readFileSync(path.join(fixtures,item.name,item.root,file),'utf8'),/\/private\/tmp\/|\/var\/folders\/|\/tmp\/kimi/);
 assert.doesNotMatch(JSON.stringify(oracle),/\/private\/tmp\/|\/var\/folders\/|\/tmp\/kimi/);
});
