// Run: node capture.mjs /path/to/coco-1.2.18+192728.aa2238062732-darwin-arm64.tar.gz
// Uses the exact bundled native ConversationHistory class; no CLI auth/model turn.
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRequire } from 'node:module';

const expected = '0dc9e79466020ab550bc3639c2814fa3d2a5090b3f14dfddd834c3520202364d';
const tarball = process.argv[2];
if (!tarball) throw new Error('pass the pinned official tarball');
if (createHash('sha256').update(readFileSync(tarball)).digest('hex') !== expected) {
  throw new Error('official package checksum mismatch');
}
const packageName = 'coco-1.2.18+192728.aa2238062732-darwin-arm64';
const scratch = join(tmpdir(), `cortex-capture-${process.pid}`);
rmSync(scratch, { recursive: true, force: true });
mkdirSync(scratch, { recursive: true });
execFileSync('tar', ['-xzf', tarball, '-C', scratch]);
const packageDir = join(scratch, packageName);
const executable = readFileSync(join(packageDir, 'cortex'));
const start = executable.indexOf('#!/usr/bin/env node\nrequire = require(');
const end = executable.indexOf(0, start);
if (start < 0 || end < 0) throw new Error('embedded Node bundle not found');
let source = executable.subarray(start, end).toString('utf8');
const launch = 'Promise.resolve().then(()=>(WBo(),mXm)).then(({main:t})=>t()).catch(t=>{';
const launchAt = source.lastIndexOf(launch);
if (launchAt < 0 || !source.includes('fZ=class t{history=[];summarization=[];')) {
  throw new Error('pinned native writer shape changed');
}
source = source.slice(0, launchAt) + 'zDs(); module.exports={ConversationHistory:fZ,readHistory:Otr};\n';
mkdirSync(join(packageDir, 'dist'), { recursive: true });
const bundle = join(packageDir, 'dist', 'index.cjs');
writeFileSync(bundle, source);
const { ConversationHistory, readHistory } = createRequire(import.meta.url)(bundle);

const RealDate = Date;
globalThis.Date = class FixedDate extends RealDate {
  constructor(...args) { super(...(args.length ? args : ['2026-10-21T10:00:00.000Z'])); }
  static now() { return RealDate.parse('2026-10-21T10:00:00.000Z'); }
};
const fixtures = join(import.meta.dirname, '../../../../crates/ai-hist/tests/fixtures/cortex/native');
rmSync(fixtures, { recursive: true, force: true });
const conversations = join(fixtures, '.snowflake/cortex/conversations');
mkdirSync(conversations, { recursive: true });
const id = '11111111-1111-4111-8111-111111111111';
const writer = new ConversationHistory(id, '/workspace/cortex-demo', [], { conversationsDir: conversations });
writer.addMessage({role:'user',id:'user-1',userSentTime:'2026-10-21T10:00:00.000Z',content:[{type:'text',text:'Create a demo table',is_user_prompt:true}]});
writer.addMessage({role:'assistant',id:'assistant-1',assistantSentTime:'2026-10-21T10:00:01.000Z',model:'claude-sonnet-4',content:[{type:'text',text:'I will inspect the schema.'},{type:'tool_use',tool_use:{tool_use_id:'tool-1',name:'Bash',input:{command:'pwd'}}}]});
writer.addMessage({role:'user',id:'tool-result-1',userSentTime:'2026-10-21T10:00:02.000Z',content:[{type:'tool_result',tool_result:{tool_use_id:'tool-1',content:[{type:'text',text:'/workspace/cortex-demo'}],status:'success'}}]});
writer.addMessage({role:'assistant',id:'assistant-2',assistantSentTime:'2026-10-21T10:00:03.000Z',model:'claude-sonnet-4',content:[{type:'text',text:'Done.'}]});
await writer.flushAllWrites();

// The released restart reader supplies the raw ID/order oracle independently
// of the write instance. It reads the complete sidecar, not metadata length.
const storage = writer.storage;
const observed = await readHistory(storage, join(conversations, `${id}.history.jsonl`));
writeFileSync(join(import.meta.dirname, 'native-reader-oracle.json'), JSON.stringify(observed.map(({role,id,content}) => ({role,id,content})), null, 2) + '\n');
const flatId = '22222222-2222-4222-8222-222222222222';
const flat = new ConversationHistory(flatId, '/workspace/cortex-demo', [], { conversationsDir: conversations, historyJsonlFormat: 'flat' });
flat.addMessage({role:'user',id:'flat-user-1',userSentTime:'2026-10-21T11:00:00.000Z',content:[{type:'text',text:'Check status',is_user_prompt:true}]});
flat.addMessage({role:'assistant',id:'flat-assistant-1',assistantSentTime:'2026-10-21T11:00:01.000Z',content:[{type:'text',text:'Checking.'},{type:'tool_use',tool_use:{tool_use_id:'flat-tool-1',name:'Bash',input:{command:'pwd'}}}]});
flat.addMessage({role:'user',id:'flat-result-1',userSentTime:'2026-10-21T11:00:02.000Z',content:[{type:'tool_result',tool_result:{tool_use_id:'flat-tool-1',content:[{type:'text',text:'/workspace/cortex-demo'}],status:'success'}}]});
await flat.flushAllWrites();
const flatObserved = await readHistory(flat.storage, join(conversations, `${flatId}.history.jsonl`));
writeFileSync(join(import.meta.dirname, 'flat-reader-oracle.json'), JSON.stringify(flatObserved.map(({role,id,content}) => ({role,id,content})), null, 2) + '\n');
const rewriteId = '33333333-3333-4333-8333-333333333333';
const rewrite = new ConversationHistory(rewriteId, '/workspace/cortex-demo', [], { conversationsDir: conversations });
rewrite.addMessage({role:'user',id:'rewrite-user',userSentTime:'2026-10-21T12:00:00.000Z',content:[{type:'text',text:'Keep this',is_user_prompt:true}]});
rewrite.addMessage({role:'assistant',id:'rewrite-assistant',assistantSentTime:'2026-10-21T12:00:01.000Z',content:[{type:'text',text:'Old answer'}]});
await rewrite.flushAllWrites();
rewrite.removeLastMessage(); // The released writer rewrites the entire sidecar.
await rewrite.flushAllWrites();
const rewrittenObserved = await readHistory(rewrite.storage, join(conversations, `${rewriteId}.history.jsonl`));
writeFileSync(join(import.meta.dirname, 'rewrite-reader-oracle.json'), JSON.stringify(rewrittenObserved.map(({role,id,content}) => ({role,id,content})), null, 2) + '\n');
const emptyId = '55555555-5555-4555-8555-555555555555';
const empty = new ConversationHistory(emptyId, '/workspace/cortex-demo', [], { conversationsDir: conversations });
empty.addMessage({role:'user',id:'empty-user',userSentTime:'2026-10-21T13:00:00.000Z',content:[{type:'text',text:'Remove me',is_user_prompt:true}]});
await empty.flushAllWrites();
writeFileSync(join(import.meta.dirname, 'empty-before.jsonl'), readFileSync(join(conversations, `${emptyId}.history.jsonl`)));
empty.removeLastMessage();
await empty.flushAllWrites();
const emptyObserved = await readHistory(empty.storage, join(conversations, `${emptyId}.history.jsonl`));
if (emptyObserved.length !== 0 || readFileSync(join(conversations, `${emptyId}.history.jsonl`)).length !== 0) {
  throw new Error('native empty rewrite was not empty');
}
writeFileSync(join(import.meta.dirname, 'empty-reader-oracle.json'), JSON.stringify(emptyObserved, null, 2) + '\n');
const untimedId = '66666666-6666-4666-8666-666666666666';
const untimed = new ConversationHistory(untimedId, '/workspace/cortex-demo', [], { conversationsDir: conversations });
untimed.addMessage({role:'user',id:'timed-user',userSentTime:'2026-10-21T14:00:00.000Z',content:[{type:'text',text:'Keep both messages',is_user_prompt:true}]});
untimed.addMessage({role:'assistant',id:'untimed-answer',content:[{type:'text',text:'Both retained.'}]});
await untimed.flushAllWrites();
const untimedObserved = await readHistory(untimed.storage, join(conversations, `${untimedId}.history.jsonl`));
if (untimedObserved.length !== 2 || untimedObserved[1].assistantSentTime !== undefined) {
  throw new Error('native untimed reader contract changed');
}
writeFileSync(join(import.meta.dirname, 'untimed-reader-oracle.json'), JSON.stringify(untimedObserved.map(({role,id,content,assistantSentTime}) => ({role,id,content,assistantSentTime})), null, 2) + '\n');
console.log(`native writer/reader captured ${observed.length} merged, ${flatObserved.length} flat, ${rewrittenObserved.length} rewritten, ${emptyObserved.length} empty, ${untimedObserved.length} untimed messages`);
