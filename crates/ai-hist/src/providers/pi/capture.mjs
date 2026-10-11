// Run only to reproduce synthetic fixtures, never part of ordinary ingestion.
// npm install --prefix /private/tmp/pi-writer --ignore-scripts @earendil-works/pi-coding-agent@1.1.0
import { SessionManager } from '/private/tmp/pi-writer/node_modules/@earendil-works/pi-coding-agent/dist/core/session-manager.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
const writer='/private/tmp/pi-writer/node_modules/@earendil-works/pi-coding-agent/dist/core/session-manager.js';
if(createHash('sha256').update(fs.readFileSync(writer)).digest('hex')!=='046b6a1109ac3f0ed893bb85bf0648709362fa926a5da75761216cf2fcf9d926')throw new Error('Pinned Pi writer checksum mismatch');
const output = path.resolve('crates/ai-hist/tests/fixtures/pi/writer-v3/.pi/agent/sessions/--fixture--');
const dir = fs.mkdtempSync(path.join(os.tmpdir(),'pi-synthetic-'));
const sm = SessionManager.create('/fixture/project',dir,{id:'custom-history-session'});
const usage = {input:20,output:5,cacheRead:7,cacheWrite:2,totalTokens:34,cost:{input:0.001,output:0.002,cacheRead:0.0001,cacheWrite:0.0002,total:0.0033}};
const u = sm.appendMessage({role:'user',content:'Inspect the fixture greeting.',timestamp:1700000001000});
sm.appendMessage({role:'assistant',content:[{type:'thinking',thinking:'I should read the fixture.'},{type:'text',text:'I will inspect the greeting.'},{type:'toolCall',id:'call-fixture',name:'read',arguments:{path:'greeting.txt'}}],api:'anthropic-messages',provider:'anthropic',model:'fixture-model',usage,stopReason:'toolUse',timestamp:1700000002000});
sm.appendMessage({role:'toolResult',toolCallId:'call-fixture',toolName:'read',content:[{type:'text',text:'Hello fixture'}],isError:false,timestamp:1700000003000});
sm.appendMessage({role:'assistant',content:[{type:'text',text:'The greeting says Hello fixture.'}],api:'anthropic-messages',provider:'anthropic',model:'fixture-model',usage:{...usage,input:2,output:1,totalTokens:12},stopReason:'stop',timestamp:1700000004000});
sm.appendUsage('cache_warm','anthropic','fixture-model',{...usage,input:0,output:0,cacheRead:100,totalTokens:102});
sm.branch(u);
sm.appendMessage({role:'user',content:'Explore an alternate branch.',timestamp:1700000005000});
sm.appendMessage({role:'assistant',content:[{type:'text',text:'This answer is on the alternate branch.'}],api:'anthropic-messages',provider:'anthropic',model:'fixture-model',usage,stopReason:'stop',timestamp:1700000006000});
sm.appendCompaction('Summarized the branch.',null,50000,undefined,false,usage);
sm.appendSessionInfo('Synthetic Pi history');
const file = sm.getSessionFile();
const reopened = SessionManager.open(file);
const fork = SessionManager.forkFrom(file,'/fixture/fork',dir,{id:'custom-fork-session'});
const forkRecords = fs.readFileSync(fork.getSessionFile(),'utf8').trim().split('\n').map(JSON.parse);
const records = fs.readFileSync(file,'utf8').trim().split('\n').map(JSON.parse);
// Sanitize wall clock and random entry IDs consistently, after upstream writer.
const ids = new Map(records.slice(1).map((e,i)=>[e.id,`entry-${i+1}`]));
for (let i=0;i<records.length;i++) {
 const r=records[i]; r.timestamp=new Date(1700000000000+i*1000).toISOString();
 for (const key of ['id','parentId','firstKeptEntryId','fromId','targetId']) if(ids.has(r[key])) r[key]=ids.get(r[key]);
}
fs.mkdirSync(output,{recursive:true});
fs.writeFileSync(path.join(output,'session.jsonl'),records.map(JSON.stringify).join('\n')+'\n');
for(let i=0;i<forkRecords.length;i++){ const r=forkRecords[i]; r.timestamp=new Date(1700000000000+i*1000).toISOString(); for(const key of ['id','parentId','firstKeptEntryId','fromId','targetId']) if(ids.has(r[key]))r[key]=ids.get(r[key]); }
forkRecords[0].parentSession='/fixture/original/session.jsonl';
fs.writeFileSync(path.join(output,'fork.jsonl'),forkRecords.map(JSON.stringify).join('\n')+'\n');
const native = reopened.getEntries().filter(e=>e.type==='message').map(e=>({id:ids.get(e.id),role:e.message.role,content:e.message.content}));
fs.writeFileSync('crates/ai-hist/src/providers/pi/oracle.json',JSON.stringify({package:'@earendil-works/pi-coding-agent@1.1.0',inspectedUpstreamRevision:'89e5c2c075535e2f427c30fc74720d65b2944ff9',reader:'SessionManager.open(...).getEntries()',messages:native,activeContextMessages:reopened.buildSessionContext().messages.length},null,2)+'\n');
fs.rmSync(dir,{recursive:true});
