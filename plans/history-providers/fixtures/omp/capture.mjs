// Offline synthetic persistence capture; no CLI, credentials, user history or model calls.
// npm install --prefix /private/tmp/omp-writer-phase2 --ignore-scripts @oh-my-pi/pi-coding-agent@18.9.2
// bun capture.mjs /private/tmp/omp-writer-phase2/node_modules/@oh-my-pi/pi-coding-agent OUTPUT
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {createHash} from 'node:crypto';
const root=process.argv[2], output=process.argv[3];
if(!root || !output) throw new Error('Pass isolated pinned public npm package path and synthetic output directory');
const writer=path.join(root,'src/session/session-manager.ts');
if(createHash('sha256').update(fs.readFileSync(writer)).digest('hex')!=='6ea1265b55a945b5ea9c9031497252b839960c37237e1f8c7c7284ddc2df6135') throw new Error('Pinned writer checksum differs');
const synthetic=fs.mkdtempSync(path.join(os.tmpdir(),'omp-synthetic-'));
process.env.PI_CODING_AGENT_DIR=path.join(synthetic,'agent');
const {SessionManager}=await import(writer);
const {loadSessionMessagesReadOnly}=await import(path.join(root,'src/session/session-loader.ts'));
const {serializeTitleSlot}=await import(path.join(root,'src/session/session-title-slot.ts')); 
const sm=SessionManager.create('/synthetic/workspace',path.join(synthetic,'sessions'));
const first=sm.appendMessage({role:'user',content:'Inspect the writer fixture.',timestamp:1767225601000});
sm.appendMessage({role:'assistant',content:[{type:'text',text:'Reading the writer fixture.'},{type:'toolCall',id:'writer-call-1',name:'read',arguments:{path:'fixture.txt'}}],api:'synthetic',provider:'synthetic',model:'fixture-model',usage:{input:12,output:3,cacheRead:0,cacheWrite:0,totalTokens:15,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:'toolUse',timestamp:1767225602000});
sm.appendMessage({role:'toolResult',toolCallId:'writer-call-1',toolName:'read',content:[{type:'text',text:'writer fixture bytes'}],isError:false,timestamp:1767225603000});
await sm.setSessionName('Synthetic writer title','user');
await sm.flush();
const file=sm.getSessionFile();
await sm.close();
const resumed=await SessionManager.open(file);
resumed.appendMessage({role:'assistant',content:[{type:'text',text:'The fixture remains readable after reopen.'}],api:'synthetic',provider:'synthetic',model:'fixture-model',usage:{input:1,output:2,cacheRead:0,cacheWrite:0,totalTokens:3,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:'stop',timestamp:1767225604000});
await resumed.flush();
const fork=await SessionManager.forkFrom(file,'/synthetic/fork',path.join(synthetic,'fork'),undefined,{copyArtifacts:false,suppressBreadcrumb:true});
await fork.flush();
const raw=fs.readFileSync(file,'utf8');
const records=raw.trimEnd().split('\n').map(JSON.parse);
const ids=new Map(records.slice(2).map((e,i)=>[e.id,(i+1).toString(16).padStart(8,'0')]));
function sanitize(text,sid) {
 const rows=text.trimEnd().split('\n').map(JSON.parse);
 for (const [i,r] of rows.entries()) {
  if(r.timestamp)r.timestamp=new Date(1767225600000+i*1000).toISOString();
  if(r.updatedAt)r.updatedAt='2026-01-01T00:00:00.000Z';
  if(r.type==='session') {r.id=sid;if(r.parentSession)r.parentSession='/synthetic/original/session.jsonl';if(r.providerPromptCacheKey===records[1].id)r.providerPromptCacheKey='019b76da-a800-7000-8000-000000000002';}
  for(const k of ['id','parentId','firstKeptEntryId','fromId','targetId'])if(ids.has(r[k]))r[k]=ids.get(r[k]);
 }
 const slot=rows.shift();
 return serializeTitleSlot(slot)+rows.map(JSON.stringify).join('\n')+'\n';
}
fs.mkdirSync(output,{recursive:true});
const session=sanitize(raw,'019b76da-a800-7000-8000-000000000002');
fs.writeFileSync(path.join(output,'session.jsonl'),session);
fs.writeFileSync(path.join(output,'fork.jsonl'),sanitize(fs.readFileSync(fork.getSessionFile(),'utf8'),'019b76da-a800-7000-8000-000000000003'));
const reopened=await SessionManager.open(path.join(output,'session.jsonl'));
const rendered=await loadSessionMessagesReadOnly(path.join(output,'session.jsonl'));
fs.writeFileSync(path.join(output,'oracle.json'),JSON.stringify({package:'@oh-my-pi/pi-coding-agent@18.9.2',writerSha256:'6ea1265b55a945b5ea9c9031497252b839960c37237e1f8c7c7284ddc2df6135',reader:'SessionManager.open().getEntries() and loadSessionMessagesReadOnly()',renderedMessages:rendered.map(m=>({role:m.role,content:m.content})),messages:reopened.getEntries().filter(e=>e.type==='message').map(e=>({id:e.id,parentId:e.parentId,role:e.message.role,content:e.message.content}))},null,2)+'\n');
await resumed.close();await fork.close();await reopened.close();
console.log(output);
fs.rmSync(synthetic,{recursive:true,force:true});
