#!/usr/bin/env node
// Execute pinned Cline serializer and persisted-message reader source in separate
// Bun processes. Only imports/type scaffolding are replaced; method bodies are
// extracted byte-for-byte from the named upstream files.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '../../../..');
const sourceDir = process.argv[2];
if (!sourceDir) throw new Error('Usage: node capture.mjs <directory containing pinned session-data.ts and runtime-host-support.ts>');
const writer = fs.readFileSync(path.join(sourceDir,'session-data.ts'),'utf8');
const reader = fs.readFileSync(path.join(sourceDir,'runtime-host-support.ts'),'utf8');
const store = fs.readFileSync(path.join(sourceDir,'session-manifest-store.ts'),'utf8');
for (const [name,content,expected] of [['session-data.ts',writer,'f79295eefdb725ee5dfcbac424791e52166dd6b09c44cd4f9eed1ae0071dfa4a'],['runtime-host-support.ts',reader,'88044afc20e9d45b9238e1b1c3d23dcbcc97abc895685191614c431c3738ad65'],['session-manifest-store.ts',store,'f9b5dba785d49b4f5033629c87af9950d07eb5e90f7a24c8a8f9e72d8c9560aa']]) {
  const actual=createHash('sha256').update(content).digest('hex');
  if(actual!==expected) throw new Error(`${name} SHA-256 ${actual} differs from pinned ${expected}`);
}
function between(source, first, after) {
  const a = source.indexOf(first), b = source.indexOf(after, a + first.length);
  if (a < 0 || b < 0) throw new Error(`Pinned source segment missing: ${first}`);
  return source.slice(a,b);
}
const serializer = between(writer, 'function trimNonEmptyString', 'export function toSessionRecord') + '\n' + between(writer, 'export type MessagesFileContext', 'export function buildManifestFromRow');
const restarter = between(reader, 'export async function readPersistedMessagesFile', 'export function cloneAccumulatedUsage');
const persistenceMethod = between(store, 'async persistSessionMessages(', 'private resolveCompactionPath(');
const temp = fs.mkdtempSync(path.join(os.tmpdir(),'cline-native-capture-'));
const fixtureRoot = path.join(root,'crates/ai-hist/tests/fixtures/cline/native/.cline/data/sessions');
const golden = JSON.parse(fs.readFileSync(path.join(fixtureRoot,'fixture-success-01/fixture-success-01.messages.json'),'utf8'));
const nativePath = path.join(fixtureRoot,'synthetic-resume/synthetic-resume.messages.json');
const ordinaryPath = path.join(fixtureRoot,'synthetic-ordinary/synthetic-ordinary.messages.json');
const inputs = JSON.stringify({golden, nativePath, ordinaryPath});
const preamble = `import {mkdirSync,writeFileSync} from 'node:fs'; import {dirname} from 'node:path'; const nanoid=()=>{throw new Error('fixture ids must be native stable ids')}; function readSessionHistoryOriginMetadata(){return undefined}; function parseSubSessionId(){return null}; function parseTeamTaskSubSessionId(){return null}; const stamps=['2026-10-22T10:00:00.000Z','2026-10-22T10:01:00.000Z','2026-10-22T10:01:10.000Z']; const nowIso=()=>stamps.shift();`;
fs.writeFileSync(path.join(temp,'writer.ts'), `${preamble}\n${serializer}\nclass CaptureWriter { async resolveSessionRow(id){return {sessionId:id,source:'cli',messagesPath:this.paths[id]}}; ${persistenceMethod} }\nconst input=${inputs};const capture=new CaptureWriter();capture.paths={'synthetic-ordinary':input.ordinaryPath,'synthetic-resume':input.nativePath};const messages=input.golden.messages;await capture.persistSessionMessages('synthetic-ordinary',[{...messages[0],id:'ordinary-u1'}, {...messages[3],id:'ordinary-a1'}]);await capture.persistSessionMessages('synthetic-resume',[{...messages[0],id:'resume-u1'}, {...messages[3],id:'resume-a1'}]);await capture.persistSessionMessages('synthetic-resume',[{...messages[0],id:'resume-u1'}, {...messages[3],id:'resume-a1'}, {id:'resume-u2',role:'user',content:[{type:'text',text:'Continue the synthetic task.'}]}, {id:'resume-a2',role:'assistant',ts:1792663260000,modelInfo:messages[3].modelInfo,metrics:messages[3].metrics,content:[{type:'text',text:'The continuation is complete.'}]}]);`);
const readerPreamble = `import {existsSync} from 'node:fs'; import {readFile} from 'node:fs/promises';`;
fs.writeFileSync(path.join(temp,'reader.ts'), `${readerPreamble}\n${restarter}\nconst paths=${JSON.stringify([ordinaryPath,nativePath])};const output={};for(const file of paths){const messages=await readPersistedMessagesFile(file);output[file.split('/').at(-1)] = messages.map(m=>({id:m.id,role:m.role,content:m.content,modelInfo:m.modelInfo??null,metrics:m.metrics??null,ts:m.ts??null}));}process.stdout.write(JSON.stringify(output,null,2)+'\\n');`);
for (const name of ['writer.ts','reader.ts']) {
  const run=spawnSync('bun',[path.join(temp,name)],{encoding:'utf8'});
  if(run.status!==0) throw new Error(`${name}: ${run.stderr}`);
  if(name==='reader.ts') fs.writeFileSync(path.join(here,'restart-reader.json'),run.stdout);
}
console.log('Executed pinned Cline serializer, exited, then executed independent persisted-message reader:',path.join(here,'restart-reader.json'));
