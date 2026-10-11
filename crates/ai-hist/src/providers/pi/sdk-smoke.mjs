// Run after building the native addon and sdk-ts. All history is synthetic.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
const sdkRequire=createRequire(new URL('../../../../../sdk-ts/package.json',import.meta.url));
const { Client }=await import(sdkRequire.resolve('@modelcontextprotocol/sdk/client/index.js'));
const { StdioClientTransport }=await import(sdkRequire.resolve('@modelcontextprotocol/sdk/client/stdio.js'));
import { discoverSessions, hydrateSession, search, getSourceCapabilities, getSessionEventsPage } from '../../../../../sdk-ts/dist/index.js';
const root=fs.mkdtempSync(path.join(os.tmpdir(),'pi-sdk-'));
const fixture=fileURLToPath(new URL('../../../tests/fixtures/pi/writer-v3/.pi/agent/sessions/--fixture--/session.jsonl',import.meta.url));
const sessionRoot=path.join(root,'sessions'); fs.mkdirSync(sessionRoot);fs.copyFileSync(fixture,path.join(sessionRoot,'session.jsonl'));
process.env.PI_CODING_AGENT_SESSION_DIR=sessionRoot;
const dbPath=path.join(root,'history.db');
try {
 const discovery=await discoverSessions({dbPath,sources:['pi'],scope:'local'});
 assert.equal(discovery.discovered,1);assert.equal(discovery.sessions[0].source,'pi');
 const hydrated=await hydrateSession({dbPath,source:'pi',sessionId:'custom-history-session'});
 assert.equal(hydrated.status,'hydrated'); assert.equal(hydrated.capability,'partial');
 for(const [query,role] of [['Inspect the fixture greeting','user'],['The greeting says Hello fixture','assistant'],['alternate branch','assistant']]) {
  const matches=await search(query,{dbPath,source:'pi',role});
  assert.ok(matches.some(m=>m.source==='pi' && m.sessionId==='custom-history-session'),`public ${role} search: ${query}`);
 }
 assert.deepEqual(await search('fixture',{dbPath,source:'claude'}),[]);
 const capabilities=await getSourceCapabilities('pi');
 assert.ok(capabilities.evidenceKinds.includes('session_event'));assert.ok(capabilities.evidenceKinds.includes('tool_call'));assert.ok(capabilities.evidenceKinds.includes('session_marker'));assert.ok(!capabilities.evidenceKinds.includes('file_edit'));assert.equal(capabilities.fullCoverage,false);
 const events=await getSessionEventsPage('custom-history-session',{dbPath,source:'pi'});assert.ok(events.events.length>0);
 const cli=fileURLToPath(new URL('../../../../../sdk-ts/dist/cli.js',import.meta.url));
 const cliMatches=JSON.parse(execFileSync(process.execPath,[cli,'search','greeting','--source','pi','--role','assistant','--db',dbPath,'--json','--no-warning','--no-bootstrap'],{encoding:'utf8'}));
 assert.ok(cliMatches.some(m=>m.source==='pi'));
 const client=new Client({name:'pi-fixture',version:'1'});
 const transport=new StdioClientTransport({command:process.execPath,args:[fileURLToPath(new URL('../../../../../sdk-ts/dist/mcp-server.js',import.meta.url))],env:{...process.env,AI_HIST_DB:dbPath},stderr:'pipe'});
 try {
  await client.connect(transport);
  const result=await client.callTool({name:'search_history',arguments:{query:'greeting',source:'pi',role:'assistant'}});
  assert.ok(!result.isError);const matches=JSON.parse(result.content[0].text);assert.ok(matches.some(m=>m.source==='pi'));
 } finally {await client.close();await transport.close();}
 console.log('Pi SDK/CLI/MCP source filtering, user/assistant search, events and honest capability assertions passed');
} finally {fs.rmSync(root,{recursive:true,force:true});}
