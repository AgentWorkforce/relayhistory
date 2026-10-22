// Run after building this checkout's native addon and sdk-ts. Synthetic history only.
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
const root=fs.mkdtempSync(path.join(os.tmpdir(),'kilo-sdk-'));
const fixture=fileURLToPath(new URL('../../../tests/fixtures/kilo/schema-7-8-15.sql',import.meta.url));
const dataDir=path.join(root,'.local','share','kilo');fs.mkdirSync(dataDir,{recursive:true});
process.env.XDG_DATA_HOME=path.join(root,'.local','share');
execFileSync('sqlite3',[path.join(dataDir,'kilo.db')],{input:fs.readFileSync(fixture),stdio:['pipe','pipe','pipe']});
const sid='default:7365735f6b696c6f546f6f6c303032';
const dbPath=path.join(root,'history.db');
try {
 const discovery=await discoverSessions({dbPath,sources:['kilo'],scope:'local'});
 assert.equal(discovery.discovered,1);assert.equal(discovery.sessions[0].source,'kilo');
 const hydrated=await hydrateSession({dbPath,source:'kilo',sessionId:sid});
 assert.equal(hydrated.status,'hydrated');assert.equal(hydrated.capability,'partial');
 for(const [query,role] of [['Find the synthetic Kilo needle','user'],['Found the synthetic Kilo needle','assistant'],['Historical Kilo answer persists','assistant']]) {
  const matches=await search(query,{dbPath,source:'kilo',role});
  assert.ok(matches.some(m=>m.source==='kilo'&&m.sessionId===sid),`SDK ${role} search: ${query}`);
 }
 assert.ok((await search('Kilo tool result needle',{dbPath,source:'kilo'})).some(m=>m.sessionId===sid),'SDK tool-result search');
 assert.deepEqual(await search('Kilo',{dbPath,source:'claude'}),[]);
 const capabilities=await getSourceCapabilities('kilo');
 assert.ok(capabilities.evidenceKinds.includes('session_event'));assert.ok(capabilities.evidenceKinds.includes('tool_call'));assert.ok(capabilities.evidenceKinds.includes('session_marker'));assert.ok(!capabilities.evidenceKinds.includes('file_edit'));assert.equal(capabilities.fullCoverage,false);
 const events=await getSessionEventsPage(sid,{dbPath,source:'kilo'});assert.ok(events.events.length>0);
 const cli=fileURLToPath(new URL('../../../../../sdk-ts/dist/cli.js',import.meta.url));
 const cliMatches=JSON.parse(execFileSync(process.execPath,[cli,'search','Historical Kilo','--source','kilo','--role','assistant','--db',dbPath,'--json','--no-warning','--no-bootstrap'],{encoding:'utf8'}));
 assert.ok(cliMatches.some(m=>m.source==='kilo'&&m.session_id===sid));
 const client=new Client({name:'kilo-fixture',version:'1'});
 const transport=new StdioClientTransport({command:process.execPath,args:[fileURLToPath(new URL('../../../../../sdk-ts/dist/mcp-server.js',import.meta.url))],env:{...process.env,AI_HIST_DB:dbPath},stderr:'pipe'});
 try {
  await client.connect(transport);
  const result=await client.callTool({name:'search_history',arguments:{query:'Historical Kilo',source:'kilo',role:'assistant'}});
  assert.ok(!result.isError);const matches=JSON.parse(result.content[0].text);assert.ok(matches.some(m=>m.source==='kilo'&&m.sessionId===sid));
 } finally {await client.close();await transport.close();}
 console.log('Kilo SDK/CLI/MCP source filtering, user/assistant/tool search, events and capability assertions passed');
} finally {fs.rmSync(root,{recursive:true,force:true});}
