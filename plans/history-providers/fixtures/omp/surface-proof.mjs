// Reproduce after rebuilding native + SDK in this checkout. Synthetic HOME only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
const root=path.resolve(process.argv[2]??'.');
if(!process.env.RH_OMP_SURFACE_CHILD){
 const home=fs.mkdtempSync(path.join(os.tmpdir(),'omp-surfaces-'));
 const env={...process.env,HOME:home,USERPROFILE:home,AI_HIST_DB:path.join(home,'history.db'),RELAYHISTORY_HOME:path.join(home,'commercial'),RH_OMP_SURFACE_CHILD:'1'};
 for(const key of ['PI_CODING_AGENT_DIR','PI_CODING_AGENT_SESSION_DIR','PI_CONFIG_DIR','OMP_PROFILE','PI_PROFILE','XDG_DATA_HOME','AI_HIST_PLUGIN_CONFIG','CLAUDE_CONFIG_DIR','CODEX_HOME','GROK_HOME','OPENCODE_DB','OPENCODE_STORAGE_DIR']) delete env[key];
 const target=path.join(home,'.omp/agent/sessions/synthetic/session.jsonl');fs.mkdirSync(path.dirname(target),{recursive:true});
 fs.copyFileSync(path.join(root,'crates/ai-hist/tests/fixtures/omp/writer-v3/.omp/agent/sessions/--fixture--/session.jsonl'),target);
 try{const result=spawnSync(process.execPath,[import.meta.filename,root],{env,encoding:'utf8'});process.stdout.write(result.stdout);process.stderr.write(result.stderr);assert.equal(result.status,0);}finally{fs.rmSync(home,{recursive:true,force:true});}
}else{
 const sdk=await import(pathToFileURL(path.join(root,'sdk-ts/dist/index.js')));
 const dbPath=process.env.AI_HIST_DB,sid='019b76da-a800-7000-8000-000000000002';
 const discovered=await sdk.discoverSessions({dbPath,sources:['omp'],scope:'local'});assert.equal(discovered.discovered,1);
 await sdk.hydrateSession({dbPath,source:'omp',sessionId:sid});
 const hits=await sdk.search('writer fixture',{dbPath,source:'omp'});assert.ok(hits.length>0);assert.ok(hits.every(h=>h.source==='omp'));
 assert.equal((await sdk.search('writer fixture',{dbPath,source:'pi'})).length,0);
 assert.ok((await sdk.getSessionEvents(sid,{dbPath,source:'omp'})).length>0);
 const capabilities=await sdk.getSourceCapabilities('omp');assert.equal(capabilities.source,'omp');
 const cli=spawnSync(process.execPath,[path.join(root,'sdk-ts/dist/cli.js'),'search','writer fixture','--source','omp','--db',dbPath,'--json'],{env:process.env,encoding:'utf8'});assert.equal(cli.status,0,cli.stderr);assert.ok(JSON.parse(cli.stdout).length>0);
 const {Client}=await import(pathToFileURL(path.join(root,'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js')));
 const {StdioClientTransport}=await import(pathToFileURL(path.join(root,'sdk-ts/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js')));
 const client=new Client({name:'omp-synthetic-acceptance',version:'1.0.0'});
 await client.connect(new StdioClientTransport({command:process.execPath,args:[path.join(root,'sdk-ts/dist/mcp-server.js')],env:process.env,stderr:'pipe'}));
 try{
  const tools=await client.listTools();assert.ok(tools.tools.find(t=>t.name==='search_history').inputSchema.properties.source.enum.includes('omp'));
  const caps=await client.callTool({name:'get_source_capabilities',arguments:{source:'omp'}});assert.ok(!caps.isError);assert.equal(JSON.parse(caps.content[0].text).source,'omp');
  const result=await client.callTool({name:'search_history',arguments:{query:'writer fixture',source:'omp'}});assert.ok(!result.isError);assert.ok(JSON.parse(result.content[0].text).length>0);
  const filtered=await client.callTool({name:'search_history',arguments:{query:'writer fixture',source:'pi'}});assert.deepEqual(JSON.parse(filtered.content[0].text),[]);
 }finally{await client.close();}
 console.log('OMP public SDK discover/hydrate/read/search + CLI search + MCP descriptor/capabilities/search/source filtering passed');
}
