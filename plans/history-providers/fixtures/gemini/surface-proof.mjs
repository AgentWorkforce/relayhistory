// Public SDK/CLI/MCP acceptance. Uses only the pinned synthetic fixture home.
// Run after native build: node plans/history-providers/fixtures/gemini/surface-proof.mjs
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {createRequire} from 'node:module';
import {mkdtemp,mkdir,copyFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {promisify} from 'node:util';
const sdkRoot=new URL('../../../../sdk-ts/',import.meta.url);
const require=createRequire(new URL('package.json',sdkRoot));
const {Client}=await import(require.resolve('@modelcontextprotocol/sdk/client/index.js'));
const {StdioClientTransport}=await import(require.resolve('@modelcontextprotocol/sdk/client/stdio.js'));
const {scrubHistoryEnv}=await import(new URL('dist/test-env.js',sdkRoot));
const saved={...process.env}; const restore=scrubHistoryEnv();
const home=await mkdtemp(join(tmpdir(),'gemini-surfaces-'));
let client;
try {
  delete process.env.GEMINI_CLI_HOME; delete process.env.SANDBOX;
  process.env.HOME=home; process.env.USERPROFILE=home;
  process.env.AI_HIST_DB=join(home,'history.db');
  process.env.RELAYHISTORY_HOME=join(home,'app');process.env.RELAYHISTORY_NO_UPDATE_CHECK='1';
  const directory=join(home,'.gemini/tmp/gemini-project/chats');await mkdir(directory,{recursive:true});
  await copyFile(new URL('cases/ordinary/.gemini/tmp/gemini-project/chats/session-2026-10-11T12-00-11111111.jsonl',import.meta.url),join(directory,'session.jsonl'));
  const sdk=await import(new URL('dist/index.js',sdkRoot));
  assert.ok(sdk.SOURCES.includes('gemini'));
  const discovery=await sdk.discoverSessions({sources:['gemini'],dbPath:process.env.AI_HIST_DB,scope:'local',sourceConnectors:[]});
  assert.equal(discovery.discovered,1);
  const sid='11111111-1111-4111-8111-111111111111';
  const report=await sdk.hydrateSession({source:'gemini',sessionId:sid,dbPath:process.env.AI_HIST_DB,scope:'local',sourceConnectors:[]});
  assert.equal(report.status,'hydrated');
  const matches=await sdk.search('synthetic Gemini needle',{source:'gemini',dbPath:process.env.AI_HIST_DB});
  assert.ok(matches.some(m=>m.role==='assistant' && m.prompt==='Found the synthetic Gemini needle.'));
  assert.ok(matches.some(m=>m.matchSource==='history'));
  assert.deepEqual(await sdk.search('synthetic Gemini needle',{source:'claude',dbPath:process.env.AI_HIST_DB}),[]);
  const usage=await sdk.getSessionUsage('gemini',sid,{dbPath:process.env.AI_HIST_DB});
  assert.equal(usage.usage.inputTokens,90);assert.equal(usage.usage.cacheReadTokens,30);
  const descriptor=await sdk.getSourceCapabilities('gemini');
  assert.equal(descriptor.source,'gemini');
  const {stdout}=await promisify(execFile)(process.execPath,[fileURLToPath(new URL('dist/cli.js',sdkRoot)),'search','synthetic Gemini needle','--source','gemini','--json','--no-warning','--db',process.env.AI_HIST_DB],{env:process.env});
  assert.ok(JSON.parse(stdout).some(m=>(m.role==='assistant') && m.prompt==='Found the synthetic Gemini needle.'));
  const env=Object.fromEntries(Object.entries(process.env).filter(([,v])=>v!==undefined));
  client=new Client({name:'gemini-fixture-proof',version:'1'});
  const transport=new StdioClientTransport({command:process.execPath,args:[fileURLToPath(new URL('dist/mcp-server.js',sdkRoot))],env,stderr:'pipe'});
  await client.connect(transport);
  const result=await client.callTool({name:'search_history',arguments:{query:'synthetic Gemini needle',source:'gemini'}});
  assert.equal(result.isError,undefined);
  assert.ok(JSON.parse(result.content[0].text).some(m=>m.role==='assistant'));
  const caps=await client.callTool({name:'get_source_capabilities',arguments:{source:'gemini'}});
  assert.equal(caps.isError,undefined);assert.equal(JSON.parse(caps.content[0].text).source,'gemini');
  console.log('Gemini SDK discovery/hydration/search/filter/descriptor, CLI search and MCP search/capabilities verified.');
} finally {
  await client?.close(); restore();
  for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
  Object.assign(process.env,saved);await rm(home,{recursive:true,force:true});
}
