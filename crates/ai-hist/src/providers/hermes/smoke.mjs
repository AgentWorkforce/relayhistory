// Run after rebuilding worktree native addon and SDK. Uses only synthetic history.
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
const root=fileURLToPath(new URL('../../../../../',import.meta.url));
const require=createRequire(join(root,'sdk-ts/package.json'));
const {Client}=await import(require.resolve('@modelcontextprotocol/sdk/client/index.js'));
const {StdioClientTransport}=await import(require.resolve('@modelcontextprotocol/sdk/client/stdio.js'));
const home=mkdtempSync(join(tmpdir(),'hermes-sdk-'));
const saved={...process.env};
try {
  for(const key of Object.keys(process.env)) if(/^(HOME|USERPROFILE|CLAUDE_CONFIG_DIR|CODEX_HOME|GROK_HOME|HERMES_HOME|PI_CODING_AGENT_DIR|XDG_|OPENCODE_|AI_HIST_|RELAYHISTORY_)/.test(key)) delete process.env[key];
  Object.assign(process.env,{HOME:home,USERPROFILE:home,HERMES_HOME:join(home,'.hermes'),AI_HIST_DB:join(home,'ledger.db'),RELAYHISTORY_HOME:join(home,'relayhistory'),RELAYHISTORY_NO_UPDATE_CHECK:'1'});
  mkdirSync(join(home,'.hermes'));
  execFileSync('python3',['-c','import sqlite3,sys; c=sqlite3.connect(sys.argv[1]);c.executescript(sys.stdin.read());c.close()',join(home,'.hermes/state.db')],{input:readFileSync(join(root,'crates/ai-hist/tests/fixtures/hermes/schema31.sql')),env:process.env});
  const sdk=await import(new URL('../../../../../sdk-ts/dist/index.js',import.meta.url));
  const id='default:636861742f6e6f742d612d757569643aceb1';
  await sdk.discoverSessions({sources:['hermes'],scope:'local'});
  await sdk.hydrateSession({source:'hermes',sessionId:id});
  const rows=await sdk.search('needle',{source:'hermes'});
  assert.ok(rows.some(r=>r.source==='hermes'&&r.role==='user'));
  assert.ok(rows.some(r=>r.source==='hermes'&&r.role==='assistant'));
  assert.deepEqual(await sdk.search('needle',{source:'claude'}),[]);
  const caps=await sdk.getSourceCapabilities('hermes');assert.equal(caps.source,'hermes');
  const cli=JSON.parse(execFileSync(process.execPath,[join(root,'sdk-ts/dist/cli.js'),'search','needle','--source','hermes','--role','assistant','--json','--no-warning'],{env:process.env,encoding:'utf8'}));
  assert.equal(cli.length,1);
  const transport=new StdioClientTransport({command:process.execPath,args:[join(root,'sdk-ts/dist/mcp-server.js')],env:process.env,stderr:'pipe'});
  const client=new Client({name:'hermes-fixture',version:'1'});
  try {
    await client.connect(transport);
    const result=await client.callTool({name:'search_history',arguments:{query:'needle',source:'hermes'}});
    assert.equal(result.isError,undefined);
    assert.ok(JSON.parse(result.content[0].text).some(r=>r.role==='assistant'&&r.source==='hermes'));
    const cap=await client.callTool({name:'get_source_capabilities',arguments:{source:'hermes'}});
    assert.equal(cap.isError,undefined);assert.equal(JSON.parse(cap.content[0].text).source,'hermes');
  } finally {await client.close();await transport.close();}
  console.log('Hermes SDK, CLI, MCP search and capability smoke passed');
} finally {
  for(const key of Object.keys(process.env)) if(!(key in saved)) delete process.env[key];Object.assign(process.env,saved);
  rmSync(home,{recursive:true,force:true});
}
