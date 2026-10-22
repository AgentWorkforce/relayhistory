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
const home=mkdtempSync(join(tmpdir(),'goose-sdk-'));
const saved={...process.env};
try {
  for(const key of Object.keys(process.env)) if(/^(HOME|USERPROFILE|CLAUDE_CONFIG_DIR|CODEX_HOME|GROK_HOME|GOOSE_PATH_ROOT|PI_CODING_AGENT_DIR|XDG_|OPENCODE_|AI_HIST_|RELAYHISTORY_)/.test(key)) delete process.env[key];
  Object.assign(process.env,{HOME:home,USERPROFILE:home,GOOSE_PATH_ROOT:join(home,'goose-root'),AI_HIST_DB:join(home,'ledger.db'),RELAYHISTORY_HOME:join(home,'relayhistory'),RELAYHISTORY_NO_UPDATE_CHECK:'1'});
  const sessionDir=join(home,'goose-root/data/sessions');mkdirSync(sessionDir,{recursive:true});
  execFileSync('python3',['-c','import sqlite3,sys; c=sqlite3.connect(sys.argv[1]);c.executescript(sys.stdin.read());c.close()',join(sessionDir,'sessions.db')],{input:readFileSync(join(root,'crates/ai-hist/tests/fixtures/goose/schema17.sql')),env:process.env});
  const sdk=await import(new URL('../../../../../sdk-ts/dist/index.js',import.meta.url));
  const discovery=await sdk.discoverSessions({sources:['goose'],scope:'local'});
  const session=discovery.sessions.find(s=>s.sessionId.endsWith(':32303236313032325f31'));
  assert.ok(session, 'native fixture session must be discovered');
  const id=session.sessionId;
  assert.match(id,/^root:/, 'explicit GOOSE_PATH_ROOT retains custom-root identity');
  await sdk.hydrateSession({source:'goose',sessionId:id});
  const events=await sdk.getSessionEventsPage(id,{source:'goose',includeText:true});
  const calls=await sdk.getSessionToolCallsPage('goose',id);
  assert.equal(calls.toolCalls[0].toolUseId,'call-native-1');
  const linked=events.events.filter(e=>e.kind==='tool_use'||e.kind==='tool_result');
  assert.equal(linked.length,2);
  assert.ok(linked.every(e=>e.toolUseId===calls.toolCalls[0].toolUseId));
  const rows=await sdk.search('needle',{source:'goose'});
  assert.ok(rows.some(r=>r.source==='goose'&&r.role==='user'));
  assert.ok(rows.some(r=>r.source==='goose'&&r.role==='assistant'));
  assert.deepEqual(await sdk.search('needle',{source:'claude'}),[]);
  const caps=await sdk.getSourceCapabilities('goose');assert.equal(caps.source,'goose');
  const cli=JSON.parse(execFileSync(process.execPath,[join(root,'sdk-ts/dist/cli.js'),'search','needle','--source','goose','--role','assistant','--json','--no-warning'],{env:process.env,encoding:'utf8'}));
  assert.equal(cli.length,1);
  const transport=new StdioClientTransport({command:process.execPath,args:[join(root,'sdk-ts/dist/mcp-server.js')],env:process.env,stderr:'pipe'});
  const client=new Client({name:'goose-fixture',version:'1'});
  try {
    await client.connect(transport);
    const result=await client.callTool({name:'search_history',arguments:{query:'needle',source:'goose'}});
    assert.equal(result.isError,undefined);
    assert.ok(JSON.parse(result.content[0].text).some(r=>r.role==='assistant'&&r.source==='goose'));
    const cap=await client.callTool({name:'get_source_capabilities',arguments:{source:'goose'}});
    assert.equal(cap.isError,undefined);assert.equal(JSON.parse(cap.content[0].text).source,'goose');
  } finally {await client.close();await transport.close();}
  console.log('Goose SDK, CLI, MCP search and capability smoke passed');
} finally {
  for(const key of Object.keys(process.env)) if(!(key in saved)) delete process.env[key];Object.assign(process.env,saved);
  rmSync(home,{recursive:true,force:true});
}
