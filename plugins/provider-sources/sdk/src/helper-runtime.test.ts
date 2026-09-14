import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { helperRequest, terminateHelperTree } from './helper.js';
import { createHistoryPlugin } from './index.js';

const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function fixture(markerDelay: number) {
  const directory = await mkdtemp(join(tmpdir(), 'history-helper-tree-'));
  const binary = join(directory, 'helper');
  const pidFile = join(directory, 'descendant.pid');
  const marker = join(directory, 'continued');
  const code = `require('node:fs').writeFileSync(${JSON.stringify(pidFile)},String(process.pid));setTimeout(()=>require('node:fs').writeFileSync(${JSON.stringify(marker)},'continued'),${markerDelay});setTimeout(()=>{},10000);`;
  const source = `require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(code)}],{stdio:'ignore'});setTimeout(()=>{},10000);`;
  await writeFile(binary, `#!${process.execPath}\n${source}\n`);
  await chmod(binary, 0o700);
  const pid = async () => { for (let attempt=0;attempt<200;attempt++) { try {return Number(await readFile(pidFile,'utf8'));} catch {await pause(10);} } throw new Error('descendant did not start'); };
  return {binary,marker,pid,source,async close(){try{process.kill(Number(await readFile(pidFile,'utf8')),'SIGKILL');}catch{}await rm(directory,{recursive:true,force:true});}};
}

test('cancellation kills the helper descendant but leaves an unrelated process alone', {skip:process.platform==='win32'}, async()=>{
  const files=await fixture(300);
  const unrelated=spawn(process.execPath,['-e','setTimeout(()=>{},10000)'],{stdio:'ignore'});
  const controller=new AbortController();
  try {
    const rejected=assert.rejects(helperRequest('fixture',{}, {binaryPath:files.binary,signal:controller.signal,timeoutMs:5000}),{code:'HISTORY_PLUGIN_CANCELLED'});
    await files.pid();controller.abort();await rejected;
    await pause(450);
    await assert.rejects(readFile(files.marker),{code:'ENOENT'});
    assert.ok(unrelated.pid);assert.doesNotThrow(()=>process.kill(unrelated.pid!,0));
  } finally {unrelated.kill('SIGKILL');await files.close();}
});

test('deadline kills the helper descendant before returning timeout', {skip:process.platform==='win32'}, async()=>{
  const files=await fixture(700);
  try {
    await assert.rejects(helperRequest('fixture',{}, {binaryPath:files.binary,timeoutMs:500}),{code:'HISTORY_PLUGIN_TIMEOUT'});
    await pause(600);
    await assert.rejects(readFile(files.marker),{code:'ENOENT'});
  } finally {await files.close();}
});

test('missing helper still reports unavailable instead of cleanup failure',async()=>{
  await assert.rejects(helperRequest('fixture',{}, {binaryPath:join(tmpdir(),'missing-history-helper-fixture-9d5409'),timeoutMs:1000}),{code:'HISTORY_PLUGIN_BINARY_MISSING'});
});

// This exercises taskkill /T on Windows too, without requiring a shebang fixture.
test('tree cleanup terminates a real descendant using the platform implementation',async()=>{
  const files=await fixture(300);
  const child=spawn(process.execPath,['-e',files.source],{stdio:'ignore',windowsHide:true,detached:process.platform!=='win32'});
  const unrelated=spawn(process.execPath,['-e','setTimeout(()=>{},10000)'],{stdio:'ignore'});
  let closed=false;const childClosed=new Promise<void>(resolve=>child.once('close',()=>{closed=true;resolve();}));
  try {
    await files.pid();await terminateHelperTree(child,childClosed,()=>closed);
    assert.equal(closed,true);await pause(450);
    await assert.rejects(readFile(files.marker),{code:'ENOENT'});
    assert.ok(unrelated.pid);assert.doesNotThrow(()=>process.kill(unrelated.pid!,0));
  } finally {child.kill('SIGKILL');unrelated.kill('SIGKILL');await files.close();}
});

test('source operation budgets reach both provider helper calls', { skip: process.platform === 'win32' }, async (t) => {
  const directory = await mkdtemp(join(tmpdir(), 'history-source-budget-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const binary = join(directory, 'helper');
  await writeFile(binary, `#!${process.execPath}\nlet input='';process.stdin.on('data',c=>input+=c);process.stdin.on('end',()=>setTimeout(()=>{const req=JSON.parse(input);process.stdout.write(JSON.stringify({version:1,ok:true,value:req.operation==='discover'?{observations:[]}:{source_stamp:'fixture',source_bytes:0,covered_kinds:[],records:[]}}));},100));\n`);
  await chmod(binary, 0o700);
  // The per-operation budget must override the helper's otherwise tiny limit.
  const source = createHistoryPlugin({ binaryPath: binary, timeoutMs: 1 }).sources![0];
  assert.deepEqual(await source.discover({ acquisitionTimeoutMs: 2000 }), { observations: [] });
  const snapshot = await source.hydrate({ key: { source: 'claude', session_id: 's', location: 'remote', connector_id: source.id, connector_instance: source.instanceId }, raw_locator: null, source_stamp: null, discovery_state: 'shallow', access_state: 'available', updated_ms: 0 }, { acquisitionTimeoutMs: 2000 });
  assert.deepEqual(snapshot.records, []);
});
