import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { helperRequest, terminateHelperTree } from './helper.js';
import { createHistoryPlugin } from './index.js';

const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function fixture(markerDelay: number, lifetime = 10_000) {
  const directory = await mkdtemp(join(tmpdir(), 'history-helper-tree-'));
  const binary = join(directory, 'helper');
  const pidFile = join(directory, 'descendant.pid');
  const marker = join(directory, 'continued');
  const code = `require('node:fs').writeFileSync(${JSON.stringify(pidFile)},String(process.pid));setTimeout(()=>require('node:fs').writeFileSync(${JSON.stringify(marker)},'continued'),${markerDelay});setTimeout(()=>{},${lifetime});`;
  const source = `require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(code)}],{stdio:'ignore'});setTimeout(()=>{},${lifetime});`;
  await writeFile(binary, `#!${process.execPath}\n${source}\n`);
  await chmod(binary, 0o700);
  const pid = async () => { for (let attempt=0;attempt<200;attempt++) { try {return Number(await readFile(pidFile,'utf8'));} catch {await pause(10);} } throw new Error('descendant did not start'); };
  return {binary,marker,pid,source,async close(){try{process.kill(Number(await readFile(pidFile,'utf8')),'SIGKILL');}catch{}await rm(directory,{recursive:true,force:true});}};
}

/** Why `pid` still looks alive, or '' once it is gone. A descendant killed
 * together with its parent is reparented and lingers as a zombie until
 * something reaps it, and `process.kill(pid,0)` keeps succeeding for that
 * zombie - but a zombie has been terminated, which is the property under test. */
function aliveReason(pid: number): string {
  try { process.kill(pid,0); }
  catch (error) { const code=(error as NodeJS.ErrnoException).code; return code==='ESRCH'?'':`kill(0) failed with ${code}`; }
  try {
    if (process.platform==='linux') { const stat=readFileSync(`/proc/${pid}/stat`,'utf8'); return stat.slice(stat.lastIndexOf(')')+2).startsWith('Z')?'':'running'; }
    if (process.platform==='darwin') return spawnSync('ps',['-o','state=','-p',String(pid)]).stdout.toString().trim().startsWith('Z')?'':'running';
  } catch { /* no readable process state: trust the kill(0) probe above */ }
  return 'running';
}
const isRunning = (pid: number) => aliveReason(pid)!=='';
/** Tearing down a tree is asynchronous - on Windows it is a spawned taskkill -
 * so wait for the descendant to actually go away rather than assuming some
 * fixed delay covers it. Bounded, and loud about the last state it saw. */
async function waitForExit(pid: number, budgetMs: number): Promise<void> {
  const deadline=Date.now()+budgetMs;
  for (let reason=aliveReason(pid);reason!=='';reason=aliveReason(pid)) {
    if (Date.now()>=deadline) throw new Error(`descendant ${pid} survived tree cleanup for ${budgetMs}ms (${reason})`);
    await pause(25);
  }
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
// The property is that the descendant is terminated and an unrelated process is
// not, so assert liveness directly instead of inferring termination from a
// marker the descendant would have written. The kill is issued ~10-20ms after
// the descendant starts, so a loaded Windows runner that needs longer than the
// marker delay just to spawn taskkill /T failed a correct cleanup.
// Timing budget: terminateHelperTree bounds the Windows taskkill at 2s and the
// whole cleanup at 3s (past that it rejects, failing this test), and the wait
// below is bounded at 5s, so any run that reaches the marker check is at most
// ~8s in - far inside the 15s marker delay, which therefore cannot fire in a
// passing run. The descendant lives 30s so the marker would in fact be written
// were the tree never killed.
test('tree cleanup terminates a real descendant using the platform implementation',async()=>{
  const files=await fixture(15_000,30_000);
  const child=spawn(process.execPath,['-e',files.source],{stdio:'ignore',windowsHide:true,detached:process.platform!=='win32'});
  const unrelated=spawn(process.execPath,['-e','setTimeout(()=>{},30000)'],{stdio:'ignore'});
  let closed=false;const childClosed=new Promise<void>(resolve=>child.once('close',()=>{closed=true;resolve();}));
  try {
    const descendant=await files.pid();
    await terminateHelperTree(child,childClosed,()=>closed);
    assert.equal(closed,true);
    await waitForExit(descendant,5_000);
    await assert.rejects(readFile(files.marker),{code:'ENOENT'});
    assert.ok(unrelated.pid);assert.ok(isRunning(unrelated.pid!),'an unrelated process must survive tree cleanup');
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
