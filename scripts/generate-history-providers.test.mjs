import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ROOT, descriptors, generate, outputs } from './generate-history-providers.mjs';

function sandbox(t) {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'history-provider-generator-'));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  for(const dir of ['providers','crates/ai-hist/src/providers']) fs.cpSync(path.join(ROOT,dir),path.join(root,dir),{recursive:true});
  for(const file of ['sdk-ts/src/sdk-common.ts','crates/ai-hist/tests/fixtures/README.md','docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md']) {
    fs.mkdirSync(path.dirname(path.join(root,file)),{recursive:true});fs.copyFileSync(path.join(ROOT,file),path.join(root,file));
  }
  for(const directory of ['providers/history-sources','providers/test-sources']) {
    for(const name of fs.readdirSync(path.join(root,directory))) {
      const descriptor=JSON.parse(fs.readFileSync(path.join(root,directory,name),'utf8'));
      for(const fixture of descriptor.fixtures) {
        for(const file of fixture.files) {
          const dest=path.join(root,'crates/ai-hist/tests',directory.endsWith('test-sources')?'provider-fixtures':'fixtures',file);
          fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,'fixture');
        }
        if(!directory.endsWith('test-sources')) {
          const dest=path.join(root,'crates/ai-hist/tests/snapshots',descriptor.id,fixture.name+'.json');
          fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,'{}');
        }
      }
    }
  }
  return root;
}
function change(root,id,fn) {
  const file=path.join(root,'providers/history-sources',id+'.json');
  const item=JSON.parse(fs.readFileSync(file,'utf8'));fn(item);fs.writeFileSync(file,JSON.stringify(item));
}
test('checked-in registration is deterministic and current',()=>{
  generate(ROOT,true);
  assert.deepEqual([...outputs(ROOT)],[...outputs(ROOT)]);
});
test('alias collisions fail before emitting output',t=>{
  const root=sandbox(t);change(root,'claude',d=>d.aliases=['codex']);
  assert.throws(()=>generate(root),/collision: codex/);
});
test('an omitted owned module fails registration',t=>{
  const root=sandbox(t);fs.unlinkSync(path.join(root,'providers/history-sources/codex.json'));
  assert.throws(()=>generate(root),/omitted from registration: codex/);
});
test('missing fixtures and normalized snapshots fail admission',t=>{
  const root=sandbox(t);
  fs.unlinkSync(path.join(root,'crates/ai-hist/tests/fixtures/claude/simple-turn.jsonl'));
  assert.throws(()=>generate(root),/Missing or unowned fixture/);
  fs.writeFileSync(path.join(root,'crates/ai-hist/tests/fixtures/claude/simple-turn.jsonl'),'fixture');
  fs.unlinkSync(path.join(root,'crates/ai-hist/tests/snapshots/claude/simple-turn.json'));
  assert.throws(()=>generate(root),/Missing snapshot/);
});
test('stale generated output fails check and regeneration restores it',t=>{
  const root=sandbox(t);generate(root);
  fs.appendFileSync(path.join(root,'crates/ai-hist/src/providers/generated_registry.rs'),'// drift\n');
  assert.throws(()=>generate(root,true),/Stale provider output/);
  generate(root);generate(root,true);
});
test('test provider is gated everywhere and absent from SDK and corpus',()=>{
  const result=outputs(ROOT);
  const source=result.get('crates/ai-hist/src/session_store/generated_sources.rs');
  assert.match(source,/#\[cfg\(test\)\]\s+#\[serde\(rename = "test-fixture"\)\]/);
  assert.match(result.get('crates/ai-hist/src/providers/generated_modules.rs'),/#\[cfg\(test\)\]\s+pub\(crate\) mod test_fixture/);
  assert.doesNotMatch(result.get('sdk-ts/src/sdk-common.ts'),/test-fixture/);
  assert.doesNotMatch(result.get('crates/ai-hist/tests/generated/provider_corpus.rs'),/test-fixture/);
  assert.deepEqual(descriptors().filter(d=>!d.testOnly).slice(0,7).map(d=>d.id),['claude','codex','cursor','grok','opencode','muse','devin']);
});
