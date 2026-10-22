import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ROOT, descriptors, generate, outputs, markedSection } from './generate-history-providers.mjs';

function sandbox(t, extraFiles = false) {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'history-provider-generator-'));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  for(const dir of ['providers','crates/ai-hist/src/providers']) fs.cpSync(path.join(ROOT,dir),path.join(root,dir),{recursive:true});
  for(const file of ['sdk-ts/src/sdk-common.ts','crates/ai-hist/tests/fixtures/README.md','docs/decisions/2026-09-19-relayhistory-owns-session-sourcing.md','docs/sourcing-sdk.md']) {
    fs.mkdirSync(path.dirname(path.join(root,file)),{recursive:true});fs.copyFileSync(path.join(ROOT,file),path.join(root,file));
  }
  if (extraFiles) for (const directory of ['providers/history-sources','providers/test-sources']) fs.writeFileSync(path.join(root,directory,'README.md'),'descriptor notes');
  for(const directory of ['providers/history-sources','providers/test-sources']) {
    for(const name of fs.readdirSync(path.join(root,directory)).filter(name=>name.endsWith('.json'))) {
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

test('generation markers reject missing, duplicate and reversed boundaries', () => {
  for (const text of ['BEGIN body', 'END body', 'END BEGIN', 'BEGIN BEGIN END', 'BEGIN END END']) {
    assert.throws(() => markedSection(text, 'BEGIN', 'END', 'new', true), /Invalid generation markers/);
  }
  assert.equal(markedSection('prefix BEGIN old END suffix', 'BEGIN', 'END', 'new'), 'prefix BEGIN\nnew\nEND suffix');
});

test('population declarations reject unknown evidence and accounting', t => {
  const root = sandbox(t);
  change(root, 'claude', d => d.population.evidence.push('future-kind'));
  assert.throws(() => generate(root), /Invalid population declaration/);
  change(root, 'claude', d => { d.population.evidence.pop(); d.population.usage_accounting = 'invented'; });
  assert.throws(() => generate(root), /Invalid population declaration/);
});

test('sandbox ignores non-JSON descriptor-directory documentation',t=>{
  const root=sandbox(t,true);
  assert.ok(descriptors(root).length);
});

// Compile the generated ordinal tables against distinct metadata sentinels.
// Both cfg modes matter: a test-only variant must not shift production entries.
test('source names, aliases and metadata agree with enum ordinals in both cfg modes', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'history-source-tables-'));
  t.after(() => fs.rmSync(dir, {recursive:true, force:true}));
  const items = descriptors();
  const generated = outputs().get('crates/ai-hist/src/session_store/generated_sources.rs')
    .replace('use serde::{Deserialize, Serialize};', '')
    .replace(', Serialize, Deserialize', '')
    .replace(/    #\[serde\(rename = "[^"\n]+"\)\]\n/g, '');
  const providers = items.map((item, index) =>
    `pub mod ${item.rust_id} { pub const METADATA: super::ProviderMetadata = super::ProviderMetadata(${index}); }`).join('\n');
  for (const testMode of [false, true]) {
    const assertions = items.filter(item => testMode || !item.testOnly).flatMap((item) => [
      `assert_eq!(Source::${item.variant}.as_str(), ${JSON.stringify(item.id)});`,
      `assert_eq!(Source::${item.variant}.metadata().0, ${items.indexOf(item)});`,
      ...[item.id, ...item.aliases].map(name => `assert_eq!(Source::parse(${JSON.stringify(name)}), Some(Source::${item.variant}));`),
    ]).join('\n');
    const input = path.join(dir, 'main.rs');
    const binary = path.join(dir, (testMode ? 'with-test-source' : 'production') + (process.platform === 'win32' ? '.exe' : ''));
    fs.writeFileSync(input, `mod providers { pub struct ProviderMetadata(pub usize); ${providers} }\n${generated}\nfn main() { ${assertions} assert_eq!(Source::parse("unknown-source"), None); }`);
    execFileSync('rustc', ['--edition', '2021', '--cap-lints', 'allow', ...(testMode ? ['--cfg', 'test'] : []), input, '-o', binary]);
    execFileSync(binary);
  }
});
