// Run pinned upstream code with synthetic inputs; no CLI, auth, network or model.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import vm from 'node:vm';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
const require = createRequire(import.meta.url);
const args = Object.fromEntries(process.argv.slice(2).reduce((a, value, index, all) => index % 2 ? a : [...a, [value, all[index + 1]]], []));
assert(args['--source'] && args['--typescript'] && args['--output'], 'Pass --source upstream-checkout --typescript typescript-module --output empty-directory');
const ts = require(path.resolve(args['--typescript']));
const source = path.resolve(args['--source']);
const output = path.resolve(args['--output']);
assert(!fs.existsSync(output) || fs.readdirSync(output).length === 0, 'Output must be empty');
fs.mkdirSync(output, {recursive: true});
const core = 'packages/core/src/';
const dependencies = [
  'services/chatRecordingService.ts', 'services/chatRecordingTypes.ts',
  'utils/paths.ts', 'utils/fileUtils.ts', 'utils/sessionUtils.ts', 'utils/partUtils.ts', 'config/storage.ts',
];
const expected = JSON.parse(fs.readFileSync(new URL('./provenance.json', import.meta.url))).sourceSha256;
for (const relative of dependencies) {
  const content = fs.readFileSync(path.join(source, core, relative));
  assert.equal(crypto.createHash('sha256').update(content).digest('hex'), expected[core + relative], `Pinned source drift: ${relative}`);
}
let counter = 0;
let tick = 0;
class FixedDate extends Date {
  constructor(...values) { super(...(values.length ? values : [Date.UTC(2026, 9, 11, 12) + tick++ * 1000])); }
  static now() { return Date.UTC(2026, 9, 11, 12) + tick++ * 1000; }
}
const fakeCrypto = {...crypto, randomUUID: () => `99999999-9999-4999-8999-${String(++counter).padStart(12, '0')}`};
function evaluate(text, imports = {}, environment = {}) {
  const javascript = ts.transpileModule(text, {compilerOptions: {target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, esModuleInterop: true}}).outputText;
  const module = {exports: {}};
  const sandbox = {process: {env: environment}, module, exports: module.exports, Date: FixedDate, WeakRef, Buffer, console, setTimeout, clearTimeout, require: name => {
    if (name in imports) return imports[name];
    if (name === 'node:crypto') return fakeCrypto;
    if (name.startsWith('node:')) return require(name);
    throw new Error(`Unapproved dependency ${name}`);
  }};
  vm.runInNewContext(javascript, sandbox, {filename: 'pinned-gemini-source.cjs'});
  return module.exports;
}
// Extract whole upstream function AST nodes, without rewriting their bodies.
function functions(relative, names, imports = {}) {
  const content = fs.readFileSync(path.join(source, core, relative), 'utf8');
  const tree = ts.createSourceFile(relative, content, ts.ScriptTarget.Latest, true);
  const selected = tree.statements.filter(n => ts.isFunctionDeclaration(n) && names.includes(n.name?.text));
  assert.equal(selected.length, names.length);
  return evaluate(Object.keys(imports).map(name => `import * as crypto from '${name}';`).join('\n') + selected.map(n => n.getText(tree)).join('\n'), imports);
}
const paths = functions('utils/paths.ts', ['getProjectHash'], {'node:crypto': fakeCrypto});
const fileUtils = functions('utils/fileUtils.ts', ['sanitizeFilenamePart']);
const sessionUtils = functions('utils/sessionUtils.ts', ['isIgnoredUserContent']);
const parts = evaluate(fs.readFileSync(path.join(source, core, 'utils/partUtils.ts'), 'utf8'));
const types = evaluate(fs.readFileSync(path.join(source, core, 'services/chatRecordingTypes.ts'), 'utf8'));
const api = evaluate(fs.readFileSync(path.join(source, core, 'services/chatRecordingService.ts'), 'utf8'), {
  '../utils/paths.js': paths,
  '../utils/fileUtils.js': fileUtils,
  '../utils/errors.js': {isNodeError: error => error instanceof Error && 'code' in error},
  '../utils/sessionOperations.js': {deleteSessionArtifactsAsync: () => {throw new Error('Deletion forbidden');}, deleteStoredSession: () => {throw new Error('Deletion forbidden');}},
  '../utils/debugLogger.js': {debugLogger: {error: (...v) => {throw new Error(v.join(' '));}, warn: (...v) => {throw new Error(v.join(' '));}}},
  '../core/geminiRequest.js': {partListUnionToString: value => parts.partToString(value, {verbose: true})},
  '../utils/sessionUtils.js': sessionUtils,
  './chatRecordingTypes.js': types,
});
const rootOracles = {};
const pathContent = fs.readFileSync(path.join(source, core, 'utils/paths.ts'), 'utf8');
const pathAst = ts.createSourceFile('paths.ts', pathContent, ts.ScriptTarget.Latest, true);
const homedirFunction = pathAst.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'homedir').getText(pathAst);
for (const [name, environment] of Object.entries({default: {}, 'custom-home': {GEMINI_CLI_HOME: '/synthetic/custom-home'}, seatbelt: {SANDBOX: 'sandbox-exec'}, 'custom-seatbelt': {GEMINI_CLI_HOME: '/synthetic/custom-home', SANDBOX: 'sandbox-exec'}, 'other-sandbox': {SANDBOX: 'docker'}})) {
  const nativePaths = evaluate("import * as os from 'node:os'; export const GEMINI_DIR='.gemini';\n" + homedirFunction, {'node:os': {...os, homedir: () => '/synthetic/native-home'}}, environment);
  const {Storage} = evaluate(fs.readFileSync(path.join(source, core, 'config/storage.ts'), 'utf8'), {
    '../utils/paths.js': nativePaths,
    './projectRegistry.js': {ProjectRegistry: class {constructor() {throw new Error('Registry mutation forbidden in root probe');}}},
    './storageMigration.js': {StorageMigration: {}},
  }, environment);
  const storage = new Storage('/synthetic/gemini-project');
  storage.projectIdentifier = 'gemini-project'; // Config boundary: registry slug is injected, not generated.
  rootOracles[name] = {environment, runtime: Storage.getGlobalRuntimeDir(), temporary: Storage.getGlobalTempDir(), project: storage.getProjectTempDir()};
}
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'gemini-research-'));
const projectRoot = '/synthetic/gemini-project';
const tempDir = path.join(temp, 'home/.gemini/tmp/gemini-project');
const mainId = '11111111-1111-4111-8111-111111111111';
const childId = '33333333-3333-4333-8333-333333333333';
const context = (id, parentSessionId) => ({promptId: id, parentSessionId, config: {getProjectRoot: () => projectRoot, storage: {getProjectTempDir: () => tempDir}, getWorkspaceContext: () => ({getDirectories: () => [projectRoot]})}, toolRegistry: {getTool: () => undefined}});
const save = (relative, value) => { const target = path.join(output, relative); fs.mkdirSync(path.dirname(target), {recursive: true}); fs.writeFileSync(target, value); };
const oracles = {};
async function captureCase(name, nativePath) {
  const relative = path.relative(path.join(temp, 'home'), nativePath);
  save(`cases/${name}/${relative}`, fs.readFileSync(nativePath));
  oracles[name] = JSON.parse(JSON.stringify(await api.loadConversationRecord(nativePath)));
  assert(oracles[name]);
}
try {
  const recorder = new api.ChatRecordingService(context(mainId));
  await recorder.initialize(undefined, 'main');
  recorder.recordMessage({type: 'user', content: 'Find the synthetic Gemini needle', id: 'user-one'});
  recorder.recordThought({subject: 'Search', description: 'Inspect the synthetic fixture'});
  recorder.recordMessage({type: 'gemini', content: [{text: 'Searching the fixture.'}], model: 'gemini-fixture-model', id: 'assistant-one'});
  recorder.recordMessageTokens({promptTokenCount: 120, candidatesTokenCount: 20, cachedContentTokenCount: 30, thoughtsTokenCount: 4, toolUsePromptTokenCount: 2, totalTokenCount: 146});
  const call = {id: 'call-real-id', name: 'read_file', args: {path: '/synthetic/needle.txt'}, timestamp: new FixedDate().toISOString(), status: 'executing'};
  recorder.recordToolCalls('gemini-fixture-model', [call]);
  recorder.recordToolCalls('gemini-fixture-model', [{...call, status: 'success', result: [{functionResponse: {id: call.id, name: call.name, response: {output: 'synthetic needle result'}}}]}]);
  recorder.recordMessage({type: 'user', content: [{functionResponse: {id: call.id, name: call.name, response: {output: 'synthetic needle result'}}}], id: 'tool-response'});
  recorder.recordMessage({type: 'gemini', content: 'Found the synthetic Gemini needle.', model: 'gemini-fixture-model', id: 'assistant-two'});
  recorder.recordMessage({type: 'info', content: 'Synthetic status notification', id: 'info-one'});
  const nativePath = recorder.conversationFile;
  await captureCase('ordinary', nativePath);
  const collision = new api.ChatRecordingService(context('11111111-2222-4222-8222-222222222222'));
  await collision.initialize(undefined, 'main');
  collision.recordMessage({type: 'user', content: 'Different full UUID with the same short prefix.', id: 'collision-user'});
  await captureCase('short-prefix-collision', collision.conversationFile);
  const history = recorder.getConversation().messages.map(message => ({id: message.id, content: {role: message.type === 'gemini' ? 'model' : 'user', parts: typeof message.content === 'string' ? [{text: message.content}] : message.content}}));
  history.find(turn => turn.id === 'tool-response').content.parts[0].functionResponse.response.output = 'synthetic masked result';
  recorder.updateMessagesFromHistory(history);
  await captureCase('tool-result-patched', nativePath);
  const resumed = new api.ChatRecordingService(context('different-process-prompt-id'));
  await resumed.initialize({filePath: nativePath, conversation: await api.loadConversationRecord(nativePath)});
  assert.equal(resumed.getIsResumedSession(), true);
  resumed.recordMessage({type: 'user', content: 'Continue after synthetic restart.', id: 'user-resumed'});
  resumed.recordMessage({type: 'gemini', content: 'The prior synthetic answer is preserved.', id: 'assistant-resumed', model: 'gemini-fixture-model'});
  await captureCase('resumed', nativePath);
  // Exercise the same recorder mutation API called by GeminiChat.setHistory.
  // No live compressor/model invocation: these summary turns are synthetic inputs.
  resumed.updateMessagesFromHistory([
    {id: 'summary-control', content: {role: 'user', parts: [{text: '<state_snapshot>Synthetic context summary</state_snapshot>'}]}},
    {id: 'assistant-resumed', content: {role: 'model', parts: [{text: 'Response after synthetic compaction.'}]}},
  ]);
  resumed.saveSummary('Synthetic compacted session');
  await captureCase('compacted', nativePath);
  resumed.recordMessage({type: 'user', content: 'This synthetic turn will be undone.', id: 'user-undone'});
  resumed.recordMessage({type: 'gemini', content: 'This synthetic answer will be undone.', id: 'assistant-undone'});
  resumed.rewindTo('user-undone');
  await captureCase('rewound', nativePath);
  const child = new api.ChatRecordingService(context(childId, mainId));
  await child.initialize(undefined, 'subagent');
  child.recordMessage({type: 'user', content: 'Investigate the synthetic child task.', id: 'child-user'});
  child.recordMessage({type: 'gemini', content: 'Synthetic child task finished.', id: 'child-assistant', model: 'gemini-fixture-model'});
  await captureCase('child', child.conversationFile);
  // Legacy JSON is a synthetic ConversationRecord serializer, not an old writer capture.
  const legacy = {...oracles.ordinary, sessionId: '22222222-2222-4222-8222-222222222222'};
  for (const key of ['messageCount', 'userMessageCount', 'firstUserMessage', 'hasResumableContent', 'memoryScratchpadIsStale']) delete legacy[key];
  const legacyPath = path.join(tempDir, 'chats/session-legacy-22222222.json');
  fs.writeFileSync(legacyPath, JSON.stringify(legacy, null, 2));
  await captureCase('legacy', legacyPath);
  const migrated = new api.ChatRecordingService(context('new-process'));
  await migrated.initialize({filePath: legacyPath, conversation: await api.loadConversationRecord(legacyPath)});
  migrated.recordMessage({type: 'user', content: 'Continue the synthetic legacy session.', id: 'legacy-resumed'});
  await captureCase('legacy-migrated', migrated.conversationFile);
  save('root-oracle.json', JSON.stringify(rootOracles, null, 2) + '\n');
  save('official-reader.json', JSON.stringify(oracles, null, 2) + '\n');
  console.log(`Captured ${Object.keys(oracles).length} synthetic cases with pinned upstream writer and official reader`);
} finally { fs.rmSync(temp, {recursive: true, force: true}); }
