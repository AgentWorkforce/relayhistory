// Execute pinned official recorder/resolver and reader modules offline.
// Inputs are fetched separately at the exact revision in provenance.json.
const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
const source=process.env.QWEN_CAPTURE_SOURCE;if(!source)throw new Error('Set QWEN_CAPTURE_SOURCE to the checksum-verified pinned source directory');
if(!process.env.TYPESCRIPT_PATH)throw new Error('Set TYPESCRIPT_PATH to an existing TypeScript compiler module');const ts=require(process.env.TYPESCRIPT_PATH);
if(!process.argv[2])throw new Error('Pass a fresh synthetic capture output directory');const out=path.resolve(process.argv[2]);if(fs.existsSync(out)&&fs.readdirSync(out).length)throw new Error('Capture output must be empty');fs.mkdirSync(out,{recursive:true});
const provenance=require('./provenance.json');for(const [relative,digest]of Object.entries(provenance.sources)){const bytes=fs.readFileSync(path.join(source,relative.replaceAll('/','__')));if(require('node:crypto').createHash('sha256').update(bytes).digest('hex')!==digest)throw new Error(`Pinned source digest mismatch: ${relative}`);}
process.env.QWEN_DISABLE_AUTO_TITLE='1';
let uuid=0;require('node:crypto').randomUUID=()=>`00000000-0000-4000-8000-${String(++uuid).padStart(12,'0')}`;
const RealDate=Date;let clock=Date.parse('2026-10-11T12:00:00Z');global.Date=class extends RealDate {constructor(...a){super(...(a.length?a:[clock]));}static now(){return clock;}};
const noop=()=>{};const logger={debug:(...a)=>console.error('native-debug',...a),info:noop,warn:noop,error:(...a)=>{throw new Error('Native recorder error '+a.map(String).join(' '));}};
const stubs={
 'utils/debugLogger':{createDebugLogger:()=>logger},
 'utils/gitUtils':{getGitBranch:()=> 'fixture-branch'},
 'agents/runtime/agent-context':{getCurrentAgentId:()=>undefined},
 'tools/tool-result-boundary-diagnostics':{observeToolResultBoundary:noop,toolResultBoundaryArtifact:()=>undefined,toolResultPartDiagnosticValues:()=>[]},
 'services/sessionTitle':{tryGenerateSessionTitle:()=>{throw new Error('Model title call forbidden');}},
};
const loaded=new Map();
function load(rel){rel=rel.replace(/\.js$/,'.ts');if(loaded.has(rel))return loaded.get(rel).exports;const key=rel.replace(/^packages\/core\/src\//,'').replace(/\.ts$/,'');if(stubs[key])return stubs[key];
 const filename=path.join(source,rel.replaceAll('/','__'));if(!fs.existsSync(filename)){return new Proxy({}, {get(_,name){if(name==='__esModule')return false;return (...args)=>{throw new Error(`Uncaptured dependency ${rel}:${String(name)}`)}}});}
 const mod=new Module(filename,module);loaded.set(rel,mod);mod.filename=filename;mod.paths=[];mod.require=(specifier)=>specifier==='async-mutex'?{Mutex:class{constructor(){throw new Error('Legacy async-mutex write path not captured');}}}:specifier.startsWith('node:')?require(specifier):specifier.startsWith('.')?load(path.posix.normalize(path.posix.join(path.posix.dirname(rel),specifier))):require(specifier);
 const compiled=ts.transpileModule(fs.readFileSync(filename,'utf8'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS,esModuleInterop:true}}).outputText;mod._compile(compiled,filename);return mod.exports;}
(async()=>{
 const {Storage}=load('packages/core/src/config/storage.ts');
 const {ChatRecordingService}=load('packages/core/src/services/chatRecordingService.ts');
 const {SessionWriterLease}=load('packages/core/src/services/session-writer-lease.ts');
 const reader=load('packages/core/src/utils/transcript-records.ts');
 const resolver={};process.env.QWEN_HOME='/synthetic/qwen-home';delete process.env.QWEN_RUNTIME_DIR;Storage.setRuntimeBaseDir(undefined);resolver.home=Storage.getRuntimeBaseDir();Storage.setRuntimeBaseDir('/synthetic/configured');resolver.configured=Storage.getRuntimeBaseDir();process.env.QWEN_RUNTIME_DIR='/synthetic/runtime';resolver.env=Storage.getRuntimeBaseDir();resolver.contextWithEnv=Storage.runWithRuntimeBaseDir('/synthetic/context',undefined,()=>Storage.getRuntimeBaseDir());resolver.pinned=Storage.runWithResolvedRuntimeBaseDir('/synthetic/pinned',()=>Storage.getRuntimeBaseDir());delete process.env.QWEN_RUNTIME_DIR;Storage.setRuntimeBaseDir(undefined);
 fs.writeFileSync(path.join(out,'resolver.json'),JSON.stringify(resolver,null,2)+'\n');
 const goldens={},rawRecords={};
 for(const [n,kind] of ['ordinary','tool','resumed','rewind','metadata','same-time'].entries()) {
 const runtime=path.join(out,kind,'.qwen');process.env.QWEN_HOME=runtime;delete process.env.QWEN_RUNTIME_DIR;
 const project='/synthetic/qwen-project';const sid=`11111111-1111-4111-8111-${String(n+1).padStart(12,'0')}`;const storage=new Storage(project);const file=path.join(storage.getProjectDir(),'chats',sid+'.jsonl');
 const config={storage,getResumedSessionData:()=>undefined,getSessionId:()=>sid,getProjectRoot:()=>project,getCliVersion:()=> '0.25.3',getModel:()=> 'fixture-model',getAuthType:()=> 'fixture-auth',getApprovalMode:()=> 'default',isInteractive:()=>false,getExperimentalZedIntegration:()=>false,getContentGeneratorConfig:()=>undefined};
 const make=async(resume)=>{const rec=new ChatRecordingService(config,undefined,true);const lease=await SessionWriterLease.acquire({runtimeBaseDir:runtime,sessionId:sid,transcriptPath:file,processKind:'interactive',qwenVersion:'0.25.3'});rec.activate(lease,resume);return [rec,lease];};
 let [recorder,lease]=await make();
 recorder.recordUserMessage([{text:'Find the synthetic Qwen needle.'}],undefined,undefined,'prompt-one');
 if(kind==='tool') {
 recorder.recordAssistantTurn({model:'fixture-model',message:[{thought:true,text:'Synthetic private reasoning.'},{functionCall:{id:'fixture-call-001',name:'fixture_lookup',args:{value:'blue'}}}]});
 recorder.recordToolResult([{functionResponse:{id:'fixture-call-001',name:'fixture_lookup',response:{result:'blue'}}}],{callId:'fixture-call-001',name:'fixture_lookup',status:'success',resultDisplay:'blue'});
 }
 recorder.recordAssistantTurn({model:'fixture-model',message:[{text:'Found the synthetic Qwen needle.'}],tokens:{promptTokenCount:100,candidatesTokenCount:20,totalTokenCount:120,cachedContentTokenCount:10}});
 await recorder.flush();
 if(kind==='resumed') {
 await lease.release();const messages=fs.readFileSync(file,'utf8').trim().split('\n').map(JSON.parse);[recorder,lease]=await make({conversation:{messages},lastCompletedUuid:messages.at(-1).uuid});
 recorder.recordUserMessage([{text:'Continue the synthetic investigation.'}]);recorder.recordAssistantTurn({model:'fixture-model',message:[{text:'The resumed investigation is complete.'}]});
 }
 if(kind==='rewind') {
 recorder.rewindRecording(0,{truncatedCount:2});recorder.recordUserMessage([{text:'Replacement synthetic Qwen request.'}]);recorder.recordAssistantTurn({model:'fixture-model',message:[{text:'Replacement synthetic answer.'}]});
 }
 if(kind==='metadata') {await recorder.recordCustomTitle('Synthetic Qwen title.','manual');await recorder.recordSessionSourcesSnapshot({version:1,revision:1,sources:[]});await recorder.recordSessionArtifactSnapshot({v:2,sessionId:sid,sequence:2,recordedAt:new Date().toISOString(),artifacts:[],tombstonedIds:[],stickyEphemeralIds:[]});}
 if(kind==='same-time') {for(let i=1;i<=6;i++){recorder.recordUserMessage([{text:`Synthetic request ${i}.`}]);recorder.recordAssistantTurn({model:'fixture-model',message:[{text:`Synthetic answer ${i}.`}]});}}
 await recorder.flush();await lease.release();
 const records=fs.readFileSync(file,'utf8').trim().split('\n').map(JSON.parse);rawRecords[kind]=records;goldens[kind]=reader.prepareTranscriptRecords(records);console.log(kind,records.length);
 }
 const derived=structuredClone(rawRecords.ordinary),fragment=structuredClone(derived[1]);fragment.message.parts=[{text:'Additional fragment.'}];fragment.timestamp='2026-10-11T12:00:01.000Z';fragment.usageMetadata={promptTokenCount:200,candidatesTokenCount:30,totalTokenCount:230};derived.push(fragment);
 const artifact=structuredClone(rawRecords.metadata.at(-1));artifact.sessionId=derived[0].sessionId;derived.push(artifact);
 fs.writeFileSync(path.join(out,'derived-reader.json'),JSON.stringify({rows:derived,reader:reader.prepareTranscriptRecords(derived)},null,2)+'\n');
 fs.writeFileSync(path.join(out,'official-reader.json'),JSON.stringify(goldens,null,2)+'\n');
})().catch(e=>{console.dir(e,{depth:8});process.exitCode=1;});
