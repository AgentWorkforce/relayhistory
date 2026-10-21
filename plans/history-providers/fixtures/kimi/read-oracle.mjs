// Executes the pinned official restart fold; never imports RelayHistory.
import fs from 'node:fs';import path from 'node:path';import{pathToFileURL}from'node:url';
const {flattenChain}=await import(pathToFileURL(process.env.KIMI_SOURCE+'/packages/agent-core-v2/src/wire/tree/fork.ts'));
const {foldWireRecords}=await import(pathToFileURL(process.env.KIMI_SOURCE+'/packages/agent-core-v2/src/agent/replayBuilder/fold.ts'));
for(const name of ['ordinary','tool','resumed','subagent','rewound']){const root=path.join(process.argv[2],name);const wires=fs.readdirSync(path.join(root,'home/.kimi-code/sessions'),{recursive:true}).filter(p=>p.endsWith('/wire.jsonl'));const result={};for(const file of wires){const records=fs.readFileSync(path.join(root,'home/.kimi-code/sessions',file),'utf8').trim().split('\n').map(JSON.parse);result[path.basename(path.dirname(file))]=foldWireRecords(flattenChain(records));}fs.writeFileSync(path.join(root,'official-reader.json'),JSON.stringify(result,null,2)+'\n');}
