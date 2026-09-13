/** Server conformance harness. Only scheduler inspection is replaced; every
 * storage, plugin preparation, HTTP send, receipt and read uses production code.
 * Stdin: {operation,dbPath,baseUrl,helperPath,jobId?,sessionId?}. Stdout: one JSON.
 */
import { mkdtemp, chmod, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import * as history from 'ai-hist';
import { createHistoryPlugin, deliveryAccount, relayHistoryInstance, getDeliveredSession } from '../dist/plugin.js';
let input='';for await(const chunk of process.stdin)input+=chunk;
const request=JSON.parse(input);
const temporary=await mkdtemp(join(tmpdir(),'rh-composed-runner-'));
try {
  const wrapper=join(temporary,'helper');
  await writeFile(wrapper,`#!${process.execPath}\nconst {spawn}=require('node:child_process');let input='';process.stdin.on('data',chunk=>input+=chunk);process.stdin.on('end',()=>{const req=JSON.parse(input);if(req.operation==='deliveryMigrationStatus'){process.stdout.write(JSON.stringify({version:1,ok:true,value:{state:'clear',jobs:['fixture-scheduler-inspection']}}));return;}const child=spawn(${JSON.stringify(request.helperPath)},[],{stdio:['pipe','inherit','inherit']});child.on('error',()=>process.exit(1));child.on('close',code=>process.exit(code??1));child.stdin.end(input);});\n`);
  await chmod(wrapper,0o700);
  const options={baseUrl:request.baseUrl,binaryPath:wrapper,instanceId:'composed-fixture',acknowledgeUninspectedLegacySchedules:true};
  const registry=new history.HistoryPluginRegistry();registry.register(createHistoryPlugin(options));
  let result;
  if(request.operation==='enable') {
    const source={id:'composed-fixture',instanceId:'source',location:'remote',supportedSources:['claude'],
      discover:async()=>({observations:[{source:'claude',session_id:request.sessionId??'composed-session',source_stamp:'fixture-listing'}]}),
      hydrate:async()=>({source_stamp:'fixture-full',source_bytes:64,covered_kinds:['session_event'],records:[{kind:'session_event',payload:{source:'claude',session_id:request.sessionId??'composed-session',event_uid:'composed-event',ts_ms:1,role:'assistant',kind:'text',text:'composed dependable delivery'}}]})};
    const sources=new history.HistoryPluginRegistry();sources.register({sources:[source]});
    await history.hydrateSession({source:'claude',sessionId:request.sessionId??'composed-session',scope:'remote',plugins:sources,dbPath:request.dbPath});
    result=await history.createHistoryDelivery({destination_id:'relayhistory',instance_id:relayHistoryInstance(options),account_id:await deliveryAccount(options),mapping_version:'relayhistory-delivery-v1',selection:{all_sources:false,sources:['claude'],sessions:[],kinds:['session_event'],excluded_sessions:[]},limits:history.DEFAULT_DELIVERY_LIMITS},{dbPath:request.dbPath});
  } else if(request.operation==='drain') {
    if(request.retry)await history.controlHistoryDelivery(request.jobId,'retry',{dbPath:request.dbPath});
    result=await history.drainHistoryDelivery(registry,{dbPath:request.dbPath,jobIds:[request.jobId],requestTimeoutMs:3000,leaseMs:10000});
  } else if(request.operation==='read')result=await getDeliveredSession({source:'claude',sessionId:request.sessionId??'composed-session'},options);
  else throw new Error('unknown fixture operation');
  process.stdout.write(JSON.stringify(result)+'\n');
} finally {await rm(temporary,{recursive:true,force:true});}
