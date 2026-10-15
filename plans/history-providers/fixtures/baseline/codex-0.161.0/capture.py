#!/usr/bin/env python3
"""Native offline Codex persistence probe. Never sends turn/start or model requests."""
import json, os, pathlib, subprocess, sys, tempfile
from rpc_reader import RpcReplies
binary=sys.argv[1] if len(sys.argv)>1 else None
if not binary:raise RuntimeError('Pass the pinned Codex 0.161.0 binary')
output=pathlib.Path(sys.argv[2] if len(sys.argv)>2 else '/private/tmp/rh-codex-offline-capture')
output.mkdir(parents=True,exist_ok=True)
with tempfile.TemporaryDirectory(prefix='rh-codex-offline-') as td:
 home=pathlib.Path(td); agent=home/'codex';agent.mkdir(); work=home/'workspace';work.mkdir(); temp=home/'tmp';temp.mkdir()
 env={'PATH':'/usr/bin:/bin:/usr/sbin:/sbin','TMPDIR':str(temp),'HOME':str(home),'USERPROFILE':str(home),'CODEX_HOME':str(agent),'CODEX_DISABLE_TELEMETRY':'1','RUST_LOG':'off'}
 stderr=open(output/'stderr.log','w')
 proc=subprocess.Popen([binary,'app-server','--listen','stdio://'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,env=env,cwd=work,bufsize=1)
 responses=RpcReplies(proc.stdout)
 transcript=[];idx=0
 def send(method,params,notification=False):
  global idx
  idx+=1;request={'method':method,'params':params}
  if not notification:request['id']=idx
  proc.stdin.write(json.dumps(request)+'\n');proc.stdin.flush();transcript.append({'request':request})
  if notification:return
  return responses.response(idx,25,lambda response:transcript.append({'response':response}))
 try:
  send('initialize',{'clientInfo':{'name':'relayhistory-offline-fixture','version':'1.0.0'},'capabilities':{'experimentalApi':True}})
  send('initialized',{},True)
  result=send('thread/start',{'cwd':str(work),'model':'synthetic-fixture','modelProvider':'synthetic','approvalPolicy':'never','sandbox':'read-only','ephemeral':False,'baseInstructions':'Synthetic offline fixture instructions.','developerInstructions':'Retain synthetic history only.','config':{'model_providers':{'synthetic':{'name':'Offline synthetic','base_url':'http://127.0.0.1:9/v1','wire_api':'responses','requires_openai_auth':False}}}})
  sid=result['thread']['id']
  items=[{'type':'message','role':'user','content':[{'type':'input_text','text':'Inspect the native Codex fixture.'}]},{'type':'message','role':'assistant','content':[{'type':'output_text','text':'Reading the native Codex fixture.'}]},{'type':'function_call','call_id':'offline-call-1','name':'read','arguments':'{"path":"fixture.txt"}'},{'type':'function_call_output','call_id':'offline-call-1','output':'native fixture bytes'},{'type':'message','role':'assistant','content':[{'type':'output_text','text':'The native fixture remains readable.'}]}]
  send('thread/inject_items',{'threadId':sid,'items':items})
  read=send('thread/read',{'threadId':sid,'includeTurns':True})
  (output/'reader.json').write_text(json.dumps(read,indent=2)+'\n')
  turns=send('thread/turns/list',{'threadId':sid,'itemsView':'full','limit':100})
  entries=send('thread/items/list',{'threadId':sid,'limit':100})
  (output/'turns.json').write_text(json.dumps(turns,indent=2)+'\n')
  (output/'items.json').write_text(json.dumps(entries,indent=2)+'\n')
  proc.stdin.close();proc.wait(timeout=10)
  proc=subprocess.Popen([binary,'app-server','--listen','stdio://'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=stderr,text=True,env=env,cwd=work,bufsize=1)
  responses=RpcReplies(proc.stdout)
  send('initialize',{'clientInfo':{'name':'relayhistory-offline-fixture','version':'1.0.0'},'capabilities':{'experimentalApi':True}})
  send('initialized',{},True)
  send('thread/resume',{'threadId':sid,'cwd':str(work),'model':'synthetic-fixture','modelProvider':'synthetic','approvalPolicy':'never','sandbox':'read-only','excludeTurns':True,'config':{'model_providers':{'synthetic':{'name':'Offline synthetic','base_url':'http://127.0.0.1:9/v1','wire_api':'responses','requires_openai_auth':False}}}})
  reopened=send('thread/items/list',{'threadId':sid,'limit':100})
  (output/'reopened-items.json').write_text(json.dumps(reopened,indent=2)+'\n')
  send('thread/inject_items',{'threadId':sid,'items':[{'type':'message','role':'user','content':[{'type':'input_text','text':'Resume the native Codex fixture.'}]},{'type':'message','role':'assistant','content':[{'type':'output_text','text':'The restarted native fixture is readable.'}]}]})
  final=send('thread/items/list',{'threadId':sid,'limit':100})
  (output/'resumed-items.json').write_text(json.dumps(final,indent=2)+'\n')
  (output/'native-id.txt').write_text(sid+'\n')
 finally:
  proc.stdin.close()
  try:proc.wait(timeout=10)
  except subprocess.TimeoutExpired:proc.terminate();proc.wait(timeout=5)
  stderr.close()
  # Artifacts contain only this isolated synthetic home; never traverse operator data.
  (output/'rpc-transcript.json').write_text(json.dumps(transcript,indent=2)+'\n')
  files=list(agent.rglob('*.jsonl'))
  for i,file in enumerate(files):(output/f'rollout-{i}.jsonl').write_bytes(file.read_bytes())
  (output/'files.json').write_text(json.dumps([str(p.relative_to(agent)) for p in files],indent=2)+'\n')
print(output)
