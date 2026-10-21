"""Run the inspected pinned official CLI against a loopback-only synthetic model.
Usage: python3 capture.py /path/to/extracted/package /empty/output /path/to/node
No dependency installation, user HOME/config, credentials or real model calls.
"""
import hashlib,json,os,shutil,subprocess,sys,threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
package,root,node=map(Path,sys.argv[1:])
assert not root.exists(), 'Use a fresh output directory'
root.mkdir(parents=True)
assert json.loads((package/'package.json').read_text())['version']=='2.171.0'
bundle=package/'dist/codebuddy-headless.js'
assert hashlib.sha256(bundle.read_bytes()).hexdigest()=='f04e3bf3ac210ed68931a947a63ef26296695766a432a90498383ec150da47b0', 'Pinned bundle hash mismatch'
original=bundle.read_text()
needle='var __webpack_exports__=__webpack_require__(__webpack_require__.s=62978);return __webpack_exports__'
assert original.count(needle)==1,'Pinned module entry changed'
exposed=package/'dist/codebuddy-fixture-api.cjs'
exposed.write_text(original.replace(needle,'return __webpack_require__(81102)'))
workspace=root/'workspace';workspace.mkdir()
home=root/'home';config=home/'.codebuddy';config.mkdir(parents=True)
base=Path(__file__).resolve().parent
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_POST(self):
  assert self.path=='/v1/chat/completions'
  request=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
  names=[t['function']['name'] for t in request.get('tools',[])]
  assert all(n=='Read' for n in names),names
  self.server.requests.append(request)
  tool=self.server.tool and len(self.server.requests)==1
  delta={'role':'assistant','content':self.server.answer}
  if tool:
   delta={'role':'assistant','tool_calls':[{'index':0,'id':'fixture-call-001','type':'function','function':{'name':'Read','arguments':json.dumps({'file_path':str(workspace/'fixture.txt')})}}]}
  chunks=[{'id':'fixture-completion','object':'chat.completion.chunk','created':1791720000,'model':'fixture-local','choices':[{'index':0,'delta':delta,'finish_reason':None}]},{'id':'fixture-completion','object':'chat.completion.chunk','created':1791720000,'model':'fixture-local','choices':[{'index':0,'delta':{},'finish_reason':'tool_calls' if tool else 'stop'}],'usage':{'prompt_tokens':23,'completion_tokens':7,'total_tokens':30}}]
  data=(''.join('data: '+json.dumps(c)+'\n\n' for c in chunks)+'data: [DONE]\n\n').encode()
  self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
env={'HOME':str(home),'PATH':str(node.parent)+':/usr/bin:/bin:/usr/sbin:/sbin','TMPDIR':str(root),'DISABLE_AUTOUPDATER':'1','DISABLE_TELEMETRY':'1','CODEBUDDY_CODE_DISABLE_NONESSENTIAL_TRAFFIC':'1','NO_COLOR':'1'}
def run(name,prompt,answer,tool=False,resume=False,spill=False,error=False):
 server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.tool=tool;server.answer=answer;server.requests=[]
 threading.Thread(target=server.serve_forever,daemon=True).start()
 (config/'models.json').write_text(json.dumps({'models':[{'id':'fixture-local','name':'Fixture local','vendor':'OpenAI','apiKey':'fixture-not-a-key','url':f'http://127.0.0.1:{server.server_port}/v1/chat/completions','supportsToolCall':tool,'maxInputTokens':128000,'maxOutputTokens':4096}],'availableModels':['fixture-local']}))
 (config/'settings.json').write_text(json.dumps({'model':'fixture-local','variantModels':{'lite':'fixture-local','reasoning':'fixture-local'},'permissions':{'allow':['Read'] if tool else [],'deny':['Bash','Write','Edit','Agent','WebFetch','WebSearch']},'enableTelemetry':False}))
 (workspace/'fixture.txt').write_text(('Synthetic spilled output blue '+('x'*90)+'\n')*(3000 if error else 50) if spill or error else 'Synthetic fixture file color blue.\n')
 sid='codebuddy-fixture-ordinary' if resume else 'codebuddy-fixture-'+name
 command=[str(node),str(package/'bin/codebuddy'),'-p',prompt,'--model','fixture-local','--tools','Read' if tool else '','--output-format','stream-json','--verbose',*(['--resume',sid] if resume else ['--session-id',sid])]
 childenv=dict(env)
 if spill:childenv['CODEBUDDY_TOOL_RESULT_THRESHOLD_KB']='1'
 dest=root/'cases'/name;dest.mkdir(parents=True)
 try: result=subprocess.run(command,cwd=workspace,env=childenv,capture_output=True,text=True,timeout=50)
 finally: server.shutdown();server.server_close()
 (dest/'stdout.jsonl').write_text(result.stdout);(dest/'stderr.txt').write_text(result.stderr)
 assert result.returncode==0,(name,result.returncode,result.stderr)
 assert len(server.requests)==(2 if tool else 1),(name,len(server.requests))
 transcript=next((config/'projects').glob('*/'+sid+'.jsonl'))
 shutil.copyfile(transcript,dest/'transcript.jsonl')
 if spill:shutil.copytree(transcript.with_suffix(''),dest/'sidecars')
 oracle=subprocess.check_output([str(node),str(base/'read-native.cjs'),str(exposed),str(transcript)],env=env,text=True,timeout=20)
 (dest/'official-reader.json').write_text(oracle)
run('ordinary','Find the synthetic CodeBuddy needle.','Found the synthetic CodeBuddy needle.')
run('resumed','Continue the synthetic CodeBuddy session.','The resumed synthetic CodeBuddy answer is green.',resume=True)
run('tool','Read the synthetic fixture file.','The synthetic fixture tool returned blue.',tool=True)
run('spill','Read the synthetic fixture file.','The synthetic fixture tool returned blue.',tool=True,spill=True)
run('error','Read the synthetic fixture file.','The synthetic tool reported its native file-size error.',tool=True,error=True)
subprocess.run([str(node),str(base/'capture-controls.cjs'),str(exposed),str(root/'controls')],env={**env,'CODEBUDDY_FIXTURE_CAPTURE':'1'},check=True,timeout=20)
(root/'pin.json').write_text(json.dumps({'version':'2.171.0','bundle_sha256':hashlib.sha256(bundle.read_bytes()).hexdigest(),'reader_entry_only_instrumentation':needle},indent=2)+'\n')
print('Captured five official CLI cases and seven native recorder controls')
