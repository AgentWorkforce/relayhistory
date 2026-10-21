"""Capture Kimi Code2.2.0 with isolated HOME and a loopback-only fixture model."""
import argparse,json,os,subprocess,threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('output',type=Path);a=p.parse_args();assert subprocess.check_output([a.binary,'--version'],text=True).strip()=='2.2.0';a.output.mkdir(parents=True,exist_ok=False)
class H(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_POST(self):
  q=json.loads(self.rfile.read(int(self.headers['Content-Length'])));s=self.server;s.calls+=1;(s.output/f'request-{s.calls}.json').write_text(json.dumps(q,indent=2))
  tool=None
  if s.case=='tool' and s.calls==1:tool=('Read',{'path':'fixture.txt'})
  if s.case=='subagent' and s.calls==1:tool=('Agent',{'prompt':'Return the synthetic child answer.','description':'Synthetic child fixture','subagent_type':'explore'})
  if tool:delta={'role':'assistant','tool_calls':[{'index':0,'id':'fixture-tool-001','type':'function','function':{'name':tool[0],'arguments':json.dumps(tool[1])}}]};reason='tool_calls'
  else:delta={'role':'assistant','content':f'Synthetic Kimi {s.case} answer {s.calls}: blue.'};reason='stop'
  cs=[{'id':f'fixture-completion-{s.calls}','object':'chat.completion.chunk','choices':[{'index':0,'delta':delta,'finish_reason':None}]},{'id':f'fixture-completion-{s.calls}','object':'chat.completion.chunk','choices':[{'index':0,'delta':{},'finish_reason':reason}],'usage':{'prompt_tokens':17,'completion_tokens':8,'total_tokens':25}}]
  b=(''.join('data: '+json.dumps(c)+'\n\n'for c in cs)+'data: [DONE]\n\n').encode();self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
for case in ['ordinary','tool','resumed','subagent']:
 out=a.output/case;work=out/'workspace';work.mkdir(parents=True);(work/'fixture.txt').write_text('Synthetic fixture file: blue.\n');home=out/'home';home.mkdir()
 s=ThreadingHTTPServer(('127.0.0.1',0),H);s.case=case;s.calls=0;s.output=out;threading.Thread(target=s.serve_forever,daemon=True).start()
 env={'PATH':os.environ['PATH'],'HOME':str(home),'KIMI_CODE_HOME':str(home/'.kimi-code'),'KIMI_DISABLE_TELEMETRY':'1','KIMI_MODEL_NAME':'fixture-local','KIMI_MODEL_PROVIDER_TYPE':'openai','KIMI_MODEL_API_KEY':'synthetic-not-a-secret','KIMI_MODEL_BASE_URL':f'http://127.0.0.1:{s.server_port}/v1','KIMI_MODEL_CAPABILITIES':'','HTTP_PROXY':'http://127.0.0.1:9','HTTPS_PROXY':'http://127.0.0.1:9','NO_PROXY':'localhost,127.0.0.1'}
 def run(args,name):
  r=subprocess.run([a.binary,*args],cwd=work,env=env,capture_output=True,text=True,timeout=40);(out/(name+'.stdout')).write_text(r.stdout);(out/(name+'.stderr')).write_text(r.stderr)
  if r.returncode:raise RuntimeError(r.stderr)
 run(['-p',f'Remember the synthetic {case} color blue.','--output-format','stream-json'],'first')
 session=next((home/'.kimi-code/sessions').glob('*/*/state.json'));sid=json.loads(session.read_text())['id'];(out/'session-id.txt').write_text(sid)
 # Read-only export after writer process exited. Resume only the known created ID.
 run(['export',sid,'-y','--no-include-global-log','-o',str(out/'official-export.zip')],'export-before')
 if case=='resumed':run(['--session',sid,'-p','Continue the same synthetic fixture.','--output-format','stream-json'],'resumed')
 run(['session','list','--json'],'reader-list')
 s.shutdown();s.server_close();print(case,sid,flush=True)
