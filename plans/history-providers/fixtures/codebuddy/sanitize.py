import json,re,hashlib,subprocess,shutil
from pathlib import Path
import sys
repo=Path(__file__).resolve().parents[4]
base=repo/'plans/history-providers/fixtures/codebuddy'
raw=Path(sys.argv[1])
node=sys.argv[2]
raw_cli=raw/'cases'
bundle=sys.argv[3]
manifest=[]; oracles={}; humans={}
for case,sid in [('ordinary','codebuddy-fixture-ordinary'),('resumed','codebuddy-fixture-ordinary'),('tool','codebuddy-fixture-tool'),('spill','codebuddy-fixture-spill3'),('error','codebuddy-fixture-error')]:
 src=raw_cli/case/'transcript.jsonl'; rows=src.read_text().splitlines()
 if case=='ordinary': rows=rows[:3]
 staged=raw/(case+'-reader.jsonl');staged.write_text('\n'.join(rows)+'\n')
 oracle=subprocess.check_output([node,str(base/'read-native.cjs'),bundle,str(staged)],env={'HOME':str(raw/'reader-home'),'PATH':'/usr/bin:/bin'},text=True)
 # Stable IDs, clock offset, and synthetic isolated paths only; preserve field shapes/text.
 data='\n'.join(rows)+'\n'; parsed=[json.loads(x) for x in rows]
 sid=parsed[0]['sessionId']
 project=next((raw/'home/.codebuddy/projects').glob('*/'+sid+'.jsonl')).parent
 start=min(r['timestamp'] for r in parsed if 'timestamp'in r)
 ids={}; counter=0
 for r in parsed:
  for v in [r.get('id'),r.get('parentId'),r.get('messageId'),r.get('providerData',{}).get('conversationRequestId'),r.get('providerData',{}).get('messageId'),r.get('providerData',{}).get('traceId')]:
   if v and v not in ids: counter+=1;ids[v]=f"fixture-{'ordinary' if case=='resumed' else case}-{counter:03}"
 def clean(v):
  if isinstance(v,dict):return {k:(1791720000000+(x-start) if k=='timestamp' and isinstance(x,int) else clean(x)) for k,x in v.items() if k not in ['_meta']}
  if isinstance(v,list):return [clean(x) for x in v]
  if isinstance(v,str):
   for a,b in ids.items():v=v.replace(a,b)
   return v.replace(str(project),'/fixture/home/.codebuddy/projects/fixture-workspace').replace(sid,'fixture-'+case).replace(str(raw/'home'),'/fixture/home').replace(str(raw/'workspace'),'/fixture/workspace')
  return v
 cleaned=[clean(r) for r in parsed]
 relative=Path('.codebuddy/projects/fixture-workspace')/('fixture-'+case+'.jsonl')
 out=base/'cases'/case/relative;out.parent.mkdir(parents=True,exist_ok=True);out.write_text(''.join(json.dumps(r,separators=(',',':'),ensure_ascii=False)+'\n' for r in cleaned))
 oracledata=clean(json.loads(oracle));oracles[case]=oracledata['history']; humans[case]=oracledata['realUserMessageIds']
 (base/'oracle'/(case+'.json')).write_text(json.dumps(oracledata,indent=2)+'\n')
 files=[relative]
 if case=='spill':
  side=raw_cli/case/'sidecars/tool-results/fixture-call-001.txt'; rel=relative.with_suffix('')/'tool-results/fixture-call-001.txt';target=base/'cases'/case/rel;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(side.read_text());files.append(rel)
 for f in files:
  target=repo/'crates/ai-hist/tests/fixtures/codebuddy'/case/f;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes((base/'cases'/case/f).read_bytes())
 manifest.append({'name':case,'native_session_id':'fixture-'+case,'files':[{ 'path':str(f),'sha256':hashlib.sha256((base/'cases'/case/f).read_bytes()).hexdigest()} for f in files], 'raw_sha256':hashlib.sha256(staged.read_bytes()).hexdigest()})
(base/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
(repo/'crates/ai-hist/tests/fixtures/codebuddy/official-reader.json').write_text(json.dumps(oracles,indent=2)+'\n')

for case in ['fork','pending-rewind','compact','reasoning','fragments','metadata','string-content']:
 source=raw/'controls'/case
 shutil.copytree(source/'.codebuddy',base/'cases'/case/'.codebuddy',dirs_exist_ok=True)
 shutil.copytree(source/'.codebuddy',repo/'crates/ai-hist/tests/fixtures/codebuddy'/case/'.codebuddy',dirs_exist_ok=True)
 oracle=json.loads((source/'active-reader.json').read_text())
 (base/'oracle'/(case+'.json')).write_text(json.dumps(oracle,indent=2)+'\n')
 oracles[case]=oracle['replay'];humans[case]=oracle['realUserMessageIds']
 file=next((source/'.codebuddy').rglob('*.jsonl'));relative=file.relative_to(source)
 manifest.append({'name':case,'native_session_id':'fixture-'+case,'writer':'Native LocalJsonlSessionStore recorder, not CLI model loop','files':[{'path':str(relative),'sha256':hashlib.sha256(file.read_bytes()).hexdigest()}]})
(base/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
(repo/'crates/ai-hist/tests/fixtures/codebuddy/official-reader.json').write_text(json.dumps(oracles,indent=2)+'\n')

(repo/'crates/ai-hist/tests/fixtures/codebuddy/official-human.json').write_text(json.dumps(humans,indent=2)+'\n')
