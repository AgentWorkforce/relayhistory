"""Sanitize only isolated paths, generated native IDs and clock values."""
import argparse,hashlib,json,re
from pathlib import Path
from datetime import datetime,timezone,timedelta
p=argparse.ArgumentParser();p.add_argument('raw',type=Path);a=p.parse_args();base=Path(__file__).resolve().parent;repo=base.parents[3];manifest=[];oracles={}
for number,name in enumerate(['ordinary','tool','resumed','subagent','rewound'],1):
 src=(a.raw/name).resolve();session=next((src/'home/.kimi-code/sessions').glob('*/*/state.json')).parent;state=json.loads((session/'state.json').read_text());sid=f'session_00000000-0000-4000-8000-{number:012d}';bucket=f'wd_fixture_{number:012d}';ids={state['id']:sid};shift=1791720000000-state['createdAt']
 def string(s):
  s=s.replace(str(src),'/fixture/kimi/'+name).replace(session.parent.name,bucket)
  for match in re.findall(r'(?:session_)?[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}|msg_[A-Z0-9]{26}',s):
   if match not in ids:ids[match]=('msg_' + f'{len(ids):026d}') if match.startswith('msg_') else f'00000000-0000-4000-9000-{len(ids):012d}'
   s=s.replace(match,ids[match])
  def iso(m):
   dt=datetime.fromisoformat(m[0].replace('Z','+00:00'))+timedelta(milliseconds=shift);return dt.isoformat(timespec='milliseconds').replace('+00:00','Z')
  return re.sub(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+Z',iso,s)
 def norm(v):
  if isinstance(v,str):return string(v)
  if isinstance(v,list):return [norm(x)for x in v]
  if isinstance(v,dict):return {k:norm(x)for k,x in v.items()}
  if isinstance(v,(int,float))and not isinstance(v,bool)and 1700000000000<v<2000000000000:return v+shift
  return v
 rel=Path('.kimi-code/sessions')/bucket/sid;dest=repo/'crates/ai-hist/tests/fixtures/kimi'/name/rel;dest.mkdir(parents=True,exist_ok=True);hashes={}
 for file in [session/'state.json',*sorted(session.glob('agents/*/wire.jsonl'))]:
  target=dest/file.relative_to(session);target.parent.mkdir(parents=True,exist_ok=True)
  values=[json.loads(l)for l in file.read_text().splitlines()]if file.name.endswith('.jsonl')else json.loads(file.read_text());v=norm(values)
  data=(''.join(json.dumps(x,separators=(',',':'),ensure_ascii=False)+'\n'for x in v)if isinstance(v,list)else json.dumps(v,separators=(',',':'),ensure_ascii=False)+'\n');target.write_text(data);hashes[str(file.relative_to(session))]={'raw_sha256':hashlib.sha256(file.read_bytes()).hexdigest(),'sanitized_sha256':hashlib.sha256(data.encode()).hexdigest()}
 oracle=norm(json.loads((src/'official-reader.json').read_text()));oracles[name]=oracle
 manifest.append({'name':name,'native_session_id':sid,'root':str(rel),'writer':'Kimi Code CLI2.2.0 official darwin-arm64 binary','files':hashes,'agents':list(oracle)})
(base/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');(repo/'crates/ai-hist/tests/fixtures/kimi/official-reader.json').write_text(json.dumps(oracles,indent=2)+'\n')
