#!/usr/bin/env python3
"""Sanitize only synthetic capture paths, generated UUIDs and wall-clock values."""
import hashlib,json,pathlib,re,sys
src=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2]) if len(sys.argv)>2 else pathlib.Path(__file__).parent
out.mkdir(parents=True,exist_ok=True)
sid=src.joinpath('native-id.txt').read_text().strip();mapped={sid:'019b76da-a800-7000-8000-000000000161'}
rows=[json.loads(line) for line in src.joinpath('rollout-0.jsonl').read_text().splitlines()]
cwd=rows[0]['payload']['cwd'];home=str(pathlib.Path(cwd).parent)
def text(value):
 value=value.replace('/private'+home,'/synthetic/codex-home').replace(home,'/synthetic/codex-home')
 def replace(m):
  original=m.group(0)
  if original not in mapped:mapped[original]=f'019b76da-a800-7000-8000-{len(mapped)+161:012d}'
  return mapped[original]
 value=re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',replace,value)
 # Filename clocks use hyphens, unlike RFC3339 timestamps. Normalize the
 # filename first and require colon-delimited clocks in the general rule.
 value=re.sub(r'rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-','rollout-2026-01-01T00-00-00-',value)
 value=re.sub(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?', '2026-01-01T00:00:00.000Z',value)
 value=re.sub(r'/sessions/\d{4}/\d{2}/\d{2}/', '/sessions/2026/01/01/',value)
 return re.sub(r'\b\d{4}-\d{2}-\d{2}\b', '2026-01-01', value)

def clean(v,key=''):
 if isinstance(v,dict):return {k:clean(x,k) for k,x in v.items()}
 if isinstance(v,list):return [clean(x,key) for x in v]
 if isinstance(v,str):
  if key=='serverName':return 'synthetic-host.local'
  return '2026-01-01' if key=='current_date' else text(v)
 if isinstance(v,(int,float)) and key in ('createdAt','updatedAt','recencyAt','create_time'):return 1767225600
 return v
rows=clean(rows)
for i,r in enumerate(rows):r['timestamp']=f'2026-01-01T00:00:{i:02d}.000Z'
out.joinpath('session.jsonl').write_text(''.join(json.dumps(r,separators=(',',':'))+'\n' for r in rows))
for name in ['reader','items','turns','reopened-items','resumed-items','rpc-transcript']:
 out.joinpath(name+'.json').write_text(json.dumps(clean(json.loads(src.joinpath(name+'.json').read_text())),indent=2)+'\n')
expected=[{'role':r['payload']['role'],'text':'\n'.join(b.get('text','') for b in r['payload']['content'])} for r in rows if r['type']=='response_item' and r['payload']['type']=='message' and r['payload']['role'] in ('user','assistant') and not r['payload']['content'][0].get('text','').startswith('<environment_context>')]
out.joinpath('expected-messages.json').write_text(json.dumps(expected,indent=2)+'\n')
print(mapped[sid],len(rows),len(expected))
