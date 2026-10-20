"""Normalize only capture IDs, clock values, and isolated fixture paths."""
import argparse,hashlib,json,re,shutil
from datetime import datetime,timezone
from pathlib import Path
parser=argparse.ArgumentParser()
parser.add_argument("raw",type=Path,help="Output directory produced by capture.py")
args=parser.parse_args()
base=Path(__file__).resolve().parent
repo=base.parents[3]
raw=args.raw.resolve()
iso=re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)')
manifest=[]
oracles={}
for index,name in enumerate(['ordinary','tool','resumed','literal-tags','long'],1):
 source=raw/name
 oldid=(source/'conversation-id.txt').read_text().strip()
 sid=f'{index:032x}'
 full=next((source/'app').rglob('transcript_full.jsonl'))
 first=json.loads(full.read_text().splitlines()[0])
 start=datetime.fromisoformat(first['created_at'].replace('Z','+00:00'))
 fixed=datetime(2026,10,11,12,0,0,tzinfo=timezone.utc)
 def normalize(text):
  def date(m):
   dt=datetime.fromisoformat(m[0].replace('Z','+00:00'))
   result=(fixed+(dt-start)).astimezone(dt.tzinfo).isoformat(timespec='seconds')
   return result.replace('+00:00','Z') if m[0].endswith('Z') else result
  return iso.sub(date,text).replace(oldid,sid).replace(str(source),'/fixture/antigravity/'+name)
 rel=Path('.gemini/antigravity/brain')/sid/'.system_generated/logs'
 dest=base/'cases'/name/rel
 dest.mkdir(parents=True,exist_ok=True)
 records={}
 for filename in ['transcript_full.jsonl','transcript.jsonl']:
  src=full.with_name(filename)
  text=normalize(src.read_text())
  (dest/filename).write_text(text)
  records[filename]={'raw_sha256':hashlib.sha256(src.read_bytes()).hexdigest(),'sanitized_sha256':hashlib.sha256(text.encode()).hexdigest()}
 oracle=normalize((source/'official-reader.json').read_text())
 oracles[name]=json.loads(oracle)
 before=normalize((source/'reader-after-restart.json').read_text())
 od=base/'oracle'/name
 od.mkdir(parents=True,exist_ok=True)
 (od/'official-reader.json').write_text(oracle)
 (od/'reader-after-restart.json').write_text(before)
 public=repo/'crates/ai-hist/tests/fixtures/antigravity'/name/rel
 public.mkdir(parents=True,exist_ok=True)
 for filename in records: shutil.copyfile(dest/filename,public/filename)
 manifest.append({'name':name,'conversation_id':sid,'root':str(rel),'writer':'google-antigravity 0.1.22 native localharness','records':records,'reader_steps':len(json.loads(oracle))})
(base/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
(repo/'crates/ai-hist/tests/fixtures/antigravity/official-reader.json').write_text(json.dumps(oracles,indent=2)+'\n')
print('Sanitized and copied',len(manifest),'native-writer cases')
