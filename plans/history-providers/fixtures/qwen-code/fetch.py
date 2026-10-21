#!/usr/bin/env python3
"""Fetch only pinned primary source modules; no packages or model calls."""
import hashlib,json,pathlib,sys,urllib.request
here=pathlib.Path(__file__).resolve().parent
manifest=json.loads((here/'provenance.json').read_text())
out=pathlib.Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
for name,digest in manifest['sources'].items():
    url=f"https://raw.githubusercontent.com/QwenLM/qwen-code/{manifest['revision']}/{name}"
    data=urllib.request.urlopen(url,timeout=30).read()
    assert hashlib.sha256(data).hexdigest()==digest,name
    (out/name.replace('/','__')).write_bytes(data)
print(f"Verified {len(manifest['sources'])} pinned source modules")
