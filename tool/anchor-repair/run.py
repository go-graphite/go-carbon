#!/usr/bin/env python3
"""Resumable daemon-side archive-anchor inspection/apply client.

Run locally on the store, keeping its auth secret and token in memory. The
client never opens the live database. Apply requires the reviewed plan and
matching external WRITE-OK in addition to the daemon's update authorization.
"""
import argparse,base64,hashlib,hmac,json,os,pathlib,time,urllib.request
p=argparse.ArgumentParser(__doc__)
p.add_argument('--endpoint',default='http://127.0.0.1:4242')
p.add_argument('--secret-file',required=True)
p.add_argument('--names',help='explicit newline-separated cohort; otherwise enumerate catalog')
p.add_argument('--journal',required=True)
p.add_argument('--apply',action='store_true')
p.add_argument('--plan');p.add_argument('--write-ok')
a=p.parse_args()
if a.apply:
 if not a.plan or not a.write_ok:p.error('apply needs --plan and --write-ok')
 digest=hashlib.sha256(pathlib.Path(a.plan).read_bytes()).hexdigest()
 approval=pathlib.Path(a.write_ok).read_text()
 if digest not in approval:p.error('WRITE-OK does not bind this plan SHA256')
secret=pathlib.Path(a.secret_file).read_bytes().strip()
mode='apply'if a.apply else'dry-run'
def request(path,body=None):
 now=int(time.time());b64=lambda b:base64.urlsafe_b64encode(b).rstrip(b'=')
 raw=b64(b'{"alg":"HS256","typ":"JWT"}')+b'.'+b64(json.dumps(dict(exp=now+300,namespaces=['*'],ops=['read','update']if a.apply else['read']),separators=(',',':')).encode())
 token=(raw+b'.'+b64(hmac.new(secret,raw,hashlib.sha256).digest())).decode()
 req=urllib.request.Request(a.endpoint+path,data=None if body is None else json.dumps(body).encode(),headers={'X-Buckyd-Authorization':token,'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=120)as r:return json.load(r)
journal=pathlib.Path(a.journal)
# Keep dry-run and apply histories distinct, and bind resumes to this endpoint
# and exact names file. A completed result never crosses those boundaries.
cohort=hashlib.sha256(pathlib.Path(a.names).read_bytes()).hexdigest()if a.names else'catalog'
identity=dict(endpoint=a.endpoint,mode=mode,cohort=cohort,plan_sha256=digest if a.apply else None)
seen=set();after='';counts=dict(names=0,changed=0,applied=0,blocked=0,errors=0)
# Advisory lock prevents duplicate clients sharing a progress file. A crash
# can leave a partial final line; discard only that unacknowledged tail.
import fcntl
with journal.open('a+')as log:
 fcntl.flock(log,fcntl.LOCK_EX|fcntl.LOCK_NB)
 log.seek(0)
 offset=0
 while True:
  line=log.readline()
  if not line:break
  if not line.endswith('\n'):
   log.seek(offset);log.truncate();log.flush();os.fsync(log.fileno());break
  row=json.loads(line)
  if row.get('identity')!=identity:raise SystemExit('journal identity differs; use a distinct journal')
  result=row['result']
  if not result.get('error') and not any(x.get('blocked')for x in result.get('archives')or[]):seen.add(result['name'])
  offset=log.tell()
 log.seek(0,2)
 def process(names):
  for name in names:
   if name in seen:continue
   rows=request('/v1/maintenance/archive-anchors',dict(names=[name],apply=a.apply))
   if len(rows)!=1 or rows[0].get('name')!=name:raise RuntimeError('maintenance response identity mismatch')
   result=rows[0];counts['names']+=1
   counts['changed']+=any(x.get('changed')for x in result.get('archives')or[])
   counts['applied']+=bool(result.get('applied'))
   counts['blocked']+=any(x.get('blocked')for x in result.get('archives')or[])
   counts['errors']+=bool(result.get('error'))
   log.write(json.dumps(dict(identity=identity,at=time.time(),result=result),separators=(',',':'))+'\n');log.flush();os.fsync(log.fileno())
   if not result.get('error') and not any(x.get('blocked')for x in result.get('archives')or[]):seen.add(name)
   print(json.dumps(dict(mode=mode,counts=counts,last=name)),flush=True)
 if a.names:process(x.strip()for x in pathlib.Path(a.names).read_text().splitlines()if x.strip())
 else:
  from urllib.parse import urlencode
  while True:
   page=request('/v1/transfer/list?'+urlencode(dict(after=after,limit=1000)))
   if not page:break
   if page!=sorted(set(page)) or page[0]<=after:raise RuntimeError('catalog pagination did not advance')
   process(page);after=page[-1]
 print(json.dumps(dict(mode=mode,counts=counts,complete=True)),flush=True)
 if counts['blocked']or counts['errors']:raise SystemExit(1)
