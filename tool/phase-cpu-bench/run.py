#!/usr/bin/env python3
"""Alternating local profiling overhead trials, with a separate sender process."""
import argparse, json, os, pathlib, re, signal, subprocess, time, urllib.request

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary',required=True)
p.add_argument('--output',required=True)
p.add_argument('--dir',required=True)
p.add_argument('--rounds',type=int,default=5)
p.add_argument('--seconds',type=int,default=60)
p.add_argument('--procs',type=int,default=4)
p.add_argument('--settle',type=int,default=5)
p.add_argument('--metrics',type=int,default=1000000)
p.add_argument('--max-load',type=float,default=8)
p.add_argument('--reuse',action='store_true')
p.add_argument('--modes',default='phase,mutex1000,mutex100,mutex10,block1000,block100,block10')
a=p.parse_args()
root=pathlib.Path(a.output);root.mkdir(parents=True,exist_ok=True)
opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
server=subprocess.Popen([a.binary,'serve','-dir',a.dir,'-procs',str(a.procs),'-metrics',str(a.metrics)]+(['-reuse'] if a.reuse else []),stdout=subprocess.PIPE,stderr=(root/'server.log').open('w'),text=True,start_new_session=True,env=dict(os.environ,GOTOOLCHAIN='go1.27.0',GOFLAGS='-mod=vendor',GOMAXPROCS=str(a.procs)))
def load():
 s=subprocess.check_output(['uptime'],text=True).strip()
 m=re.search(r'load averages?:\s*([0-9.]+)',s)
 return s,float(m[1])
def request(path,data=None,timeout=150):
 req=urllib.request.Request(url+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
 with opener.open(req,timeout=timeout) as r:
  body=r.read()
 return json.loads(body) if body else None
try:
 line=server.stdout.readline()
 if not line:raise RuntimeError('server failed before catalog verification; see server.log')
 ready=json.loads(line);url=ready['control']
 (root/'fixture.json').write_text(json.dumps(ready,indent=2)+'\n')
 (root/'driver.pid').write_text(str(os.getpid())+'\n')
 (root/'server.pid').write_text(str(server.pid)+'\n')
 print(json.dumps(dict(event='fixture_ready',**ready)),flush=True)
 sequence=[]
 for round in range(a.rounds):
  modes=a.modes.split(',')
  if round%2:modes=list(reversed(modes))
  sequence.append((round,'off'))
  for mode in modes:sequence.extend([(round,mode),(round,'off')])
 index=0
 while index<len(sequence):
  round,name=sequence[index]
  before,level=load()
  if level>a.max_load:
   request('/pause?value=1')
   while level>a.max_load:
    print(json.dumps(dict(event='waiting_for_load',uptime=before,load=level,case=index)),flush=True)
    time.sleep(20)
    before,level=load()
   request('/pause?value=0')
   request('/warm',timeout=600)
  cfg=dict(phase=name=='phase',mutex=int(name[5:]) if name.startswith('mutex') else 0,block=int(name[5:]) if name.startswith('block') else 0,seconds=a.seconds,settle=a.settle)
  before,level=load()
  start=time.monotonic()
  row=request('/run',cfg)
  after,post=load()
  row.update(index=index,round=round,case=name,uptime_before=before,uptime_after=after,load_before=level,load_after=post,request_wall=time.monotonic()-start)
  row['valid']=not row['dropped'] and not row['receiver_errors'] and not row.get('p99_overflow',False) and row['persisted_points']>0 and row['load_before']<=a.max_load and row['load_after']<=a.max_load and (name!='phase' or row['profile_windows']>=1)
  with (root/'runs.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
  print(json.dumps(row),flush=True)
  if row['valid']:index+=1
finally:
 try:request('/quit',timeout=5)
 except Exception:pass
 try:server.wait(timeout=15)
 except subprocess.TimeoutExpired:
  os.killpg(server.pid,signal.SIGTERM)
  try:server.wait(timeout=3)
  except subprocess.TimeoutExpired:os.killpg(server.pid,signal.SIGKILL);server.wait()
