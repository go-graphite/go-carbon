#!/usr/bin/env python3
"""Summarize valid repeated A/B cases against their bracketing off controls."""
import argparse, csv, json, math, pathlib, statistics

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('directory')
a=p.parse_args();root=pathlib.Path(a.directory)
rows=[json.loads(s) for s in (root/'runs.jsonl').read_text().splitlines()]
valid=[r for r in rows if r['valid']]
metrics=['cpu_seconds_per_million','points_per_second','update_p99_us']
off=[r for r in valid if r['case']=='off']
paired={}
for r in valid:
 if r['case']=='off':continue
 before=[v for v in off if v['index']<r['index']]
 after=[v for v in off if v['index']>r['index']]
 if not before or not after:continue
 left=max(before,key=lambda v:v['index']);right=min(after,key=lambda v:v['index'])
 row=dict(r)
 for metric in metrics:
  control=(left[metric]+right[metric])/2
  row[metric+'_control']=control
  row[metric+'_delta_pct']=100*(r[metric]/control-1)
 paired.setdefault(r['case'],[]).append(row)
lines=['# Pipeline A/B measurements','',f'Accepted {len(valid)} / {len(rows)} raw cases; rejected cases remain in runs.jsonl.','',
 '| Mode | n | CPU s/M points, median [min,max] | points/s, median [min,max] | p99 µs, median [min,max] |',
 '| --- | ---: | --- | --- | --- |']
def spread(values):
 return f'{statistics.median(values):.3f} [{min(values):.3f}, {max(values):.3f}]'
for mode,cases in [('off',off)]+sorted(paired.items()):
 lines.append('| '+mode+' | '+str(len(cases))+' | '+' | '.join(spread([r[m] for r in cases]) for m in metrics)+' |')
lines+=['','Paired changes use the average of the immediately preceding and following accepted off controls.','',
 '| Mode | CPU change %, median [min,max] | throughput change %, median [min,max] | p99 change %, median [min,max] |',
 '| --- | --- | --- | --- |']
for mode,cases in sorted(paired.items()):
 lines.append('| '+mode+' | '+' | '.join(spread([r[m+'_delta_pct'] for r in cases]) for m in metrics)+' |')
lines+=['','Approximate 95% intervals below use a t interval on paired log ratios (n=5: t=2.776). These describe this fixture and do not remove host/load or shared-control dependence.','']
summary={}
for mode,cases in sorted(paired.items()):
 summary[mode]={'n':len(cases)}
 for metric in metrics:
  xs=[math.log(r[metric]/r[metric+'_control']) for r in cases]
  mean=statistics.mean(xs)
  critical=2.776 if len(xs)==5 else 3.182 if len(xs)==4 else 4.303 if len(xs)==3 else 12.706
  error=critical*statistics.stdev(xs)/math.sqrt(len(xs)) if len(xs)>1 else float('inf')
  lo,hi=100*math.expm1(mean-error),100*math.expm1(mean+error)
  summary[mode][metric]=dict(change_pct=100*math.expm1(mean),lower_pct=lo,upper_pct=hi)
  lines.append(f'- {mode}, {metric}: {100*math.expm1(mean):+.3f}% [{lo:+.3f}%, {hi:+.3f}%].')
(root/'summary.md').write_text('\n'.join(lines)+'\n')
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
with (root/'runs.csv').open('w',newline='') as f:
 fields=['index','round','case','valid']+metrics+['received_points','persisted_points','updates','input_unique_metrics','dropped','receiver_errors','cache_points_end','profile_windows','profile_seconds','p99_bucket_lower_us','p99_bucket_upper_us','load_before','load_after']
 writer=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(rows)
print('\n'.join(lines))
