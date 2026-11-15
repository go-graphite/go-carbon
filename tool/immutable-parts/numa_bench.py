#!/usr/bin/env python3
"""Run independent offline pipelines, binding all runtime and DB threads per node.

Each child synthesizes a disjoint metric-ID subset. This is an offline numeric
experiment, not an adapter for the portable production backend or live intake.
"""
import argparse
import datetime
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import time


def cpus(value):
    result = []
    for item in value.split(','):
        pair = item.split('-')
        result.extend(range(int(pair[0]), int(pair[-1]) + 1))
    if not result or len(result) != len(set(result)):
        raise ValueError('empty or repeated CPU set')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--nodes', required=True, help='node:cpus;node:cpus')
    parser.add_argument('--producers', type=int, required=True, help='per node')
    parser.add_argument('--owners', type=int, required=True, help='per node')
    parser.add_argument('--dbs', type=int, required=True, help='per node')
    parser.add_argument('--duration', type=int, default=12)
    parser.add_argument('--rate', type=int, default=300000000, help='aggregate')
    parser.add_argument('--max-rows', type=int, default=2097152)
    parser.add_argument('--settle', type=int, default=2)
    args = parser.parse_args()
    nodes = [item.split(':', 1) for item in args.nodes.split(';')]
    all_cpus = [cpu for _, mask in nodes for cpu in cpus(mask)]
    if len(set(all_cpus)) != len(all_cpus):
        raise ValueError('node CPU sets overlap')
    args.out.mkdir(parents=True, exist_ok=False)
    children = []
    try:
        for index, (node, mask) in enumerate(nodes):
            env = os.environ.copy()
            env['GOMAXPROCS'] = str(len(cpus(mask)))
            env.pop('CORE_OWNER_CPUS', None)
            env.pop('CORE_PRODUCER_CPUS', None)
            cmd = ['numactl', '--physcpubind=' + mask, '--membind=' + node,
                   args.binary, '-mode', 'core-bench', '-out',
                   str(args.out / ('db-' + str(index))), '-shards', str(args.dbs),
                   '-core-owners', str(args.owners), '-core-producers',
                   str(args.producers), '-core-pipeline', str(index),
                   '-core-pipelines', str(len(nodes)), '-max-rows',
                   str(args.max_rows), '-core-append-parts', '-core-duration',
                   str(args.duration) + 's', '-core-rate',
                   str(args.rate // len(nodes)), '-core-settle', str(args.settle) + 's']
            output = (args.out / (str(index) + '.jsonl')).open('w')
            errors = (args.out / (str(index) + '.err')).open('w')
            child = subprocess.Popen(cmd, env=env, stdout=output, stderr=errors)
            children.append((child, output, errors, cmd))
        (args.out / 'processes.json').write_text(json.dumps([
            {'pid': child.pid, 'argv': cmd} for child, _, _, cmd in children]))
        limit = time.monotonic() + args.duration + args.settle + 30
        for child, _, _, _ in children:
            if child.wait(timeout=max(.1, limit - time.monotonic())):
                raise RuntimeError('pipeline failed; inspect per-node stderr')
        for _, output, errors, _ in children:
            output.close()
            errors.close()
        rows = [[json.loads(line) for line in
                 (args.out / (str(i) + '.jsonl')).read_text().splitlines()]
                for i in range(len(nodes))]
        summary = summarize(rows)
        summary.update(cpus=len(all_cpus), producers=args.producers * len(nodes),
                       owners=args.owners * len(nodes), dbs=args.dbs * len(nodes),
                       nodes=nodes, argv=[cmd for _, _, _, cmd in children])
        (args.out / 'summary.json').write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary), flush=True)
    finally:
        for child, output, errors, _ in children:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            output.close()
            errors.close()
        # Retain measurement files; synthetic database directories are transient.
        for i in range(len(nodes)):
            directory = args.out / ('db-' + str(i))
            if directory.exists():
                shutil.rmtree(directory)


def summarize(rows):
    starts = [next(x for x in group if x['stage'] == 'core_start') for group in rows]
    ends = [next(x for x in group if x['stage'] == 'core_ingest_complete') for group in rows]
    finals = [next(x for x in group if x['stage'] == 'core_complete') for group in rows]
    samples = [[(0., 0)] + [(x['seconds'], x['committed']) for x in group
                if x['stage'] in ('core_sample', 'core_ingest_complete')] for group in rows]
    # Use time since each pipeline's own start. Also report start skew explicitly;
    # a large skew invalidates comparison of independently aligned windows.
    start_times = [datetime.datetime.fromisoformat(re.sub(r'(\.\d{6})\d+', r'\1', x['when_cet'])).timestamp() for x in starts]
    seconds = max(x['seconds'] for x in ends)
    n = sum(x['committed'] for x in ends)
    def at(series, t):
        for (a, x), (b, y) in zip(series, series[1:]):
            if a <= t <= b:
                return x + (y-x) * (t-a) / (b-a)
        raise ValueError('missing sample interval')
    windows = []
    for t in range(10, int(min(x['seconds'] for x in ends)) + 1, 10):
        windows.append(sum(at(series, t) - at(series, t-10)
                           for series in samples) / 10 / 1e6)
    writes = sum(end['proc'].get('write_bytes', 0) - start['proc'].get('write_bytes', 0)
                 for end, start in zip(ends, starts))
    cpu = sum(end['proc'].get('cpu_seconds_CLK_TCK100', 0) -
              start['proc'].get('cpu_seconds_CLK_TCK100', 0) for end, start in zip(ends, starts))
    return dict(when_cet=datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=1))).isoformat(),
                committed=n, seconds=seconds, million_per_second=n/seconds/1e6,
                min_10s=min(windows) if windows else None, windows_10s=windows,
                cores=cpu/seconds, write_MB_s=writes/seconds/1e6,
                start_skew_s=max(start_times)-min(start_times),
                debt_bytes=sum(x['estimated_debt_bytes'] for x in finals),
                stalls=sum(x['write_stalls'] for x in finals),
                peak_RSS_GB=sum(x['proc'].get('VmHWM_bytes', 0) for x in finals)/1e9,
                allocated_bytes_per_point_including_open=sum(x['total_alloc'] for x in finals)/n,
                gc_cycles=[x['num_gc'] for x in finals])


if __name__ == '__main__':
    main()
