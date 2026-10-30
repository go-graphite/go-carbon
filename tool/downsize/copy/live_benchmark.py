#!/usr/bin/env python3
"""Local whole-store copies with a separate live source, paced TCP and HTTP reads."""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import select
import signal
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=120) as response:
        return json.load(response)


def populated(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == 'values' and any(isinstance(x, (int, float)) and math.isfinite(x) for x in child):
                return True
            if key == 'datapoints' and any(x[0] is not None for x in child):
                return True
            if populated(child):
                return True
    elif isinstance(value, list):
        return any(populated(x) for x in value)
    return False


def reap(proc):
    _, status, usage = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    rss = usage.ru_maxrss * (1 if os.uname().sysname == 'Darwin' else 1024)
    return proc.returncode, usage.ru_utime + usage.ru_stime, rss


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-binary', required=True)
    parser.add_argument('--copy-binary', required=True)
    parser.add_argument('--fixture', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--duration', type=int, default=12)
    parser.add_argument('--ingest', type=int, default=10000)
    parser.add_argument('--reads', type=int, default=1000)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--cpus', type=int, default=4)
    parser.add_argument('--rate', type=int, default=1000000000)
    parser.add_argument('--profile', action='store_true')
    parser.add_argument('--lease', help='optional real controller admission lease; no synthetic health is generated')
    parser.add_argument('--read-p99-ms', type=float, default=16, help='foreground upper bound; abort on violation')
    args = parser.parse_args()
    if args.duration < 1 or args.cpus < 1 or min(args.ingest, args.reads, args.rate) < 0:
        parser.error('duration must be positive; rates must be nonnegative')
    root = Path(args.output).resolve()
    root.mkdir()
    fixture = Path(args.fixture).resolve()
    clone = ['cp', '-cR'] if os.uname().sysname == 'Darwin' else ['cp', '-a', '--reflink=auto']
    subprocess.run(clone + [str(fixture / 'live'), str(root / 'live')], check=True)
    source_log = (root / 'source.err').open('w')
    env = dict(os.environ, GOMAXPROCS=str(args.cpus))
    source = subprocess.Popen(
        [args.source_binary, '--root', str(root), '--cpus', str(args.cpus)],
        stdout=subprocess.PIPE, stderr=source_log, text=True, env=env,
    )
    stop = threading.Event()
    lock = threading.Lock()
    counters = dict(sent=0, reads=0, nonempty=0, errors=0, buckets=[0]*64)

    def snapshot():
        with lock:
            return dict(counters, buckets=list(counters['buckets']))

    def produce():
        host, port = endpoint['tcp'].rsplit(':', 1)
        with socket.create_connection((host, int(port))) as connection:
            started, sent = time.monotonic(), 0
            while not stop.wait(.01):
                due = int(args.ingest*(time.monotonic()-started))-sent
                n = min(max(0, due), max(1, args.ingest//10))
                body = ''.join(
                    f'synthetic.production.cluster.service.{j%1000:08d}.cpu.usage {j%97} {int(time.time())}\n'
                    for j in range(sent, sent+n)
                )
                connection.sendall(body.encode())
                sent += n
                with lock:
                    counters['sent'] += n

    def ingest():
        try:
            produce()
        except Exception:
            with lock:
                counters['errors'] += 1

    def read(worker):
        deadline = time.monotonic()
        for i in range(worker, 1 << 60, 8):
            deadline += 8/max(1, args.reads)
            if stop.wait(max(0, deadline-time.monotonic())):
                return
            query = urllib.parse.urlencode({
                'target': f'synthetic.production.cluster.service.{i%1000:08d}.cpu.usage',
                'from': int(time.time())-60, 'until': int(time.time())+1, 'format': 'json',
            })
            started = time.monotonic()
            try:
                value = get(endpoint['render']+'/render/?'+query)
                present = populated(value)
                elapsed = max(1, int((time.monotonic()-started)*1e6))
                with lock:
                    counters['reads'] += 1
                    counters['nonempty'] += int(present)
                    counters['buckets'][min(63, elapsed.bit_length())] += 1
            except Exception:
                if not stop.is_set():
                    with lock:
                        counters['errors'] += 1

    def phase(name, action):
        before, sample = snapshot(), get(endpoint['control']+'/stats')
        start = time.monotonic()
        details = action()
        seconds = time.monotonic()-start
        after, end = snapshot(), get(endpoint['control']+'/stats')
        buckets = [a-b for a, b in zip(after['buckets'], before['buckets'])]
        total, count, p99 = sum(buckets), 0, 0
        for i, n in enumerate(buckets):
            count += n
            if total and count*100 >= total*99:
                p99 = 1 << i
                break
        row = dict(
            phase=name, seconds=seconds,
            source_cpu_seconds=end['cpu_seconds']-sample['cpu_seconds'],
            offered_points=after['sent']-before['sent'],
            committed_points=end['committed_points']-sample['committed_points'],
            reads=after['reads']-before['reads'],
            nonempty=after['nonempty']-before['nonempty'],
            read_p99_us_upper=p99,
            read_errors=after['errors']-before['errors'],
            write_errors=end['write_errors']-sample['write_errors'],
            cache_before=sample['cache_points'], cache_after=end['cache_points'],
            **details,
        )
        if 'bytes' in row:
            row['MB_per_second'] = row['bytes']/seconds/1e6
            row['cpu_seconds_per_stored_GB'] = (row['source_cpu_seconds']+row['target_cpu_seconds'])/row['bytes']*1e9
        with (root/'results.jsonl').open('a') as out:
            out.write(json.dumps(row)+'\n')
        print(json.dumps(row), flush=True)
        if row['read_p99_us_upper'] > args.read_p99_ms*1000:
            raise ValueError('foreground read p99 exceeds budget')
        if args.reads and seconds >= 2 and row['reads'] < args.reads*seconds*.95:
            raise ValueError('foreground read rate is below 95% of requested load')
        if (args.ingest and seconds >= 2
                and row['committed_points'] < row['offered_points']*.98):
            raise ValueError('foreground commit rate is below 98% of input')
        if row['read_errors'] or row['write_errors'] or row['nonempty'] != row['reads']:
            raise ValueError('foreground correctness failed')

    threads = []
    try:
        if not select.select([source.stdout], [], [], 120)[0]:
            raise TimeoutError('source did not become ready')
        endpoint = json.loads(source.stdout.readline())
        if args.ingest:
            threads.append(threading.Thread(target=ingest))
        if args.reads:
            threads.extend(threading.Thread(target=read, args=(i,)) for i in range(8))
        for thread in threads:
            thread.start()
        time.sleep(2)
        phase('idle', lambda: (time.sleep(args.duration) or {}))

        def checkpoint():
            nonlocal manifest
            request = urllib.request.Request(endpoint['copy']+'/admin/store-copy', method='POST')
            with urllib.request.urlopen(request, timeout=120) as response:
                manifest = json.load(response)
            return dict(files=len(manifest['files']))

        manifest = None
        phase('checkpoint', checkpoint)
        (root/'manifest.json').write_text(json.dumps(manifest))
        total = sum(f['size'] for f in manifest['files'])
        source_profile = None
        profiler = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        if args.profile:
            def profile():
                with urllib.request.urlopen(endpoint['control']+f'/debug/pprof/profile?seconds={args.duration}', timeout=args.duration+30) as response:
                    (root/'source.cpu').write_bytes(response.read())
            source_profile = profiler.submit(profile)
        copies = []

        def copying():
            started, target_cpu, target_rss = time.monotonic(), 0, 0
            iteration = 0
            while time.monotonic()-started < args.duration:
                destination = root/('stage-'+str(iteration))
                cmd = [
                    args.copy_binary, 'snapshot', '--action', 'pull',
                    '--source', endpoint['copy'], '--id', manifest['id'],
                    '--destination', str(destination), '--workers', str(args.workers),
                    '--bytes-per-second', str(args.rate), '--local-benchmark', '--apply',
                ]
                if args.lease:
                    cmd += ['--lease', str(Path(args.lease).resolve())]
                if args.profile and not iteration:
                    cmd += ['--cpuprofile', str(root/'target.cpu')]
                with (root/f'pull-{iteration}.out').open('w') as out:
                    target = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, env=env)
                    code, used, rss = reap(target)
                if code:
                    raise ValueError('pull failed')
                target_cpu += used
                target_rss = max(target_rss, rss)
                copies.append(destination)
                if len(copies) > 1:
                    shutil.rmtree(copies[-2])
                iteration += 1
            return dict(
                bytes=total*iteration, copies=iteration,
                target_cpu_seconds=target_cpu, target_RSS=target_rss,
            )

        phase('whole-copy', copying)
        if source_profile:
            source_profile.result()
        profiler.shutdown()
        state = json.loads((copies[-1]/'copy-state.json').read_text())
        expected = {f['name'][6:]: f for f in manifest['files'] if f['name'].startswith('store/')}
        # The transport already verified every chunk; independently read back the
        # last complete staging store while the source remains live.
        for name, entry in expected.items():
            if (copies[-1]/'store'/name).stat().st_size != entry['size']:
                raise ValueError('checkpoint file size differs')
        for key, digest in state['chunks'].items():
            name, offset, length = key.rsplit(':', 2)
            with (copies[-1]/name).open('rb') as stream:
                stream.seek(int(offset))
                if hashlib.sha256(stream.read(int(length))).hexdigest() != digest:
                    raise ValueError('staging read-back differs')
        print(json.dumps(dict(verified_chunks=len(state['chunks']), byte_readback=True)), flush=True)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=10)
        source.send_signal(signal.SIGTERM)
        reap(source)
        source_log.close()


if __name__ == '__main__':
    main()
