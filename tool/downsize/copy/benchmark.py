#!/usr/bin/env python3
"""Local-only A/B of immutable checkpoint transfers; never invokes SSH."""
import argparse
import hashlib
import json
from pathlib import Path
import resource
import shlex
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
import os


def digest_tree(directory):
    result = {}
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.name not in ('LOCK', 'NEEDS-CATCHUP'):
            h = hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda: stream.read(1 << 20), b''):
                    h.update(block)
            result[path.name] = h.hexdigest()
    return result


def usage():
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


def sync_file(path):
    with path.open('rb') as stream:
        os.fsync(stream.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--checkpoint', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--workers', default='1,2,4,8,16')
    args = parser.parse_args()
    checkpoint = Path(args.checkpoint).resolve()
    output = Path(args.output).resolve()
    output.mkdir(exist_ok=True)
    size = sum(p.stat().st_size for p in checkpoint.iterdir() if p.is_file() and p.name != 'LOCK')
    reference = digest_tree(checkpoint)
    cases = [('tar-zstd-1-T2-pipe', None), ('rsync-local-W', None)] + [('parallel-http-' + n, int(n)) for n in args.workers.split(',')]
    rows = []
    for case, workers in cases:
        for iteration in range(args.rounds):
            dest = output / (case + '-' + str(iteration))
            dest.mkdir()
            before = usage()
            server = None
            started = time.monotonic()
            try:
                if workers:
                    server = subprocess.Popen([args.binary, '--action', 'serve-offline', '--source', 'http://127.0.0.1:0', '--destination', str(checkpoint)], stdout=subprocess.PIPE, text=True)
                    endpoint = json.loads(server.stdout.readline())
                    cmd = [args.binary, '--action', 'pull', '--source', endpoint['url'], '--id', endpoint['id'], '--destination', str(dest), '--workers', str(workers), '--bytes-per-second', '0', '--local-benchmark', '--apply']
                    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
                    copied = dest / 'store'
                elif case.startswith('tar'):
                    command = f'tar -C {shlex.quote(str(checkpoint))} -cf - . | zstd -1 -T2 -q | zstd -d -q | tar -C {shlex.quote(str(dest))} -xf -'
                    subprocess.run(['bash', '-o', 'pipefail', '-c', command], check=True)
                    copied = dest
                else:
                    subprocess.run(['rsync', '-aW', str(checkpoint) + '/', str(dest) + '/'], check=True, stdout=subprocess.DEVNULL)
                    copied = dest
                # Include durable writeback in every method's wall time. The Go
                # transfer already syncs; its second sync has no dirty data.
                with ThreadPoolExecutor(max_workers=8) as pool:
                    list(pool.map(sync_file, [p for p in copied.iterdir() if p.is_file()]))
                elapsed = time.monotonic() - started
            finally:
                if server:
                    server.terminate()
                    server.wait(timeout=10)
            cpu = usage() - before
            if digest_tree(copied) != reference:
                raise ValueError('transfer byte parity failed: ' + case)
            row = dict(case=case, iteration=iteration, bytes=size, seconds=elapsed, MB_per_second=size / elapsed / 1e6, cpu_seconds=cpu, cpu_seconds_per_GB=cpu / (size / 1e9), sha256_parity=True)
            rows.append(row)
            print(json.dumps(row, sort_keys=True), flush=True)
            (output / 'results.json').write_text(json.dumps(rows, indent=2))
            shutil.rmtree(dest)


if __name__ == '__main__':
    main()
