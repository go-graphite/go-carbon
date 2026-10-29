#!/usr/bin/env python3
"""Target capacity/configuration preflight. Transfers use the shared resize engine."""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess


def atomic(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.next')
    with temporary.open('w') as out:
        json.dump(value, out, sort_keys=True)
        out.write('\n')
        out.flush()
        os.fsync(out.fileno())
    temporary.replace(path)


def utc_after(seconds):
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)).isoformat().replace('+00:00', 'Z')


def memory_bytes():
    if Path('/proc/meminfo').exists():
        rows = dict((line.split(':')[0], int(line.split()[1]) * 1024)
                    for line in Path('/proc/meminfo').read_text().splitlines())
        return rows['MemTotal'], rows['MemAvailable']
    total = int(subprocess.check_output(['sysctl', '-n', 'hw.memsize']))
    return total, total  # macOS preflight only; runtime tuning uses Linux samples.


def physical_cores():
    if Path('/sys/devices/system/cpu').exists():
        pairs = set()
        for cpu in Path('/sys/devices/system/cpu').glob('cpu[0-9]*'):
            topology = cpu / 'topology'
            pairs.add(((topology / 'physical_package_id').read_text(), (topology / 'core_id').read_text()))
        return len(pairs)
    return int(subprocess.check_output(['sysctl', '-n', 'hw.physicalcpu']))


def preflight(args):
    import tomllib
    destination = Path(args.destination)
    if not destination.is_absolute() or destination.resolve() != destination:
        raise ValueError('destination must be absolute and canonical')
    parent = destination
    while not parent.exists():
        parent = parent.parent
    free = shutil.disk_usage(parent).free
    total, available = memory_bytes()
    cores = physical_cores()
    config = tomllib.loads(Path(args.config).read_text())
    whisper, cache = config.get('whisper', {}), config.get('cache', {})
    input_log = config.get('input-log', {})
    # Include fixed input-log shard buffers (1024 * 32768 * 24 B), point
    # buffers, duplicate encoder buffers, block/state caches and Go headroom.
    components = {
        'block_cache': whisper.get('store-cache-size', 256 << 20),
        'metadata_cache': whisper.get('store-metadata-cache-size', 0),
        'memtables': whisper.get('store-memtable-max-unflushed-bytes', 0) or (8 << 30),
        'point_cache_estimate': cache.get('max-size', 0) * 32,
        'input_log_buffers_reserve': (2 << 30) if input_log.get('enabled') else 0,
        'index_runtime_reserve': args.runtime_reserve_bytes,
    }
    budget = math.ceil(sum(components.values()) * 1.25)
    checks = {
        'disk': free >= args.store_bytes + args.log_reserve_bytes + args.disk_reserve_bytes,
        'memory': budget <= min(total * .80, args.memory_limit_bytes or total),
        'cores': cores >= args.min_cores,
        'backend': whisper.get('storage-backend') == 'pebble-chunk',
        'input_log': input_log.get('enabled') is True,
        'workers': 1 <= whisper.get('workers', 1) <= cores * 4,
        'compactors': 0 <= whisper.get('store-compaction-workers', 0) <= min(64, cores),
        'max_cpu': 0 < config.get('common', {}).get('max-cpu', 1) <= cores,
    }
    report = dict(version=1, hostname=socket.gethostname(), destination=str(destination),
                  expires=utc_after(24 * 3600), passed=all(checks.values()), checks=checks,
                  free_bytes=free, total_memory_bytes=total, available_memory_bytes=available,
                  estimated_memory_bytes=budget, memory_components=components, physical_cores=cores,
                  required_disk_bytes=args.store_bytes + args.log_reserve_bytes + args.disk_reserve_bytes,
                  disk_reserve_bytes=args.disk_reserve_bytes, store_bytes=args.store_bytes, log_reserve_bytes=args.log_reserve_bytes,
                  config_sha256=hashlib.sha256(Path(args.config).read_bytes()).hexdigest())
    report['config_path'] = str(Path(args.config).resolve())
    print(json.dumps(report, sort_keys=True))
    if args.apply:
        atomic(args.output, report)
    if not report['passed']:
        raise ValueError('preflight failed')



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    pre = commands.add_parser('preflight')
    pre.add_argument('--destination', required=True)
    pre.add_argument('--config', required=True)
    pre.add_argument('--store-bytes', type=int, required=True)
    pre.add_argument('--log-reserve-bytes', type=int, required=True)
    pre.add_argument('--disk-reserve-bytes', type=int, default=100 << 30)
    pre.add_argument('--runtime-reserve-bytes', type=int, default=4 << 30)
    pre.add_argument('--memory-limit-bytes', type=int, default=0)
    pre.add_argument('--min-cores', type=int, default=8)
    pre.add_argument('--output', default='preflight.json')
    pre.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    preflight(args)


if __name__ == '__main__':
    main()
