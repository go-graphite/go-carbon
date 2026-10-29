#!/usr/bin/env python3
"""Temporarily reject external carbonserver reads after replica parity checks."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request


def fetch(base, metric, start, end):
    query = urllib.parse.urlencode(dict(target=metric, **{'from': start}, until=end, format='json'))
    with urllib.request.urlopen(base.rstrip('/') + '/render/?' + query, timeout=10) as response:
        data = json.load(response)
    rows = data.get('metrics', [])
    if len(rows) != 1 or rows[0].get('name') != metric:
        raise ValueError('replica did not return exact probe metric: ' + metric)
    row = rows[0]
    values = row.get('values', [])
    absent = row.get('isAbsent', [False] * len(values))
    step = int(row.get('stepTime', 0))
    begin = int(row.get('startTime', 0))
    if step <= 0 or not values or len(absent) != len(values):
        raise ValueError('invalid or empty replica history')
    return {begin + i * step: v for i, v in enumerate(values) if not absent[i] and v not in ('NaN', None)}


def check_peer(source, peer, probes, source_site, peer_site, allow_same_site=False, end=None):
    if source_site == peer_site and not allow_same_site or source.rstrip('/') == peer.rstrip('/') or len(set(probes)) < 3:
        raise ValueError('need a different site replica and at least three distinct cluster probes')
    end = end or (int(time.time()) - 120)
    start = end - 3600
    checked = 0
    for metric in probes:
        old, other = fetch(source, metric, start, end), fetch(peer, metric, start, end)
        if not old or any(other.get(timestamp) != value for timestamp, value in old.items()):
            raise ValueError('peer history is missing/different: ' + metric)
        checked += len(old)
    return checked


def rules(tag, ports):
    result = []
    for binary, loopback in [('iptables', '127.0.0.1/32'), ('ip6tables', '::1/128')]:
        for port in ports:
            result.append([binary, 'INPUT', '!', '-s', loopback, '-p', 'tcp', '--dport', str(port),
                           '-m', 'comment', '--comment', tag, '-j', 'REJECT', '--reject-with', 'tcp-reset'])
    return result


def remove(rows):
    for binary, *rule in rows:
        # At most one rule is installed by this invocation.
        subprocess.run([binary, '-D', *rule], capture_output=True, timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True)
    parser.add_argument('--peer', required=True)
    parser.add_argument('--source-site', required=True)
    parser.add_argument('--peer-site', required=True)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--probes', required=True, help='JSON array of representative metric names for this cluster, including cold history')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--grpc-port', type=int)
    parser.add_argument('--seconds', type=int, default=3600)
    parser.add_argument('--ready-file', help='operator creates this only after target readiness/parity is approved')
    parser.add_argument('--parity-only', action='store_true', help='compare source and target histories without changing firewall rules; allows same site')
    parser.add_argument('--until', type=int, help='exclusive end of parity window, unix seconds; default now-120 for cross-site validation')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 21600 or not 1 <= args.port <= 65535 or args.grpc_port is not None and not 1 <= args.grpc_port <= 65535:
        parser.error('invalid timeout or port')
    probes = json.loads(Path(args.probes).read_text())
    if not args.parity_only and urllib.parse.urlparse(args.source).hostname not in ('127.0.0.1', '::1'):
        parser.error('run on the source host and use a loopback --source URL for read blocking')
    points = check_peer(args.source, args.peer, probes, args.source_site, args.peer_site, args.parity_only, args.until)
    if args.parity_only:
        print(json.dumps(dict(action='parity', passed=True, verified_points=points, source=args.source, target=args.peer, cluster=args.cluster)))
        return
    tag = 'store-copy-' + hashlib.sha256((args.cluster + str(args.port)).encode()).hexdigest()[:12]
    rows = rules(tag, [args.port] + ([args.grpc_port] if args.grpc_port else []))
    print(json.dumps(dict(action='block_reads', verified_points=points, cluster=args.cluster,
                          peer=args.peer, restore_after_seconds=args.seconds, commands=[[r[0], '-I', *r[1:]] for r in rows], apply=args.apply)), flush=True)
    if not args.apply:
        return
    # Arm independent cleanup before changing any firewall rule. Each transient
    # timer runs an exact argv deletion, with no shell or wildcard rule removal.
    stamp = str(time.time_ns())
    for i, (binary, *rule) in enumerate(rows):
        if subprocess.run([binary, '-C', *rule], capture_output=True, timeout=10).returncode == 0:
            raise ValueError('read-block rule already exists; keep its original watcher')
        subprocess.run(['systemd-run', '--on-active=' + str(args.seconds) + 's',
                        '--unit=' + tag + '-' + stamp + '-' + str(i), binary, '-D', *rule], check=True, timeout=15)
    try:
        for binary, *rule in rows:
            subprocess.run([binary, '-I', rule[0], '1', *rule[1:]], check=True, timeout=10)
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            if args.ready_file and Path(args.ready_file).exists():
                break
            check_peer(args.source, args.peer, probes, args.source_site, args.peer_site)
            time.sleep(5)
    finally:
        remove(rows)
        print(json.dumps(dict(action='reads_restored', tag=tag)), flush=True)


if __name__ == '__main__':
    main()
