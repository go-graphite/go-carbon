#!/usr/bin/env python3
"""Temporarily reject external carbonserver reads after replica parity checks."""
import argparse
import hashlib
import json
import os
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


def retained_block(cluster, ports, state_path):
    """Keep explicit operator isolation until a separate release, with no lease."""
    tag = 'store-copy-' + hashlib.sha256((cluster + str(ports[0])).encode()).hexdigest()[:12]
    rows = rules(tag, ports)
    state = Path(state_path)
    # Exclusive creation prevents taking ownership of another operator's block.
    record = dict(version=1, cluster=cluster, ports=ports, tag=tag, status='installing')
    with state.open('x') as stream:
        json.dump(record, stream)
        stream.flush()
        os.fsync(stream.fileno())
    installed = []
    try:
        for binary, *rule in rows:
            result = subprocess.run([binary, '-C', *rule], capture_output=True, timeout=10)
            if result.returncode != 1:
                raise ValueError('rule already exists or firewall check failed; retained state kept')
            subprocess.run([binary, '-I', rule[0], '1', *rule[1:]], check=True, timeout=10)
            installed.append([binary, *rule])
        for binary, *rule in rows:
            subprocess.run([binary, '-C', *rule], check=True, timeout=10)
        record['status'] = 'active'
        temporary = state.with_suffix(state.suffix + '.next')
        with temporary.open('x') as stream:
            json.dump(record, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, state)
    except BaseException:
        # A partial failure happens before any copy is admitted. Never delete a
        # pre-existing rule. Keep the state so failures remain inspectable.
        remove(installed)
        raise
    return dict(action='reads_blocked_retained', tag=tag, state=str(state), ports=ports,
                release='python3 read_block.py --release-state ' + str(state))


def release_retained(state_path):
    state = Path(state_path)
    record = json.loads(state.read_text())
    if record.get('status') != 'active':
        raise ValueError('incomplete retained installation; inspect ownership before manual cleanup')
    ports = record['ports']
    if record.get('version') != 1 or not isinstance(record.get('cluster'), str) or not 1 <= len(ports) <= 2 or any(type(p) is not int or not 1 <= p <= 65535 for p in ports):
        raise ValueError('invalid retained block state')
    tag = 'store-copy-' + hashlib.sha256((record['cluster'] + str(ports[0])).encode()).hexdigest()[:12]
    if record.get('tag') != tag:
        raise ValueError('retained block tag mismatch')
    for binary, *rule in rules(tag, ports):
        result = subprocess.run([binary, '-C', *rule], capture_output=True, timeout=10)
        if result.returncode == 0:
            subprocess.run([binary, '-D', *rule], check=True, timeout=10)
        elif result.returncode != 1:
            raise ValueError('firewall check failed; retained state kept')
        result = subprocess.run([binary, '-C', *rule], capture_output=True, timeout=10)
        if result.returncode != 1:
            raise ValueError('retained rule still present or check failed')
    state.unlink()
    return dict(action='reads_restored', tag=tag)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source')
    parser.add_argument('--peer')
    parser.add_argument('--source-site')
    parser.add_argument('--peer-site')
    parser.add_argument('--cluster')
    parser.add_argument('--probes', help='JSON array of representative metric names for this cluster, including cold history')
    parser.add_argument('--retain-block', action='store_true', help='explicit operator hold: no expiry or exit cleanup; requires --apply and --state')
    parser.add_argument('--state', help='exclusive retained-block state file')
    parser.add_argument('--release-state', help='release a retained block ONLY after copied names are removed or current')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--grpc-port', type=int)
    parser.add_argument('--seconds', type=int, default=3600)
    parser.add_argument('--ready-file', help='operator creates this only after target readiness/parity is approved')
    parser.add_argument('--parity-only', action='store_true', help='compare source and target histories without changing firewall rules; allows same site')
    parser.add_argument('--until', type=int, help='exclusive end of parity window, unix seconds; default now-120 for cross-site validation')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.release_state:
        if args.retain_block or args.apply or args.parity_only:
            parser.error('--release-state is a standalone action')
        print(json.dumps(release_retained(args.release_state)), flush=True)
        return
    for field in ('source', 'peer', 'source_site', 'peer_site', 'cluster', 'probes'):
        if not getattr(args, field):
            parser.error('--' + field.replace('_', '-') + ' is required')
    if args.retain_block and (not args.apply or not args.state or args.parity_only or args.ready_file):
        parser.error('--retain-block requires --apply --state and cannot use --parity-only or --ready-file')
    if not 1 <= args.seconds <= 21600 or not 1 <= args.port <= 65535 or args.grpc_port is not None and not 1 <= args.grpc_port <= 65535:
        parser.error('invalid timeout or port')
    probes = json.loads(Path(args.probes).read_text())
    if not args.parity_only and urllib.parse.urlparse(args.source).hostname not in ('127.0.0.1', '::1'):
        parser.error('run on the source host and use a loopback --source URL for read blocking')
    points = check_peer(args.source, args.peer, probes, args.source_site, args.peer_site, args.parity_only, args.until)
    if args.parity_only:
        print(json.dumps(dict(action='parity', passed=True, verified_points=points, source=args.source, target=args.peer, cluster=args.cluster)))
        return
    if args.retain_block:
        result = retained_block(args.cluster, [args.port] + ([args.grpc_port] if args.grpc_port else []), args.state)
        result['verified_points'] = points
        print(json.dumps(result), flush=True)
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
