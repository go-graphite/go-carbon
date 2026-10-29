#!/usr/bin/env python3
"""Query fleet health, migration, cost, memory, input logging and capacity."""
import argparse
import json
import math
import os
from pathlib import Path

from common import client, http_flags, lines, output, positive_int
from queries import label_name, queries, selector


REPORTS = ('health', 'migration', 'cost', 'gc', 'input-log', 'io', 'parity', 'capacity')


def query(api, expression, host_label, start=None, end=None, step=60, at=None):
    params = {'query': expression}
    if start is None:
        path = '/api/v1/query'
        if at is not None:
            params['time'] = at
    else:
        path = '/api/v1/query_range'
        params.update(start=start, end=end, step=step)
    data = api.json(path, params)
    if data.get('status') != 'success':
        raise ValueError('Prometheus query failed')
    if data.get('warnings'):
        raise ValueError('Prometheus returned warnings; results may be incomplete: ' + str(data['warnings']))
    payload = data.get('data', {})
    if payload.get('resultType') != ('vector' if start is None else 'matrix'):
        raise ValueError('expected a vector or matrix query result')
    result = {}
    for series in payload['result']:
        host = series['metric'].get(host_label)
        if not host:
            raise ValueError('query result lacks target label ' + host_label)
        for timestamp, raw in series.get('values', [series.get('value')]):
            key = (host, float(timestamp))
            if key in result:
                raise ValueError('duplicate target; aggregate query by ' + host_label)
            value = float(raw)
            result[key] = value if math.isfinite(value) else None
    return result


def collect(api, expressions, args):
    rows = {}
    # Every query uses the same evaluation time, including instant queries.
    for name, expression in expressions.items():
        for (host, timestamp), value in query(api, expression, args.host_label,
                                             args.start, args.end, args.step, args.at).items():
            rows.setdefault((host, timestamp), {'target': host, 'timestamp': timestamp})[name] = value
    return [{**{name: None for name in expressions}, **row}
            for _, row in sorted(rows.items())]


def compare(rows, peers, ratio=1.25):
    if not isinstance(peers, dict):
        raise ValueError('peer file must be a JSON object')
    by_host = {row['target']: row for row in rows}
    result = []
    for candidate, baselines in sorted(peers.items()):
        if not isinstance(candidate, str) or not isinstance(baselines, list) or not baselines or any(not isinstance(p, str) for p in baselines) or candidate in baselines or len(set(baselines)) != len(baselines):
            raise ValueError('each candidate needs distinct peer names excluding itself')
        base = by_host.get(candidate)
        eligible = [by_host[p] for p in baselines if p in by_host and by_host[p].get('up') == 1
                    and by_host[p].get('migration_in_flight') == 0 and by_host[p].get('migration_failed') == 0]
        verdict = {'target': candidate, 'peers_used': [p['target'] for p in eligible], 'ratios': {}, 'verdict': 'unknown'}
        if (not base or base.get('up') != 1 or base.get('migration_in_flight') != 0
                or base.get('migration_failed') != 0 or not eligible):
            result.append(verdict)
            continue
        complete = len(eligible) == len(baselines)
        for name in ('read_p99_seconds', 'cpu_cores', 'rss_bytes'):
            values = [row.get(name) for row in eligible]
            actual = base.get(name)
            if actual is None or any(v is None for v in values) or sum(values) <= 0:
                complete = False
                continue
            verdict['ratios'][name] = actual / (sum(values) / len(values))
        if complete:
            verdict['verdict'] = 'regression' if any(v > ratio for v in verdict['ratios'].values()) else 'within-threshold'
        result.append(verdict)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default=os.getenv('PROMETHEUS_URL'), help='Prometheus-compatible base URL, including any tenant path prefix')
    parser.add_argument('--report', choices=REPORTS, default='health')
    parser.add_argument('--selector', default='', help='PromQL label matchers, without braces')
    parser.add_argument('--host-label', default='instance')
    parser.add_argument('--target', action='append', default=[], help='exact target label value; repeatable')
    parser.add_argument('--targets-file', help='one exact target label value per line')
    parser.add_argument('--window', default='10m')
    parser.add_argument('--start', type=float, help='range start as Unix seconds')
    parser.add_argument('--end', type=float, help='range end as Unix seconds')
    parser.add_argument('--at', type=float, help='instant evaluation time as Unix seconds')
    parser.add_argument('--step', type=positive_int, default=60, help='range step in seconds')
    parser.add_argument('--backend', choices=('pebble', 'whisper'), default='pebble', help='backend point/update counters for the cost report')
    parser.add_argument('--mountpoint', default='/var/lib/carbon')
    parser.add_argument('--query', action='append', default=[], metavar='NAME=PROMQL', help='add or override a report column')
    parser.add_argument('--peers', help='JSON object mapping candidate targets to arrays of peer targets')
    parser.add_argument('--max-ratio', type=float, default=1.25)
    parser.add_argument('--print-queries', action='store_true', help='show generated queries without network access')
    http_flags(parser)
    args = parser.parse_args()
    try:
        import time
        if args.start is not None:
            if args.end is None or not all(math.isfinite(v) for v in (args.start, args.end)) or args.end <= args.start or args.at is not None:
                raise ValueError('range requires finite start < end and no --at')
        elif args.end is not None:
            raise ValueError('--end requires --start')
        if args.at is None:
            args.at = time.time()
        if not math.isfinite(args.at) or not math.isfinite(args.max_ratio) or args.max_ratio <= 0:
            raise ValueError('time and ratio must be finite; ratio must be positive')
        if args.report == 'parity' and (not args.peers or args.start is not None):
            raise ValueError('parity requires --peers and an instant query')
        hosts = args.target + (lines(args.targets_file) if args.targets_file else [])
        peers = json.loads(Path(args.peers).read_text()) if args.report == 'parity' else None
        if peers is not None:
            compare([], peers)  # Validate the peer manifest before making requests.
            hosts = sorted(set(hosts) | set(peers) | {p for baselines in peers.values() for p in baselines})
        select = selector(args.selector, label_name(args.host_label), hosts)
        expressions = queries(args.report, select, args.host_label, args.window, args.mountpoint, args.backend)
        if args.report == 'parity':
            expressions['migration_in_flight'] = 'max by (%s) (max_over_time(store_migration_in_flight{%s}[%s]))' % (args.host_label, select, args.window)
        for custom in args.query:
            name, expression = custom.split('=', 1)
            if not name or not expression or name in ('target', 'timestamp'):
                raise ValueError('query requires a non-reserved name and an expression')
            expressions[name] = expression
        if args.print_queries:
            output(expressions)
            return
        if not args.endpoint:
            raise ValueError('--endpoint or PROMETHEUS_URL is required')
        rows = collect(client(args, args.endpoint), expressions, args)
        # Explicit targets remain visible even when no metric matched them.
        present = {r['target'] for r in rows}
        if args.start is None:
            rows.extend({'target': host, 'timestamp': args.at, **{name: None for name in expressions}}
                        for host in sorted(set(hosts) - present))
        result = {'report': args.report, 'requested_targets': hosts, 'queries': expressions, 'rows': rows}
        if args.start is not None:
            result['range'] = {'start': args.start, 'end': args.end, 'step': args.step}
        if args.report == 'parity':
            result['comparison'] = compare(rows, peers, args.max_ratio)
        output(result)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        parser.exit(2, str(exc) + '\n')


if __name__ == '__main__':
    main()
