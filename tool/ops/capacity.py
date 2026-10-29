#!/usr/bin/env python3
"""Estimate resource floors from timestamped Prometheus report rows."""
import argparse
from collections import Counter
import json
import math
from pathlib import Path

from common import output, positive


RESOURCES = ('cpu_cores', 'memory_bytes', 'disk_bytes', 'read_bytes_per_second', 'write_bytes_per_second', 'network_bytes_per_second')


def percentile(values, quantile):
    values = sorted(values)
    position = (len(values) - 1) * quantile
    lo = math.floor(position)
    hi = math.ceil(position)
    return values[lo] + (values[hi] - values[lo]) * (position - lo)


def size(rows, capacities, quantile=0.95, utilization=0.7, spares=1):
    if not rows or not 0 <= quantile <= 1 or not 0 < utilization <= 1 or spares < 0:
        raise ValueError('need samples, quantile in [0,1], utilization in (0,1], and nonnegative spares')
    hosts = {row['target'] for row in rows}
    timestamps = {host: set() for host in hosts}
    for row in rows:
        timestamps[row['target']].add(row['timestamp'])
    if any(times != next(iter(timestamps.values())) for times in timestamps.values()):
        raise ValueError('targets have different sample grids; use a complete range')
    result = {}
    for resource, capacity in capacities.items():
        if not math.isfinite(capacity) or capacity <= 0:
            raise ValueError('capacity must be positive and finite')
        values = {host: [] for host in hosts}
        seen = set()
        for row in rows:
            value = row.get(resource)
            timestamp = row['timestamp']
            key = (row['target'], timestamp)
            if key in seen:
                raise ValueError('duplicate target timestamp')
            seen.add(key)
            if not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp):
                raise ValueError('invalid timestamp')
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError('missing or invalid ' + resource + '; supply a complete range for every target')
            values[row['target']].append(value)
        demand = sum(percentile(v, quantile) for v in values.values())
        result[resource] = {'sum_of_target_quantiles': demand, 'capacity_per_host': capacity,
                            'hosts_with_headroom': math.ceil(demand / (capacity * utilization))}
    if not result:
        raise ValueError('supply at least one resource capacity')
    floor = max(v['hosts_with_headroom'] for v in result.values())
    return {'targets': len(hosts), 'quantile': quantile, 'utilization': utilization,
            'resources': result, 'resource_floor_hosts': floor, 'spares': spares,
            'hosts_including_spares': floor + spares}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', help='JSON from prometheus.py --report capacity --start ... --end ...')
    parser.add_argument('--quantile', type=float, default=0.95)
    parser.add_argument('--utilization', type=float, default=0.7)
    parser.add_argument('--spares', type=int, default=1)
    for name in RESOURCES:
        parser.add_argument('--' + name.replace('_', '-'), type=positive)
    args = parser.parse_args()
    try:
        report = json.loads(Path(args.report).read_text())
        rows = report['rows']
        present = {row['target'] for row in rows}
        if set(report.get('requested_targets', [])) - present:
            raise ValueError('requested targets have no samples')
        if 'range' in report:
            span = report['range']
            expected = math.floor((span['end'] - span['start']) / span['step']) + 1
            counts = Counter(row['target'] for row in rows)
            if any(count != expected for count in counts.values()):
                raise ValueError('incomplete range; missing target timestamps')
        capacities = {name: getattr(args, name) for name in RESOURCES if getattr(args, name) is not None}
        output(size(rows, capacities, args.quantile, args.utilization, args.spares))
    except (ValueError, KeyError, TypeError, OSError) as exc:
        parser.exit(2, str(exc) + '\n')


if __name__ == '__main__':
    main()
