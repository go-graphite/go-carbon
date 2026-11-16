#!/usr/bin/env python3
"""Report raw endurance results and a predeclared external-writer exclusion.

The filtered diagnostic never replaces the raw gate. Input is the NUMA runner's
trial directory and its monitor's system.jsonl (including a pre-run baseline).
Process write_bytes measures accounted writes, not physical device writeback.
"""
import argparse
import bisect
import datetime
import json
from pathlib import Path
import re

from numa_bench import summarize


class Series:
    def __init__(self, values):
        self.values = sorted(values)
        self.times = [t for t, _ in self.values]
        if any(a >= b for a, b in zip(self.times, self.times[1:])):
            raise ValueError('duplicate or unordered sample time')

    def at(self, t):
        i = max(0, bisect.bisect_left(self.times, t) - 1)
        if i + 1 >= len(self.values):
            raise ValueError('sample does not cover requested time')
        (a, x), (b, y) = self.values[i:i+2]
        if not a <= t <= b:
            raise ValueError('sample does not cover requested time')
        return x + (y-x) * (t-a) / (b-a)


def stamp(value):
    return datetime.datetime.fromisoformat(re.sub(r'(\.\d{6})\d+', r'\1', value)).timestamp()


def report(trial, system, baseline, factor):
    if factor <= 0:
        raise ValueError('exclusion factor must be positive and predeclared')
    groups = [[json.loads(line) for line in p.read_text().splitlines()]
              for p in sorted(trial.glob('*.jsonl'))]
    raw = summarize(groups)
    starts = [next(x for x in g if x['stage'] == 'core_start') for g in groups]
    ends = [next(x for x in g if x['stage'] == 'core_ingest_complete') for g in groups]
    start = min(stamp(x['when_cet']) for x in starts)
    end = min(x['seconds'] for x in ends)
    points = [Series([(0., 0)] + [(x['seconds'], x['committed']) for x in g
                     if x['stage'] in ('core_sample', 'core_ingest_complete')])
              for g in groups]
    debt = [Series([(0., 0)] + [(x['seconds'], x['estimated_debt_bytes']) for x in g
                   if x['stage'] in ('core_sample', 'core_ingest_complete')])
            for g in groups]
    first, last = baseline['first'], baseline['last']
    identities = {pid: x['start_ticks'] for pid, x in first['daemon_io'].items()}
    if not identities:
        raise ValueError('no external daemon identity in baseline')

    def accounted(sample):
        if sample['io_permission_denied']:
            raise ValueError('incomplete process IO attribution')
        total = 0
        for pid, incarnation in identities.items():
            p = sample['daemon_io'][pid]
            if p['start_ticks'] != incarnation:
                raise ValueError('daemon process incarnation changed')
            total += p['write_bytes']
        return total

    dt = last['monotonic'] - first['monotonic']
    if dt < 25:
        raise ValueError('baseline is shorter than 25 seconds')
    baseline_rate = (accounted(last) - accounted(first)) / dt
    if baseline_rate <= 0:
        raise ValueError('nonpositive baseline write rate')
    values = []
    for line in system.open():
        row = json.loads(line)
        values.append((stamp(row['when_cet']) - start, accounted(row)))
    daemon = Series(values)
    if any(y < x for (_, x), (_, y) in zip(daemon.values, daemon.values[1:])):
        raise ValueError('daemon write counter reset')
    threshold = factor * baseline_rate

    def external_rate(t):
        return (daemon.at(t) - daemon.at(t-10)) / 10

    def rate(t):
        return sum(s.at(t) - s.at(t-10) for s in points) / 1e7

    # Include predicate boundaries as well as throughput extrema. Otherwise an
    # eligible minimum at a threshold crossing could be omitted.
    boundaries = {10., end}
    for s in points + [daemon]:
        for t in s.times:
            boundaries.update(x for x in (t, t+10) if 10 <= x <= end)
    ordered = sorted(boundaries)
    for a, b in zip(ordered, ordered[1:]):
        x, y = external_rate(a) - threshold, external_rate(b) - threshold
        if x * y < 0:
            boundaries.add(a - x * (b-a) / (y-x))
    ordered = sorted(boundaries)
    eligible = [(rate(t), t) for t in ordered
                if external_rate(t) <= threshold * (1 + 1e-12)]
    minimum, minimum_end = min(eligible) if eligible else (None, None)
    eligible_seconds = sum(b-a for a, b in zip(ordered, ordered[1:])
                           if external_rate((a+b)/2) <= threshold)
    aligned = [t for t in range(10, int(end)+1, 10)
               if external_rate(t) <= threshold]
    # Unfiltered debt accounting: never remove interference windows here.
    debt_start = max(0., end-600)
    times = [debt_start] + [float(t) for t in range(int(debt_start)+1, int(end)+1)]
    if times[-1] != end:
        times.append(end)
    debts = [sum(s.at(t) for s in debt) for t in times]
    mean_t, mean_d = sum(times)/len(times), sum(debts)/len(debts)
    slope = sum((t-mean_t)*(d-mean_d) for t, d in zip(times, debts)) / sum(
        (t-mean_t)**2 for t in times)
    cpu_ns = raw['cores'] / raw['million_per_second'] * 1000
    return dict(raw=raw, cpu_ns_per_point=cpu_ns,
                written_bytes_per_point=raw['write_MB_s']/raw['million_per_second'],
                raw_gate=raw['min_10s'] >= .9*raw['million_per_second'] and cpu_ns <= 120,
                exclusion=dict(factor=factor, baseline_seconds=dt,
                               baseline_daemon_write_MB_s=baseline_rate/1e6,
                               threshold_MB_s=threshold/1e6,
                               min_10s=minimum, min_10s_end=minimum_end,
                               worst_percent_of_raw_mean=(100*minimum/raw['million_per_second']
                                                          if minimum is not None else None),
                               gate=(minimum >= .9*raw['million_per_second'] and cpu_ns <= 120
                                     if minimum is not None else None),
                               sliding_endpoint_eligible_fraction=eligible_seconds/(end-10),
                               eligible_aligned_windows=len(aligned),
                               total_aligned_windows=int(end)//10,
                               eligible_aligned_mean_M_s=(sum(rate(t) for t in aligned)/len(aligned)
                                                          if aligned else None),
                               raw_worst_daemon_MB_s=external_rate(raw['min_10s_end'])/1e6,
                               daemon_identities=identities),
                unfiltered_debt=dict(seconds=end-debt_start, slope_bytes_per_second=slope,
                                     first_bytes=debts[0], last_bytes=debts[-1],
                                     min_bytes=min(debts), max_bytes=max(debts)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trial', required=True, type=Path)
    parser.add_argument('--system', required=True, type=Path)
    parser.add_argument('--baseline', required=True, type=Path)
    parser.add_argument('--factor', required=True, type=float)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    result = report(args.trial, args.system, json.loads(args.baseline.read_text()), args.factor)
    args.out.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
