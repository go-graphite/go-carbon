#!/usr/bin/env python3
"""Collect NVMe SMART health as Prometheus node-exporter textfile metrics."""
import argparse
import json
import math
import re
import subprocess
import time
from pathlib import Path

from common import atomic_write, positive


FIELDS = {'critical_warning': 'critical_warning', 'temperature': 'temperature_celsius',
          'available_spare': 'available_spare_percent', 'available_spare_threshold': 'available_spare_threshold_percent',
          'percentage_used': 'percentage_used', 'power_on_hours': 'power_on_hours',
          'unsafe_shutdowns': 'unsafe_shutdowns_total', 'media_errors': 'media_errors_total',
          'num_err_log_entries': 'error_log_entries_total',
          'data_units_read': 'data_units_read_total', 'data_units_written': 'data_units_written_total'}


def render(device, data):
    health = data.get('nvme_smart_health_information_log')
    if not isinstance(health, dict):
        raise ValueError('no NVMe SMART health log')
    label = '{device=' + json.dumps(device) + '}'
    result = []
    for field, name in FIELDS.items():
        value = health.get(field)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
            raise ValueError('invalid SMART value for ' + field)
        result.append('go_carbon_nvme_' + name + label + ' ' + str(value))
    if not result:
        raise ValueError('NVMe SMART health log is empty')
    return result


def collect(devices, executable='smartctl', timeout=30, run=subprocess.run, clock=time.time):
    result, errors = [], []
    for device in devices:
        ok = False
        try:
            proc = run([executable, '-a', '-j', '--', device], capture_output=True, text=True, timeout=timeout)
            # smartctl bits 0..2 mean command/open/SMART retrieval failure.
            # Bits 3..7 report unhealthy devices; retain their diagnostic values.
            if proc.returncode < 0 or proc.returncode & 7:
                raise ValueError('SMART retrieval failed (exit %d)' % proc.returncode)
            result.extend(render(device, json.loads(proc.stdout)))
            ok = True
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            errors.append({'device': device, 'error': str(exc)})
        label = '{device=' + json.dumps(device) + '}'
        result.append('go_carbon_nvme_scrape_success' + label + ' ' + ('1' if ok else '0'))
        if ok:
            result.append('go_carbon_nvme_last_success_timestamp_seconds' + label + ' ' + str(clock()))
    return '\n'.join(result) + '\n', errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--device', action='append', required=True, help='NVMe controller or namespace; repeatable')
    parser.add_argument('--smartctl', default='smartctl')
    parser.add_argument('--timeout', type=positive, default=30)
    parser.add_argument('--output', help='atomic .prom textfile; otherwise stdout')
    args = parser.parse_args()
    if len(set(args.device)) != len(args.device) or any(not re.fullmatch(r'/dev/[A-Za-z0-9_./-]+', d) or '..' in Path(d).parts for d in args.device):
        parser.error('devices must be distinct paths under /dev')
    body, errors = collect(args.device, args.smartctl, args.timeout)
    if args.output:
        if not args.output.endswith('.prom'):
            parser.error('textfile output must end in .prom')
        atomic_write(args.output, body.encode(), 0o644)
    else:
        print(body, end='')
    for error in errors:
        print(json.dumps(error), file=__import__('sys').stderr)
    raise SystemExit(1 if errors else 0)


if __name__ == '__main__':
    main()
