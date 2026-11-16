#!/usr/bin/env python3
"""Bounded fio comparison on fresh private files. Never uses raw devices/syncfs.

The caller must preflight disk space and monitor any colocated daemon. Compare
buffered/direct I/O, 1/4/16 MiB blocks, and per-close/end-of-stage fsync scheduling.
End-of-stage still syncs each file; this is not one shared filesystem sync.
Reference: https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-end-fsync
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--gib', type=int, default=4,
                        help='total data per trial; eight equal files')
    parser.add_argument('--timeout', type=int, default=30,
                        help='wall seconds per trial, including final sync')
    args = parser.parse_args()
    if args.gib < 1 or args.timeout < 1:
        parser.error('gib and timeout must be positive')
    fio = shutil.which('fio')
    if not fio:
        raise SystemExit('fio is unavailable; install outside this script')
    version = subprocess.check_output([fio, '--version'], text=True).strip()
    if not version.startswith('fio-'):
        raise SystemExit('fio executable is not the Flexible I/O tester')
    args.out.mkdir(parents=True, exist_ok=False)
    if shutil.disk_usage(args.out).free < (args.gib + 8) * (1 << 30):
        raise SystemExit('insufficient free space for sweep and safety margin')
    total = args.gib * (1 << 30)
    for direct in (0, 1):
        for mib in (1, 4, 16):
            for schedule in ('close', 'end'):
                if (args.out / 'STOP').exists():
                    raise SystemExit('sweep stopped by monitor/operator')
                label = f'direct{direct}-{mib}MiB-{schedule}'
                data = args.out / (label + '-files')
                data.mkdir()
                cmd = [fio, '--name=sweep', '--directory=' + str(data),
                       '--rw=write', '--ioengine=psync', '--iodepth=1',
                       '--numjobs=1', '--nrfiles=8', '--openfiles=1',
                       '--file_service_type=sequential', '--fallocate=none',
                       '--create_on_open=1', '--size=' + str(total),
                       '--filesize=' + str(total // 8), '--bs=' + str(mib << 20),
                       '--direct=' + str(direct), '--group_reporting=1',
                       '--fsync_on_close=' + str(int(schedule == 'close')),
                       '--end_fsync=' + str(int(schedule == 'end')),
                       '--output-format=json',
                       '--output=' + str(args.out / (label + '.fio.json'))]
                start = time.monotonic()
                when = datetime.datetime.now().astimezone().isoformat()
                try:
                    # poll() permits a colocated-daemon monitor to stop only
                    # this worker's active fio process by creating STOP.
                    with (args.out / (label + '.log')).open('w') as log:
                        child = subprocess.Popen(cmd, stdout=log, stderr=log,
                                                 start_new_session=True)
                        try:
                            while child.poll() is None:
                                if (args.out / 'STOP').exists() or time.monotonic() - start > args.timeout:
                                    raise RuntimeError('monitor stop or trial timeout')
                                time.sleep(.1)
                            rc = child.returncode
                        finally:
                            if child.poll() is None:
                                os.killpg(child.pid, signal.SIGTERM)
                                try:
                                    child.wait(timeout=5)
                                except subprocess.TimeoutExpired:
                                    os.killpg(child.pid, signal.SIGKILL)
                                    child.wait()
                    if rc:
                        raise RuntimeError(f'fio failed ({rc}); inspect {label}.log')
                    jobs = json.loads((args.out / (label + '.fio.json')).read_text())['jobs']
                    if any(job['error'] for job in jobs):
                        raise RuntimeError('fio reported I/O errors')
                    seconds = time.monotonic() - start
                    summary = dict(when_cet=when, label=label, fio=version,
                                   wall_seconds=seconds,
                                   MB_s=sum(j['write']['io_bytes'] for j in jobs) /
                                   seconds / 1e6,
                                   argv=cmd)
                    with (args.out / 'summary.jsonl').open('a') as f:
                        f.write(json.dumps(summary) + '\n')
                    print(json.dumps(summary), flush=True)
                finally:
                    shutil.rmtree(data)


if __name__ == '__main__':
    main()
