"""Read-only Linux IO attribution for the offline benchmark.

Process write_bytes counts page dirtying, not physical writeback. Compare its
deltas with cancelled_write_bytes, Dirty/Writeback and whole-device counters.
PID plus start_ticks identifies a process across samples. No command lines or
environment variables are recorded. Other users' counters may require privilege;
permission_denied makes incomplete attribution visible in every sample.
"""
import datetime
import json
import os
from pathlib import Path
import threading
import time


def snapshot(proc=Path('/proc')):
    result = dict(when_cet=datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=1))).isoformat(),
        monotonic=time.monotonic(), diskstats=(proc / 'diskstats').read_text(),
        memory={k: v.strip() for k, v in
                (line.split(':', 1) for line in (proc / 'meminfo').read_text().splitlines())
                if k in ('Dirty', 'Writeback', 'MemAvailable')},
        processes={}, permission_denied=0, disappeared=0)
    for path in proc.iterdir():
        if not path.name.isdigit():
            continue
        try:
            # Read start time on both sides of the counters: don't combine two
            # incarnations when a short-lived process's PID is reused.
            before = (path / 'stat').read_text()
            start = int(before[before.rfind(')') + 2:].split()[19])
            counters = {k: int(v) for k, v in
                        (line.split(':', 1) for line in (path / 'io').read_text().splitlines())}
            comm = (path / 'comm').read_text().strip()
            try:
                executable = os.readlink(path / 'exe')
            except FileNotFoundError:
                executable = ''  # Kernel threads have no executable link.
            after = (path / 'stat').read_text()
            if int(after[after.rfind(')') + 2:].split()[19]) != start:
                result['disappeared'] += 1
                continue
            result['processes'][path.name] = dict(
                comm=comm, exe=executable, start_ticks=start, **counters)
        except PermissionError:
            result['permission_denied'] += 1
        except (FileNotFoundError, ProcessLookupError):
            result['disappeared'] += 1
    result['sample_seconds'] = time.monotonic() - result['monotonic']
    return result


class HostIO:
    def __init__(self, output):
        self.output = output
        self.stop = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.run, name='host-io', daemon=True)

    def run(self):
        try:
            with self.output.open('x') as stream:
                while not self.stop.is_set():
                    tick = time.monotonic()
                    stream.write(json.dumps(snapshot()) + '\n')
                    stream.flush()
                    self.stop.wait(max(0, 1 - (time.monotonic() - tick)))
        except Exception as exc:
            self.error = exc

    def start(self):
        self.thread.start()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError('host IO sampler did not stop')
        if self.error is not None:
            raise RuntimeError('host IO trace failed') from self.error
