#!/usr/bin/env python3
"""Exercise real supervised shared-store restarts. No third-party Python modules.

Build ./test/input-handoff/wire and pass --wire and --carbon executable paths.
All fixtures, stores, logs, and child processes are removed on completion.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def eventually(fn, timeout=30):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            answer = fn()
            if answer:
                return answer
        except (OSError, ValueError, KeyError) as error:
            last = error
        time.sleep(.005)
    raise RuntimeError(f'timed out: {last}')


def entries(path):
    # Ignore the final partial line while a process is still writing.
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True)
            if line.startswith('{') and line.endswith('\n')]


def metrics(p):
    with urllib.request.urlopen(f'http://127.0.0.1:{p}/metrics', timeout=2) as response:
        return dict(line.split()[:2] for line in response.read().decode().splitlines()
                    if line and not line.startswith('#') and '{' not in line)


def stored(http, fixture, max_index):
    seen = set()
    for m in range(fixture['names']):
        name = f'example.application.component.metric_{m:02d}'
        query = urllib.parse.urlencode(dict(target=name, **{'from': fixture['base']-1,
                    'until': fixture['base']+max_index//fixture['names']+2, 'format': 'json'}))
        with urllib.request.urlopen(f'http://127.0.0.1:{http}/render/?'+query, timeout=5) as response:
            data = json.load(response)
        for series in data.get('metrics', []):
            for n, value in enumerate(series['values']):
                if series['isAbsent'][n]:
                    continue
                ts = series['startTime']+n*series['stepTime']
                i = (ts-fixture['base'])*fixture['names']+m
                assert value == i+.125, (i, value)
                assert i not in seen, i
                seen.add(i)
    return seen


def run(root, binary, fixtures, mode, enabled, experiment, input_log_only=False, dump_enabled=None):
    root.mkdir()
    for d in ['data', 'store', 'dump', 'input-log']:
        (root/d).mkdir()
    tcp, http, prom = port(), port(), port()
    (root/'schemas.conf').write_text('[all]\npattern = .*\nretentions = 1s:2h\n')
    config = f'''[common]
user = ""
max-cpu = 4
metric-endpoint = ""
metric-interval = "10ms"
[[logging]]
logger = ""
level = "info"
encoding = "json"
file = "stderr"
[[logging]]
logger = "stat"
level = "error"
file = "stderr"
[cache]
max-size = 1000000
[whisper]
enabled = true
storage-backend = "pebble-chunk"
store-encoding = "exact"
store-dir = "store"
store-cache-size = 16777216
store-memtable-size = 8388608
compressed = true
out-of-order = true
flock = true
schemas-file = "schemas.conf"
data-dir = "data"
workers = 2
[tags]
enabled = false
[udp]
enabled = false
[tcp]
enabled = true
listen = "127.0.0.1:{tcp}"
[pickle]
enabled = false
[grpc]
enabled = false
[carbonlink]
enabled = false
[carbonserver]
enabled = true
listen = "127.0.0.1:{http}"
scan-frequency = "200ms"
trie-index = true
concurrent-index = true
realtime-index = 10000
file-list-cache = "flc"
file-list-cache-version = 2
query-cache-enabled = false
find-cache-enabled = false
glob-cache-enabled = false
[prometheus]
enabled = true
[pprof]
enabled = false
listen = "127.0.0.1:{prom}"
[dump]
enabled = {str(not input_log_only if dump_enabled is None else dump_enabled).lower()}
input-log-only = {str(input_log_only).lower()}
path = "dump"
format = "compact"
restore-per-second = 0
handoff-supervisor = true
store-input-handoff = {str(enabled).lower()}
[input-log]
enabled = true
background-replay = {str(input_log_only).lower()}
path = "input-log"
'''
    (root/'carbon.conf').write_text(config)
    fixture = json.loads((fixtures/'fixture.json').read_text())
    chunks = [(fixtures/mode/f'{i:03d}').read_bytes() for i in range(fixture['batches'])]
    preamble = (fixtures/'preamble').read_bytes() if mode == 'stream' else b''
    procs, logs, sockets = [], [], []
    def start(n):
        path = root/f'{n}.log'
        log = path.open('w'); logs.append(log)
        p = subprocess.Popen([str(binary), '-config', 'carbon.conf'], cwd=root,
                             stdout=log, stderr=log, start_new_session=True)
        procs.append(p)
        eventually(lambda: any(e.get('message') == 'started' for e in entries(path)))
        return p
    def connect():
        s = socket.create_connection(('127.0.0.1', tcp), timeout=2)
        s.settimeout(5)
        if preamble:
            s.sendall(preamble)
        sockets.append(s)
        return s
    try:
        old = start('old')
        conn = connect()
        # Exact receiver count before the restart, independent of sendall.
        for data in chunks[:16]:
            conn.sendall(data)
        eventually(lambda: float(metrics(prom).get('metrics_received_tcp_total', 0)) == 4096)
        time.sleep(.5)  # Publish the mapped index required for a checkpoint.
        sent, failed, reconnects = 4096, 0, 0
        accepted_before = 4096
        boundary = chunks[16]
        if experiment == 'partial':
            # Hold the last byte of a line/frame until the successor starts.
            # The old receiver cannot emit this last line/frame yet.
            conn.sendall(boundary[:-1])
            time.sleep(.05)
            # The plaintext handler keeps its final <1024 rows local while
            # blocked on a partial next line; its public counter lags here.
            accepted_before = 4351 if mode == 'plaintext' else 4096
            sent += 255 if mode == 'plaintext' else 0
        queued = []
        if experiment == 'queued':
            conn.close()
            queued = [connect() for _ in range(32)]
            # Stop both supervisor and worker. Every following successful
            # write is accepted by TCP while go-carbon cannot read any bytes.
            os.killpg(old.pid, signal.SIGSTOP)
            try:
                for i, data in enumerate(chunks[16:]):
                    queued[i % len(queued)].sendall(data)
                    sent += 256
                time.sleep(.1)
            finally:
                os.killpg(old.pid, signal.SIGCONT)
        restart_errors = []
        def restart():
            try:
                old.send_signal(signal.SIGUSR2)
                old.wait(30)
                start('next')
            except BaseException as error:
                restart_errors.append(error)
        restart_thread = threading.Thread(target=restart)
        restart_thread.start()
        if experiment == 'partial':
            restart_thread.join(40)
            if restart_errors:
                raise RuntimeError(restart_errors)
            if enabled:
                conn.sendall(boundary[-1:])
                sent += 1 if mode == 'plaintext' else 256
            else:
                # Observe close before reconnecting; the entire boundary
                # chunk was attempted, but its prefix cannot be retransmitted
                # safely without application acknowledgements.
                try:
                    assert conn.recv(1) == b''
                except ConnectionResetError:
                    pass
                conn.close()
                conn = connect()
                failed += 1 if mode == 'plaintext' else 256
                reconnects += 1
            for data in chunks[17:33]:
                conn.sendall(data)
                sent += 256
            target = (8448 if enabled else 8447 if mode == 'plaintext' else 8192) - accepted_before
            eventually(lambda: float(metrics(prom).get('metrics_received_tcp_total', 0)) == target)
        elif experiment == 'continuous':
            for data in chunks[16:]:
                try:
                    conn.sendall(data)
                    sent += 256
                except OSError:
                    failed += 256
                    reconnects += 1
                    conn.close()
                    conn = eventually(connect)
                time.sleep(.005 if input_log_only else .0005)
            restart_thread.join(40)
        else:
            restart_thread.join(40)
            for queued_conn in queued:
                queued_conn.close()
        if restart_thread.is_alive() or restart_errors:
            raise RuntimeError(f'restart failed: {restart_errors}')
        conn.close()
        eventually(lambda: float(metrics(prom).get('input_log_background_replay_active', 0)) == 0)
        time.sleep(.5)
        last = 33*256 if experiment == 'partial' else fixture['batches']*256
        seen = stored(http, fixture, last)
        next_received = int(float(metrics(prom).get('metrics_received_tcp_total', 0)))
        combined = entries(root/'old.log') + entries(root/'next.log')
        dump = next(e for e in combined if e.get('message') == ('input log stop finished' if input_log_only else 'dump finished'))
        gap = next(e for e in combined if e.get('message') == 'previous process handed over')
        result = dict(protocol=mode, enabled=enabled, experiment=experiment,
                      attempted=sent+failed, socket_written_complete_points=sent,
                      failed_or_incomplete_points=failed, unique_stored=len(seen),
                      receiver_before=(accepted_before if experiment == 'partial' else len(seen)-next_received),
                      receiver_before_source=('complete prefix, checked against render' if experiment == 'partial'
                                              else 'stored identities minus successor receiver counter'),
                      receiver_after=next_received,
                      reconnects=reconnects, input_handoff=dump['input_handoff'],
                      input_gap_seconds=gap['input_unavailable_seconds'],
                      input_stop_runtime=dump.get('input_stop_runtime', 0),
                      input_log_only=input_log_only, dump_enabled=(not input_log_only if dump_enabled is None else dump_enabled),
                      background_replay=any(e.get('message') == 'replaying input log behind live receivers' for e in entries(root/'next.log')),
                      lost_points=sent-len(seen),
                      dump_files_written=sum(e.get('message') == 'start cache dump' for e in combined),
                      recovery_files=[p.name for p in (root/'dump').iterdir() if not p.name.startswith('.')],
                      replayed_points=int(float(metrics(prom).get('input_log_replayed_points_total', 0))),
                      planned_close_logs=sum(e.get('message') == 'input connection closed during shutdown' for e in combined),
                      errors=[e for e in combined if e.get('level', '').upper() == 'ERROR'])
        if experiment == 'partial':
            assert accepted_before+next_received == len(seen), result
            assert len(seen) == (8448 if enabled else 8447 if mode == 'plaintext' else 8192), result
        if input_log_only:
            assert result['background_replay'], 'journal-only handoff requires background replay dependency'
            handed = next(e for e in combined if e.get('message') == 'input handed over')
            result['cached_points_at_handoff'] = handed['cached_points']
            result['replay_cache_ratio'] = result['replayed_points']/max(1, handed['cached_points'])
            assert result['replay_cache_ratio'] <= 1.1, result
            assert result['dump_files_written'] == 0 and not result['recovery_files'], result
            assert result['input_gap_seconds'] <= 1, result
            if experiment == 'continuous':
                assert next_received > 0, 'continuous sender must cross the handoff'
        if enabled:
            assert dump['input_handoff'] and reconnects == 0 and len(seen) == sent, result
            assert not result['errors'], result
        print(json.dumps(result), flush=True)
        return result
    except BaseException:
        for log in root.glob('*.log'):
            print(log.name, log.read_text()[-12000:], flush=True)
        raise
    finally:
        for s in sockets:
            s.close()
        for p in procs:
            try:
                os.killpg(p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            p.wait()
        # Check the whole group, including a released worker whose parent
        # already exited. Waiting only for the supervisor misses that worker.
        def group_gone(group):
            try:
                os.killpg(group, 0)
                return False
            except ProcessLookupError:
                return True
        for p in procs:
            eventually(lambda: group_gone(p.pid), timeout=5)
        for log in logs:
            log.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--carbon', type=Path, required=True)
    ap.add_argument('--wire', type=Path, required=True)
    ap.add_argument('--scratch', type=Path, required=True)
    ap.add_argument('--output', type=Path, required=True)
    ap.add_argument('--enabled', choices=['false', 'true', 'both'], default='both')
    ap.add_argument('--input-log-only', action='store_true', help='experimental journal-only stop; requires --enabled true')
    ap.add_argument('--iterations', type=int, default=1)
    ap.add_argument('--dump-enabled', choices=['auto', 'false', 'true'], default='auto')
    args = ap.parse_args()
    if args.input_log_only and args.enabled != 'true':
        ap.error('--input-log-only requires --enabled true')
    if args.iterations <= 0:
        ap.error('--iterations must be positive')
    dump_enabled = None if args.dump_enabled == 'auto' else args.dump_enabled == 'true'
    results = []
    with tempfile.TemporaryDirectory(prefix='ih-', dir=args.scratch.resolve()) as temporary:
        root = Path(temporary)
        fixtures = root/'fixtures'; fixtures.mkdir()
        subprocess.run([str(args.wire.resolve()), str(fixtures)], check=True)
        for iteration in range(args.iterations):
            for enabled in ([False, True] if args.enabled == 'both' else [args.enabled == 'true']):
                for experiment in ['partial', 'continuous', 'queued']:
                    for mode in ['plaintext', 'zstd', 'stream']:
                        results.append(run(root/f'{iteration}-{int(enabled)}-{experiment}-{mode}', args.carbon.resolve(), fixtures, mode, enabled, experiment, args.input_log_only, dump_enabled))
                        args.output.write_text(json.dumps(results, indent=2)+'\n')
    args.output.write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
