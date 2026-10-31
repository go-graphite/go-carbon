#!/usr/bin/env python3
"""Opt-in full executable smoke test; receiver integration checks duplicates."""
import argparse
import getpass
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request


def address():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return f'127.0.0.1:{sock.getsockname()[1]}'


def wait(predicate, seconds=30):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, ValueError):
            pass
        time.sleep(0.02)
    raise RuntimeError('timed out waiting for local processes')


def listening(addr):
    with socket.create_connection(tuple([addr.split(':')[0], int(addr.split(':')[1])]), timeout=0.2):
        return True


def run(args, root, connections):
    processes = []
    logs = []
    writer = None
    stop_writer = threading.Event()
    failures = []
    try:
        def launch(binary, config, name):
            log = open(root / (name + '.log'), 'ab')
            logs.append(log)
            process = subprocess.Popen([binary, '-config', str(config)], stdout=log, stderr=log,
                                       start_new_session=True)
            processes.append(process)
            return process

        def store(name):
            path = root / name
            path.mkdir()
            (path / 'data').mkdir()
            (path / 'dump').mkdir()
            (path / 'schemas.conf').write_text('[all]\npattern = .*\nretentions = 1s:1h\n')
            tcp, http = address(), address()
            config = path / 'config.toml'
            config.write_text(f'''[common]
user = {json.dumps(getpass.getuser())}
metric-endpoint = ""
metric-interval = "1h"
graph-prefix = "example.stats"
max-cpu = 0
[whisper]
data-dir = {json.dumps(str(path / 'data'))}
schemas-file = {json.dumps(str(path / 'schemas.conf'))}
workers = 2
compressed = true
out-of-order = true
[tcp]
enabled = true
listen = "{tcp}"
[udp]
enabled = false
[pickle]
enabled = false
[grpc]
enabled = false
[carbonlink]
enabled = false
[carbonserver]
enabled = true
listen = "{http}"
trie-index = true
concurrent-index = true
realtime-index = 100
scan-frequency = "100ms"
file-list-cache = {json.dumps(str(path / "file-list-cache"))}
file-list-cache-version = 2
query-cache-enabled = false
find-cache-enabled = false
glob-cache-enabled = false
[dump]
enabled = true
path = {json.dumps(str(path / 'dump'))}
handoff-supervisor = true
restore-per-second = 0
[[logging]]
logger = ""
file = "stdout"
level = "info"
encoding = "json"
''')
            process = launch(args.carbon, config, name)
            def ready():
                if process.poll() is not None:
                    raise RuntimeError((root / (name + '.log')).read_text())
                return listening(tcp)
            wait(ready)
            wait(lambda: listening(http))
            return process, config, tcp, http

        old, compressed_config, compressed_tcp, compressed_http = store('compressed')
        _, _, plaintext_tcp, plaintext_http = store('plaintext')
        relay_tcp = address()
        (root / 'clusters.toml').write_text(f'''[[cluster]]
name = "example"
type = "toall"
[[cluster.hosts]]
name = "127.0.0.1"
port = {compressed_tcp.split(':')[1]}
mtcp = {connections}
[[cluster.hosts]]
name = "127.0.0.1"
port = {plaintext_tcp.split(':')[1]}
compression = "none"
''')
        (root / 'rules.toml').write_text('[[rule]]\nprefixes=["example."]\nclusters=["example"]\n')
        relay_config = root / 'relay.toml'
        relay_config.write_text(f'''ClustersConfig = {json.dumps(str(root / 'clusters.toml'))}
RulesConfig = {json.dumps(str(root / 'rules.toml'))}
ListenTCP = "{relay_tcp}"
ListenUDP = ""
ListenGRPC = ""
PprofPort = -1
PromPort = 0
TCPOutCompression = "zstd"
TCPOutBufFlushPeriodSec = 1
TCPOutConnectionRefreshPeriodSec = 1
TCPInitialConnCheck = false
WorkerPoolSize = 1
MainQueueBatchSize = 1
''')
        relay = launch(args.nanotube, relay_config, 'relay')
        wait(lambda: listening(relay_tcp))
        count = 900
        base = int(time.time()) - count - 10

        def series(http):
            url = f'http://{http}/render/?target=example.point&from={base-1}&until={base+count}&format=json'
            with urllib.request.urlopen(url, timeout=2) as response:
                body = json.load(response)
            result = {}
            for metric in body.get('metrics', body.get('Metrics', [])):
                start = metric.get('startTime', metric.get('StartTime'))
                step = metric.get('stepTime', metric.get('StepTime'))
                values = metric.get('values', metric.get('Values', []))
                absent = metric.get('isAbsent', metric.get('IsAbsent', [False] * len(values)))
                result.update({start + i * step: v for i, v in enumerate(values) if not absent[i]})
            return result

        def send():
            try:
                host, port = relay_tcp.split(':')
                with socket.create_connection((host, int(port))) as sock:
                    for i in range(count):
                        if stop_writer.is_set():
                            return
                        sock.sendall(f'example.point {i + 0.25:.2f} {base + i}\n'.encode())
                        time.sleep(0.006)
            except Exception as error:
                failures.append(error)

        writer = threading.Thread(target=send)
        writer.start()
        wait(lambda: len(series(compressed_http)) >= 100)
        old.send_signal(signal.SIGUSR2)
        if old.wait(timeout=15) != 0:
            raise RuntimeError('predecessor failed')
        launch(args.carbon, compressed_config, 'successor')
        writer.join(timeout=20)
        if writer.is_alive() or failures:
            raise RuntimeError(f'input writer failed: {failures}')
        expected = {base+i: i+0.25 for i in range(count)}
        try:
            wait(lambda: series(compressed_http) == expected)
            wait(lambda: series(plaintext_http) == expected)
        except RuntimeError:
            for name, http in [('compressed', compressed_http), ('plaintext', plaintext_http)]:
                got = series(http)
                missing = [(k, v, got.get(k)) for k, v in expected.items() if got.get(k) != v]
                print(name, 'points', len(got), 'mismatches', len(missing), 'first', missing[:5], flush=True)
            for name in ('compressed', 'successor', 'relay'):
                print(name, '\n'.join(line for line in (root / (name + '.log')).read_text().splitlines() if '\"stat\"' not in line)[-10000:], flush=True)
            raise
        relay.send_signal(signal.SIGTERM)
        if relay.wait(timeout=10) != 0:
            raise RuntimeError('relay drain failed')
        old_logs = (root / 'compressed.log').read_text()
        successor_logs = (root / 'successor.log').read_text()
        if 'input handed over' not in old_logs or 'took over input from stopping instance' not in successor_logs:
            raise RuntimeError('restart did not use connection handoff')
        print(f'PASS full go-carbon/nanotube binaries: mtcp={connections}; {count} exact stored points per host; mixed plaintext/zstd; USR2 handoff')
    finally:
        stop_writer.set()
        if writer is not None:
            writer.join(timeout=2)
        for process in processes:
            # The supervisor may have released while its worker still exits.
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        if failures:
            raise RuntimeError(f'writer errors: {failures}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--carbon', required=True)
    parser.add_argument('--nanotube', required=True)
    parser.add_argument('--scratch', required=True, help='scratch root (no system temporary directory)')
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        parser.error("full application handoff requires Linux peer verification; use the Unix receiver integration elsewhere")
    for connections in (1, 4):
        with tempfile.TemporaryDirectory(prefix='f-', dir=args.scratch) as directory:
            run(args, Path(directory), connections)


if __name__ == '__main__':
    main()
