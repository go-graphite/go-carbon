#!/usr/bin/env python3
"""Swap a preinstalled binary and supervise a local automatic rollback guard."""
import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

from common import HTTP, atomic_write, counter_increase, output, positive, samples


DEFAULT_COUNTERS = ['persister_pebble_write_failures_total', 'input_log_write_errors_total']


def digest(path):
    result = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def command(args, timeout=60):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=timeout).stdout.strip()


def swap_dropin(text, old, new):
    for path in (old, new):
        if not re.fullmatch(r'/[A-Za-z0-9_./-]+', path) or '..' in Path(path).parts:
            raise ValueError('binary paths must be absolute, without spaces or systemd expansion characters')
    starts = [line for line in text.splitlines() if line.startswith('ExecStart=') and line != 'ExecStart=']
    if len(starts) != 1 or not starts[0].startswith('ExecStart=' + old + ' ') or starts[0].endswith('\\'):
        raise ValueError('drop-in needs exactly one simple ExecStart with the expected old binary')
    return text.replace(starts[0], starts[0].replace('ExecStart=' + old + ' ', 'ExecStart=' + new + ' ', 1), 1)


def save_state(directory, state):
    atomic_write(directory / 'state.json', json.dumps(state, sort_keys=True).encode())


def read_state(directory):
    return json.loads((directory / 'state.json').read_text())


def metrics(state):
    url = urlsplit(state['metrics_endpoint'])
    if url.hostname not in ('localhost', '127.0.0.1', '::1'):
        raise ValueError('canary health endpoint must be loopback')
    token = Path(state['token_file']).read_text().strip() if state.get('token_file') else None
    api = HTTP(state['metrics_endpoint'], timeout=state['http_timeout'], token=token)
    return samples(api.get('/metrics'), state['counters'])


def ready(state, run=command, readlink=os.readlink):
    if run(['systemctl', 'is-active', state['service']]) != 'active':
        raise ValueError('service is not active')
    pid = run(['systemctl', 'show', state['service'], '-p', 'MainPID', '--value'])
    if not pid.isdecimal() or pid == '0' or readlink('/proc/' + pid + '/exe') != state['binary']:
        raise ValueError('new binary is not serving')


def rollback(directory, state, run=command):
    dropin = Path(state['drop_in'])
    original = (directory / 'previous.conf').read_bytes()
    candidate = (directory / 'candidate.conf').read_bytes()
    current = dropin.read_bytes()
    if current not in (original, candidate):
        raise ValueError('drop-in changed externally; restore the saved configuration manually')
    if digest(state['old_binary']) != state['old_sha256']:
        raise ValueError('previous binary changed; automatic rollback refused')
    # A repeated rollback is safe after a failed daemon-reload or restart.
    atomic_write(dropin, original, state['drop_in_mode'])
    run(['systemctl', 'daemon-reload'])
    run(['systemctl', 'restart', state['service']], timeout=state['restart_timeout'])
    state['status'] = 'rolled-back'
    save_state(directory, state)


def guard(directory, run=command, sleep=time.sleep, clock=time.monotonic, get_metrics=metrics, check_ready=ready):
    state = read_state(directory)
    start = clock()
    try:
        # The guard owns the swap and restart, so the invoking terminal may exit.
        dropin = Path(state['drop_in'])
        if dropin.read_bytes() != (directory / 'previous.conf').read_bytes():
            raise ValueError('drop-in changed before activation')
        if digest(state['binary']) != state['sha256']:
            raise ValueError('candidate binary changed before activation')
        atomic_write(dropin, (directory / 'candidate.conf').read_bytes(), state['drop_in_mode'])
        state['status'] = 'starting'
        save_state(directory, state)
        run(['systemctl', 'daemon-reload'])
        run(['systemctl', 'restart', state['service']], timeout=state['restart_timeout'])
        deadline = clock() + state['startup_timeout']
        while True:
            try:
                check_ready(state, run=run)
                current = get_metrics(state)
                break
            except (OSError, ValueError, subprocess.SubprocessError):
                if clock() >= deadline:
                    raise ValueError('candidate did not become healthy before startup timeout') from None
                sleep(min(state['interval'], max(0, deadline - clock())))
        previous = {key: 0 for key in current}
        until = clock() + state['duration']
        state['status'] = 'guarding'
        save_state(directory, state)
        while True:
            delta = counter_increase(previous, current)
            if delta > state['max_errors']:
                raise ValueError('error increase %g exceeds %g' % (delta, state['max_errors']))
            state['last_delta'] = delta
            state['elapsed_seconds'] = clock() - start
            save_state(directory, state)
            if clock() >= until:
                break
            previous = current
            sleep(min(state['interval'], max(0, until - clock())))
            # Subsequent HTTP or readiness failures trigger rollback immediately.
            check_ready(state, run=run)
            current = get_metrics(state)
        state['status'] = 'complete'
        save_state(directory, state)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        state['failure'] = str(exc)
        state['status'] = 'rollback-required'
        save_state(directory, state)
        rollback(directory, state, run)
        raise


def start(args, run=command):
    binary, old, dropin, config = [Path(p).absolute() for p in
                                    (args.binary, args.old_binary, args.drop_in, args.config)]
    for path in (binary, old, dropin, config):
        if path.resolve() != path or not path.is_file():
            raise ValueError('paths must be existing regular files without symlink components')
    if binary == old or not os.access(binary, os.X_OK) or not os.access(old, os.X_OK):
        raise ValueError('candidate and previous binary must be distinct executable files')
    if not re.fullmatch(r'[a-fA-F0-9]{64}', args.sha256) or digest(binary) != args.sha256.lower():
        raise ValueError('candidate digest mismatch')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@-]*', args.service):
        raise ValueError('invalid service name')
    if not args.max_errors >= 0 or not args.max_errors < float('inf'):
        raise ValueError('max errors must be finite and nonnegative')
    original = dropin.read_text()
    candidate = swap_dropin(original, str(old), str(binary))
    exec_line = next(line for line in original.splitlines() if line.startswith('ExecStart=' + str(old) + ' '))
    arguments = shlex.split(exec_line[len('ExecStart='):])
    if arguments.count('-config') != 1 or arguments.index('-config') + 1 >= len(arguments) or arguments[arguments.index('-config') + 1] != str(config):
        raise ValueError('--config must match the drop-in ExecStart configuration')
    counters = args.error_counter or DEFAULT_COUNTERS
    state = dict(binary=str(binary), old_binary=str(old), old_sha256=digest(old), sha256=args.sha256.lower(),
                 drop_in=str(dropin), drop_in_mode=dropin.stat().st_mode & 0o777,
                 service=args.service, metrics_endpoint=args.metrics_endpoint, token_file=args.token_file,
                 counters=counters, duration=args.duration, interval=args.interval,
                 max_errors=args.max_errors, startup_timeout=args.startup_timeout,
                 restart_timeout=args.restart_timeout, http_timeout=args.http_timeout, status='prepared')
    if not args.apply:
        output({'action': 'binary canary', 'state': state, 'candidate_drop_in': candidate, 'apply': False})
        return
    # These commands only run on explicit application, on the local machine.
    metrics(state)
    run([str(binary), '-config', str(config), '-check-config'])
    directory = Path(args.state_dir).absolute()
    if directory.resolve() != directory:
        raise ValueError('state path must not contain symlinks')
    directory.mkdir(mode=0o700)
    atomic_write(directory / 'previous.conf', dropin.read_bytes())
    atomic_write(directory / 'candidate.conf', candidate.encode())
    save_state(directory, state)
    # A fixed unit serializes canaries across state directories on this host.
    run(['systemd-run', '--unit=go-carbon-binary-canary', '--description=' + str(directory), '--property=Type=exec', '--',
         sys.executable, str(Path(__file__).resolve()), 'guard', str(directory)])
    output({'state_dir': str(directory), 'unit': 'go-carbon-binary-canary', 'status': 'scheduled'})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    swap = commands.add_parser('run', help='dry-run by default; guard performs activation when applied')
    for flag in ('binary', 'old-binary', 'sha256', 'drop-in', 'config', 'state-dir'):
        swap.add_argument('--' + flag, required=True)
    swap.add_argument('--service', default='go-carbon.service')
    swap.add_argument('--metrics-endpoint', default='http://127.0.0.1:7007')
    swap.add_argument('--token-file', help='bearer token file readable by the detached guard')
    swap.add_argument('--error-counter', action='append', help='required counter; repeat to replace both defaults')
    swap.add_argument('--duration', type=positive, default=10800)
    swap.add_argument('--interval', type=positive, default=120)
    swap.add_argument('--max-errors', type=float, default=20)
    swap.add_argument('--startup-timeout', type=positive, default=300)
    swap.add_argument('--restart-timeout', type=positive, default=600)
    swap.add_argument('--http-timeout', type=positive, default=10)
    swap.add_argument('--apply', action='store_true')
    for action in ('guard', 'show', 'rollback'):
        sub = commands.add_parser(action)
        sub.add_argument('state_dir', type=Path)
    args = parser.parse_args()
    try:
        if args.action == 'run':
            start(args)
        elif args.action == 'show':
            output(read_state(args.state_dir))
        elif args.action == 'guard':
            guard(args.state_dir)
        else:
            # Stop the automatic worker before a manual rollback.
            load = command(['systemctl', 'show', 'go-carbon-binary-canary.service', '-p', 'LoadState', '--value'])
            if load != 'not-found':
                description = command(['systemctl', 'show', 'go-carbon-binary-canary.service', '-p', 'Description', '--value'])
                if description != str(args.state_dir.absolute()):
                    raise ValueError('active guard belongs to another state directory')
                command(['systemctl', 'stop', 'go-carbon-binary-canary.service'])
            rollback(args.state_dir, read_state(args.state_dir))
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        parser.exit(1, str(exc) + '\n')


if __name__ == '__main__':
    main()
