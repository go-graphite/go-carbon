"""Bounded one-node migration admission; only atomically replaces its lease."""
from pathlib import Path
from collections import deque
import argparse, concurrent.futures, fcntl, json, math, os, re, stat, time, urllib.request

HOSTS = {}
HISTOGRAMS = ('http_request_duration_seconds_exp', 'disk_wait_seconds_exp')
# Probe marginal conversion load before admitting the larger worker groups.
# Every stage remains subject to the same request and resource guards.
STEPS = (0, 1, 2, 4, 8, 16, 24, 32, 48, 64)
DEVICE = 'nvme0n1'



def atomic(path, value):
    temporary = path.with_suffix('.next')
    with temporary.open('w') as stream:
        json.dump(value, stream, indent=2); stream.write('\n')
        stream.flush(); os.fsync(stream.fileno())
    temporary.chmod(0o644); os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def protect_source_root(path):
    """Keep the bound source inode present after the last metric is converted."""
    root = Path(path).resolve(strict=True)
    info = root.stat()
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('incremental source is not a directory')
    want = {'version': 1, 'path': str(root), 'device': info.st_dev, 'inode': info.st_ino}
    marker = root / '.go-carbon-incremental-source-root.keep'
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0)
    try:
        descriptor = os.open(marker, flags)
    except FileNotFoundError:
        descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(want, stream, sort_keys=True); stream.write('\n')
            stream.flush(); os.fsync(stream.fileno())
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        return want
    with os.fdopen(descriptor, 'r') as stream:
        saved = os.fstat(stream.fileno())
        if not stat.S_ISREG(saved.st_mode) or saved.st_size > 4096:
            raise ValueError('invalid source-root guard')
        if json.load(stream) != want:
            raise ValueError('source-root guard identity differs')
    return want


def metric(values, name):
    return sum(v for k, v in values.items() if k.split('{', 1)[0] == name)


def delta(previous, current, name):
    a, b = metric(previous['values'], name), metric(current['values'], name)
    if b < a: raise ValueError('counter reset: ' + name)
    return b - a


def histogram_p99(buckets, count):
    """Linear interpolation of duration buckets; retain bounds for inspection."""
    if not count:
        return None, None, None
    rank = count * .99
    lower, previous = 0.0, 0.0
    for upper, cumulative in sorted(buckets.items()):
        if upper < lower or cumulative < previous:
            raise ValueError('nonmonotone duration histogram')
        if cumulative >= rank:
            if not math.isfinite(upper):
                # Never admit on an unbounded tail estimate.
                return math.inf, lower, upper
            width = cumulative - previous
            estimate = lower + (upper - lower) * (rank - previous) / width if width else upper
            return estimate, lower, upper
        lower, previous = upper, cumulative
    return None, None, None


def summarize(previous, current):
    if metric(previous['values'], 'process_start_time_seconds') != metric(current['values'], 'process_start_time_seconds'):
        raise ValueError('worker generation changed')
    seconds = current['time'] - previous['time']
    if seconds < 20: raise ValueError('insufficient steady window')
    result = {'seconds': seconds, 'cache': current['cache']['size'],
              'cache_growth_per_second': (current['cache']['size'] - previous['cache']['size']) / seconds,
              'rss_bytes': metric(current['values'], 'process_resident_memory_bytes')}
    for name, field in [('process_cpu_seconds_total', 'cpu_cores'), ('metrics_received_tcp_total', 'points_per_second'),
                        ('go_memstats_alloc_bytes_total', 'allocation_bytes_per_second')]:
        result[field] = delta(previous, current, name) / seconds
    for name in HISTOGRAMS:
        count = delta(previous, current, name + '_count')
        buckets = {}
        for key, value in current['values'].items():
            if key.split('{', 1)[0] != name + '_bucket': continue
            old = previous['values'].get(key)
            if old is None or value < old: raise ValueError('histogram reset')
            bound = float(re.search(r'\ble="([^"]+)"', key).group(1))
            buckets[bound] = buckets.get(bound, 0) + value - old
        p99, lower, upper = histogram_p99(buckets, count)
        result[name] = {'count': count, 'p99_seconds': p99,
                        'p99_lower_seconds': lower, 'p99_upper_seconds': upper,
                        'mean_seconds': delta(previous, current, name + '_sum') / count if count else None}
    return result


def backlog_window(history):
    """Use a complete flush-cycle window for trend, keeping short read guards."""
    result = {}
    for name in HOSTS:
        latest = history[-1][name]
        generation = metric(latest['values'], 'process_start_time_seconds')
        rows = [row[name] for row in history
                if latest['time'] - row[name]['time'] <= 315
                and metric(row[name]['values'], 'process_start_time_seconds') == generation]
        if len(rows) < 15 or rows[-1]['time'] - rows[0]['time'] < 285:
            return None
        origin = rows[0]['time']; xs = [row['time'] - origin for row in rows]
        ys = [row['cache']['size'] for row in rows]
        mean_x, mean_y = sum(xs) / len(xs), sum(ys) / len(ys)
        denominator = sum((x - mean_x) ** 2 for x in xs)
        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denominator
        result[name] = {'seconds': xs[-1], 'mean_cache': mean_y,
                        'cache_growth_per_second': slope, 'samples': len(rows)}
    return result


def decide(current, baseline, workers, healthy):
    candidate, control = current['candidate'], current['control']
    reasons = []
    if candidate['cache'] > max(180_000_000, control['cache'] * 1.6, baseline['candidate']['cache'] * 1.5):
        reasons.append('write backlog exceeded observed envelope')
    trend = current.get('backlog_window')
    observed, peer = (trend['candidate'], trend['control']) if trend else (candidate, control)
    level = observed.get('mean_cache', observed.get('cache', 0))
    peer_level = peer.get('mean_cache', peer.get('cache', 0))
    if level > max(120_000_000, peer_level * 1.25, baseline['candidate']['cache'] * 1.25) and observed['cache_growth_per_second'] > 150_000:
        reasons.append('write backlog growing above observed envelope')
    if candidate['cpu_cores'] > 130: reasons.append('CPU reserve')
    if candidate['rss_bytes'] > max(64 * 1024**3, baseline['candidate']['rss_bytes'] * 1.5): reasons.append('RSS reserve')
    enough = True
    # Shared fetch timing includes catalog/header work; CWhisper's disk timer
    # starts after opening its header. Compare end-to-end requests across
    # backends, retaining the internal phase histogram for diagnostics.
    for name in ('http_request_duration_seconds_exp',):
        measured = candidate[name]; before = baseline['candidate'][name]
        if measured['count'] < 20 or before['p99_seconds'] is None:
            enough = False; continue
        cap = max(.002, before['p99_seconds'] * 1.5)
        peer_now, peer_before = control[name]['p99_seconds'], baseline['control'][name]['p99_seconds']
        # Shared bursts may follow the replica; preserve the original guard too.
        if peer_now and math.isfinite(peer_now):
            cap = max(cap, peer_now * 1.5)
        if measured['p99_seconds'] is None or not math.isfinite(measured['p99_seconds']) or measured['p99_seconds'] > cap:
            reasons.append(name + ' p99 exceeded pre-update envelope')
        peer_now = control[name]['p99_seconds']
        if peer_now and math.isfinite(peer_now) and measured['p99_seconds']:
            # A slow previous candidate must not grant more conversion budget.
            # Equal current tails remain admissible, even if the candidate was
            # faster in the old baseline; real peer regressions throttle.
            relative_cap = max(.002, peer_now * 1.5)
            if measured['p99_seconds'] > relative_cap:
                reasons.append(name + ' regressed relative to unchanged replica')
    if current.get('write_stall_active'): reasons.append('foreground storage write stalled')
    if current.get('io_full_avg10', 0) > 5: reasons.append('IO pressure')
    io = current.get('physical_io', {})
    if io.get('busy_percent', 0) > 90 and io.get('await_ms', 0) > 10: reasons.append('physical data-device IO latency')
    if reasons:
        return max(0, workers // 2), 0, reasons
    healthy = healthy + 1 if enough else 0
    if healthy >= 2:
        return next((n for n in STEPS if n > workers), workers), 0, ['stable real request and backlog windows']
    return workers, healthy, ['holding for real request observations']


def fetch(host):
    began = time.time()
    with urllib.request.urlopen('http://' + host + ':7007/metrics', timeout=5) as response:
        values = {line.split()[0]: float(line.split()[-1]) for line in response.read().decode().splitlines()
                  if line and not line.startswith('#')}
    with urllib.request.urlopen('http://' + host + ':8080/admin/info?scopes=cache', timeout=5) as response:
        cache = json.load(response)['cache']
    return {'time': began, 'values': values, 'cache': cache}


def physical_io():
    # lsblk -s verified the data LV has exactly this physical leaf.
    for line in Path('/proc/diskstats').read_text().splitlines():
        fields = line.split()
        if fields[2] == DEVICE:
            return {'ios': int(fields[3]) + int(fields[7]), 'request_ms': int(fields[6]) + int(fields[10]), 'busy_ms': int(fields[12])}
    return None


def capture(pool):
    pending = {name: pool.submit(fetch, host) for name, host in HOSTS.items()}
    return {name: job.result() for name, job in pending.items()}


def identify(cfg, sample):
    """Bind endpoint observations to the serving worker, not its supervisor."""
    import hashlib
    if os.uname().nodename.split('.')[0] != cfg['hostname'].split('.')[0]:
        raise ValueError('wrong controller host')
    configuration = Path(cfg['candidate_config'])
    if hashlib.sha256(configuration.read_bytes()).hexdigest() != cfg['candidate_config_sha256']:
        raise ValueError('candidate configuration changed')
    start = metric(sample['values'], 'process_start_time_seconds')
    boot = next(int(line.split()[1]) for line in Path('/proc/stat').read_text().splitlines() if line.startswith('btime '))
    ticks = os.sysconf('SC_CLK_TCK')
    matched = []
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit(): continue
        try:
            fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
            if abs(boot + int(fields[19]) / ticks - start) > .005: continue
            arguments = (entry / 'cmdline').read_bytes().split(b'\0')
            if os.fsencode(configuration) not in arguments: continue
            if hashlib.sha256((entry / 'exe').read_bytes()).hexdigest() != cfg['candidate_binary_sha256']: continue
            matched.append(int(entry.name))
        except (OSError, ValueError, IndexError):
            continue
    if len(matched) != 1:
        raise ValueError('cannot identify exactly one expected serving worker')
    return matched[0]


def admission_limit(cfg, now=None):
    """Operator cap and source-maintenance windows only affect admissions."""
    import datetime
    now = now or datetime.datetime.now()  # Same local timezone as ordinary cron.
    minute = now.hour * 60 + now.minute
    for begin, end in cfg.get('quiet_local_minutes', []):
        inside = begin <= minute < end if begin <= end else minute >= begin or minute < end
        if inside: return 0, ['scheduled source-maintenance window']
    path = Path(cfg['limits_file'])
    if path.stat().st_size > 4096: raise ValueError('operator limits file too large')
    limits = json.loads(path.read_text())
    cap = limits['max_workers']
    if type(cap) is not int or cap < 0 or cap > STEPS[-1]: raise ValueError('invalid operator worker cap')
    if type(limits.get('paused', False)) is not bool: raise ValueError('invalid operator pause')
    if limits.get('paused', False): return 0, ['operator pause']
    free = os.statvfs(cfg['source_directory'])
    if free.f_bavail * free.f_frsize < cfg['min_free_bytes']:
        return 0, ['free-space reserve']
    # Scheduled writers have a lead-in quiet window, so existing imports drain
    # before they run. Unexpected writers immediately stop new admissions.
    source = os.fsencode(cfg['source_directory'])
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit(): continue
        try:
            command = (entry / 'cmdline').read_bytes()
        except OSError:
            continue
        if source not in command: continue
        if any(word in command for word in [b'merge-ooo', b'cleanup-orphan', b'rsync']) or (b'find\0' in command and b'-delete' in command):
            return 0, ['external source writer active']
    return cap, []


def load_settings(path):
    cfg = json.loads(Path(path).read_text())
    if cfg['version'] != 1 or cfg['candidate_host'] != '127.0.0.1':
        raise ValueError('version 1 and a loopback candidate are required')
    for key in ['directory', 'limits_file', 'source_directory', 'candidate_config']:
        value = Path(cfg[key])
        if not value.is_absolute() or str(value) != cfg[key] or '..' in value.parts:
            raise ValueError('controller paths must be canonical and absolute')
    for key in ['candidate_config_sha256', 'candidate_binary_sha256']:
        if not re.fullmatch('[a-f0-9]{64}', cfg[key]): raise ValueError('invalid identity hash')
    for begin, end in cfg.get('quiet_local_minutes', []):
        if not 0 <= begin < 1440 or not 0 <= end < 1440: raise ValueError('invalid maintenance window')
    if cfg['min_free_bytes'] < 0: raise ValueError('invalid free-space reserve')
    return cfg


def main():
    global HOSTS, DEVICE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--baseline', action='store_true', help='record both existing services for one minute; never write a worker lease')
    options = parser.parse_args()
    cfg = load_settings(options.config)
    HOSTS = {'candidate': cfg['candidate_host'], 'control': cfg['control_host']}
    DEVICE = cfg['physical_device']
    directory = Path(cfg['directory'])
    lock = (directory / 'controller.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    baseline_path = directory / 'resource-baseline.json'
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        if options.baseline:
            rows = []
            for ordinal in range(5):
                rows.append(capture(pool))
                if ordinal < 4: time.sleep(15)
            baseline = {name: summarize(rows[0][name], rows[-1][name]) for name in HOSTS}
            atomic(baseline_path, {'time': time.time(), 'summary': baseline, 'samples': rows})
            print(json.dumps(baseline)); return
        # Reuse the captured pre-update observations, with the same quantile
        # calculation as current traffic; no new stock baseline or service roll.
        saved_baseline = json.loads(baseline_path.read_text())
        rows = saved_baseline['samples']
        baseline = {name: summarize(rows[0][name], rows[-1][name]) for name in HOSTS}
        control_path = directory / 'migration-control.json'
        generation = json.loads(control_path.read_text())['generation']
        workers, healthy, history = 0, 0, []
        with (directory / 'controller-samples.jsonl').open('a') as stream:
            while True:
                began = time.monotonic()
                result = {'time': time.time(), 'workers_before': workers}
                finished = False
                try:
                    latest = capture(pool)
                    result['serving_pid'] = identify(cfg, latest['candidate'])
                    result['source_root_guard'] = protect_source_root(cfg['source_directory'])
                    if history and any(metric(latest[name]['values'], 'process_start_time_seconds') != metric(history[-1][name]['values'], 'process_start_time_seconds') for name in HOSTS):
                        history.clear()
                    latest['physical_io'] = physical_io()
                    history.append(latest); history = history[-22:]
                    with urllib.request.urlopen('http://127.0.0.1:8080/admin/info?scopes=storage', timeout=5) as response:
                        storage = json.load(response)['storage']
                    result['migration'] = migration = storage['migration']
                    if migration['mode'] != 'incremental': raise ValueError('wrong migration mode')
                    if migration['phase'] == 'shared':
                        workers, healthy, finished = 0, 0, True
                        result['reasons'] = ['remaining-source walk completed']
                    else:
                        if migration['phase'] == 'failed' or migration.get('error'): raise ValueError('conversion failed: ' + migration.get('error', ''))
                        cap, admission_reasons = admission_limit(cfg)
                        if len(history) < 3: raise ValueError('collecting initial observation window')
                        recent = history[-5:]
                        current = {name: summarize(recent[0][name], recent[-1][name]) for name in HOSTS}
                        current['backlog_window'] = backlog_window(history)
                        current['write_stall_active'] = storage['engine']['WriteStallActive']
                        pressure = Path('/proc/pressure/io')
                        if pressure.exists():
                            current['io_full_avg10'] = next(float(line.split()[1].split('=')[1]) for line in pressure.read_text().splitlines() if line.startswith('full '))
                        first_io, last_io = recent[0]['physical_io'], recent[-1]['physical_io']
                        if first_io and last_io:
                            elapsed = current['candidate']['seconds']; ios = last_io['ios'] - first_io['ios']
                            current['physical_io'] = {'device': DEVICE, 'iops': ios / elapsed, 'await_ms': (last_io['request_ms'] - first_io['request_ms']) / max(1, ios), 'busy_percent': (last_io['busy_ms'] - first_io['busy_ms']) / elapsed / 10}
                        result['summary'] = current
                        workers, healthy, result['reasons'] = decide(current, baseline, workers, healthy)
                        workers = min(workers, cap)
                        if admission_reasons:
                            healthy = 0
                            result['reasons'].extend(admission_reasons)
                except Exception as exc:
                    workers, healthy = 0, 0
                    result['reasons'] = [repr(exc)]
                generation += 1
                lease = {'version': 1, 'generation': generation, 'workers': workers, 'expires_at': int(time.time()) + 60}
                atomic(control_path, lease); result['lease'] = lease
                stream.write(json.dumps(result) + '\n'); stream.flush()
                atomic(directory / 'CONTROLLER-STATUS.json', result)
                print(json.dumps({'time': result['time'], 'workers': workers, 'reasons': result['reasons']}), flush=True)
                if finished: return
                time.sleep(max(0, 15 - (time.monotonic() - began)))


if __name__ == '__main__': main()

