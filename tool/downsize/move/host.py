#!/usr/bin/env python3
"""Host-local mover telemetry and optional expiring read isolation (stdlib only)."""
import argparse
import hashlib
import hmac
import http.server
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def metrics(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        data = response.read(32 << 20).decode()
    result = {}
    for line in data.splitlines():
        if line and not line.startswith('#'):
            key, value, *_ = line.split()
            result[key] = float(value)
    return result


def total(values, name, required=True):
    found = [v for k, v in values.items() if k.split('{', 1)[0] == name]
    if required and not found:
        raise ValueError('missing required metric: ' + name)
    return sum(found)


def read_p99(previous, current):
    if not previous:
        return 0, 0
    name = 'http_request_duration_seconds_exp'
    count = total(current, name + '_count') - total(previous, name + '_count')
    if count < 0:
        raise ValueError('read histogram reset')
    buckets = {}
    for key, value in current.items():
        if key.split('{', 1)[0] != name + '_bucket':
            continue
        if key not in previous or value < previous[key]:
            raise ValueError('read histogram changed/reset')
        bound = float(re.search(r'\ble="([^"]+)"', key).group(1))
        buckets[bound] = buckets.get(bound, 0) + value - previous[key]
    lower = old = 0
    for upper, cumulative in sorted(buckets.items()):
        if cumulative >= count * .99 and count:
            if not math.isfinite(upper):
                return 1e9, count
            return 1000 * (lower + (upper-lower) * (count*.99-old) / max(1, cumulative-old)), count
        lower, old = upper, cumulative
    return 0, count


def replica_serves(url, names):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc or parsed.username or parsed.path.strip('/') or parsed.query or parsed.fragment:
        raise ValueError('replica must be an origin URL')
    if not names or len(names) > 16:
        raise ValueError('1..16 replica probe names required')
    for name in names:
        query = urllib.parse.urlencode({'target': name, 'from': int(time.time())-86400, 'until': int(time.time()), 'format': 'json'})
        result = fetch_json(url.rstrip('/') + '/render/?' + query)
        if isinstance(result, list):
            matches = [r for r in result if r.get('target') == name]
            if not matches or not any(any(p[0] is not None for p in r.get('datapoints', [])) for r in matches):
                raise ValueError('replica did not render present affected metric: ' + name)
            continue
        rows = result.get('metrics', [])
        matches = [r for r in rows if r.get('name') == name]
        if not matches:
            raise ValueError('replica did not render affected metric: ' + name)
        if not any(any(not absent for absent in r.get('isAbsent', [])) or (not r.get('isAbsent') and r.get('values')) for r in matches):
            raise ValueError('replica has no present values: ' + name)


def rules(port, tag):
    return [
        ['iptables', 'INPUT', '!', '-s', '127.0.0.1/32', '-p', 'tcp', '--dport', str(port), '-m', 'comment', '--comment', tag, '-j', 'REJECT', '--reject-with', 'tcp-reset'],
        ['ip6tables', 'INPUT', '!', '-s', '::1/128', '-p', 'tcp', '--dport', str(port), '-m', 'comment', '--comment', tag, '-j', 'REJECT', '--reject-with', 'tcp-reset'],
    ]


def unblock(port, tag):
    for rule in rules(port, tag):
        for _ in range(8):
            if subprocess.run([rule[0], '-D'] + rule[1:], capture_output=True).returncode:
                break


class Host:
    def __init__(self, args):
        self.args = args
        self.previous = None
        self.histogram = None
        self.lock = threading.RLock()
        self.lease = None
        self.tag = 'ds-move-' + args.cluster
        self.state = Path(args.state)
        self.secret = os.environ.get(args.token_env, '')
        if not self.secret:
            raise ValueError('control secret environment variable is empty')
        cores = set()
        for cpu in Path('/sys/devices/system/cpu').glob('cpu[0-9]*'):
            cores.add(((cpu/'topology/physical_package_id').read_text().strip(), (cpu/'topology/core_id').read_text().strip()))
        self.cores = len(cores)
        if self.state.exists():
            self.lease = json.loads(self.state.read_text())
        if args.apply and not getattr(args, "offline", False):
            for tool in ('iptables', 'ip6tables', 'systemd-run'):
                if not shutil.which(tool):
                    raise ValueError('read isolation requires ' + tool)
        threading.Thread(target=self.watch, daemon=True).start()

    def sample(self):
        with self.lock:
            pid = int(Path(self.args.pid_file).read_text().strip())
            stat = Path('/proc/%d/stat' % pid).read_text().rsplit(')', 1)[1].split()
            generation = '%d:%s' % (pid, stat[19])
            mem = {line.split(':')[0]: int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines()}
            status = dict(line.split(':', 1) for line in Path('/proc/%d/status' % pid).read_text().splitlines())
            cpu = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
            disk = {d: [int(v) for v in Path('/sys/class/block/%s/stat' % d).read_text().split()] for d in self.args.devices.split(',')}
            nic = tuple(int(Path('/sys/class/net/%s/statistics/%s_bytes' % (self.args.nic, direction)).read_text()) for direction in ('rx', 'tx'))
            offline = getattr(self.args, 'offline', False)
            values = {} if offline else metrics(self.args.metrics_url)
            admin = {} if offline else fetch_json(self.args.admin_url + '/admin/info?scopes=storage,cache')
            phase = 'shared' if offline else admin['storage']['migration']['phase']
            cached = 0 if offline else int(admin['cache']['size'])
            volume = os.statvfs(self.args.store_dir)
            p99, samples = read_p99(self.histogram, values)
            self.histogram = values
            now = time.time()
            sample = {'Time': now, 'Generation': generation, 'StoragePhase': phase, 'CachePoints': cached,
                      'DiskBytes': volume.f_blocks*volume.f_frsize, 'DiskFreeBytes': volume.f_bavail*volume.f_frsize, 'PhysicalCores': self.cores,
                      'CPUCores': 0, 'DiskUtil': 0, 'DiskAwaitMS': 0, 'NICBytesPerSecond': 0,
                      'NICCapacityBytes': min(self.args.nic_mbps * 1e6, self.args.nic_gbps * 1e9 / 8),
                      'NICIngressBytes': 0, 'NICEgressBytes': 0, 'DiskWriteBytes': 0, 'DiskWriteCapacity': self.args.disk_write_mbps * 1e6,
                      'RSS': int(status['VmRSS'].split()[0])*1024, 'Threads': int(status['Threads']),
                      'MemoryBytes': mem['MemTotal'], 'AvailableBytes': mem['MemAvailable'],
                      'WriteErrors': 0 if offline else total(values, 'input_log_write_errors_total') + total(values, self.args.write_error_metric),
                      'Unlogged': 0 if offline else total(values, 'input_log_unlogged_points_total'),
                      'ReadP99MS': p99, 'ReadSamples': samples,
                      'Offline': offline, 'ReadsBlocked': offline or bool(self.lease and self.lease['expires_at'] > now and self.args.apply)}
            if self.previous:
                old_time, old_cpu, old_disk, old_nic = self.previous
                seconds = now - old_time
                delta = [a-b for a, b in zip(cpu, old_cpu)]
                ticks = sum(delta)
                sample['CPUCores'] = (ticks - delta[3] - delta[4]) / max(1, ticks) * self.cores
                for device, row in disk.items():
                    old = old_disk[device]
                    completed = row[0]+row[4]-old[0]-old[4]
                    sample['DiskUtil'] = max(sample['DiskUtil'], (row[9]-old[9])/seconds/10)
                    sample['DiskAwaitMS'] = max(sample['DiskAwaitMS'], (row[3]+row[7]-old[3]-old[7])/max(1, completed))
                    sample['DiskWriteBytes'] += (row[6]-old[6])*512/seconds
                sample['NICIngressBytes'] = (nic[0]-old_nic[0])/seconds
                sample['NICEgressBytes'] = (nic[1]-old_nic[1])/seconds
                sample['NICBytesPerSecond'] = max(sample['NICIngressBytes'], sample['NICEgressBytes'])
            self.previous = now, cpu, disk, nic
            if not all(math.isfinite(v) for v in sample.values() if isinstance(v, (float, int))):
                raise ValueError('nonfinite host sample')
            return sample

    def reads(self, body):
        with self.lock:
            if body.get('cluster') != self.args.cluster or body.get('site') != self.args.site:
                raise ValueError('cluster/site mismatch')
            if getattr(self.args, 'offline', False):
                return {'blocked': True, 'offline': True}
            if body.get('unblock'):
                if self.args.apply:
                    self.restore()
                return {'blocked': False, 'apply': self.args.apply}
            expiry = body.get('expires_at', 0)
            if not time.time() < expiry <= time.time()+120:
                raise ValueError('lease must expire within 120 seconds')
            if not body.get('replica_site') or body['replica_site'] == self.args.site:
                raise ValueError('replica must be in a different site')
            replica_serves(body['replica_url'], body['metrics'])
            if self.lease and self.lease.get('plan_id') != body['plan_id']:
                raise ValueError('another plan owns read isolation')
            actions = rules(self.args.read_port, self.tag)
            if not self.args.apply:
                return {'blocked': False, 'apply': False, 'would_insert': actions, 'expires_at': expiry}
            # An independent systemd timer exists BEFORE the first reject rule.
            # Renew by installing the next timer before cancelling the old one.
            unit = 'ds-move-unblock-' + hashlib.sha256((body['plan_id']+str(time.time_ns())).encode()).hexdigest()[:16]
            subprocess.run(['systemd-run', '--quiet', '--collect', '--on-active=120s', '--unit='+unit,
                            sys.executable, str(Path(__file__).resolve()), 'unblock', '--read-port', str(self.args.read_port), '--tag', self.tag], check=True)
            previous = self.lease
            self.state.parent.mkdir(parents=True, exist_ok=True)
            body = dict(body, timer=unit)
            temporary = self.state.with_suffix('.next')
            with temporary.open('w') as stream:
                json.dump(body, stream); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, self.state)
            self.lease = body
            try:
                for rule in actions:
                    if subprocess.run([rule[0], '-C']+rule[1:], capture_output=True).returncode:
                        subprocess.run([rule[0], '-I', rule[1], '1']+rule[2:], check=True)
            except Exception:
                self.restore()
                raise
            if previous:
                subprocess.run(['systemctl', 'stop', previous['timer']+'.timer'], capture_output=True)
            return {'blocked': True, 'expires_at': expiry}

    def restore(self):
        unblock(self.args.read_port, self.tag)
        self.lease = None
        self.state.unlink(missing_ok=True)

    def watch(self):
        while True:
            time.sleep(1)
            with self.lock:
                if self.args.apply and self.lease and self.lease['expires_at'] <= time.time():
                    self.restore()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.handle_request(False)

    def do_POST(self):
        self.handle_request(True)

    def handle_request(self, post):
        host = self.server.host
        if not hmac.compare_digest(self.headers.get('X-Move-Control', ''), host.secret):
            self.send_error(403); return
        try:
            if self.path == '/sample' and not post:
                result = host.sample()
            elif self.path == '/reads' and post:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length < 65536:
                    raise ValueError('invalid body length')
                result = host.reads(json.loads(self.rfile.read(length)))
            else:
                self.send_error(404); return
            encoded = json.dumps(result, allow_nan=False).encode()
            self.send_response(200); self.send_header('Content-Type', 'application/json'); self.send_header('Content-Length', str(len(encoded))); self.end_headers(); self.wfile.write(encoded)
        except Exception as error:
            self.send_error(503, str(error))

    def log_message(self, *_):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    server = sub.add_parser('serve')
    server.add_argument('--bind', default='127.0.0.1:9079')
    server.add_argument('--pid-file', required=True)
    server.add_argument('--offline', action='store_true', help='stopped replacement target: worker PID, host telemetry only; restart without this flag before targets_ready')
    server.add_argument('--devices', required=True, help='physical data devices, comma separated')
    server.add_argument('--nic', required=True)
    server.add_argument('--nic-gbps', type=float, default=25)
    server.add_argument('--nic-mbps', type=float, default=1000, help='usable MB/s in each direction')
    server.add_argument('--disk-write-mbps', type=float, default=2000)
    server.add_argument('--cluster', required=True)
    server.add_argument('--site', required=True)
    server.add_argument('--store-dir', required=True)
    server.add_argument('--admin-url', default='http://127.0.0.1:8080')
    server.add_argument('--metrics-url', default='http://127.0.0.1:7007/metrics')
    server.add_argument('--write-error-metric', default='persister_pebble_write_failures_total', help='actual exported persister error counter; absent metric fails closed')
    server.add_argument('--token-env', default='MOVE_CONTROL_TOKEN')
    server.add_argument('--read-port', type=int, default=8080)
    server.add_argument('--state', default='/run/go-carbon/ds-move-reads.json')
    server.add_argument('--apply', action='store_true', help='allow expiring firewall writes')
    restore = sub.add_parser('unblock')
    restore.add_argument('--read-port', type=int, required=True)
    restore.add_argument('--tag', required=True)
    args = parser.parse_args()
    if args.command == 'serve' and (args.nic_gbps <= 0 or args.nic_mbps <= 0 or args.disk_write_mbps <= 0):
        parser.error('host bandwidth capacities must be positive')
    if args.command == 'unblock':
        unblock(args.read_port, args.tag); return
    address, port = args.bind.rsplit(':', 1)
    httpd = http.server.ThreadingHTTPServer((address, int(port)), Handler)
    httpd.host = Host(args)
    try:
        httpd.serve_forever()
    finally:
        if args.apply:
            httpd.host.restore()
        httpd.server_close()


if __name__ == '__main__':
    main()
