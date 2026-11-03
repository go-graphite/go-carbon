import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('move_host', Path(__file__).with_name('host.py'))
host_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host_module)


class HostTests(unittest.TestCase):
    def test_delayed_http_does_not_inflate_proc_rates(self):
        clock = [100.0]
        delays = iter((.99, .01))
        def read(path, *args, **kwargs):
            p = str(path)
            if p == '/pid': return '123'
            if p.endswith('/123/stat'): return '123 (worker) ' + ' '.join(['1'] * 30)
            if p.endswith('/123/status'): return 'VmRSS: 1000 kB\nThreads: 10\n'
            if p == '/proc/meminfo': return 'MemTotal: 100000 kB\nMemAvailable: 50000 kB\n'
            if p == '/proc/stat': return 'cpu ' + ' '.join([str(int(clock[0]*100))]*8)
            if p.endswith('/nvme0n1/stat'):
                row = [0]*17; row[0] = int(clock[0]*100); row[6] = int(clock[0]*1000)
                return ' '.join(map(str,row))
            if p.endswith('_bytes'): return str(int(clock[0]*100000))
            raise AssertionError(p)
        def metrics(_):
            clock[0] += next(delays)
            return {'input_log_write_errors_total':0,'persister_pebble_write_failures_total':0,
                    'input_log_unlogged_points_total':0,'persister_pebble_points_total':123}
        with tempfile.TemporaryDirectory() as root:
            h = self.host(root); h.previous=None; h.histogram=None; h.cores=96
            h.args = types.SimpleNamespace(pid_file='/pid',devices='nvme0n1',nic='bond0',
                metrics_url='metrics',admin_url='admin',store_dir='store',nic_mbps=1000,
                nic_gbps=25,disk_write_mbps=1100,write_error_metric='persister_pebble_write_failures_total',apply=False)
            admin={'storage':{'migration':{'phase':'shared'}},'cache':{'size':0}}
            volume=types.SimpleNamespace(f_blocks=1000000,f_frsize=4096,f_bavail=900000)
            with mock.patch.object(host_module.Path,'read_text',read), mock.patch.object(host_module.time,'time',side_effect=lambda:clock[0]), mock.patch.object(host_module,'metrics',side_effect=metrics), mock.patch.object(host_module,'fetch_json',return_value=admin), mock.patch.object(host_module,'read_p99',return_value=(0,0)), mock.patch.object(host_module.os,'statvfs',return_value=volume):
                first=h.sample(); clock[0]=101.0; second=h.sample()
            self.assertEqual(first['Time'],100.0)
            self.assertEqual(second['Time'],101.0)
            self.assertEqual(second['DiskWriteBytes'],512000)
            self.assertEqual(second['NICIngressBytes'],100000)

    def test_foreground_counter_contract_and_missing_data(self):
        values = {'input_log_write_errors_total': 0.0,
                  'persister_pebble_write_failures_total': 0.0,
                  'input_log_unlogged_points_total': 0.0,
                  'persister_pebble_points_total': 12345.0}
        result = host_module.foreground_counters(values, 'persister_pebble_write_failures_total')
        self.assertTrue(result['ForegroundMeasured'])
        for field in ('WriteErrors', 'Unlogged', 'WritePoints'):
            self.assertIs(type(result[field]), int)
        self.assertEqual(result['WritePoints'], 12345)
        del values['persister_pebble_points_total']
        with self.assertRaises(ValueError):
            host_module.foreground_counters(values, 'persister_pebble_write_failures_total')
        offline = host_module.foreground_counters({}, 'unused', offline=True)
        self.assertFalse(offline['ForegroundMeasured'])
        self.assertEqual(offline['WritePoints'], 0)

    def test_write_ceiling_defaults_to_metal_budget_and_accepts_override(self):
        argv = ['serve', '--pid-file', '/run/store.pid', '--devices', 'nvme0n1',
                '--nic', 'eth0', '--cluster', 'metrics', '--site', 'region-a',
                '--store-dir', '/var/lib/carbon/store']
        args = host_module.parse_args(argv)
        self.assertEqual(args.disk_write_mbps, 770)
        self.assertLessEqual(args.disk_write_mbps * .98, 1100 * .7)
        self.assertEqual(host_module.parse_args(argv + ['--disk-write-mbps', '400']).disk_write_mbps, 400)
        for value in ('0', '-1', 'nan', 'inf'):
            with self.subTest(value=value), mock.patch('sys.stderr'), self.assertRaises(SystemExit):
                host_module.parse_args(argv + ['--disk-write-mbps', value])

    def host(self, root, apply=False):
        host = host_module.Host.__new__(host_module.Host)
        host.args = types.SimpleNamespace(cluster='metrics', site='region-a', apply=apply, read_port=8080)
        host.lease = None
        host.lock = threading.RLock()
        host.state = Path(root)/'state.json'
        host.tag = 'test-move'
        return host

    def lease(self):
        return {'cluster': 'metrics', 'site': 'region-a', 'replica_site': 'region-b',
                'replica_url': 'http://127.0.0.1:9999', 'metrics': ['affected.metric'],
                'expires_at': int(time.time())+90, 'plan_id': 'plan'}

    def test_replica_proof_and_preview_do_not_write(self):
        with tempfile.TemporaryDirectory() as root:
            host = self.host(root)
            with mock.patch.object(host_module, 'replica_serves') as probe, mock.patch.object(host_module.subprocess, 'run') as run:
                result = host.reads(self.lease())
                self.assertFalse(result['blocked'])
                self.assertEqual(len(result['would_insert']), 2)
                probe.assert_called_once()
                run.assert_not_called()
                self.assertFalse(host.state.exists())

    def test_missing_replica_never_blocks(self):
        with tempfile.TemporaryDirectory() as root:
            host = self.host(root, True)
            with mock.patch.object(host_module, 'replica_serves', side_effect=ValueError('not serving')), mock.patch.object(host_module.subprocess, 'run') as run:
                with self.assertRaises(ValueError):
                    host.reads(self.lease())
                run.assert_not_called()
            for field, value in [('replica_site', 'region-a'), ('site', 'region-c'), ('expires_at', int(time.time())+3600)]:
                lease = self.lease(); lease[field] = value
                with self.assertRaises(ValueError):
                    host.reads(lease)

    def test_independent_timer_precedes_firewall_and_renewal(self):
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            return types.SimpleNamespace(returncode=1 if '-C' in command or '-D' in command else 0)
        with tempfile.TemporaryDirectory() as root:
            host = self.host(root, True)
            with mock.patch.object(host_module, 'replica_serves'), mock.patch.object(host_module.subprocess, 'run', side_effect=run):
                self.assertTrue(host.reads(self.lease())['blocked'])
                self.assertEqual(calls[0][0], 'systemd-run')
                self.assertTrue(any(c[0] == 'iptables' and '-I' in c for c in calls[1:]))
                self.assertTrue(any(c[0] == 'ip6tables' and '-I' in c for c in calls[1:]))
                old = json.loads(host.state.read_text())['timer']
                calls.clear()
                host.reads(self.lease())
                self.assertEqual(calls[0][0], 'systemd-run')
                self.assertIn(['systemctl', 'stop', old+'.timer'], calls)
                host.reads(dict(self.lease(), unblock=True))
                self.assertIsNone(host.lease)
                self.assertFalse(host.state.exists())

    def test_firewall_error_restores_both_families(self):
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            if command[0] == 'ip6tables' and '-I' in command:
                raise RuntimeError('insert failed')
            return types.SimpleNamespace(returncode=1 if '-C' in command or '-D' in command else 0)
        with tempfile.TemporaryDirectory() as root:
            host = self.host(root, True)
            with mock.patch.object(host_module, 'replica_serves'), mock.patch.object(host_module.subprocess, 'run', side_effect=run):
                with self.assertRaises(RuntimeError):
                    host.reads(self.lease())
                self.assertTrue(any(c[0] == 'iptables' and '-D' in c for c in calls))
                self.assertTrue(any(c[0] == 'ip6tables' and '-D' in c for c in calls))
                self.assertIsNone(host.lease)

    def test_probe_requires_exact_name_and_present_values(self):
        with mock.patch.object(host_module, 'fetch_json', return_value={'metrics': [{'name': 'metric', 'values': [1], 'isAbsent': [False]}]}):
            host_module.replica_serves('http://127.0.0.1:9999', ['metric'])
            with self.assertRaises(ValueError):
                host_module.replica_serves('http://127.0.0.1:9999', ['different'])
        with mock.patch.object(host_module, 'fetch_json', return_value={'metrics': [{'name': 'metric', 'values': [0], 'isAbsent': [True]}]}):
            with self.assertRaises(ValueError):
                host_module.replica_serves('http://127.0.0.1:9999', ['metric'])
        with mock.patch.object(host_module, 'fetch_json', return_value=[{'target': 'metric', 'datapoints': [[0, 123]]}]):
            host_module.replica_serves('http://127.0.0.1:9999', ['metric'])
        with mock.patch.object(host_module, 'fetch_json', return_value=[{'target': 'metric', 'datapoints': [[None, 123]]}]):
            with self.assertRaises(ValueError):
                host_module.replica_serves('http://127.0.0.1:9999', ['metric'])

    def test_offline_target_uses_no_firewall_or_replica(self):
        with tempfile.TemporaryDirectory() as root:
            host = self.host(root, True)
            host.args.offline = True
            with mock.patch.object(host_module, 'replica_serves') as probe, mock.patch.object(host_module.subprocess, 'run') as run:
                self.assertTrue(host.reads(self.lease())['offline'])
                probe.assert_not_called()
                run.assert_not_called()

    def test_p99_and_reset(self):
        prefix = 'http_request_duration_seconds_exp'
        previous = {prefix+'_count': 10, prefix+'_bucket{le="0.01"}': 10, prefix+'_bucket{le="+Inf"}': 10}
        current = {prefix+'_count': 110, prefix+'_bucket{le="0.01"}': 110, prefix+'_bucket{le="+Inf"}': 110}
        p99, count = host_module.read_p99(previous, current)
        self.assertAlmostEqual(p99, 9.9)
        self.assertEqual(count, 100)
        with self.assertRaises(ValueError):
            host_module.read_p99(current, previous)


if __name__ == '__main__':
    unittest.main()
