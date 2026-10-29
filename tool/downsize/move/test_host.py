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
