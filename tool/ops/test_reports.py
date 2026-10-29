import argparse
import io
import json
import unittest
from unittest.mock import patch

from capacity import size
from common import HTTP, counter_increase, positive, samples
from prometheus import collect, compare, query
from queries import queries, selector


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ReportsTest(unittest.TestCase):
    def test_http_endpoint_auth_query_and_timeout(self):
        requests = []
        def open_request(req, timeout):
            requests.append((req, timeout))
            return Response(b'{"status":"success"}')
        api = HTTP('https://metrics.example/prometheus/', 7, 'secret', 'tenant-a', open_request)
        self.assertEqual(api.json('/api/v1/query', {'query': 'a + b'})['status'], 'success')
        req, timeout = requests[0]
        self.assertEqual(req.full_url, 'https://metrics.example/prometheus/api/v1/query?query=a+%2B+b')
        self.assertEqual(req.get_header('Authorization'), 'Bearer secret')
        self.assertEqual(req.get_header('X-scope-orgid'), 'tenant-a')
        self.assertEqual(timeout, 7)
        for endpoint in ('file:///tmp/x', 'https://user:pass@example.test', 'http://example.test?token=x'):
            with self.assertRaises(ValueError):
                HTTP(endpoint)

    def test_prometheus_text_timestamp_labels_and_resets(self):
        before = samples('# ignored\nx{host="a b",path="a\\\\b"} 100 123\nx{host="c"} 5\n', ['x'])
        after = samples('x{path="a\\\\b",host="a b"} 2 456\nx{host="c"} 35\n', ['x'])
        self.assertEqual(counter_increase(before, after), 32)
        for body in ('other 0', 'x NaN', 'x -1', 'x{host="a",host="b"} 1', 'x 1\nx 2'):
            with self.assertRaises(ValueError):
                samples(body, ['x'])
        with self.assertRaises(ValueError):
            counter_increase(before, samples('x 0', ['x']))

    def test_query_nonfinite_missing_label_and_duplicates(self):
        class API:
            def __init__(self, result, result_type='vector', warnings=None):
                self.result, self.result_type, self.warnings = result, result_type, warnings
            def json(self, path, params):
                self.path, self.params = path, params
                return {'status': 'success', 'warnings': self.warnings, 'data': {'resultType': self.result_type, 'result': self.result}}
        entry = {'metric': {'instance': 'node-a:7007'}, 'value': [100, 'NaN']}
        api = API([entry])
        self.assertEqual(query(api, 'x', 'instance', at=100), {('node-a:7007', 100): None})
        self.assertEqual(api.params['time'], 100)
        for api in (API([entry, entry]), API([{'metric': {}, 'value': [100, '1']}]), API([], warnings=['partial'])):
            with self.assertRaises(ValueError):
                query(api, 'x', 'instance')
        api = API([{'metric': {'instance': 'node-a'}, 'values': [[100, '1'], [110, '2']]}], 'matrix')
        self.assertEqual(len(query(api, 'x', 'instance', 100, 110, 10)), 2)
        self.assertEqual(api.path, '/api/v1/query_range')

    def test_join_does_not_invent_zero_samples(self):
        args = argparse.Namespace(host_label='instance', start=None, end=None, step=60, at=100)
        with patch('prometheus.query', side_effect=[{('node-a', 100): 4}, {('node-b', 100): 2}]):
            rows = collect(None, {'cpu': 'cpu', 'rss': 'rss'}, args)
        self.assertEqual(rows[0]['rss'], None)
        self.assertEqual(rows[1]['cpu'], None)

    def test_query_selection_units_and_metric_names(self):
        select = selector('job="carbon"', 'instance', ['node-a:7007', 'node.b:7007'])
        self.assertIn('node\\\\.b', select)
        cost = queries('cost', select)
        self.assertIn('persister_pebble_points_total', cost['points_per_second'])
        self.assertIn('stage="total"', cost['total_seconds_per_point'])
        self.assertNotIn('clamp_min', ' '.join(cost.values()))
        for report in ('health', 'migration', 'cost', 'gc', 'input-log', 'io', 'capacity'):
            self.assertTrue(queries(report))
        for window in ('0m', '1m] or up', 'NaN'):
            with self.assertRaises(ValueError):
                queries('health', window=window)

    def test_parity_excludes_converting_and_incomplete_peers(self):
        row = dict(up=1, migration_in_flight=0, migration_failed=0, read_p99_seconds=1, cpu_cores=2, rss_bytes=100)
        rows = [dict(row, target='a'), dict(row, target='b'), dict(row, target='c', migration_in_flight=1)]
        result = compare(rows, {'a': ['b', 'c']})[0]
        self.assertEqual(result['peers_used'], ['b'])
        self.assertEqual(result['verdict'], 'unknown')
        self.assertEqual(compare(rows, {'a': ['b']})[0]['verdict'], 'within-threshold')
        rows[0]['cpu_cores'] = 3
        self.assertEqual(compare(rows, {'a': ['b']})[0]['verdict'], 'regression')
        with self.assertRaises(ValueError):
            compare(rows, {'a': ['a']})

    def test_capacity_headroom_quantile_and_unknown_samples(self):
        rows = [{'target': host, 'timestamp': t, 'cpu_cores': value} for host in ('a', 'b') for t, value in [(1, 2), (2, 4)]]
        sized = size(rows, {'cpu_cores': 8}, quantile=1, utilization=0.5, spares=1)
        self.assertEqual(sized['resources']['cpu_cores']['sum_of_target_quantiles'], 8)
        self.assertEqual(sized['hosts_including_spares'], 3)
        for invalid in ([dict(rows[0], cpu_cores=None)], rows + [rows[0]]):
            with self.assertRaises(ValueError):
                size(invalid, {'cpu_cores': 8})
        with self.assertRaises(argparse.ArgumentTypeError):
            positive('nan')


if __name__ == '__main__':
    unittest.main()
