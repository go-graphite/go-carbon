import json
import re
import unittest
from pathlib import Path


class DashboardTest(unittest.TestCase):
    def test_importable_dashboard_has_variables_and_nonoverlapping_panels(self):
        path = Path(__file__).resolve().parents[2] / 'doc/pebble-storage/operations-dashboard.json'
        dashboard = json.loads(path.read_text())
        self.assertIsNone(dashboard['uid'])
        self.assertIsNone(dashboard['id'])
        variables = {v['name'] for v in dashboard['templating']['list']}
        self.assertTrue({'dataSource', 'host_label', 'job', 'target'} <= variables)
        ids, occupied = set(), set()
        for panel in dashboard['panels']:
            self.assertNotIn(panel['id'], ids)
            ids.add(panel['id'])
            pos = panel['gridPos']
            self.assertTrue(0 <= pos['x'] < 24 and pos['x'] + pos['w'] <= 24)
            for x in range(pos['x'], pos['x'] + pos['w']):
                for y in range(pos['y'], pos['y'] + pos['h']):
                    self.assertNotIn((x, y), occupied)
                    occupied.add((x, y))
            if panel['type'] == 'row':
                continue
            self.assertEqual(panel['datasource']['uid'], '${dataSource}')
            self.assertFalse(panel['fieldConfig']['defaults']['custom']['spanNulls'])
            for target in panel['targets']:
                self.assertIn('${host_label:raw}', target['expr'])
                self.assertIn('${target:regex}', target['expr'])
                referenced = set(re.findall(r'\$\{(\w+)(?::\w+)?\}', target['expr']))
                self.assertLessEqual(referenced, variables)
                self.assertIn('description', panel)


if __name__ == '__main__':
    unittest.main()
