import importlib.util
from pathlib import Path
import unittest

def load(name):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

block=load('read_block')

class ReadIsolationTests(unittest.TestCase):
    def test_replica_required_and_history_must_match(self):
        original=block.fetch
        try:
            block.fetch=lambda base,*args:{100:1,101:2} if base=='old' else {100:1,101:2,102:3}
            self.assertEqual(block.check_peer('old','peer',['a','b','c'],'region-a','region-b'),6)
            with self.assertRaises(ValueError):block.check_peer('old','peer',['a','b','c'],'region-a','region-a')
            block.fetch=lambda base,*args:{100:1,101:2} if base=='old' else {100:1}
            with self.assertRaises(ValueError):block.check_peer('old','peer',['a','b','c'],'region-a','region-b')
        finally:
            block.fetch=original

    def test_block_rules_cover_ipv4_ipv6_and_both_ports(self):
        rules=block.rules('unique',[8080,7004]);self.assertEqual(len(rules),4)
        self.assertEqual({row[0] for row in rules},{'iptables','ip6tables'})
        self.assertTrue(all('tcp-reset' in row for row in rules))

if __name__=='__main__':unittest.main()
