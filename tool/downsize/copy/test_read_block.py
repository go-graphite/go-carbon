import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('read_block', Path(__file__).with_name('read_block.py'))
block = importlib.util.module_from_spec(spec)
spec.loader.exec_module(block)


class RetainedBlockTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = Path(self.directory.name) / 'block.json'
        self.installed = set()
        self.commands = []
        self.runner = patch.object(block.subprocess, 'run', side_effect=self.firewall)
        self.runner.start()
        self.addCleanup(self.runner.stop)

    def firewall(self, args, check=False, **kwargs):
        self.commands.append(args)
        binary, action, *rule = args
        if action == '-I':
            rule.pop(1)
        key = (binary, *rule)
        rc = 0
        if action == '-C':
            rc = 0 if key in self.installed else 1
        elif action == '-I':
            self.installed.add(key)
        elif action == '-D':
            self.installed.remove(key)
        else:
            self.fail('unexpected command: ' + repr(args))
        if check and rc:
            raise subprocess.CalledProcessError(rc, args)
        return subprocess.CompletedProcess(args, rc)

    def test_hold_survives_return_without_timer_and_release_verifies_absence(self):
        result = block.retained_block('tuning-canary', [8080, 8081], self.state)
        self.assertEqual(result['action'], 'reads_blocked_retained')
        self.assertEqual(len(self.installed), 4)
        self.assertTrue(self.state.exists())
        self.assertFalse(any(cmd[0] == 'systemd-run' for cmd in self.commands))
        block.release_retained(self.state)
        self.assertEqual(self.installed, set())
        self.assertFalse(self.state.exists())

    def test_existing_state_is_not_overwritten(self):
        self.state.write_text('original')
        with self.assertRaises(FileExistsError):
            block.retained_block('tuning-canary', [8080], self.state)
        self.assertEqual(self.state.read_text(), 'original')
        self.assertEqual(self.commands, [])

    def test_preexisting_rule_is_not_removed(self):
        block.retained_block('tuning-canary', [8080], self.state)
        self.state.unlink()
        original = self.installed.copy()
        with self.assertRaises(ValueError):
            block.retained_block('tuning-canary', [8080], self.state)
        self.assertEqual(self.installed, original)

    def test_partial_install_failure_removes_only_owned_rules(self):
        real_firewall = self.firewall
        def fail_ipv6(args, **kwargs):
            if args[:2] == ['ip6tables', '-I']:
                raise subprocess.CalledProcessError(2, args)
            return real_firewall(args, **kwargs)
        with patch.object(block.subprocess, 'run', side_effect=fail_ipv6):
            with self.assertRaises(subprocess.CalledProcessError):
                block.retained_block('tuning-canary', [8080], self.state)
        self.assertEqual(self.installed, set())
        self.assertTrue(self.state.exists())

    def test_check_error_never_admits_copy(self):
        with patch.object(block.subprocess, 'run', return_value=subprocess.CompletedProcess([], 2)):
            with self.assertRaises(ValueError):
                block.retained_block('tuning-canary', [8080], self.state)
        self.assertEqual(self.installed, set())

    def test_release_failure_keeps_state(self):
        block.retained_block('tuning-canary', [8080], self.state)
        with patch.object(block.subprocess, 'run', return_value=subprocess.CompletedProcess([], 2)):
            with self.assertRaises(ValueError):
                block.release_retained(self.state)
        self.assertTrue(self.state.exists())
        self.assertEqual(len(self.installed), 2)

    def test_failed_install_cannot_release_preexisting_rules(self):
        block.retained_block('tuning-canary', [8080], self.state)
        self.state.unlink()
        original = self.installed.copy()
        with self.assertRaises(ValueError):
            block.retained_block('tuning-canary', [8080], self.state)
        with self.assertRaises(ValueError):
            block.release_retained(self.state)
        self.assertEqual(self.installed, original)


if __name__ == '__main__':
    unittest.main()
