import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('volume_binding', Path(__file__).with_name('volume_binding.py'))
binding = importlib.util.module_from_spec(spec)
spec.loader.exec_module(binding)
UUID = 'ac561b83-31d7-4e11-aada-87ff24d866b1'


class VolumeBindingTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source, self.store, self.archive = [self.root / n for n in ['source', 'store', 'archive']]
        for p in [self.source, self.store, self.archive]:
            p.mkdir()
        (self.store / 'LOCK').touch()
        (self.store / 'CHUNKSTORE').write_bytes(b'go-carbon-chunks-v6\n')
        self.sst = self.store / '000001.sst'
        self.sst.write_bytes(b'unchanged sentinel SST')
        self.wal = self.store / '000002.log'
        self.wal.write_bytes(b'unchanged sentinel WAL')
        self.proof = self.root / 'volume-binding.json'
        self.real_volume = binding.volume
        self.volume = patch.object(binding, 'volume', return_value=UUID)
        self.volume.start()
        self.addCleanup(self.volume.stop)
        roots = {k: binding.identity(p) for k, p in [
            ('source', self.source), ('store', self.store), ('archive', self.archive)]}
        proof = {'version': 1, 'roots': roots}
        values = binding.marker_bytes(proof, {k: v['device'] for k, v in roots.items()})
        for name, p in binding.paths(proof).items():
            v = values[name]
            p.write_bytes(v if isinstance(v, bytes) else json.dumps(v).encode())
            p.chmod(0o640)

    def record(self, archive=True):
        return binding.record(self.proof, self.source, self.store,
                              self.archive if archive else None)

    def simulate_old_device(self):
        proof = json.loads(self.proof.read_bytes())
        for row in proof['roots'].values():
            row['device'] += 17
        self.proof.write_text(json.dumps(proof))
        values = binding.marker_bytes(proof, {k: v['device'] for k, v in proof['roots'].items()})
        for name, p in binding.paths(proof).items():
            value = values[name]
            p.write_bytes(value if isinstance(value, bytes) else json.dumps(value).encode())

    def snapshot(self):
        return {str(p.relative_to(self.root)): p.read_bytes()
                for p in self.root.rglob('*') if p.is_file()}

    def test_no_change_and_record_does_not_overwrite(self):
        self.record()
        before = self.snapshot()
        self.assertFalse(binding.rebind(self.proof)['changed'])
        self.assertEqual(before, self.snapshot())
        with self.assertRaises(FileExistsError):
            self.record()

    def test_device_renumber_only_preserves_data_and_modes(self):
        self.record()
        self.simulate_old_device()
        self.assertEqual(binding.rebind(self.proof, inspect=True)['pending_changes'], 3)
        self.assertEqual(binding.rebind(self.proof)['updated_markers'], 3)
        self.assertFalse(binding.rebind(self.proof)['changed'])
        self.assertEqual(self.sst.read_bytes(), b'unchanged sentinel SST')
        self.assertEqual(self.wal.read_bytes(), b'unchanged sentinel WAL')
        for p in binding.paths(json.loads(self.proof.read_bytes())).values():
            self.assertEqual(p.stat().st_mode & 0o777, 0o640)
        self.assertEqual(len(list(self.root.glob('volume-rebind-*'))), 1)

    def test_record_without_archive(self):
        (self.store / 'INCREMENTAL-SOURCE-ARCHIVE.json').unlink()
        self.record(archive=False)
        self.simulate_old_device()
        self.assertEqual(binding.rebind(self.proof)['updated_markers'], 2)

    def test_foreign_filesystem_is_rejected_before_mutation(self):
        self.record()
        self.simulate_old_device()
        before = self.snapshot()
        with patch.object(binding, 'volume', return_value='foreign'):
            with self.assertRaisesRegex(ValueError, 'replaced'):
                binding.rebind(self.proof)
        self.assertEqual(before, self.snapshot())

    def test_each_replaced_directory_is_rejected_before_mutation(self):
        self.record()
        for name in ['source', 'store', 'archive']:
            original = self.proof.read_bytes()
            proof = json.loads(original)
            proof['roots'][name]['inode'] += 1
            self.proof.write_text(json.dumps(proof))
            before = self.snapshot()
            with self.assertRaisesRegex(ValueError, 'replaced'):
                binding.rebind(self.proof)
            self.assertEqual(before, self.snapshot())
            self.proof.write_bytes(original)

    def test_unexpected_binding_is_rejected(self):
        self.record()
        (self.store / 'INCREMENTAL-FILE-MIGRATION').write_bytes(b'foreign source\n')
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'unexpected'):
            binding.rebind(self.proof)
        self.assertEqual(before, self.snapshot())

    def test_partial_repair_can_resume_after_each_marker_write(self):
        self.record()
        proof_before = self.proof.read_bytes()
        for fail_at in [1, 2, 3, 4]:
            self.proof.write_bytes(proof_before)
            self.simulate_old_device()
            real = binding.atomic
            calls = []

            def interrupted(p, data):
                calls.append(p)
                if len(calls) == fail_at:
                    raise RuntimeError('crash before write')
                real(p, data)

            with patch.object(binding, 'atomic', side_effect=interrupted):
                with self.assertRaises(RuntimeError):
                    binding.rebind(self.proof)
            binding.rebind(self.proof)
            self.assertEqual(binding.rebind(self.proof, inspect=True)['pending_changes'], 0)

    def test_live_pebble_record_lock_excludes_rebind(self):
        self.record()
        self.simulate_old_device()
        code = "import fcntl,sys;f=open(sys.argv[1],'r+');fcntl.lockf(f,fcntl.LOCK_EX);print('locked',flush=True);sys.stdin.read()"
        child = subprocess.Popen([sys.executable, '-c', code, str(self.store / 'LOCK')],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'locked')
            before = self.snapshot()
            with self.assertRaises(BlockingIOError):
                binding.rebind(self.proof)
            self.assertEqual(before, self.snapshot())
            self.assertEqual(binding.rebind(self.proof, inspect=True)['pending_changes'], 3)
        finally:
            child.communicate('')
        binding.rebind(self.proof)

    def test_symlinked_or_oversize_files_rejected(self):
        self.record()
        guard = self.source / '.go-carbon-incremental-source-root.keep'
        original = guard.read_bytes()
        guard.unlink()
        guard.symlink_to(self.sst)
        with self.assertRaises(OSError):
            binding.rebind(self.proof)
        guard.unlink()
        guard.write_bytes(b'x' * 16385)
        with self.assertRaisesRegex(ValueError, 'invalid binding'):
            binding.rebind(self.proof)
        guard.write_bytes(original)

    def test_wrong_format_or_unrecorded_archive_is_rejected(self):
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'unrecorded archive'):
            self.record(archive=False)
        self.assertEqual(before, self.snapshot())
        (self.store / 'CHUNKSTORE').write_bytes(b'go-carbon-chunks-v5\n')
        with self.assertRaisesRegex(ValueError, 'V6'):
            self.record()
        self.assertFalse(self.proof.exists())

    def test_missing_uuid_or_wrong_filesystem_rejected(self):
        for data in [{'filesystems': [{'uuid': None, 'fstype': 'xfs'}]},
                     {'filesystems': [{'uuid': UUID, 'fstype': 'ext4'}]}]:
            with patch.object(binding.subprocess, 'run', return_value=type(
                    'Result', (), {'stdout': json.dumps(data)})()):
                # Exercise the real implementation hidden by setUp's mock.
                with self.assertRaises(ValueError):
                    self.real_volume(self.source)


class StableVolumeBindingTest(VolumeBindingTest):
    def modern(self, guard=True, token=False):
        (self.store / 'INCREMENTAL-SOURCE-ARCHIVE.json').unlink()
        self.record(archive=False)
        (self.store / 'CHUNKSTORE').write_bytes(b'go-carbon-chunks-v9\n')
        ino = self.source.stat().st_ino
        self.stable = 'birth:%d:1792067681:561279018' % ino
        if token:
            self.stable = 'token:' + 'ac' * 16
            (self.source / '.go-carbon-migration-source').write_text('ac' * 16 + '\n')
        (self.store / 'INCREMENTAL-FILE-MIGRATION').write_text(
            'go-carbon-incremental-v2\n%s\n%s\n' % (self.source, self.stable))
        if not guard:
            (self.source / '.go-carbon-incremental-source-root.keep').unlink()
        self.stat_result = SimpleNamespace(stdout='%d\n%d\n1792067681\n2026-10-15 12:34:41.561279018 +0000\n' %
                                           (self.source.stat().st_dev, ino))
        self.stat = patch.object(binding.subprocess, 'run', return_value=self.stat_result)
        self.stat.start()
        self.addCleanup(self.stat.stop)

    def test_modern_validation_is_read_only_with_or_without_keep_file(self):
        self.modern()
        for guard in [True, False]:
            if not guard:
                (self.source / '.go-carbon-incremental-source-root.keep').unlink()
            before = self.snapshot()
            for inspect in [True, False]:
                result = binding.rebind(self.proof, inspect=inspect)
                self.assertEqual(result['source_identity'], self.stable)
                self.assertEqual(result['pending_changes'], 0)
                self.assertFalse(result['changed'])
                self.assertEqual(before, self.snapshot())

    def test_wrong_volume_or_swapped_disk_refuses_without_writes(self):
        self.modern()
        for name in ['source', 'store']:
            before = self.snapshot()
            with patch.object(binding, 'volume', side_effect=lambda p: 'foreign' if p == getattr(self, name) else UUID):
                for inspect in [True, False]:
                    with self.assertRaisesRegex(ValueError, 'replaced'):
                        binding.rebind(self.proof, inspect=inspect)
            self.assertEqual(before, self.snapshot())

    def test_wrong_birth_inode_or_path_refuses_without_writes(self):
        self.modern()
        marker = self.store / 'INCREMENTAL-FILE-MIGRATION'
        original = marker.read_text()
        for text in [original.replace('561279018', '561279019'),
                     original.replace('birth:%d:' % self.source.stat().st_ino, 'birth:42:'),
                     original.replace(str(self.source), str(self.archive))]:
            marker.write_text(text)
            before = self.snapshot()
            for inspect in [True, False]:
                with self.assertRaises(ValueError):
                    binding.rebind(self.proof, inspect=inspect)
            self.assertEqual(before, self.snapshot())
        marker.write_text(original)

    def test_unavailable_or_racing_stat_refuses_without_writes(self):
        self.modern()
        good = self.stat_result.stdout
        for text in [good.replace('1792067681', '0'), good.replace('561279018', '561279'),
                     good.replace(str(self.source.stat().st_dev) + '\n', '42\n', 1),
                     good.replace(str(self.source.stat().st_ino) + '\n', '42\n', 1), 'unavailable']:
            self.stat_result.stdout = text
            before = self.snapshot()
            with self.assertRaises(ValueError):
                binding.rebind(self.proof)
            self.assertEqual(before, self.snapshot())

    def test_modern_never_rebinds_proof_after_device_renumbering(self):
        self.modern(guard=False)
        proof = json.loads(self.proof.read_bytes())
        for row in proof['roots'].values():
            row['device'] += 17
        self.proof.write_text(json.dumps(proof))
        before = self.snapshot()
        self.assertFalse(binding.rebind(self.proof)['changed'])
        self.assertEqual(before, self.snapshot())

    def test_bad_or_symlinked_keep_and_unrecorded_archive_refuse(self):
        self.modern()
        guard = self.source / '.go-carbon-incremental-source-root.keep'
        guard.write_text('{}')
        with self.assertRaisesRegex(ValueError, 'unexpected directory binding'):
            binding.rebind(self.proof)
        guard.unlink()
        guard.symlink_to(self.sst)
        with self.assertRaises(OSError):
            binding.rebind(self.proof)
        guard.unlink()
        (self.store / 'INCREMENTAL-SOURCE-ARCHIVE.json').symlink_to(self.root / 'absent')
        with self.assertRaisesRegex(ValueError, 'archive policy'):
            binding.rebind(self.proof)

    def test_explicit_supported_formats_only_and_v2_required_for_modern(self):
        self.modern()
        for n in [6, 7, 9, 11, 12]:
            (self.store / 'CHUNKSTORE').write_text('go-carbon-chunks-v%d\n' % n)
            self.assertFalse(binding.rebind(self.proof)['changed'])
        for n in [5, 8, 10, 13]:
            (self.store / 'CHUNKSTORE').write_text('go-carbon-chunks-v%d\n' % n)
            with self.assertRaisesRegex(ValueError, 'format required'):
                binding.rebind(self.proof)
        (self.store / 'CHUNKSTORE').write_bytes(b'go-carbon-chunks-v9\n')
        proof = json.loads(self.proof.read_bytes())
        legacy = binding.marker_bytes(proof, {k: v['device'] for k, v in proof['roots'].items()})['source']
        (self.store / 'INCREMENTAL-FILE-MIGRATION').write_bytes(legacy)
        with self.assertRaisesRegex(ValueError, 'stable V2'):
            binding.rebind(self.proof)

    def test_modern_token_checks_exact_source_token(self):
        self.modern(token=True)
        self.assertEqual(binding.rebind(self.proof)['source_identity'], self.stable)
        token = self.source / '.go-carbon-migration-source'
        for value in ['ab' * 16 + '\n', 'not-a-token\n']:
            token.write_text(value)
            with self.assertRaises(ValueError):
                binding.rebind(self.proof)

    def test_exact_donor_v9_v2_fixtures_that_previously_raised_v6_required(self):
        for kind in ['with-guard', 'without-guard']:
            with self.subTest(host=kind):
                fixture = json.loads((Path(__file__).parent / 'fixtures' / 'volume-binding' / (kind + '.json')).read_text())
                proof = fixture['proof']
                roots = proof['roots']
                source = Path(roots['source']['path'])
                store = Path(roots['store']['path'])
                files = {str(self.proof): json.dumps(proof).encode(),
                         str(store / 'CHUNKSTORE'): fixture['format'].encode(),
                         str(store / 'INCREMENTAL-FILE-MIGRATION'): fixture['source_marker'].encode()}
                if fixture['guard'] is not None:
                    files[str(source / '.go-carbon-incremental-source-root.keep')] = fixture['guard'].encode()
                seconds, fraction = fixture['source_marker'].split('\n')[2].split(':')[2:]
                stat_result = SimpleNamespace(stdout='64775\n131\n%s\n2026-10-15 12:00:00.%s +0000\n' % (seconds, fraction))
                before = dict(files)
                with patch.object(binding, 'read_regular', side_effect=lambda p: files[str(p)]), \
                        patch.object(binding, 'identity', side_effect=lambda p: next(dict(v) for v in roots.values() if v['path'] == str(p))), \
                        patch.object(binding.os.path, 'lexists', side_effect=lambda p: str(p) in files), \
                        patch.object(binding.Path, 'lstat', return_value=SimpleNamespace(st_dev=64775, st_ino=131)), \
                        patch.object(binding.subprocess, 'run', return_value=stat_result), \
                        patch.object(binding, 'atomic') as atomic:
                    for inspect in [True, False]:
                        result = binding.rebind(self.proof, inspect=inspect)
                        self.assertEqual(result['source_identity'], fixture['source_marker'].split('\n')[2])
                        self.assertEqual(result['roots'], roots)
                        self.assertFalse(result['changed'])
                    atomic.assert_not_called()
                    self.assertEqual(before, files)


class VolumeBindingInstallTest(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('installer', Path(__file__).with_name('install_volume_binding.py'))
        self.installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.installer)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.target, self.hold, self.proof = [root / n for n in ['hook.py', 'hold.json', 'proof.json']]
        self.target.write_bytes(b'previous reviewed hook')
        self.hold.write_text(json.dumps({'disabled_message': 'original DO_NOT_REENABLE reason', 'extra': 42}))
        self.proof.write_bytes(b'proof sentinel')
        self.previous = hashlib.sha256(self.target.read_bytes()).hexdigest()
        self.source = Path(__file__).with_name('volume_binding.py').read_bytes()
        self.qualified = {'changed': False, 'pending_changes': 0, 'source_identity': 'birth:131:1:000000001'}
        # Exercise the real file reader and atomic writer from the shipped hook,
        # while isolating the already-tested host identity check.
        self.real_module = self.installer.types.ModuleType
        def module(name):
            value = self.real_module(name)
            value.__dict__['test_qualification'] = self.qualified
            return value
        self.module_patch = patch.object(self.installer.types, 'ModuleType', side_effect=module)
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        self.source += b'\ndef rebind(proof, inspect=False):\n    assert inspect\n    return test_qualification\n'

    def install(self):
        return self.installer.install(self.source, self.target, self.proof, self.hold, self.previous, 'volume-binding: hook MR test')

    def test_only_hook_and_hold_annotation_change_and_repeat_is_idempotent(self):
        proof = self.proof.read_bytes()
        result = self.install()
        self.assertEqual(result['hook_sha256'], hashlib.sha256(self.source).hexdigest())
        self.assertEqual(self.target.read_bytes(), self.source)
        self.assertEqual(json.loads(self.hold.read_bytes()), {'disabled_message': 'original DO_NOT_REENABLE reason | volume-binding: hook MR test', 'extra': 42})
        self.assertEqual(proof, self.proof.read_bytes())
        hold = self.hold.read_bytes()
        self.install()
        self.assertEqual(hold, self.hold.read_bytes())

    def test_foreign_hook_or_missing_hold_or_pending_rebind_prevents_all_writes(self):
        original_hold = self.hold.read_bytes()
        for scenario in ['foreign-hook', 'missing-sentinel', 'pending-rebind']:
            self.hold.write_bytes(original_hold)
            self.target.write_bytes(b'previous reviewed hook')
            self.qualified['pending_changes'] = 0
            if scenario == 'foreign-hook':
                self.target.write_bytes(b'unexpected foreign hook')
            elif scenario == 'missing-sentinel':
                self.hold.write_text(json.dumps({'disabled_message': 'ordinary reason'}))
            else:
                self.qualified['pending_changes'] = 1
            before = [p.read_bytes() for p in [self.target, self.hold, self.proof]]
            with self.assertRaises(ValueError):
                self.install()
            self.assertEqual(before, [p.read_bytes() for p in [self.target, self.hold, self.proof]])


if __name__ == '__main__':
    unittest.main()
