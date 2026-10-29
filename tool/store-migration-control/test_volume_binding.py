import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
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


if __name__ == '__main__':
    unittest.main()
