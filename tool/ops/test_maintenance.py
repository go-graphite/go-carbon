import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cleanup import inventory, plan, quarantine, references, stopped
from expiration import dotted_glob, render, translate
from nvme_smart import collect, render as smart_metrics
from status import inventory as source_inventory, shared_ready


class MaintenanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def test_prune_requires_explicit_complete_status(self):
        migration = {'phase': 'shared', 'in_flight': 0, 'skipped': 0, 'error': ''}
        self.assertTrue(shared_ready({'storage': {'migration': migration}}))
        for key, bad in (('phase', 'files'), ('in_flight', 1), ('skipped', 1), ('error', 'failed')):
            self.assertFalse(shared_ready({'storage': {'migration': dict(migration, **{key: bad})}}))
        for key in ('phase', 'in_flight', 'skipped'):
            missing = dict(migration)
            del missing[key]
            self.assertFalse(shared_ready({'storage': {'migration': missing}}))

    def test_source_inventory_counts_and_limits(self):
        (self.root / 'empty').mkdir()
        (self.root / 'sample.wsp').write_bytes(b'')
        (self.root / 'sample.wsp.lock').write_bytes(b'')
        (self.root / 'link').symlink_to(self.root / 'empty')
        result = source_inventory(self.root)
        self.assertEqual(result['whisper_files'], 1)
        self.assertEqual(result['lock_files'], 1)
        self.assertEqual(result['empty_directories'], 1)
        self.assertEqual(result['symlinks'], 1)
        self.assertTrue(source_inventory(self.root, 1)['truncated'])

    def test_quarantine_manifest_and_no_deletion(self):
        source, destination = self.root / 'source', self.root / 'quarantine'
        source.mkdir()
        destination.mkdir()
        (source / 'artifact').write_bytes(b'old artifact')
        selected = plan(source, ['artifact'], os.getuid(), 1, 0, 100, now=1e12)
        manifest = quarantine(source, destination, selected, os.getuid(), 100, 'go-carbon',
                              check_stopped=lambda _: None, find_refs=lambda _: set())
        self.assertFalse((source / 'artifact').exists())
        self.assertEqual((manifest.parent / 'artifact').read_bytes(), b'old artifact')
        self.assertTrue(json.loads(manifest.read_text())['entries'][0]['moved'])

    def test_quarantine_refuses_active_or_changed_artifact(self):
        source, destination = self.root / 'source', self.root / 'quarantine'
        source.mkdir()
        destination.mkdir()
        artifact = source / 'artifact'
        artifact.write_bytes(b'old')
        selected = plan(source, ['artifact'], os.getuid(), 1, 0, 100, now=1e12)
        with self.assertRaises(ValueError):
            quarantine(source, destination, selected, os.getuid(), 100, 'go-carbon',
                       check_stopped=lambda _: None, find_refs=lambda paths: set(paths))
        artifact.write_bytes(b'changed')
        with self.assertRaises(ValueError):
            quarantine(source, destination, selected, os.getuid(), 100, 'go-carbon',
                       check_stopped=lambda _: None, find_refs=lambda _: set())
        self.assertTrue(artifact.exists())
        with self.assertRaises(ValueError):
            plan(source, ['../artifact'], os.getuid(), 1, 0, 100, now=1e12)

    def test_symlink_hardlink_and_inventory_cap(self):
        (self.root / 'a').write_bytes(b'a')
        (self.root / 'link').symlink_to(self.root / 'a')
        with self.assertRaises(ValueError):
            inventory(self.root / 'link', self.root.stat().st_dev, os.getuid())
        os.link(self.root / 'a', self.root / 'hard')
        with self.assertRaises(ValueError):
            inventory(self.root / 'a', self.root.stat().st_dev, os.getuid())
        (self.root / 'hard').unlink()
        (self.root / 'link').unlink()
        with self.assertRaises(ValueError):
            inventory(self.root, self.root.stat().st_dev, os.getuid(), limit=1)

    def test_process_references_and_permission_errors(self):
        proc = self.root / 'proc'
        process = proc / '123'
        (process / 'fd').mkdir(parents=True)
        artifact = self.root / 'artifact'
        (process / 'fd' / '0').symlink_to(str(artifact) + '/file')
        (process / 'cmdline').write_bytes(b'')
        (process / 'maps').write_bytes(b'')
        self.assertEqual(references([artifact], proc), {artifact})
        with patch('cleanup.os.readlink', side_effect=PermissionError('denied')):
            with self.assertRaises(PermissionError):
                references([artifact], proc)

    def test_stopped_refuses_running_or_unknown_service(self):
        def run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, 'active\n', '')
        with self.assertRaises(ValueError):
            stopped('go-carbon', run)
        with self.assertRaises(ValueError):
            stopped('--help', run)

    def test_find_rounding_and_fenced_paths(self):
        base = '/srv/whisper'
        rule = translate('find /srv/whisper/jobs -type f -name "*.wsp" -mtime +7 -delete', base)
        self.assertEqual(rule, {'pattern': r'^jobs\.', 'expiration': '168h', 'grace': '24h'})
        zero = translate('find /srv/whisper -type f -name "*.wsp" -mmin +0 -delete', base)
        self.assertEqual(zero['expiration'], '1m')
        self.assertNotIn('grace', zero)
        glob = translate('find /srv/whisper -type f -name "*.wsp" -path "/srv/whisper/jobs/*" ! -path "/srv/whisper/jobs/keep/*" -mmin +59 -delete', base)
        self.assertEqual(glob['pattern'], r'^jobs\..*$')
        self.assertEqual(glob['expiration'], '59m')
        self.assertIn('keep', glob['exclude'])
        import re
        self.assertTrue(re.fullmatch(dotted_glob('/srv/whisper/jobs/*/value.wsp', base), 'jobs.a.b.value'))
        for unsupported in ('find /outside -type f -name "*.wsp" -mtime +1 -delete',
                            'find /srv/whisper -type f -name "*.wsp" -mtime +1 -exec rm {} ;',
                            'find /srv/whisper -type f -name "*.wsp" -mtime +1 -delete ; true',
                            'find /srv/whisper -type f -name "*.wsp" -mtime +1 -o -delete'):
            with self.assertRaises(ValueError):
                translate(unsupported, base)
        with self.assertRaises(ValueError):
            render([{'name': 'bad\nsection', 'find': 'find /x'}], base)

    def test_expiration_overlap_uses_earliest_age_and_rejects_early_delete(self):
        base = '/srv/whisper'
        rules = [
            {'name': 'broad', 'find': 'find /srv/whisper -type f -name "*.wsp" -mtime +30 -delete'},
            {'name': 'narrow', 'find': 'find /srv/whisper/jobs -type f -name "*.wsp" -mtime +7 -delete'}]
        policy = render(rules, base)
        self.assertLess(policy.index('[narrow]'), policy.index('[broad]'))
        with self.assertRaises(ValueError):
            translate('find /srv/whisper -delete -type f -name "*.wsp" -mtime +7', base)

    def test_old_directory_cannot_hide_recent_descendants(self):
        artifact = self.root / 'artifact'
        artifact.mkdir()
        (artifact / 'fresh').write_bytes(b'new')
        os.utime(artifact, (1, 1))
        with self.assertRaises(ValueError):
            plan(self.root, ['artifact'], os.getuid(), 86400, 0, 100)

    def test_smart_health_status_bits_and_failed_collection(self):
        healthy = {'nvme_smart_health_information_log': {'temperature': 42, 'media_errors': 2, 'data_units_written': 123}}
        def run(args, **kwargs):
            return subprocess.CompletedProcess(args, 8, json.dumps(healthy), '')
        text, errors = collect(['/dev/nvme0'], run=run, clock=lambda: 100)
        self.assertIn('temperature_celsius{device="/dev/nvme0"} 42', text)
        self.assertIn('last_success_timestamp_seconds{device="/dev/nvme0"} 100', text)
        self.assertEqual(errors, [])
        def fail(args, **kwargs):
            return subprocess.CompletedProcess(args, 2, '{}', '')
        text, errors = collect(['/dev/nvme0'], run=fail)
        self.assertIn('scrape_success{device="/dev/nvme0"} 0', text)
        self.assertNotIn('last_success', text)
        self.assertEqual(len(errors), 1)
        with self.assertRaises(ValueError):
            smart_metrics('/dev/nvme0', {'nvme_smart_health_information_log': {'temperature': float('nan')}})


if __name__ == '__main__':
    unittest.main()
