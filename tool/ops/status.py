#!/usr/bin/env python3
"""Read live migration status and check readiness for source-directory pruning."""
import argparse
import os
from pathlib import Path

from common import client, http_flags, lines, output, positive_int


def shared_ready(data):
    migration = data.get('storage', {}).get('migration')
    if not isinstance(migration, dict):
        return False
    # A missing field is unknown, even when its expected value is zero.
    return (migration.get('phase') == 'shared' and migration.get('in_flight') == 0
            and migration.get('skipped') == 0 and migration.get('error', '') == '')


def inventory(root, limit=10000):
    """Bounded, read-only inventory; truncation never proves pruning complete."""
    root = Path(root)
    if root.is_symlink() or root.resolve() != root.absolute() or not root.is_dir():
        raise ValueError('source root must be an existing directory without symlink components')
    device = root.stat().st_dev
    counts = {'entries': 0, 'empty_directories': 0, 'whisper_files': 0, 'lock_files': 0, 'other_files': 0, 'symlinks': 0, 'truncated': False}
    def walk_error(error):
        raise error
    for directory, dirs, files in os.walk(root, followlinks=False, onerror=walk_error):
        if directory != str(root) and not dirs and not files:
            counts['empty_directories'] += 1
        for name in dirs + files:
            if counts['entries'] >= limit:
                counts['truncated'] = True
                return counts
            path = Path(directory) / name
            counts['entries'] += 1
            if path.is_symlink():
                counts['symlinks'] += 1
                if name in dirs:
                    dirs.remove(name)
                continue
            if path.stat().st_dev != device:
                raise ValueError('nested filesystem in source tree')
            if name in files:
                key = 'whisper_files' if name.endswith(('.wsp', '.wsp.ooo')) else 'lock_files' if name.endswith('.lock') else 'other_files'
                counts[key] += 1
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', action='append', default=[], help='admin base URL; repeatable')
    parser.add_argument('--targets-file', help='one admin base URL per line')
    parser.add_argument('--require-shared', action='store_true', help='exit 1 unless every target is ready for pruning')
    parser.add_argument('--source-root', help='read-only local source inventory (one local target only)')
    parser.add_argument('--max-entries', type=positive_int, default=10000)
    http_flags(parser)
    args = parser.parse_args()
    targets = args.target + (lines(args.targets_file) if args.targets_file else [])
    if not targets:
        parser.error('supply --target or --targets-file')
    if args.source_root and len(targets) != 1:
        parser.error('local inventory requires exactly one target')
    rows, bad = [], False
    for target in targets:
        try:
            data = client(args, target).json('/admin/info', {'scopes': 'storage,cache'})
            ready = shared_ready(data)
            row = {'target': target, 'ready_for_prune': ready, 'status': data}
            if args.source_root:
                row['source_inventory'] = inventory(args.source_root, args.max_entries)
            bad |= args.require_shared and not ready
        except (ValueError, OSError, AttributeError) as exc:
            row = {'target': target, 'error': str(exc), 'ready_for_prune': False}
            bad = True
        rows.append(row)
    output(rows)
    raise SystemExit(1 if bad else 0)


if __name__ == '__main__':
    main()
