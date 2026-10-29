#!/usr/bin/env python3
"""Quarantine explicitly selected stale artifacts while go-carbon is stopped."""
import argparse
import hashlib
import json
import os
import stat
import subprocess
import time
import uuid
from pathlib import Path

from common import atomic_write, lines, output, positive, positive_int


def regular_directory(path):
    path = Path(path).absolute()
    if path.resolve() != path or not path.is_dir():
        raise ValueError('directory must exist without symlink components')
    return path


def inventory(path, device, owner, limit=100000):
    fingerprint, allocated, entries, newest = hashlib.sha256(), 0, 0, 0
    stack = [path]
    while stack:
        current = stack.pop()
        info = current.lstat()
        if info.st_dev != device or info.st_uid != owner or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise ValueError('artifact contains a symlink, special file, different filesystem or different owner')
        if info.st_nlink != 1 and stat.S_ISREG(info.st_mode):
            raise ValueError('hard-linked artifacts are not eligible')
        entries += 1
        if entries > limit:
            raise ValueError('artifact exceeds inventory limit')
        row = (str(current.relative_to(path)), info.st_dev, info.st_ino, info.st_mode,
               info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        fingerprint.update(json.dumps(row).encode())
        allocated += info.st_blocks * 512
        newest = max(newest, info.st_mtime)
        if stat.S_ISDIR(info.st_mode):
            stack.extend(sorted(current.iterdir(), reverse=True))
    return {'fingerprint': fingerprint.hexdigest(), 'allocated_bytes': allocated, 'entries': entries, 'newest_mtime': newest}


def references(paths, proc_root=Path('/proc')):
    if not proc_root.is_dir():
        raise ValueError('Linux /proc is required for reference checks')
    found = set()
    for proc in proc_root.iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            # Do not inspect this collector's own argv: it contains candidate names.
            if proc.name == str(os.getpid()):
                continue
            links = [proc / 'cwd', proc / 'exe'] + list((proc / 'fd').iterdir())
            for link in links:
                try:
                    target = os.readlink(link).removesuffix(' (deleted)')
                except FileNotFoundError:
                    continue
                for path in paths:
                    if target == str(path) or target.startswith(str(path) + '/'):
                        found.add(path)
            for name in ('cmdline', 'maps'):
                data = (proc / name).read_bytes()
                for path in paths:
                    if str(path).encode() in data:
                        found.add(path)
        except FileNotFoundError:
            continue  # Process exited during the scan.
        # Permission errors propagate: an incomplete scan never proves inactivity.
    return found


def stopped(service, run=subprocess.run):
    if not service or service.startswith('-') or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.@-' for c in service):
        raise ValueError('invalid service name')
    proc = run(['systemctl', 'show', service, '-p', 'ActiveState', '--value'],
               capture_output=True, text=True, check=True, timeout=30)
    if proc.stdout.strip() not in ('inactive', 'failed'):
        raise ValueError('service must be stopped throughout artifact cleanup')


def plan(root, names, owner, min_age, min_bytes, limit, now=None):
    now = time.time() if now is None else now
    selected = []
    if not names or len(set(names)) != len(names):
        raise ValueError('supply distinct top-level candidate names')
    for name in names:
        if not name or name in ('.', '..') or Path(name).name != name:
            raise ValueError('candidate must be a top-level name below root')
        path = root / name
        info = path.lstat()
        if now - info.st_mtime < min_age:
            raise ValueError('artifact is too recent: ' + name)
        entry = inventory(path, root.stat().st_dev, owner, limit)
        if now - entry['newest_mtime'] < min_age:
            raise ValueError('artifact contains recent entries: ' + name)
        if entry['allocated_bytes'] < min_bytes:
            raise ValueError('artifact is smaller than min-bytes: ' + name)
        selected.append({'name': name, **entry})
    return selected


def quarantine(root, destination, selected, owner, limit, service, check_stopped=stopped, find_refs=references):
    if root == destination or root in destination.parents or destination in root.parents:
        raise ValueError('root and quarantine directories must be disjoint')
    if root.stat().st_dev != destination.stat().st_dev:
        raise ValueError('quarantine must be on the same filesystem for atomic renames')
    paths = [root / entry['name'] for entry in selected]
    check_stopped(service)
    if find_refs(paths):
        raise ValueError('artifact is referenced by a process')
    directory = destination / ('artifacts-' + uuid.uuid4().hex)
    directory.mkdir(mode=0o700)
    manifest = {'root': str(root), 'entries': [{**entry, 'moved': False} for entry in selected]}
    atomic_write(directory / 'manifest.json', json.dumps(manifest, indent=2).encode())
    # fd-relative rename pins both directories; no recursive deletion is performed.
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    rootfd, destfd = os.open(root, flags), os.open(directory, flags)
    try:
        for entry in manifest['entries']:
            check_stopped(service)
            path = root / entry['name']
            if inventory(path, os.fstat(rootfd).st_dev, owner, limit) != {k: entry[k] for k in ('fingerprint', 'allocated_bytes', 'entries', 'newest_mtime')}:
                raise ValueError('artifact changed since planning')
            if find_refs([path]):
                raise ValueError('artifact became referenced')
            os.rename(entry['name'], entry['name'], src_dir_fd=rootfd, dst_dir_fd=destfd)
            os.fsync(rootfd)
            os.fsync(destfd)
            entry['moved'] = True
            atomic_write(directory / 'manifest.json', json.dumps(manifest, indent=2).encode())
    finally:
        os.close(rootfd)
        os.close(destfd)
    return directory / 'manifest.json'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, help='dedicated inactive artifact directory, never a live store or input log')
    parser.add_argument('--candidate', action='append', default=[])
    parser.add_argument('--candidates-file', help='one top-level artifact name per line')
    parser.add_argument('--quarantine', help='existing directory on the same filesystem; required with --apply')
    parser.add_argument('--owner-uid', type=int, default=os.getuid())
    parser.add_argument('--min-age-days', type=positive, default=7)
    parser.add_argument('--min-bytes', type=int, default=0)
    parser.add_argument('--max-entries', type=positive_int, default=100000)
    parser.add_argument('--service', default='go-carbon.service')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        if args.owner_uid < 0 or args.min_bytes < 0:
            raise ValueError('UID and minimum size must be nonnegative')
        root = regular_directory(args.root)
        names = args.candidate + (lines(args.candidates_file) if args.candidates_file else [])
        selected = plan(root, names, args.owner_uid, args.min_age_days * 86400, args.min_bytes, args.max_entries)
        if args.apply:
            if not args.quarantine:
                raise ValueError('--quarantine is required with --apply')
            manifest = quarantine(root, regular_directory(args.quarantine), selected, args.owner_uid, args.max_entries, args.service)
            output({'manifest': str(manifest)})
        else:
            output({'apply': False, 'candidates': selected, 'reference_checks': 'performed only with --apply'})
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        parser.exit(1, str(exc) + '\n')


if __name__ == '__main__':
    main()
