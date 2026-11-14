"""Validate modern source identities and fence legacy V6 device renumbering.

Record on a verified running node, then invoke rebind in ExecStartPre. A proof
without a filesystem UUID is never accepted. SST/WAL files are never rewritten.
"""
from pathlib import Path
import argparse
import contextlib
import fcntl
import json
import os
import re
import stat
import subprocess
import time


def read_regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 16384:
            raise ValueError('invalid binding file: ' + str(path))
        return stream.read(16385)


def atomic(path, data):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError('binding target is not regular')
    temporary = path.with_name(path.name + '.rebind-' + str(os.getpid()))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fchown(stream.fileno(), info.st_uid, info.st_gid)
        os.fchmod(stream.fileno(), stat.S_IMODE(info.st_mode))
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_dir(path.parent)


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def volume(path):
    result = subprocess.run(
        ['findmnt', '--json', '-T', str(path), '-o', 'UUID,FSTYPE'],
        check=True, capture_output=True, text=True, timeout=10)
    rows = json.loads(result.stdout)['filesystems']
    if len(rows) != 1 or rows[0].get('fstype') != 'xfs':
        raise ValueError('automatic rebinding is restricted to XFS')
    uuid = rows[0].get('uuid', '')
    if not isinstance(uuid, str) or not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', uuid):
        raise ValueError('filesystem UUID unavailable')
    return uuid


def identity(path):
    if not path.is_absolute() or str(path) != os.path.realpath(path):
        raise ValueError('binding directories must be canonical without symlinks')
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('binding root is not a directory')
    return {'path': str(path), 'uuid': volume(path), 'inode': info.st_ino,
            'device': info.st_dev}


def paths(proof):
    source = Path(proof['roots']['source']['path'])
    store = Path(proof['roots']['store']['path'])
    result = {'source': store / 'INCREMENTAL-FILE-MIGRATION',
              'guard': source / '.go-carbon-incremental-source-root.keep'}
    if 'archive' in proof['roots']:
        result['archive'] = store / 'INCREMENTAL-SOURCE-ARCHIVE.json'
    return result


def marker_bytes(proof, devices):
    roots = proof['roots']
    source = roots['source']
    result = {'source': ('go-carbon-incremental-v1\n%s\n%d:%d\n' %
                        (source['path'], devices['source'], source['inode'])).encode(),
              'guard': {'version': 1, 'path': source['path'],
                        'device': devices['source'], 'inode': source['inode']}}
    if 'archive' in roots:
        archive = roots['archive']
        result['archive'] = {'Version': 1, 'Path': archive['path'],
                             'Device': devices['archive'], 'Inode': archive['inode']}
    return result


# Keep this explicit list aligned with checkFormatForContinuation in store.go.
CONTINUATION_FORMATS = {('go-carbon-chunks-v%d\n' % n).encode()
                        for n in [6, 7, 9, 11, 12]}


def stable_source_identity(source, current, recorded):
    if recorded.startswith('token:'):
        token = read_regular(source / '.go-carbon-migration-source')
        if not re.fullmatch(b'[0-9a-f]{32}\n', token):
            raise ValueError('invalid source identity token')
        return 'token:' + token.decode().rstrip('\n')
    if not recorded.startswith('birth:'):
        raise ValueError('unsupported stable source identity')
    # GNU stat obtains nanosecond birth time with statx, just as the Go reader
    # does. Do not round through a float or accept an unavailable birth time.
    result = subprocess.run(
        ['stat', '--printf=%d\n%i\n%W\n%w\n', '--', str(source)],
        check=True, capture_output=True, text=True, timeout=10)
    fields = result.stdout.splitlines()
    if len(fields) != 4 or not all(re.fullmatch(r'[0-9]+', x) for x in fields[:3]):
        raise ValueError('source directory birth time unavailable')
    device, inode, seconds = map(int, fields[:3])
    fraction = re.search(r'\.([0-9]{9}) [+-][0-9]{4}$', fields[3])
    after = source.lstat()
    if ((device, inode) != (current['device'], current['inode']) or
            (after.st_dev, after.st_ino) != (device, inode)):
        raise ValueError('source changed while reading stable identity')
    if not fraction or seconds == 0:
        raise ValueError('source directory birth time unavailable')
    return 'birth:%d:%d:%s' % (inode, seconds, fraction[1])


def validate_stable_markers(proof, current, raw):
    roots = proof['roots']
    store = Path(roots['store']['path'])
    # Modern archive-policy adoption is deliberately outside this read-only
    # path. Never ignore a policy merely because its proof did not record it.
    if 'archive' in roots or os.path.lexists(store / 'INCREMENTAL-SOURCE-ARCHIVE.json'):
        raise ValueError('stable source validation requires no archive policy')
    source = Path(roots['source']['path'])
    parts = raw.decode().split('\n')
    if len(parts) != 4 or parts[0] != 'go-carbon-incremental-v2' or parts[1] != str(source) or parts[3]:
        raise ValueError('unexpected stable source binding')
    wanted = stable_source_identity(source, current['source'], parts[2])
    if parts[2] != wanted:
        raise ValueError('unexpected stable source identity')
    guard = paths(proof)['guard']
    # The modern Go binding does not require the historical source-root keep
    # file. When one remains, validate it too; do not create or rewrite it.
    if os.path.lexists(guard):
        expected = marker_bytes(proof, {k: v['device'] for k, v in current.items()})['guard']
        if json.loads(read_regular(guard)) != expected:
            raise ValueError('unexpected directory binding: ' + str(guard))
    return wanted


def validate_markers(proof, current):
    store = Path(proof['roots']['store']['path'])
    format_marker = read_regular(store / 'CHUNKSTORE')
    if format_marker not in CONTINUATION_FORMATS:
        raise ValueError('supported continuation format required (V6/V7/V9/V11/V12)')
    source_marker = read_regular(paths(proof)['source'])
    if source_marker.startswith(b'go-carbon-incremental-v2\n'):
        validate_stable_markers(proof, current, source_marker)
        return []
    if format_marker != b'go-carbon-chunks-v6\n':
        raise ValueError('modern continuation format requires a stable V2 source binding')
    if 'archive' not in proof['roots'] and (store / 'INCREMENTAL-SOURCE-ARCHIVE.json').exists():
        raise ValueError('unrecorded archive policy')
    previous = marker_bytes(proof, {k: v['device'] for k, v in proof['roots'].items()})
    wanted = marker_bytes(proof, {k: v['device'] for k, v in current.items()})
    changes = []
    for name, path in paths(proof).items():
        raw = read_regular(path)
        actual = raw if name == 'source' else json.loads(raw)
        if actual not in [previous[name], wanted[name]]:
            raise ValueError('unexpected directory binding: ' + str(path))
        if actual != wanted[name]:
            value = wanted[name]
            changes.append((path, raw, value if isinstance(value, bytes)
                            else json.dumps(value, sort_keys=True).encode() + b'\n'))
    return changes


def current_roots(proof):
    if proof.get('version') != 1 or set(proof.get('roots', {})) not in [
            {'source', 'store'}, {'source', 'store', 'archive'}]:
        raise ValueError('invalid volume proof')
    current = {k: identity(Path(v['path'])) for k, v in proof['roots'].items()}
    for name, old in proof['roots'].items():
        now = current[name]
        if any(old.get(key) != now[key] for key in ['path', 'uuid', 'inode']):
            raise ValueError('filesystem or directory replaced: ' + name)
    return current


def record(destination, source, store, archive=None):
    roots = {'source': identity(source), 'store': identity(store)}
    if archive is not None:
        roots['archive'] = identity(archive)
    if len({x['uuid'] for x in roots.values()}) != 1:
        raise ValueError('online migration roots must share the recorded filesystem')
    for a, left in roots.items():
        for b, right in roots.items():
            if a != b and os.path.commonpath([left['path'], right['path']]) == left['path']:
                raise ValueError('migration roots must be disjoint')
    proof = {'version': 1, 'recorded_at': time.time(), 'roots': roots}
    if validate_markers(proof, roots):
        raise ValueError('record requires already-valid bindings')
    # Recording is read-only with respect to a live database. Do not replace a
    # previous proof: an identity change requires explicit operator diagnosis.
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(json.dumps(proof, indent=2).encode() + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    sync_dir(destination.parent)
    return proof


@contextlib.contextmanager
def store_lock(store):
    # Pebble's LOCK inode is never created or replaced by this helper.
    fd = os.open(store / 'LOCK', os.O_RDWR | os.O_NOFOLLOW)
    try:
        # Pebble uses F_SETLK record locks, not flock(2). The two lock classes
        # are independent on Linux; flock would allow changing a live store.
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def rebind(proof_path, inspect=False):
    proof = json.loads(read_regular(proof_path))
    current = current_roots(proof)
    source_marker = read_regular(paths(proof)['source'])
    if source_marker.startswith(b'go-carbon-incremental-v2\n'):
        validate_markers(proof, current)
        # Stable identities never need device-number rebinding, even after a
        # reboot. In particular do not rewrite the existing proof or markers.
        return {'changed': False, 'pending_changes': 0, 'updated_markers': 0,
                'source_identity': source_marker.decode().split('\n')[2],
                'roots': current}
    if inspect:
        changes = validate_markers(proof, current)
        return {'changed': False, 'pending_changes': len(changes), 'roots': current}
    # An ordinary same-boot handoff can still have the old worker alive.
    # With identical bindings this is a read-only check and needs no DB lock.
    if proof['roots'] == current and not validate_markers(proof, current):
        return {'changed': False, 'updated_markers': 0, 'roots': current}
    with store_lock(Path(proof['roots']['store']['path'])):
        # Recheck after locking, before any write. A partial prior attempt may
        # contain old and current device numbers; each must match the proof.
        current = current_roots(proof)
        changes = validate_markers(proof, current)
        if changes:
            history = proof_path.parent / ('volume-rebind-%d-%d' % (time.time_ns(), os.getpid()))
            history.mkdir(mode=0o700)
            for i, (path, old, new) in enumerate(changes):
                with (history / ('%d-%s.before' % (i, path.name))).open('xb') as stream:
                    stream.write(old)
                    stream.flush()
                    os.fsync(stream.fileno())
            sync_dir(history)
            sync_dir(history.parent)
            for path, old, new in changes:
                atomic(path, new)
        # Updating the proof last makes an interrupted multi-marker repair
        # resumable without accepting an unrecorded filesystem or inode.
        if proof['roots'] != current:
            proof['roots'] = current
            proof['last_rebind_at'] = time.time()
            atomic(proof_path, json.dumps(proof, indent=2).encode() + b'\n')
        return {'changed': bool(changes), 'updated_markers': len(changes), 'roots': current}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['record', 'rebind', 'inspect'])
    parser.add_argument('--proof', type=Path, required=True)
    parser.add_argument('--source', type=Path)
    parser.add_argument('--store', type=Path)
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args()
    if args.mode == 'record':
        if args.source is None or args.store is None:
            parser.error('record requires --source and --store')
        result = record(args.proof, args.source, args.store, args.archive)
    else:
        if any(x is not None for x in [args.source, args.store, args.archive]):
            parser.error('rebind/inspect uses only the recorded proof')
        result = rebind(args.proof, inspect=args.mode == 'inspect')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
