"""Replace only a held node's existing volume hook after read-only qualification.

Run from the landed repository. --source-stdin supports transport without staging
host files. This installer never changes a unit, config, proof or marker.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import types


def install(source, target, proof, hold, expected_previous, reason):
    module = types.ModuleType('qualified_volume_binding')
    exec(compile(source, 'qualified_volume_binding.py', 'exec'), module.__dict__)
    old = module.read_regular(target)
    previous = hashlib.sha256(old).hexdigest()
    wanted = hashlib.sha256(source).hexdigest()
    if previous not in [expected_previous, wanted]:
        raise ValueError('installed hook differs from expected previous SHA256')
    hold_raw = module.read_regular(hold)
    state = json.loads(hold_raw)
    message = state.get('disabled_message')
    if not isinstance(message, str) or 'DO_NOT_REENABLE' not in message:
        raise ValueError('existing DO_NOT_REENABLE Puppet hold required')
    if not reason or '\n' in reason or '\r' in reason:
        raise ValueError('single-line hold annotation required')
    qualified = module.rebind(proof, inspect=True)
    if qualified.get('pending_changes') != 0 or qualified.get('changed'):
        raise ValueError('hook-only installation requires already-valid identity')
    # Recheck the two mutable control files immediately before replacing them.
    if module.read_regular(target) != old or module.read_regular(hold) != hold_raw:
        raise ValueError('hook or Puppet hold changed during qualification')
    if previous != wanted:
        module.atomic(target, source)
    annotation = ' | ' + reason
    if annotation not in message:
        state['disabled_message'] = message + annotation
        module.atomic(hold, json.dumps(state).encode() + b'\n')
    if hashlib.sha256(module.read_regular(target)).hexdigest() != wanted:
        raise ValueError('installed hook SHA256 mismatch')
    return {'hook_sha256': wanted, 'previous_sha256': previous,
            'hold_preserved': True, 'identity': module.rebind(proof, inspect=True)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--proof', type=Path, required=True)
    parser.add_argument('--expected-previous-sha256', required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--hold-reason', required=True)
    parser.add_argument('--source-stdin', action='store_true')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('installation requires root')
    source = sys.stdin.buffer.read(16385) if args.source_stdin else Path(__file__).with_name('volume_binding.py').read_bytes()
    if hashlib.sha256(source).hexdigest() != args.source_sha256:
        parser.error('source SHA256 mismatch')
    target = Path('/etc/go-carbon-storage-guard/volume_binding.py')
    hold = Path('/opt/puppetlabs/puppet/cache/state/agent_disabled.lock')
    print(json.dumps(install(source, target, args.proof, hold,
                             args.expected_previous_sha256, args.hold_reason), sort_keys=True))


if __name__ == '__main__':
    main()
