#!/usr/bin/env python3
"""Translate a conservative subset of find-based cron rules into expiration policy."""
import argparse
import json
import re
import shlex
from pathlib import PurePosixPath, Path


# find -path wildcards cross directory boundaries. Metric names use dots there.
def dotted_glob(pattern, base):
    prefix = base.rstrip('/') + '/'
    if not pattern.startswith(prefix):
        raise ValueError('path patterns must be absolute and below --data-root')
    pattern = pattern[len(prefix):]
    if pattern.endswith('.wsp'):
        pattern = pattern[:-4]
    elif pattern.endswith('*'):
        pass
    else:
        raise ValueError('path must end in .wsp or *')
    if '.' in pattern:
        raise ValueError('literal dots in path components do not map to metric components')
    result = []
    for char in pattern:
        if char in '[]\\\n\r':
            raise ValueError('character classes and escapes are not supported')
        result.append('.*' if char == '*' else '.' if char == '?' else r'\.' if char in '/.' else re.escape(char))
    return '^' + ''.join(result) + '$'


def translate(command, base):
    tokens = shlex.split(command)
    if len(tokens) < 3 or tokens[0] not in ('find', '/usr/bin/find', '/bin/find'):
        raise ValueError('supply the find command from the cron, without shell wrappers')
    root = tokens[1].rstrip('/')
    base = base.rstrip('/')
    if (not base.startswith('/') or not root.startswith('/') or '..' in PurePosixPath(root).parts
            or not (root == base or root.startswith(base + '/')) or any(c in root for c in '*?[]\\')):
        raise ValueError('find root must be a literal directory below data-root')
    if any('.' in part for part in PurePosixPath(root[len(base):]).parts):
        raise ValueError('dotted path components cannot map unambiguously to metrics')
    predicates, exclusions, age, unit = [], [], None, None
    file_type, wsp, delete = False, False, False
    index = 2
    while index < len(tokens):
        token = tokens[index]
        if token == '-delete':
            if delete or index != len(tokens) - 1:
                raise ValueError('-delete must occur once, at the end')
            delete = True
            index += 1
            continue
        negated = token in ('!', '-not')
        if negated:
            index += 1
            token = tokens[index] if index < len(tokens) else ''
        if index + 1 >= len(tokens):
            raise ValueError('missing find predicate argument')
        value = tokens[index + 1]
        if token == '-type' and value == 'f' and not negated and not file_type:
            file_type = True
        elif token == '-name' and value == '*.wsp' and not negated and not wsp:
            wsp = True
        elif token in ('-path', '-wholename'):
            if not value.startswith(root + '/'):
                raise ValueError('path predicates must be fenced by the find root')
            (exclusions if negated else predicates).append(dotted_glob(value, base))
        elif token in ('-mtime', '-mmin') and not negated and age is None and re.fullmatch(r'\+[0-9]+', value):
            age, unit = int(value[1:]), '24h' if token == '-mtime' else '1m'
        else:
            raise ValueError('unsupported find predicate: ' + token)
        index += 2
    if not file_type or not wsp or not delete or age is None or len(predicates) > 1:
        raise ValueError('need -type f -name "*.wsp", one +N age, -delete and at most one path predicate')
    pattern = predicates[0] if predicates else ('^' + re.escape(root[len(base):].strip('/').replace('/', '.')) + r'\.' if root != base else '^')
    # GNU find +N compares floor(age / unit) > N; preserve its extra whole unit.
    duration = str(age * 24) + 'h' if unit == '24h' else str(age) + 'm'
    if age == 0:
        # expiration=0 exempts metrics in the native policy, so encode +0 as 1 unit.
        duration, grace = unit, None
    else:
        grace = unit
    if (age + 1) * (86400 if unit == '24h' else 60) > 9223372036:
        raise ValueError('expiration exceeds the supported Go duration range')
    result = {'pattern': pattern, 'expiration': duration}
    if grace:
        result['grace'] = grace
    if exclusions:
        result['exclude'] = '|'.join('(?:' + value + ')' for value in exclusions)
    return result


def render(rules, base):
    sections, seen = [], set()
    for rule in rules:
        name = rule['name']
        if not re.fullmatch(r'[A-Za-z0-9_-]+', name) or name in seen:
            raise ValueError('rule names must be unique simple section names')
        seen.add(name)
        translated = translate(rule['find'], base)
        def seconds(value):
            return int(value[:-1]) * (3600 if value.endswith('h') else 60)
        ttl = seconds(translated['expiration']) + seconds(translated.get('grace', '0m'))
        sections.append((ttl, name, '[' + name + ']\n' + '\n'.join(k + ' = ' + v for k, v in translated.items())))
    if not sections:
        raise ValueError('at least one rule is required')
    return '# Shortest effective age first: independent find jobs delete on the earliest match.\n# Review activity semantics and start with store-expiration-dry-run=true.\n\n' + '\n\n'.join(section for _, _, section in sorted(sections)) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('rules', help='JSON array of {name, find}; most specific rules first')
    parser.add_argument('--data-root', default='/var/lib/carbon/whisper')
    args = parser.parse_args()
    try:
        print(render(json.loads(Path(args.rules).read_text()), args.data_root), end='')
    except (ValueError, KeyError, TypeError, OSError) as exc:
        parser.exit(2, str(exc) + '\n')


if __name__ == '__main__':
    main()
