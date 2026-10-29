"""Shared HTTP, metric and output helpers for the operator commands."""
import argparse
import json
import math
import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HTTP:
    def __init__(self, base, timeout=10, token=None, tenant=None, opener=None):
        url = urllib.parse.urlsplit(base)
        if url.scheme not in ('http', 'https') or not url.netloc or url.username or url.query or url.fragment:
            raise ValueError('endpoint must be an http(s) URL without credentials, query or fragment')
        self.base = base.rstrip('/')
        self.timeout = timeout
        self.headers = {}
        if token:
            self.headers['Authorization'] = 'Bearer ' + token
        if tenant:
            self.headers['X-Scope-OrgID'] = tenant
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open

    def get(self, path, params=None):
        url = self.base + path
        if params:
            url += '?' + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers=self.headers)
        try:
            with self.opener(req, timeout=self.timeout) as response:
                data = response.read(32 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            raise ValueError('HTTP status %d' % exc.code) from None
        if len(data) > 32 * 1024 * 1024:
            raise ValueError('response exceeds 32 MiB; narrow the selection or time range')
        return data.decode('utf-8')

    def json(self, path, params=None):
        return json.loads(self.get(path, params))


def http_flags(parser):
    parser.add_argument('--timeout', type=positive, default=10, help='HTTP timeout in seconds')
    parser.add_argument('--token-env', default='GO_CARBON_OPS_TOKEN', help='environment variable containing a bearer token')
    parser.add_argument('--tenant', default=os.getenv('PROMETHEUS_TENANT_ID'), help='optional X-Scope-OrgID')


def client(args, base):
    return HTTP(base, args.timeout, os.getenv(args.token_env), args.tenant)


def positive(text):
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError('must be finite and greater than zero')
    return value


def positive_int(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError('must be greater than zero')
    return value


def lines(path):
    return [line.strip() for line in Path(path).read_text().splitlines()
            if line.strip() and not line.lstrip().startswith('#')]


def atomic_write(path, data, mode=0o600):
    path = Path(path)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.' + path.name + '-')
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def output(value):
    print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))


SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+([^\s]+)(?:\s+.*)?$')
LABEL = re.compile(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\[\\"n])*)"\s*(?:,|$)')


def samples(body, names):
    """Keep each series separate so one counter reset cannot hide another's rise."""
    result = {}
    for line in body.splitlines():
        if not line or line.startswith('#'):
            continue
        match = SAMPLE.fullmatch(line)
        if not match or match[1] not in names:
            continue
        labels, position = {}, 0
        raw = (match[2] or '{}')[1:-1]
        while position < len(raw):
            label = LABEL.match(raw, position)
            if not label or label[1] in labels:
                raise ValueError('invalid labels for ' + match[1])
            labels[label[1]] = json.loads('"' + label[2] + '"')
            position = label.end()
        value = float(match[3])
        if not math.isfinite(value) or value < 0:
            raise ValueError('invalid counter ' + match[1])
        key = (match[1], tuple(sorted(labels.items())))
        if key in result:
            raise ValueError('duplicate series for ' + match[1])
        result[key] = value
    missing = set(names) - {key[0] for key in result}
    if missing:
        raise ValueError('missing counters: ' + ', '.join(sorted(missing)))
    return result


def counter_increase(previous, current):
    if previous.keys() != current.keys():
        raise ValueError('counter series changed; cannot compare safely')
    return sum(value - previous[key] if value >= previous[key] else value
               for key, value in current.items())
