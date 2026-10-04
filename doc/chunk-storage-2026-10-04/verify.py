#!/usr/bin/env python3
"""Validate summary.csv against the exact TestStorageScale log fields."""

import csv
import re
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).parent
CONFIG = re.compile(
    r"^\s+storage_scale_test\.go:\d+: engine=(?P<engine>\S+) "
    r"metrics=(?P<metrics>\d+) workers=\d+ "
    r"(?:cache-MiB=(?P<cache_mib>\d+) memtable-MiB=\d+|"
    r"cache-memtable-MiB=(?P<legacy_cache_mib>\d+)) "
    r"rounds=(?P<rounds>\d+) late=(?P<late>true|false) "
    r"setup=(?P<setup>\S+)\s*$"
)
TIMED = re.compile(
    r"^\s+storage_scale_test\.go:\d+: engine=(?P<engine>\S+) "
    r"points=\d+ reads=\d+ wall=(?P<wall>\S+) "
    r"cpu-us/point=(?P<cpu_us_point>\S+) "
    r"allocated-B/point=(?P<allocated_b_per_point>\S+) "
    r"batch-p95-us=\S+ batch-p99-us=\S+ read-p99-us=\S+ "
    r"maxrss-KiB=(?P<rss_kib>\d+)\s*$"
)
FINAL = re.compile(
    r"^\s+storage_scale_test\.go:\d+: engine=(?P<engine>\S+) "
    r"verified-metrics=(?P<metrics>\d+) maintenance-sample=\d+ maintenance=\S+ "
    r"files=(?P<files>\d+) logical-bytes=(?P<logical_bytes>\d+) "
    r"allocated-bytes=(?P<allocated_bytes>\d+)\s*$"
)


def one_match(pattern, text, source):
    matches = [pattern.match(line) for line in text.splitlines()]
    matches = [match for match in matches if match]
    if len(matches) != 1:
        raise ValueError(f"{source}: expected one {pattern.pattern!r} match, got {len(matches)}")
    return matches[0].groupdict()


def seconds(value):
    matched = re.fullmatch(r"(?:(?P<minutes>\d+)m)?(?P<seconds>\d+(?:\.\d+)?)s", value)
    if not matched:
        raise ValueError(f"unsupported duration {value!r}")
    return Decimal(matched.group("minutes") or 0) * 60 + Decimal(matched.group("seconds"))


def equal_decimal(actual, expected, field, source):
    if Decimal(actual) != Decimal(expected):
        raise ValueError(f"{source}: {field}={actual}, want {expected}")


with (ROOT / "summary.csv").open(newline="") as summary:
    rows = list(csv.DictReader(summary))

for row in rows:
    source = ROOT / row["source_log"]
    text = source.read_text()
    config = one_match(CONFIG, text, source)
    timed = one_match(TIMED, text, source)
    final = one_match(FINAL, text, source)
    for field in ("engine", "metrics"):
        if row[field] != config[field] or row[field] != final[field]:
            raise ValueError(f"{source}: {field} does not match configuration and final line")
    for field in ("rounds", "late"):
        if row[field] != config[field]:
            raise ValueError(f"{source}: {field}={row[field]}, want {config[field]}")
    cache_mib = config["cache_mib"] or config["legacy_cache_mib"]
    if row["cache_mib"] != cache_mib:
        raise ValueError(f"{source}: cache_mib={row['cache_mib']}, want {cache_mib}")
    for field in ("engine", "rss_kib"):
        if row[field] != timed[field]:
            raise ValueError(f"{source}: {field}={row[field]}, want {timed[field]}")
    for field in ("allocated_bytes", "logical_bytes", "files"):
        if row[field] != final[field]:
            raise ValueError(f"{source}: {field}={row[field]}, want {final[field]}")
    equal_decimal(row["cpu_us_point"], timed["cpu_us_point"], "cpu_us_point", source)
    equal_decimal(row["allocated_b_per_point"], timed["allocated_b_per_point"], "allocated_b_per_point", source)
    equal_decimal(row["wall_sec"], seconds(timed["wall"]), "wall_sec", source)
    equal_decimal(row["setup_sec"], seconds(config["setup"]), "setup_sec", source)

print(f"verified {len(rows)} summary rows")
