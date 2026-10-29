"""Shared snapshot units, validation and atomic local output."""
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path
from windows import excluded

FIELDS = (
    "cpu_cores", "working_set_bytes", "store_bytes", "write_mbps",
    "device_util_percent", "iops", "input_points_per_second", "metric_count",
    "read_requests_per_second", "read_p99_seconds",
)
GUARDS = ("write_errors_per_second", "stall_seconds_per_second",
          "unlogged_points_per_second", "input_log_errors_per_second")
OPTIONAL_FIELDS = ("device_await_ms",)
MEMORY_FIELDS = ("anon_bytes", "active_file_bytes", "slab_bytes", "heap_live_bytes",
                 "block_cache_bytes", "metadata_cache_bytes", "memtable_bytes", "process_rss_bytes",
                 "go_heap_objects_bytes", "go_heap_free_bytes", "go_heap_unused_bytes", "go_heap_stacks_bytes",
                 "application_heap_live_bytes", "non_heap_anon_bytes", "block_cache_hit_ratio", "metadata_cache_hit_ratio")
GO_CARBON_FIELDS = ("input_points_per_second", "read_requests_per_second",
                    "read_p99_seconds", "store_bytes")


def has_go_carbon(host):
    return any(host["metrics"].get(f, {}).get("peak") is not None for f in GO_CARBON_FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as out:
            json.dump(value, out, indent=2, sort_keys=True, allow_nan=False)
            out.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def positive(value, name, zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name}: expected a finite number")
    if value < 0 or (not zero and value == 0):
        raise ValueError(f"{name}: must be {'nonnegative' if zero else 'positive'}")
    return value


def summarize(series, start, end, step, exclusions=()):
    """Nearest-rank p95; coverage uses time span, including gaps at either edge."""
    points = sorted((t, v) for t, v in series.items()
                    if start <= t <= end and not excluded(t, exclusions)
                    and v is not None and math.isfinite(v) and v >= 0)
    eligible = [t for t in range(int(start), int(end) + 1, int(step)) if not excluded(t, exclusions)]
    expected = len(eligible)
    if not points or not eligible:
        return {"peak": None, "p95": None, "last": None, "samples": 0,
                "coverage": 0, "expected_samples": expected}
    # Graphite archives may be coarser than the requested resolution. Never
    # count replicated or coarser archive points as a full-resolution window.
    buckets = {int((t - start) // step) for t, _ in points}
    values = sorted(v for _, v in points)
    result = {"peak": values[-1], "p95": values[math.ceil(len(values) * .95) - 1],
              "p99": values[math.ceil(len(values) * .99) - 1],
              "first": points[0][1], "last": points[-1][1], "samples": len(points),
              "first_at": points[0][0], "last_at": points[-1][0],
              "eligible_start": eligible[0], "eligible_end": eligible[-1],
              "coverage": min(1, len(buckets) / expected), "expected_samples": expected}
    # Least-squares net growth includes retention/expiry and compactions. Keep
    # negative growth in the evidence; the calculator never credits shrinkage.
    if len(points) > 1:
        x = [(t - points[0][0]) / 86400 for t, _ in points]
        mean_x, mean_y = sum(x) / len(x), sum(v for _, v in points) / len(points)
        denominator = sum((t - mean_x) ** 2 for t in x)
        result["growth_per_day"] = sum((t - mean_x) * (v - mean_y)
                                       for t, (_, v) in zip(x, points)) / denominator
    return result


def combine(series, operation=sum):
    """Combine only simultaneous valid observations; a missing part stays missing."""
    if not series or any(not part for part in series):
        return {}
    common = set(series[0]).intersection(*series[1:])
    return {t: operation(part[t] for part in series)
            for t in common if all(part[t] is not None for part in series)}
