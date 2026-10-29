"""UTC interval parsing, exclusion masks and conservative activity windows."""
import math
from datetime import datetime, timezone


def timestamp(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("ISO timestamps need a timezone (use Z for UTC)")
        result = parsed.timestamp()
    if not math.isfinite(result):
        raise ValueError("timestamp must be finite")
    return result


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def interval(value):
    for index, char in enumerate(value):
        if char != "-" or index == 0:
            continue
        try:
            start = timestamp(value[:index])
            end_text = value[index + 1:]
            if len(end_text) in (6, 9) and end_text.endswith("Z") and ":" in end_text:
                end_text = iso(start)[:10] + "T" + end_text
            end = timestamp(end_text)
        except (ValueError, TypeError):
            continue
        if end > start:
            return (start, end)
    raise ValueError(f"invalid exclusion {value!r}; use START-END with epoch seconds or timezone-qualified ISO")


def merge(intervals):
    result = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if result and start <= result[-1][1]:
            result[-1] = (result[-1][0], max(end, result[-1][1]))
        else:
            result.append((start, end))
    return result


def parse_exclusions(values):
    return merge(interval(value) for value in values or [])


def excluded(at, intervals):
    return any(start <= at < end for start, end in intervals)


def describe(intervals):
    return [{"start": start, "end": end, "start_utc": iso(start), "end_utc": iso(end),
             "seconds": end - start} for start, end in merge(intervals)]


def complement(start, end, intervals):
    result, cursor = [], start
    for low, high in merge(intervals):
        if high <= cursor or low >= end:
            continue
        if low > cursor:
            result.append((cursor, min(low, end)))
        cursor = max(cursor, min(high, end))
    if cursor < end:
        result.append((cursor, end))
    return result


EVENT_FIELDS = ("process_start_time_seconds", "migration_in_flight", "migration_converted_rate")


def activity(series, start, end, step, restart_settle=900):
    """Unknown samples are unsafe; a restart masks its uncertainty and warmup."""
    unsafe, unknown = [], []
    previous = None
    for at in range(int(start), int(end), int(step)):
        values = [series.get(field, {}).get(at) for field in EVENT_FIELDS]
        if any(value is None for value in values) or (values[0] is not None and (values[0] <= 0 or values[0] > at + step)):
            unknown.append((at, min(end, at + step)))
        process, in_flight, rate = values
        if process is not None:
            if previous is not None and previous[1] != process:
                unsafe.append({"start": previous[0], "end": min(end, at + restart_settle), "reason": "restart"})
            if start <= process < end and (previous is None or previous[1] != process):
                unsafe.append({"start": max(start, process - step), "end": min(end, process + restart_settle), "reason": "restart"})
            previous = (at, process)
        if (in_flight or 0) > 0 or (rate or 0) > 0:
            unsafe.append({"start": max(start, at - step), "end": min(end, at + step), "reason": "conversion"})
    compact = [{"start": a, "end": b, "reason": reason} for reason in ("restart", "conversion")
               for a, b in merge((event["start"], event["end"]) for event in unsafe if event["reason"] == reason)]
    return {"window": {"start": start, "end": end}, "unsafe": compact,
            "unknown": describe(unknown), "missing_fields": [field for field in EVENT_FIELDS if not series.get(field)],
            "restart_settle_seconds": restart_settle}
