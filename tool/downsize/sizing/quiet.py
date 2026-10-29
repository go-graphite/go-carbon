#!/usr/bin/env python3
"""Find the longest fully observed windows without restarts/conversion/prunes."""
import argparse
import json
import sys
from collections import defaultdict
from common import has_go_carbon, read_json, write_json
from windows import complement, describe, parse_exclusions, timestamp


def find_windows(snapshot, start=None, end=None, exclusions=(), minimum=900, limit=10):
    start = snapshot["window"]["start"] if start is None else max(start, snapshot["window"]["start"])
    end = snapshot["window"]["end"] if end is None else min(end, snapshot["window"]["end"])
    if end <= start or minimum <= 0 or limit <= 0:
        raise ValueError("quiet range/minimum/limit must be positive")
    groups, all_masks = defaultdict(list), list(exclusions)
    for host in snapshot["hosts"]:
        if has_go_carbon(host):
            groups[(host["cluster"], host["site"])].append(host)
    result = {"range": describe([(start, end)])[0], "prune_or_operator_exclusions": describe(exclusions), "groups": []}
    for (cluster, site), hosts in sorted(groups.items()):
        masks, unknown_hosts, reasons = list(exclusions), [], defaultdict(int)
        for host in hosts:
            markers = host.get("activity")
            if not markers:
                masks.append((start, end))
                unknown_hosts.append(host["host"])
                continue
            covered = markers.get("window", {})
            if not covered:
                masks.append((start, end))
                unknown_hosts.append(host["host"])
                continue
            masks.extend([(start, min(end, covered["start"])), (max(start, covered["end"]), end)])
            for interval in markers["unknown"]:
                masks.append((interval["start"], interval["end"]))
                if interval["start"] < end and interval["end"] > start and host["host"] not in unknown_hosts:
                    unknown_hosts.append(host["host"])
            for event in markers["unsafe"]:
                masks.append((event["start"], event["end"]))
                reasons[event["reason"]] += 1
        all_masks.extend(masks)
        windows = sorted(complement(start, end, masks), key=lambda value: (value[1]-value[0], value[0]), reverse=True)
        result["groups"].append({"cluster": cluster, "site": site, "unknown_hosts": unknown_hosts,
                                  "activity_intervals": dict(reasons), "windows": describe_by_length(windows, minimum, limit)})
    result["fleet_windows"] = describe_by_length(sorted(complement(start, end, all_masks),
        key=lambda value: (value[1]-value[0], value[0]), reverse=True), minimum, limit) if groups else []
    return result


def describe_by_length(windows, minimum, limit):
    return [describe([window])[0] for window in windows if window[1]-window[0] >= minimum][:limit]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    parser.add_argument("--from", dest="start", type=timestamp)
    parser.add_argument("--until", dest="end", type=timestamp)
    parser.add_argument("--exclude", action="append", default=[], metavar="START-END", help="known prune or other one-off windows")
    parser.add_argument("--min-seconds", type=int, default=900)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--output", default="quiet-windows.json")
    args = parser.parse_args(argv)
    try:
        result = find_windows(read_json(args.snapshot), args.start, args.end, parse_exclusions(args.exclude), args.min_seconds, args.limit)
        write_json(args.output, result)
        print(json.dumps(result, indent=2))
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(f"quiet: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
