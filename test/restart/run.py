#!/usr/bin/env python3
"""Load one real go-carbon process, restart it, and check retained sample values.

Run variants sequentially with identical resource limits. The TCP protocol has
no application acknowledgement: stop the writer and observe receiver counters
before restarting, rather than treating sendall as proof of acceptance.
"""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--metrics", type=int, default=10000)
    ap.add_argument("--seconds", type=int, default=45)
    ap.add_argument("--readers", type=int, default=4)
    ap.add_argument("--min-points", type=int, default=0)
    ap.add_argument("--max-delay", default="0s")
    ap.add_argument("--max-updates", type=int, default=0)
    ap.add_argument("--drain-before-restart", action="store_true")
    ap.add_argument("--require-snapshot", action="store_true")
    ap.add_argument("--names-file", type=Path)
    ap.add_argument("--gogc", type=int, default=100)
    ap.add_argument("--label", required=True)
    args = ap.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    config = out / "config"
    config.mkdir()
    (config / "storage-schemas.conf").write_text("[all]\npattern = .*\nretentions = 1s:2h,60s:1d\n")
    (config / "storage-aggregation.conf").write_text("[all]\npattern = .*\nxFilesFactor = 0\naggregationMethod = average\n")
    batch = ""
    if args.min_points or args.max_delay != "0s":
        batch = f'writeout-min-points = {args.min_points}\nwriteout-max-delay = "{args.max_delay}"\n'
    (config / "go-carbon.conf").write_text(('''[common]
max-cpu = 2
user = "root"
graph-prefix = "lab.{host}"
metric-endpoint = "udp://observer:2003"
metric-interval = "1s"
[whisper]
enabled = true
data-dir = "/data/whisper"
schemas-file = "/config/storage-schemas.conf"
aggregation-file = "/config/storage-aggregation.conf"
workers = 8
max-updates-per-second = MAX_UPDATES
sparse-create = true
flock = true
compressed = true
out-of-order = true
out-of-order-compact-rate = 5
out-of-order-compact-threshold = 65536
[cache]
max-size = 4000000
write-strategy = "noop"
bloom-size = 10000000
''' + batch + '''[tcp]
enabled = true
listen = ":2003"
[udp]
enabled = false
[pickle]
enabled = false
[carbonlink]
enabled = false
[carbonserver]
enabled = true
listen = ":8080"
trie-index = true
trigram-index = false
concurrent-index = true
realtime-index = 100000
scan-frequency = "10s"
file-list-cache = "/data/file-list-cache.bin"
file-list-cache-version = 2
no-service-when-index-is-not-ready = true
query-cache-enabled = false
find-cache-enabled = false
glob-cache-enabled = false
metrics-as-counters = true
do-not-log-404s = true
[dump]
enabled = true
path = "/data/dump"
restore-per-second = 0
[pprof]
enabled = true
listen = ":7007"
[prometheus]
enabled = true
[[logging]]
logger = ""
file = "stdout"
level = "info"
encoding = "json"
encoding-time = "iso8601"
encoding-duration = "seconds"
[[logging]]
logger = "access"
file = "none"
''').replace("MAX_UPDATES", str(args.max_updates)))
    env = dict(os.environ, CARBON_BINARY=str(args.binary.resolve()), LAB_CONFIG=str(config),
               LAB_GOGC=str(args.gogc))
    base = ["docker", "compose", "-p", "carbon-restart-lab", "-f", str(HERE / "compose.yaml")]

    def compose(*cmd, **kwargs):
        return subprocess.run(base + list(cmd), env=env, check=True, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)

    if compose("ps", "--all", "--quiet").stdout.strip():
        raise RuntimeError("carbon-restart-lab already has containers; finish or clean up that run first")

    def fetch(path, port=18080, timeout=3):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}" + path, timeout=timeout) as r:
            return json.load(r)

    def resources():
        script = '''import json, os
from pathlib import Path
s = Path('/proc/1/stat').read_text().split()
status = dict(line.split(':', 1) for line in Path('/proc/1/status').read_text().splitlines())
print(json.dumps({'cpu_seconds': (int(s[13])+int(s[14]))/os.sysconf('SC_CLK_TCK'),
 'rss_peak_bytes': int(status['VmHWM'].split()[0])*1024,
 'cgroup_peak_bytes': int(Path('/sys/fs/cgroup/memory.peak').read_text())}))'''
        return json.loads(compose("exec", "-T", "store", "python3", "-c", script).stdout)

    def ready():
        try:
            fetch("/metrics/find/?query=__restart_lab_absent__&format=json")
            return True
        except urllib.error.HTTPError as e:
            return e.code == 404
        except (OSError, TimeoutError):
            return False

    def wait_for(predicate, limit=120):
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.1)
        raise RuntimeError("condition did not become true before deadline")

    names = [f"load.service{i // 1000}.host{i // 10}.metric{i}.value" for i in range(args.metrics)]
    if args.names_file:
        names = args.names_file.read_text().splitlines()[:args.metrics]
        if len(names) != args.metrics or len(set(names)) != len(names):
            raise ValueError("names file must contain enough distinct names")
        if any(not name or any(c.isspace() for c in name) for name in names):
            raise ValueError("names must be nonempty and contain no whitespace")
    # Check several names per namespace, including the first and last.
    sampled = sorted(set([0, args.metrics - 1] + list(range(0, args.metrics, max(1, args.metrics // 100)))))
    epoch = int(time.time()) - args.seconds - 120
    expected = {}
    sent = 0
    reads = []
    stop = threading.Event()
    phase = ["load"]

    def reader(worker):
        n = worker
        while not stop.is_set():
            target = names[sampled[n % len(sampled)]]
            query = urllib.parse.urlencode({"target": target, "from": epoch - 1,
                                           "until": int(time.time()), "format": "json"})
            start = time.monotonic()
            try:
                fetch("/render/?" + query)
                code = 200
            except urllib.error.HTTPError as e:
                code = e.code
            except (OSError, TimeoutError):
                code = 0
            reads.append({"phase": phase[0], "code": code, "seconds": time.monotonic() - start})
            n += 1
            stop.wait(0.04)

    def samples(data):
        result = {}
        for m in data["metrics"]:
            for i, (value, absent) in enumerate(zip(m["values"], m["isAbsent"])):
                if not absent:
                    result[m["name"], m["startTime"] + i * m["stepTime"]] = value
        return result

    def check_values():
        actual = {}
        for start in range(0, len(names), 50):
            params = [("target", name) for name in names[start:start + 50]]
            params += [("from", epoch - 1), ("until", epoch + args.seconds + 3), ("format", "json")]
            query = urllib.parse.urlencode(params)
            actual.update(samples(fetch("/render/?" + query, timeout=10)))
        missing = [k for k in expected if k not in actual]
        changed = [k for k in expected if k in actual and actual[k] != expected[k]]
        return {"expected": len(expected), "missing": len(missing), "changed": len(changed),
                "examples": (missing + changed)[:5]}

    summary = {"label": args.label, "metrics": args.metrics, "seconds": args.seconds,
               "batch_min_points": args.min_points, "batch_max_delay": args.max_delay,
               "max_updates": args.max_updates,
               "gogc": args.gogc,
               "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(), "passed": False}

    def drained_after(boundary):
        s = fetch("/", port=18090)
        sizes = [v for k, v in s.items() if k.endswith((".cache.size", ".cache.notConfirmed"))]
        return len(sizes) == 2 and all(v["last"] == 0 and v["observed_at"] > boundary for v in sizes)
    try:
        compose("up", "-d", "observer")
        # The named volume is exclusively owned by this Compose project.
        compose("run", "--rm", "--no-deps", "--entrypoint", "mkdir", "store", "-p", "/data/whisper", "/data/dump")
        compose("up", "-d", "store")
        wait_for(ready)
        summary["initial_resources"] = resources()
        workers = [threading.Thread(target=reader, args=(i,), daemon=True) for i in range(args.readers)]
        for w in workers:
            w.start()
        start = time.monotonic()
        with socket.create_connection(("127.0.0.1", 12003), timeout=10) as sock:
            sock.settimeout(30)
            for tick in range(args.seconds):
                timestamp = epoch + tick
                # Leave holes for delayed arrival tests, while newer points advance the compressed blocks.
                if tick % 7 != 3:
                    payload = "".join(f"{name} {i * 1000 + tick} {timestamp}\n" for i, name in enumerate(names))
                    sock.sendall(payload.encode())
                    sent += args.metrics
                    for i in range(len(names)):
                        expected[names[i], timestamp] = float(i * 1000 + tick)
                time.sleep(max(0, start + tick + 1 - time.monotonic()))
            for tick in range(args.seconds):
                if tick % 7 == 3:
                    payload = "".join(f"{name} {i * 1000 + tick} {epoch + tick}\n" for i, name in enumerate(names))
                    sock.sendall(payload.encode())
                    sent += args.metrics
                    for i in range(len(names)):
                        expected[names[i], epoch + tick] = float(i * 1000 + tick)
        summary["sent_points"] = sent
        # TCP intake statistics establish a processed boundary before SIGUSR2.
        def received():
            s = fetch("/", port=18090)
            return sum(v["sum"] for k, v in s.items() if k.endswith(".tcp.metricsReceived")) >= sent
        wait_for(received)
        summary["before_restart_stats"] = fetch("/", port=18090)
        summary["load_resources"] = resources()
        summary["before_restart_values"] = check_values()
        if args.drain_before_restart:
            boundary = time.time()
            wait_for(lambda: drained_after(boundary))
        if args.require_snapshot:
            def complete_snapshot():
                script = "import json; from pathlib import Path; p=Path('/data/.file-list-cache.bin.snapshot.json'); print(json.dumps(json.loads(p.read_text()) if p.exists() else {}))"
                manifest = json.loads(compose("exec", "-T", "store", "python3", "-c", script).stdout)
                return manifest.get("Records", 0) >= len(set(names))
            wait_for(complete_snapshot)
        summary["pre_restart_resources"] = resources()
        summary["pre_restart_stats"] = fetch("/", port=18090)
        phase[0] = "restart"
        restart = time.monotonic()
        compose("restart", "store", timeout=150)
        wait_for(ready)
        summary["restart_to_ready_seconds"] = time.monotonic() - restart
        if args.require_snapshot:
            log = compose("logs", "--no-color", "store").stdout
            updates = [json.loads(line[line.index("{"):]) for line in log.splitlines()
                       if '"message":"file list updated"' in line]
            assert any(u.get("index_type") == "snapshot+trie" and u.get("read_from_cache") for u in updates), "restart did not use the complete snapshot"
            summary["snapshot_startup_verified"] = True
        phase[0] = "recovered"
        summary["after_restart_values"] = check_values()
        # Validate persistence again after cache drain; do not count cache-only reads as disk durability.
        boundary = time.time()
        wait_for(lambda: drained_after(boundary))
        time.sleep(2)
        summary["after_drain_values"] = check_values()
        summary["final_stats"] = fetch("/", port=18090)
        summary["recovered_resources"] = resources()
        summary["elapsed_seconds"] = time.monotonic() - start
        for key in ["before_restart_values", "after_restart_values", "after_drain_values"]:
            assert not summary[key]["missing"] and not summary[key]["changed"], (key, summary[key])
        overflow = {k: v for k, v in summary["final_stats"].items()
                    if k.endswith((".cache.overflow", ".cache.droppedRealtimeIndex")) and v["sum"]}
        assert not overflow, overflow
        summary["passed"] = True
    except Exception as exc:
        summary["passed"] = False
        summary["error"] = repr(exc)
        raise
    finally:
        stop.set()
        for w in locals().get("workers", []):
            w.join(5)
        summary["read_summary"] = {}
        for p in {r["phase"] for r in reads}:
            group = [r for r in reads if r["phase"] == p]
            times = sorted(r["seconds"] for r in group)
            summary["read_summary"][p] = {"codes": dict(collections.Counter(r["code"] for r in group)),
                                        "p99_seconds": times[min(len(times)-1, int(len(times)*0.99))]}
        summary["reads"] = reads
        (out / "result.json").write_text(json.dumps(summary, indent=2))
        try:
            (out / "store.log").write_text(compose("logs", "--no-color", "store").stdout)
        finally:
            compose("down", "--volumes", "--remove-orphans")
        print(json.dumps({k: v for k, v in summary.items() if k != "reads" and not k.endswith("_stats")}), flush=True)


if __name__ == "__main__":
    main()
