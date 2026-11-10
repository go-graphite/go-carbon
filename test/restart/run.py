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


def run_sigkill(args, compose, fetch, wait_for, ready, resources, names, config, out):
    """Check unique deterministic samples, including the unacknowledged kill window.

    A completed TCP send is not a durable acknowledgement. Missing samples are
    reported as loss, never silently excluded from the oracle. Retrying a failed
    batch is idempotent: it uses the same names, timestamps and values.
    """
    rounds = max(2, args.points_per_second * args.seconds * 2 // len(names))
    if rounds > 7000 or args.points_per_second <= 0:
        raise ValueError("select a rate/duration/name count fitting the 2h retention")
    epoch = int(time.time()) - rounds - 120
    stopped = threading.Event()
    halfway = threading.Event()
    finished = threading.Event()
    state = {"rounds": 0, "send_errors": 0, "last_send": 0, "max_send_gap_seconds": 0}
    summary = {"label": args.label, "mode": "sigkill", "background_replay": args.background_replay,
               "requested_points_per_second": args.points_per_second, "writeout_delay": "30m",
               "out_of_order": False, "passed": False,
               "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()}

    def prometheus():
        with urllib.request.urlopen(f"http://127.0.0.1:{args.prom_port}/metrics", timeout=5) as response:
            return response.read().decode()

    def metric(name):
        for line in prometheus().splitlines():
            if line.startswith(name + " ") or line.startswith(name + "{"):
                return float(line.rsplit(" ", 1)[1])
        return None

    def writer():
        sock = None
        begun = time.monotonic()
        try:
            for tick in range(rounds):
                payload = "".join(f"{name} {i * 1000000 + tick}.25 {epoch + tick}\n"
                                  for i, name in enumerate(names)).encode()
                while not stopped.is_set():
                    try:
                        if sock is None:
                            sock = socket.create_connection(("127.0.0.1", args.tcp_port), timeout=1)
                            sock.settimeout(1)
                        sock.sendall(payload)
                        now = time.monotonic()
                        if state["last_send"]:
                            state["max_send_gap_seconds"] = max(state["max_send_gap_seconds"], now-state["last_send"])
                        state["last_send"] = now
                        state["rounds"] = tick + 1
                        state["elapsed_seconds"] = now - begun
                        state["achieved_points_per_second"] = (tick + 1)*len(names)/(now - begun)
                        if tick >= rounds // 2:
                            halfway.set()
                        break
                    except OSError:
                        state["send_errors"] += 1
                        if sock:
                            sock.close()
                        sock = None
                        stopped.wait(0.01)
                if stopped.is_set():
                    return
                stopped.wait(max(0, begun + (tick+1)*len(names)/args.points_per_second - time.monotonic()))
        except Exception as exc:
            state["error"] = repr(exc)
        finally:
            if sock:
                sock.close()
            finished.set()

    thread = None
    try:
        compose("up", "-d", "observer")
        compose("run", "--rm", "--no-deps", "--entrypoint", "mkdir", "store", "-p", "/data/whisper", "/data/dump")
        compose("up", "-d", "store")
        wait_for(ready)
        summary["initial_resources"] = resources()
        thread = threading.Thread(target=writer, daemon=True)
        thread.start()
        wait_for(halfway.is_set, limit=180)
        summary["rounds_before_kill"] = state["rounds"]
        summary["before_kill_resources"] = resources()
        killed = time.monotonic()
        compose("kill", "-s", "SIGKILL", "store")
        compose("up", "-d", "--no-deps", "store")
        wait_for(ready, limit=180)
        summary["kill_to_reads_ready_seconds"] = time.monotonic() - killed
        wait_for(finished.is_set, limit=180)
        summary["sender"] = dict(state)
        assert state["rounds"] == rounds and "error" not in state, state
        summary["points_sent"] = state["rounds"] * len(names)
        wait_for(lambda: metric("input_log_background_replay_active") in (None, 0), limit=180)
        summary["restart_input_unavailable_seconds"] = metric("restart_input_unavailable_seconds")
        summary["input_log_replay_seconds"] = metric("input_log_replay_seconds")
        summary["input_log_replayed_points_total"] = metric("input_log_replayed_points_total")
        if not summary["input_log_replayed_points_total"]:
            raise AssertionError("SIGKILL did not exercise a nonempty input-log replay")
        # Flush the 30-minute live cache after measuring crash recovery.
        path = config / "go-carbon.conf"
        path.write_text(path.read_text().replace("writeout-min-points = 1000000", "writeout-min-points = 0")
                        .replace('writeout-max-delay = "30m"', 'writeout-max-delay = "0s"'))
        boundary = time.time()
        compose("kill", "-s", "SIGHUP", "store")

        def drained():
            sizes = [v for k, v in fetch("/", port=args.stats_port).items()
                     if k.endswith((".cache.size", ".cache.notConfirmed"))]
            return len(sizes) == 2 and all(v["last"] == 0 and v["observed_at"] > boundary for v in sizes)
        wait_for(drained, limit=180)
        lost, changed, examples = 0, 0, []
        for start in range(0, len(names), 25):
            selected = names[start:start+25]
            params = [("target", name) for name in selected]
            params += [("from", epoch-1), ("until", epoch+rounds), ("format", "json")]
            data = fetch("/render/?" + urllib.parse.urlencode(params), timeout=60)
            found = {m["name"]: m for m in data["metrics"]}
            for i, name in enumerate(selected, start):
                series = found.get(name)
                for tick in range(state["rounds"]):
                    offset = (epoch+tick-series["startTime"]) if series else -1
                    if series is None or series["stepTime"] != 1 or offset < 0 or offset >= len(series["values"]) or series["isAbsent"][offset]:
                        lost += 1
                        if len(examples) < 5:
                            examples.append([name, epoch+tick, "missing"])
                    elif series["values"][offset] != i*1000000+tick+0.25:
                        changed += 1
                        if len(examples) < 5:
                            examples.append([name, epoch+tick, "changed"])
        summary.update(lost_points=lost, changed_points=changed, examples=examples,
                       final_resources=resources(), passed=changed == 0)
        assert changed == 0, summary
    except Exception as exc:
        summary["error"] = repr(exc)
        raise
    finally:
        stopped.set()
        if thread:
            thread.join(5)
        (out / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
        try:
            (out / "store.log").write_text(compose("logs", "--no-color", "store").stdout)
        finally:
            compose("down", "--volumes", "--remove-orphans")
        print(json.dumps(summary), flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="carbon-restart-lab")
    ap.add_argument("--port-offset", type=int, default=0, help="offset all four localhost ports for an isolated run")
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
    ap.add_argument("--crash-recovery", action="store_true", help="kill and reopen during pending-point recovery")
    ap.add_argument("--sigkill", action="store_true", help="crash under continuous input and report the input gap and every lost point")
    ap.add_argument("--background-replay", action="store_true", help="enable experimental input-log background replay")
    ap.add_argument("--points-per-second", type=int, default=2000000)
    ap.add_argument("--names-file", type=Path)
    ap.add_argument("--gogc", type=int, default=100)
    ap.add_argument("--label", required=True)
    args = ap.parse_args()
    args.http_port = 18080 + args.port_offset
    args.tcp_port = 12003 + args.port_offset
    args.prom_port = 17007 + args.port_offset
    args.stats_port = 18090 + args.port_offset
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
    if args.sigkill:
        text = (config / "go-carbon.conf").read_text()
        text = text.replace("out-of-order = true", "out-of-order = false")
        text = text.replace("max-size = 4000000", f"max-size = {max(4000000, args.points_per_second * args.seconds * 3)}")
        # Keep a real 30-minute cache backlog. Both binaries receive identical
        # input-log settings; only the candidate enables background replay.
        text = text.replace(batch, 'writeout-min-points = 1000000\nwriteout-max-delay = "30m"\n', 1) if batch else text.replace('[tcp]', 'writeout-min-points = 1000000\nwriteout-max-delay = "30m"\n[tcp]', 1)
        text += '\n[input-log]\nenabled = true\npath = "/data/input-log"\nsync-interval = "1s"\nsegment-size = 16777216\ntruncate-lag = "0s"\n'
        if args.background_replay:
            text += 'background-replay = true\n'
        (config / "go-carbon.conf").write_text(text)
    env = dict(os.environ, CARBON_BINARY=str(args.binary.resolve()), LAB_CONFIG=str(config),
               LAB_GOGC=str(args.gogc), LAB_HTTP_PORT=str(args.http_port),
               LAB_TCP_PORT=str(args.tcp_port), LAB_PPROF_PORT=str(args.prom_port),
               LAB_STATS_PORT=str(args.stats_port))
    base = ["docker", "compose", "-p", args.project, "-f", str(HERE / "compose.yaml")]

    def compose(*cmd, **kwargs):
        return subprocess.run(base + list(cmd), env=env, check=True, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)

    if compose("ps", "--all", "--quiet").stdout.strip():
        raise RuntimeError(f"{args.project} already has containers; finish or clean up that run first")

    def fetch(path, port=None, timeout=3):
        port = args.http_port if port is None else port
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
        s = fetch("/", port=args.stats_port)
        sizes = [v for k, v in s.items() if k.endswith((".cache.size", ".cache.notConfirmed"))]
        return len(sizes) == 2 and all(v["last"] == 0 and v["observed_at"] > boundary for v in sizes)
    if args.sigkill:
        run_sigkill(args, compose, fetch, wait_for, ready, resources, names, config, out)
        return
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
        with socket.create_connection(("127.0.0.1", args.tcp_port), timeout=10) as sock:
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
            s = fetch("/", port=args.stats_port)
            return sum(v["sum"] for k, v in s.items() if k.endswith(".tcp.metricsReceived")) >= sent
        wait_for(received)
        summary["before_restart_stats"] = fetch("/", port=args.stats_port)
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
        summary["pre_restart_stats"] = fetch("/", port=args.stats_port)
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
        if args.crash_recovery:
            log = compose("logs", "--no-color", "store").stdout
            assert "serving reads from pending checkpoint" in log, "pending checkpoint path was not exercised"
            assert "pending checkpoint persisted, starting receivers" not in log, "recovery drained before the crash test"
            phase[0] = "crash_restart"
            crash_started = time.monotonic()
            compose("kill", "-s", "SIGKILL", "store")
            compose("up", "-d", "--no-deps", "store")
            wait_for(ready)
            summary["crash_restart_to_ready_seconds"] = time.monotonic() - crash_started
            summary["after_crash_values"] = check_values()
            assert not summary["after_crash_values"]["missing"] and not summary["after_crash_values"]["changed"], summary["after_crash_values"]
            phase[0] = "recovered"
        # Validate persistence again after cache drain; do not count cache-only reads as disk durability.
        boundary = time.time()
        wait_for(lambda: drained_after(boundary))
        time.sleep(2)
        summary["after_drain_values"] = check_values()
        summary["final_stats"] = fetch("/", port=args.stats_port)
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
