"""Collect go-carbon's interval statistics without storing them in the test database."""
import collections
import http.server
import json
import socket
import threading
import time

lock = threading.Lock()
stats = collections.defaultdict(lambda: {"last": 0, "sum": 0, "max": 0, "samples": 0})


def receive():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 2003))
    while True:
        data, _ = sock.recvfrom(65535)
        with lock:
            for line in data.decode().splitlines():
                try:
                    name, value, _ = line.split()
                    value = float(value)
                except ValueError:
                    continue
                item = stats[name]
                item["last"] = value
                item["sum"] += value
                item["max"] = max(item["max"], value)
                item["samples"] += 1
                item["observed_at"] = time.time()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        with lock:
            body = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


threading.Thread(target=receive, daemon=True).start()
http.server.ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
