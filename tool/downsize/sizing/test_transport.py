import contextlib
import http.server
import json
import threading
import unittest
import urllib.error
import urllib.parse

import collect


@contextlib.contextmanager
def endpoint():
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.path, self.headers.get("Authorization")))
            if self.path.startswith("/redirect/"):
                self.send_response(302)
                self.send_header("Location", "/credential-leak")
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success"}).encode())

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01})
    worker.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), requests
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


class TransportTests(unittest.TestCase):
    def test_explicit_endpoints_query_parameters_and_authentication(self):
        with endpoint() as (url, requests):
            client = collect.MetricsHTTP(url + "/prometheus", url + "/graphite")
            self.assertEqual(client.range_query("prometheus", 'metric{label="a b"}', "test-token",
                             start=0, end=600, step=300, timeout=2, retries=0), {"status": "success"})
            path, token = requests[0]
            parsed = urllib.parse.urlsplit(path)
            self.assertEqual(parsed.path, "/prometheus/api/v1/query_range")
            self.assertEqual(urllib.parse.parse_qs(parsed.query),
                             {"query": ['metric{label="a b"}'], "start": ["0"], "end": ["600"], "step": ["300"]})
            self.assertEqual(token, "Bearer test-token")
            client.get_json("/render?target=example", "", timeout=2, retries=0)
            self.assertEqual(requests[1], ("/graphite/render?target=example", None))

    def test_redirect_does_not_forward_credentials(self):
        with endpoint() as (url, requests):
            client = collect.MetricsHTTP(url + "/redirect", None)
            with self.assertRaises(urllib.error.HTTPError):
                client.range_query("prometheus", "metric", "test-token",
                                   start=0, end=600, step=300, timeout=2, retries=0)
            self.assertEqual(len(requests), 1)

    def test_missing_or_credentialed_urls_are_rejected(self):
        for url in ["file:///tmp/metrics", "https://user:password@example.net", "https://example.net?token=secret"]:
            with self.assertRaises(ValueError):
                collect.MetricsHTTP(url, None)
        with self.assertRaises(ValueError):
            collect.MetricsHTTP(None, None).get_json("/render", "", timeout=2, retries=0)
