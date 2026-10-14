import json
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

try:
    from talk import ollama_cooldown_proxy as proxy
except ImportError as error:
    raise unittest.SkipTest(f"needs the demo dependencies: {error}")


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


class FakeUpstreamHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.dumps({"message": {"content": "answer"}}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *arguments):
        return


class CooldownGateTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.gate = proxy.CooldownGate(requests_per_cooldown=3, cooldown_seconds=120, clock=self.clock)

    def test_the_third_request_is_served_and_the_fourth_is_told_to_wait(self):
        self.assertEqual([self.gate.admit() for _ in range(3)], [None, None, None])
        self.assertEqual(self.gate.admit(), 120)

    def test_the_wait_shrinks_as_time_passes_and_requests_resume_afterwards(self):
        for _ in range(3):
            self.gate.admit()
        self.clock.now += 50
        self.assertEqual(self.gate.admit(), 70)
        self.clock.now += 70
        self.assertIsNone(self.gate.admit())

    def test_rejected_requests_do_not_count_towards_the_next_cooldown(self):
        for _ in range(3):
            self.gate.admit()
        for _ in range(10):
            self.gate.admit()
        self.clock.now += 121
        self.assertEqual([self.gate.admit() for _ in range(3)], [None, None, None])
        self.assertGreater(self.gate.admit(), 0)


class CooldownProxyTest(unittest.TestCase):
    def setUp(self):
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstreamHandler)
        proxy.CooldownProxyHandler.gate = proxy.CooldownGate(requests_per_cooldown=2, cooldown_seconds=0.4)
        self.proxy = ThreadingHTTPServer(("127.0.0.1", 0), proxy.CooldownProxyHandler)
        self.upstream_patch = mock.patch.object(proxy, "UPSTREAM_URL", f"http://127.0.0.1:{self.upstream.server_port}")
        self.upstream_patch.start()
        for server in (self.upstream, self.proxy):
            threading.Thread(target=server.serve_forever, daemon=True).start()

    def tearDown(self):
        for server in (self.upstream, self.proxy):
            server.shutdown()
            server.server_close()
        self.upstream_patch.stop()

    def post(self):
        request = urllib.request.Request(f"http://127.0.0.1:{self.proxy.server_port}/api/chat", data=b"{}", method="POST")
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status

    def test_requests_are_forwarded_then_refused_with_retry_after_then_forwarded_again(self):
        self.assertEqual([self.post(), self.post()], [200, 200])
        with self.assertRaises(urllib.error.HTTPError) as refused:
            self.post()
        self.assertEqual(refused.exception.code, 429)
        self.assertEqual(refused.exception.headers["Retry-After"], "1")
        time.sleep(0.5)
        self.assertEqual(self.post(), 200)

    def test_every_429_is_logged_so_the_proxy_terminal_shows_it(self):
        messages = []
        sink = proxy.logger.add(lambda message: messages.append(message.record["message"]), level="WARNING")
        try:
            self.post()
            self.post()
            with self.assertRaises(urllib.error.HTTPError):
                self.post()
        finally:
            proxy.logger.remove(sink)
        self.assertIn("429 sent, Retry-After 1s", messages)


if __name__ == "__main__":
    unittest.main()
