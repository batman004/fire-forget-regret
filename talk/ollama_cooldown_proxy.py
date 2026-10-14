import json
import math
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from loguru import logger

UPSTREAM_URL = os.environ.get("OLLAMA_UPSTREAM", "http://localhost:11434")
LISTEN_PORT = int(os.environ.get("TALK_PROXY_PORT", "11435"))
REQUESTS_PER_COOLDOWN = int(os.environ.get("TALK_COOLDOWN_EVERY", "10"))
COOLDOWN_SECONDS = float(os.environ.get("TALK_COOLDOWN_SECONDS", "10"))
UPSTREAM_TIMEOUT_SECONDS = 600
PROGRESS_LOG_INTERVAL = 100


class CooldownGate:
    def __init__(self, requests_per_cooldown: int, cooldown_seconds: float, clock=time.monotonic):
        self.requests_per_cooldown = requests_per_cooldown
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock
        self.lock = threading.Lock()
        self.served = 0
        self.cooling_until = 0.0

    def admit(self) -> float | None:
        with self.lock:
            now = self.clock()
            if now < self.cooling_until:
                return self.cooling_until - now
            self.served += 1
            self.log_progress()
            if self.served % self.requests_per_cooldown == 0:
                self.cooling_until = now + self.cooldown_seconds
                logger.warning("served {} requests, cooling down for {:.0f}s", self.served, self.cooldown_seconds)
            return None

    def log_progress(self) -> None:
        if self.served % PROGRESS_LOG_INTERVAL == 0:
            logger.info("served {} requests", self.served)


class CooldownProxyHandler(BaseHTTPRequestHandler):
    gate: CooldownGate

    def do_POST(self) -> None:
        wait_seconds = self.gate.admit()
        if wait_seconds is not None:
            self.reject(wait_seconds)
            return
        self.forward()

    def reject(self, wait_seconds: float) -> None:
        self.respond(429, json.dumps({"error": "cooling down"}).encode(), {"Retry-After": str(math.ceil(wait_seconds))})

    def forward(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        request = urllib.request.Request(UPSTREAM_URL + self.path, data=body, headers={"content-type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=UPSTREAM_TIMEOUT_SECONDS) as upstream:
                self.respond(upstream.status, upstream.read())
        except urllib.error.HTTPError as error:
            self.respond(error.code, error.read())
        except OSError as error:
            self.respond(502, json.dumps({"error": str(error)}).encode())

    def respond(self, status: int, body: bytes, extra_headers: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *arguments) -> None:
        return


def main() -> None:
    CooldownProxyHandler.gate = CooldownGate(REQUESTS_PER_COOLDOWN, COOLDOWN_SECONDS)
    server = ThreadingHTTPServer(("127.0.0.1", LISTEN_PORT), CooldownProxyHandler)
    logger.info("cooldown proxy on port {} in front of {}: {}s pause after every {} requests",
                LISTEN_PORT, UPSTREAM_URL, COOLDOWN_SECONDS, REQUESTS_PER_COOLDOWN)
    server.serve_forever()


if __name__ == "__main__":
    main()
