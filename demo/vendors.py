"""Fake third-party vendors on localhost: the 'internet' for the demo.

    python -m demo vendors --chaos flaky

One stdlib HTTP server plays five companies:

    POST /fraud/check            risk scoring       slow, sometimes 503 or hangs
    POST /payments/charges       payment provider   moves money, honours Idempotency-Key
    POST /warehouse/reservations warehouse          503s under load
    POST /carrier/labels         shipping carrier   rate limited (429 + Retry-After), big payload
    POST /email/messages         email provider     occasionally 503

    GET  /ledger                 what every vendor has done, per order
    POST /reset                  forget everything

    GET  /admin/control          is the network paused, are LLM calls refused
    POST /admin/control          {"paused": bool, "llm_stopped": bool}

Pause holds pipeline calls (they wait, then finish when you unpause). Stopping
LLM calls refuses only /llm/..., so intel and the dashboard stay up. Admin and
GET inspection routes keep working either way. Drive it from another terminal
with `python -m demo control`.

The vendors keep their own state. Killing the pipeline does not undo a charge,
which is the whole point: the money moved even if your process forgot it did.

Failures are seeded on (endpoint, order, attempt number), so a given sequence
of demo commands misbehaves the same way every rehearsal.
"""

from __future__ import annotations

import base64
import hashlib
import json
import random
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import board, ui
from .secfakes import SecurityFakes

DEFAULT_PORT = 8765

# Probability of each failure mode per endpoint.
#   503        refused, nothing happened
#   429        rate limited, nothing happened, Retry-After given
#   hang       never answers within the client's timeout, nothing happened
#   502_after  the work WAS done, but the response was lost on the way back
PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "calm": {},
    "flaky": {
        "fraud": {"503": 0.10},
        "warehouse": {"503": 0.10},
        "carrier": {"429": 0.20},
        "email": {"503": 0.05},
        "llm": {"503": 0.04, "429": 0.04, "truncate": 0.05},
        "intel": {"503": 0.08},
    },
    "storm": {
        "fraud": {"503": 0.15, "hang": 0.08},
        "payments": {"503": 0.08, "502_after": 0.10},
        "warehouse": {"503": 0.20},
        "carrier": {"429": 0.35},
        "email": {"503": 0.15},
        "llm": {"503": 0.12, "429": 0.12, "truncate": 0.10, "hang": 0.05},
        "intel": {"503": 0.20},
    },
}

ROUTES = {
    "/fraud/check": "fraud",
    "/payments/charges": "payments",
    "/warehouse/reservations": "warehouse",
    "/carrier/labels": "carrier",
    "/email/messages": "email",
}

LABEL_BYTES = 48_000  # a shipping label PDF is not small


@dataclass
class OrderLedger:
    charges: list[float] = field(default_factory=list)
    reservations: int = 0
    labels: int = 0
    emails: int = 0
    replays: int = 0  # requests answered from the idempotency cache


class Vendors:
    def __init__(self, chaos: str, seed: int, latency: tuple[float, float], hang: float):
        self.chaos = PROFILES[chaos]
        self.chaos_name = chaos
        self.seed = seed
        self.latency = latency
        self.hang = hang
        self.lock = threading.Lock()
        self._open = threading.Event()
        self._open.set()
        self.security = SecurityFakes(self.chaos, self.rng, latency, hang)
        self.reset()

    @property
    def paused(self) -> bool:
        return not self._open.is_set()

    @property
    def llm_stopped(self) -> bool:
        with self.security.lock:
            return self.security.stopped

    def set_paused(self, paused: bool) -> None:
        if paused:
            self._open.clear()
            ui.vendor_log("net", "-", "fail", "paused: pipeline calls wait until unpause")
        else:
            self._open.set()
            ui.vendor_log("net", "-", "info", "unpaused: held calls continue")

    def set_llm_stopped(self, stopped: bool) -> None:
        with self.security.lock:
            self.security.stopped = stopped
        ui.vendor_log("llm", "-", "fail" if stopped else "info",
                      "outgoing calls stopped" if stopped else "outgoing calls allowed")

    def wait_until_open(self) -> None:
        """Block a pipeline call while the network is paused. Admin routes must not call this."""
        if self._open.is_set():
            return
        ui.vendor_log("net", "-", "fail", "holding a call until the network is unpaused")
        self._open.wait()

    def control_state(self) -> dict:
        return {"paused": self.paused, "llm_stopped": self.llm_stopped, "chaos": self.chaos_name}

    def reset(self) -> None:
        with self.lock:
            self.ledger: dict[str, OrderLedger] = defaultdict(OrderLedger)
            self.idempotency: dict[str, dict] = {}
            self.attempts: dict[tuple[str, str], int] = defaultdict(int)
            self.injected = 0
        self.security.reset()

    def rng(self, vendor: str, order: str) -> random.Random:
        with self.lock:
            self.attempts[vendor, order] += 1
            n = self.attempts[vendor, order]
        return random.Random(f"{self.seed}:{vendor}:{order}:{n}")

    def handle(self, vendor: str, body: dict, key: str | None) -> tuple[int, dict, dict]:
        order = body.get("order_id", "?")
        rng = self.rng(vendor, order)
        time.sleep(rng.uniform(*self.latency))

        roll = rng.random()
        for mode, p in self.chaos.get(vendor, {}).items():
            if roll < p:
                break
            roll -= p
        else:
            mode = None

        if mode == "503":
            return self._injected(vendor, order, 503, "service unavailable")
        if mode == "429":
            retry_after = round(rng.uniform(0.4, 1.0), 1)
            self._injected(vendor, order, 429, f"rate limited, retry after {retry_after}s")
            return 429, {"error": "rate limited"}, {"Retry-After": str(retry_after)}
        if mode == "hang":
            time.sleep(self.hang)
            return self._injected(vendor, order, 504, "hung, nobody waited")

        if key is not None:
            with self.lock:
                cached = self.idempotency.get(key)
                if cached is not None:
                    self.ledger[order].replays += 1
            if cached is not None:
                ui.vendor_log(vendor, order, "replay", f"idempotency key seen, not repeating ({key})")
                return 200, cached, {"Idempotent-Replayed": "true"}

        response = self._do(vendor, order, body)
        if key is not None:
            with self.lock:
                self.idempotency[key] = response

        if mode == "502_after":
            return self._injected(vendor, order, 502, "did the work, then dropped the response")
        return 200, response, {}

    def _injected(self, vendor, order, status, reason):
        with self.lock:
            self.injected += 1
        ui.vendor_log(vendor, order, "fail", f"{status} {reason}")
        return status, {"error": reason}, {}

    def _do(self, vendor: str, order: str, body: dict) -> dict:
        with self.lock:
            entry = self.ledger[order]
            if vendor == "fraud":
                score = int(hashlib.sha1(order.encode()).hexdigest()[:4], 16) % 30 / 100
                result = {"order_id": order, "score": score, "decision": "approve"}
                ui.vendor_log(vendor, order, "ok", f"approved (score {result['score']})")
            elif vendor == "payments":
                entry.charges.append(body["amount"])
                n = len(entry.charges)
                charge_id = "ch_" + hashlib.sha1(f"{order}:{n}".encode()).hexdigest()[:10]
                result = {"charge_id": charge_id, "amount": body["amount"]}
                if n == 1:
                    ui.vendor_log(vendor, order, "ok", f"charged €{body['amount']:.2f}  {charge_id}")
                else:
                    ui.vendor_log(vendor, order, "dup",
                                  f"charged AGAIN: {n}x, €{sum(entry.charges):.2f} total")
            elif vendor == "warehouse":
                entry.reservations += 1
                result = {"reservation_id": f"rsv_{order}_{entry.reservations}"}
                kind = "ok" if entry.reservations == 1 else "dup"
                ui.vendor_log(vendor, order, kind,
                              f"reserved {body['qty']}x {body['sku']}" + ("" if kind == "ok" else f" ({entry.reservations}x)"))
            elif vendor == "carrier":
                entry.labels += 1
                tracking = "TRK" + hashlib.sha1(f"{order}:{entry.labels}".encode()).hexdigest()[:9].upper()
                pdf = b"%PDF-1.4\n% shipping label " + tracking.encode() + b"\n" + b"0" * LABEL_BYTES
                result = {"tracking": tracking, "label_pdf": base64.b64encode(pdf).decode()}
                kind = "ok" if entry.labels == 1 else "dup"
                ui.vendor_log(vendor, order, kind,
                              f"label {tracking}" + ("" if kind == "ok" else f" (paid for {entry.labels} labels)"))
            elif vendor == "email":
                entry.emails += 1
                result = {"message_id": f"msg_{order}_{entry.emails}"}
                kind = "ok" if entry.emails == 1 else "dup"
                ui.vendor_log(vendor, order, kind,
                              f"sent '{body['template']}' to {body['to']}" + ("" if kind == "ok" else f" ({entry.emails}x)"))
            else:
                raise KeyError(vendor)
        return result

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "chaos": self.chaos_name,
                "injected_failures": self.injected,
                "orders": {
                    order: {
                        "charges": len(e.charges),
                        "charged_total": round(sum(e.charges), 2),
                        "reservations": e.reservations,
                        "labels": e.labels,
                        "emails": e.emails,
                        "replays": e.replays,
                    }
                    for order, e in sorted(self.ledger.items())
                },
            }


class Handler(BaseHTTPRequestHandler):
    server: VendorServer
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path in ("/", "/board"):
            return self._send_html(board.PAGE)
        if self.path == "/admin/board":
            return self._send(200, board.payload(self.server.vendors), {"Cache-Control": "no-store"})
        if self.path == "/admin/control":
            return self._send(200, self.server.vendors.control_state())
        if self.path == "/health":
            self._send(200, {"ok": True, **self.server.vendors.control_state()})
        elif self.path == "/ledger":
            self._send(200, self.server.vendors.snapshot())
        elif self.server.vendors.security.owns(self.path):
            self._send(*self.server.vendors.security.handle("GET", self.path, {}))
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return self._send(400, {"error": "invalid json"})

        if self.path == "/admin/control":
            return self._apply_control(body)

        if self.path == "/reset":
            self.server.vendors.reset()
            ui.vendor_log("vendors", "-", "info", "ledger reset")
            return self._send(200, {"ok": True})

        if not self.path.startswith("/admin"):
            self.server.vendors.wait_until_open()

        if self.server.vendors.security.owns(self.path):
            return self._send(*self.server.vendors.security.handle("POST", self.path, body))

        vendor = ROUTES.get(self.path)
        if vendor is None:
            return self._send(404, {"error": "not found"})
        try:
            status, payload, headers = self.server.vendors.handle(
                vendor, body, self.headers.get("Idempotency-Key"))
        except (KeyError, TypeError) as exc:
            return self._send(400, {"error": f"bad request: {exc}"})
        self._send(status, payload, headers)

    def _apply_control(self, body: dict):
        vendors = self.server.vendors
        if "paused" not in body and "llm_stopped" not in body:
            return self._send(400, {"error": "say paused and/or llm_stopped"})
        if "paused" in body:
            vendors.set_paused(bool(body["paused"]))
        if "llm_stopped" in body:
            vendors.set_llm_stopped(bool(body["llm_stopped"]))
        self._send(200, vendors.control_state())

    def _send_html(self, html: str):
        data = html.encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _send(self, status: int, payload: dict, headers: dict | None = None):
        data = json.dumps(payload).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the client gave up (timeout) or was killed mid-request

    def log_message(self, format, *args):
        pass  # we print our own, nicer log lines


class VendorServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 256  # many concurrent workers connect at once

    def __init__(self, address, vendors: Vendors):
        super().__init__(address, Handler)
        self.vendors = vendors


def serve(port: int, chaos: str, seed: int, latency: tuple[float, float], hang: float) -> None:
    vendors = Vendors(chaos, seed, latency, hang)
    server = VendorServer(("127.0.0.1", port), vendors)
    ui.vendor_banner(port, chaos, seed, latency)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(file=sys.stderr)
    finally:
        server.server_close()
