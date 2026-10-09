"""A small async client for the vendor APIs (urllib in a worker thread).

It turns HTTP outcomes into the two errors the engine understands:
TransientError (retry it) and PermanentError (don't).
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import urllib.error
import urllib.request

from stitch import PermanentError, TransientError, idempotency_key

BASE_URL = os.environ.get("VENDOR_URL", "http://127.0.0.1:8765")
TIMEOUT = float(os.environ.get("VENDOR_TIMEOUT", "2.0"))


class VendorsDown(Exception):
    pass


async def post(path: str, payload: dict, *, timeout: float = TIMEOUT) -> dict:
    """POST JSON to a vendor, sending the current step's idempotency key if any."""
    headers = {"Content-Type": "application/json"}
    key = idempotency_key()
    if key is not None:
        headers["Idempotency-Key"] = key
    return await asyncio.to_thread(_post, path, payload, headers, timeout)


def _post(path: str, payload: dict, headers: dict, timeout: float) -> dict:
    request = urllib.request.Request(
        BASE_URL + path, data=json.dumps(payload).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        reason = _error_text(exc)
        if exc.code == 429:
            raise TransientError(f"429 {reason}", retry_after=float(exc.headers.get("Retry-After", 1)))
        if exc.code >= 500:
            raise TransientError(f"{exc.code} {reason}")
        raise PermanentError(f"{exc.code} {reason}")
    except (TimeoutError, socket.timeout):
        raise TransientError(f"no response within {timeout:.0f}s")
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            raise TransientError(f"no response within {timeout:.0f}s")
        raise TransientError(f"connection failed: {exc.reason}")
    except ConnectionError as exc:
        raise TransientError(f"connection failed: {exc}")


def _error_text(exc: urllib.error.HTTPError) -> str:
    try:
        return json.load(exc).get("error", exc.reason)
    except Exception:
        return str(exc.reason)


def get(path: str, *, timeout: float = 1.0) -> dict:
    try:
        with urllib.request.urlopen(BASE_URL + path, timeout=timeout) as response:
            return json.load(response)
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise VendorsDown(f"vendors not reachable at {BASE_URL}") from exc


def post_sync(path: str) -> dict:
    request = urllib.request.Request(BASE_URL + path, data=b"{}", method="POST")
    try:
        with urllib.request.urlopen(request, timeout=1.0) as response:
            return json.load(response)
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise VendorsDown(f"vendors not reachable at {BASE_URL}") from exc


def post_json(path: str, payload: dict) -> dict:
    """Plain synchronous POST for the admin commands."""
    request = urllib.request.Request(
        BASE_URL + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=2.0) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{path}: {exc.code} {_error_text(exc)}")
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise VendorsDown(f"vendors not reachable at {BASE_URL}") from exc
