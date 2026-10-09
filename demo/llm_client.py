"""Client for the (fake) LLM API, mapping HTTP outcomes to the errors a Pool understands.

    429 insufficient_quota   QuotaExhausted   long cooldown: waiting won't add credits
    429 rate_limit           RateLimited      short cooldown, honours Retry-After
    5xx, timeout, refused    ProviderDown     outage: breaker opens after a few
    200 but cut off          ProviderDown     the answer is unusable, try elsewhere
    other 4xx                PermanentError   our request is wrong; no provider will accept it
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import urllib.error
import urllib.request

from stitch import PermanentError, ProviderDown, QuotaExhausted, RateLimited

from . import client

TIMEOUT = float(os.environ.get("LLM_TIMEOUT", "2.5"))


class LLMClient:
    """One provider: a region (or an in-house model) behind the same API shape."""

    def __init__(self, name: str, config: dict):
        self.name = name
        self.region = config.get("region", name)

    async def complete(self, task: str, event: dict) -> dict:
        payload = {"task": task, "event": event}
        status, body, headers = await asyncio.to_thread(self._post, payload)
        if status == 200:
            if body.get("finish_reason") != "stop":
                raise ProviderDown("returned a truncated answer")
            return {"text": body["text"], "model": body["model"],
                    "tokens": body["usage"]["total_tokens"], "provider": self.name}

        kind = (body.get("error") or {}).get("type", "")
        if status == 429 and kind == "insufficient_quota":
            raise QuotaExhausted("out of credits")
        if status == 429:
            raise RateLimited("rate limited", retry_after=float(headers.get("Retry-After", 1)))
        if status >= 500:
            raise ProviderDown(f"{status} {kind or 'server error'}")
        raise PermanentError(f"{status} {kind}")

    def _post(self, payload: dict) -> tuple[int, dict, dict]:
        request = urllib.request.Request(
            f"{client.BASE_URL}/llm/{self.region}/complete", data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
                return response.status, json.load(response), dict(response.headers)
        except urllib.error.HTTPError as exc:
            try:
                body = json.load(exc)
            except Exception:
                body = {}
            return exc.code, body, dict(exc.headers)
        except (TimeoutError, socket.timeout):
            raise ProviderDown(f"no response within {TIMEOUT:g}s")
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                raise ProviderDown(f"no response within {TIMEOUT:g}s")
            raise ProviderDown(f"connection failed ({exc.reason})")
        except ConnectionError as exc:
            raise ProviderDown(f"connection failed ({exc})")
