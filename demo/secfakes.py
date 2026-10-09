"""Fake services for the security-triage pipeline, served by the same localhost server.

    POST /llm/<region>/complete   an LLM API with regions, a token budget and outages
    POST /intel/lookup            a threat-intelligence feed
    POST /ui/events/<id>          the dashboard's API (an upsert)
    GET  /ui/events               what the dashboard is showing right now
    GET  /admin/llm               credits, calls and outages per region
    POST /admin/llm               {"region", "credits"|"add"|"outage"}

The LLM does no language modelling. It builds a plausible answer from the event
so the demo is deterministic and offline, but it fails the way real ones do:

  * 429 insufficient_quota when the account runs out of tokens (not the same
    as a rate limit, and waiting a few seconds will not fix it)
  * 429 rate_limit with Retry-After
  * 503 when the region is down
  * 200 with finish_reason "length" and a cut-off answer
"""

from __future__ import annotations

import hashlib
import json
import random
import threading
import time
from collections.abc import Callable

from . import events, ui

REGION_MODELS = {
    "eu-west-1": ("large-eu", 1.0),
    "us-east-1": ("large-us", 1.0),
    "local-small": ("small-7b", 0.5),
}
DEFAULT_CREDITS = 4000  # tokens per region
LOCAL_CREDITS = 10**9   # the in-house model has no meter


class SecurityFakes:
    def __init__(self, chaos: dict[str, dict[str, float]], rng: Callable[[str, str], random.Random],
                 latency: tuple[float, float], hang: float, credits: int = DEFAULT_CREDITS):
        self.chaos = chaos
        self.rng = rng
        self.latency = latency
        self.hang = hang
        self.initial_credits = credits
        self.lock = threading.Lock()
        self.stopped = False  # control server: refuse every region, leave the rest of the IO up
        self.reset()

    def reset(self) -> None:
        with self.lock:
            self.regions = {
                name: {"credits": LOCAL_CREDITS if name == "local-small" else self.initial_credits,
                       "outage": False, "calls": 0, "tokens": 0, "refused": 0}
                for name in REGION_MODELS
            }
            self.ui: dict[str, dict] = {}

    owns_prefixes = ("/llm/", "/intel/", "/ui/", "/admin/")

    def owns(self, path: str) -> bool:
        return path.startswith(self.owns_prefixes)

    # -- routing ----------------------------------------------------------

    def handle(self, method: str, path: str, body: dict) -> tuple[int, dict, dict]:
        if path.startswith("/llm/") and method == "POST":
            return self.llm(path.split("/")[2], body)
        if path == "/intel/lookup" and method == "POST":
            return self.intel(body)
        if path.startswith("/ui/events/") and method == "POST":
            return self.ui_put(path.rsplit("/", 1)[1], body)
        if path == "/ui/events" and method == "GET":
            with self.lock:
                return 200, {"events": list(self.ui.values())}, {}
        if path == "/admin/llm":
            return self.admin(body) if method == "POST" else self.admin_state()
        return 404, {"error": "not found"}, {}

    # -- the LLM ----------------------------------------------------------

    def llm(self, region: str, body: dict) -> tuple[int, dict, dict]:
        if region not in REGION_MODELS:
            return 404, {"error": {"type": "unknown_region", "message": region}}, {}
        event_id = body.get("event", {}).get("id", "?")
        with self.lock:
            stopped = self.stopped
        if stopped:
            ui.vendor_log("llm", event_id, "fail", f"{region} refused: outgoing calls stopped")
            return 503, {"error": {"type": "unavailable", "message": "llm calls stopped"}}, {}
        rng = self.rng(f"llm:{region}", event_id)
        time.sleep(rng.uniform(*self.latency))

        with self.lock:
            state = self.regions[region]
            outage, credits = state["outage"], state["credits"]
        if outage:
            ui.vendor_log("llm", event_id, "fail", f"{region} 503 region unavailable")
            return 503, {"error": {"type": "unavailable", "message": f"{region} is down"}}, {}
        if credits <= 0:
            with self.lock:
                state["refused"] += 1
            ui.vendor_log("llm", event_id, "fail", f"{region} 429 insufficient_quota (0 tokens left)")
            return 429, {"error": {"type": "insufficient_quota",
                                   "message": "You exceeded your current quota"}}, {}

        roll = rng.random()
        for mode, p in self.chaos.get("llm", {}).items():
            if roll < p:
                break
            roll -= p
        else:
            mode = None
        if mode == "503":
            ui.vendor_log("llm", event_id, "fail", f"{region} 503 overloaded")
            return 503, {"error": {"type": "overloaded", "message": "try again"}}, {}
        if mode == "429":
            wait = round(rng.uniform(0.5, 1.2), 1)
            ui.vendor_log("llm", event_id, "fail", f"{region} 429 rate_limit, retry after {wait}s")
            return 429, {"error": {"type": "rate_limit", "message": "slow down"}}, {"Retry-After": str(wait)}
        if mode == "hang":
            time.sleep(self.hang)
            return 504, {"error": {"type": "timeout", "message": "gave up"}}, {}

        task = body.get("task", "summarise")
        text = self._answer(region, task, body.get("event", {}))
        finish = "stop"
        if mode == "truncate":
            text, finish = text[: len(text) // 2], "length"

        model, quality = REGION_MODELS[region]
        tokens = max(60, (len(json.dumps(body.get("event", {}))) + len(text)) // 4)
        with self.lock:
            tokens = min(tokens, state["credits"]) if region != "local-small" else tokens
            state["credits"] -= tokens
            state["calls"] += 1
            state["tokens"] += tokens
            left = state["credits"]
        note = " (truncated)" if finish == "length" else ""
        ui.vendor_log("llm", event_id, "ok",
                      f"{region} {task} {tokens} tokens, {left if region != 'local-small' else '∞'} left{note}")
        return 200, {"model": model, "text": text, "finish_reason": finish,
                     "usage": {"total_tokens": tokens}}, {}

    def _answer(self, region: str, task: str, event: dict) -> str:
        if not event:
            return ""
        seed = int(hashlib.sha1(event["id"].encode()).hexdigest()[:6], 16)
        if task == "classify":
            base = events.rule_based_severity(event)
            levels = ["low", "medium", "high", "critical"]
            # the model sometimes disagrees with the SIEM's default, like real ones do
            shift = [0, 0, 0, 1, -1][seed % 5]
            level = levels[max(0, min(3, levels.index(base) + shift))]
            conf = round((0.62 + (seed % 30) / 100) * REGION_MODELS[region][1] + 0.2, 2)
            return json.dumps({"severity": level, "confidence": min(conf, 0.99)})
        facts = events.facts(event)
        if region == "local-small":
            return f"{facts}."
        title = events.KINDS[event["type"]][0]
        return (f"{title} on {event['asset']}. {facts}. "
                f"Source {event['ip']} is previously unseen on this asset. "
                f"Recommended: {events.action(event)}.")

    # -- threat intel -----------------------------------------------------

    def intel(self, body: dict) -> tuple[int, dict, dict]:
        ip, event_id = body.get("ip", "?"), body.get("event_id", "?")
        rng = self.rng("intel", event_id)
        time.sleep(rng.uniform(*self.latency) / 2)
        if rng.random() < self.chaos.get("intel", {}).get("503", 0):
            ui.vendor_log("intel", event_id, "fail", "503 feed unavailable")
            return 503, {"error": "unavailable"}, {}
        seed = int(hashlib.sha1(ip.encode()).hexdigest()[:4], 16)
        rep = ["clean", "suspicious", "known-bad", "known-bad"][seed % 4]
        ui.vendor_log("intel", event_id, "ok", f"{ip} is {rep}")
        return 200, {"ip": ip, "reputation": rep, "feeds": 1 + seed % 4}, {}

    # -- the dashboard ----------------------------------------------------

    def ui_put(self, event_id: str, body: dict) -> tuple[int, dict, dict]:
        with self.lock:
            previous = self.ui.get(event_id)
            version = (previous["version"] + 1) if previous else 1
            self.ui[event_id] = {**body, "id": event_id, "version": version, "updated_at": time.time()}
        kind = "ok"
        what = f"v{version} {body.get('severity')} via {body.get('source')}"
        if previous and previous.get("source") != body.get("source"):
            what += f" (replaced {previous.get('source')})"
        ui.vendor_log("ui", event_id, kind, what)
        return 200, {"version": version}, {}

    # -- admin ------------------------------------------------------------

    def admin(self, body: dict) -> tuple[int, dict, dict]:
        region = body.get("region")
        if region not in self.regions:
            return 404, {"error": f"unknown region {region!r}"}, {}
        with self.lock:
            state = self.regions[region]
            if "credits" in body:
                state["credits"] = int(body["credits"])
            if "add" in body:
                state["credits"] += int(body["add"])
            if "outage" in body:
                state["outage"] = bool(body["outage"])
            snapshot = dict(state)
        ui.vendor_log("llm", "-", "info", f"admin: {region} credits={snapshot['credits']} outage={snapshot['outage']}")
        return 200, snapshot, {}

    def admin_state(self) -> tuple[int, dict, dict]:
        with self.lock:
            return 200, {"regions": {k: dict(v) for k, v in self.regions.items()}}, {}
