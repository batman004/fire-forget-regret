"""Security-event triage: enrich, summarise with an LLM, classify, publish to the dashboard.

    enrich     threat-intel feed       flaky, retried
    summarise  LLM (provider pool)     credits run out, regions go down
    classify   LLM (provider pool)     same pool, same failure modes
    publish    dashboard API           an upsert, so publishing twice is safe

The dashboard is what analysts look at. If the LLM is unavailable the question
is what they see: nothing (stale), or the SIEM's own rule-based summary marked
as such (degraded). `--on-outage` picks between publishing the fallback and
parking the event until a provider's cooldown ends.

The workflow is the decorator from stitch; nothing in it knows about crashes,
fallbacks or cooldowns.
"""

from __future__ import annotations

import json
import time

import stitch
from stitch import Defaults, Park, Pool, Retry, step, workflow

from . import client, events
from .llm_client import LLMClient

# Demo-scale timings. In production these are minutes, not seconds.
TUNING = Defaults(failure_threshold=2, cooldown=6.0, max_cooldown=30.0,
                  quota_cooldown=15.0, rate_limit_cooldown=1.0)

llm = Pool(
    "llm",
    factory=LLMClient,
    defaults=[("eu-west-1", 1, {"region": "eu-west-1"})],
    tuning=TUNING,
)

POLICY = {"on_outage": "degrade"}  # or "park"; set from the command line

CALL = Retry(attempts=4, base=0.3, cap=2.0)


def _fallback_or_park(cause: BaseException | None):
    """Under the 'park' policy a missing provider means wait, not publish a placeholder."""
    if POLICY["on_outage"] == "park":
        until = getattr(cause, "retry_at", None) or time.time() + 5.0
        raise Park(until, "waiting for an LLM provider")


def summary_fallback(event: dict, intel: dict, *, cause=None) -> dict:
    _fallback_or_park(cause)
    return {"text": events.rule_based_summary(event), "source": "rule-based", "provider": None}


def severity_fallback(event: dict, summary: dict, *, cause=None) -> dict:
    _fallback_or_park(cause)
    return {"severity": events.rule_based_severity(event), "confidence": 0.4,
            "source": "rule-based", "provider": None}


@step(retry=CALL, timeout=8)
async def enrich(event: dict) -> dict:
    intel = await client.post("/intel/lookup", {"event_id": event["id"], "ip": event["ip"]})
    return {"reputation": intel["reputation"], "feeds": intel["feeds"]}


@step(retry=Retry(attempts=2, base=0.2, cap=0.5), timeout=10, degrade=summary_fallback)
async def summarise(event: dict, intel: dict) -> dict:
    enriched = {**event, "reputation": intel["reputation"]}
    answer = await llm.call(lambda provider: provider.complete("summarise", enriched))
    return {"text": answer["text"], "source": "llm", "provider": answer["provider"],
            "tokens": answer["tokens"]}


@step(retry=Retry(attempts=2, base=0.2, cap=0.5), timeout=10, degrade=severity_fallback)
async def classify(event: dict, summary: dict) -> dict:
    answer = await llm.call(lambda provider: provider.complete("classify", event))
    try:
        verdict = json.loads(answer["text"])
        severity = verdict["severity"]
    except (ValueError, KeyError):
        raise stitch.TransientError(f"{answer['provider']} returned unparseable output")
    return {"severity": severity, "confidence": verdict["confidence"], "source": "llm",
            "provider": answer["provider"], "tokens": answer["tokens"]}


@step(retry=CALL, timeout=8)
async def publish(event: dict, intel: dict, summary: dict, verdict: dict) -> dict:
    analysed_by_llm = summary["source"] == "llm" and verdict["source"] == "llm"
    reply = await client.post(f"/ui/events/{event['id']}", {
        "title": events.KINDS[event["type"]][0],
        "asset": event["asset"],
        "reputation": intel["reputation"],
        "severity": verdict["severity"],
        "confidence": verdict["confidence"],
        "summary": summary["text"],
        "source": "llm" if analysed_by_llm else "rule-based",
        "provider": verdict.get("provider") or summary.get("provider"),
    })
    return {"version": reply["version"], "source": "llm" if analysed_by_llm else "rule-based"}


@workflow(name="triage", key=lambda event: event["id"])
async def triage(event: dict) -> dict:
    intel = await enrich(event)
    summary = await summarise(event, intel)
    verdict = await classify(event, summary)
    published = await publish(event, intel, summary, verdict)
    return {"severity": verdict["severity"], "source": published["source"]}


STEPS = {"enrich": "intel", "summarise": "summary", "classify": "severity", "publish": "ui"}
