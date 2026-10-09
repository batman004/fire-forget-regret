"""A deterministic stream of security events, like a SIEM would emit.

A generator, not a list, for the same reason as orders: memory stays flat.
"""

from __future__ import annotations

import random
from collections.abc import Iterator

# type -> (title, base severity, what the summary says, recommended action)
KINDS = {
    "failed_login_burst": ("Failed login burst", "medium",
                           "{count} failed sign-ins for {user} from {ip} in {window}",
                           "force a password reset and require MFA"),
    "impossible_travel": ("Impossible travel", "high",
                          "{user} signed in from {city} and {far_city} {window} apart",
                          "revoke active sessions and contact the user"),
    "malware_detected": ("Malware detected", "critical",
                         "EDR quarantined {count} files on {asset}, dropper contacted {ip}",
                         "isolate the host and start incident response"),
    "port_scan": ("Port scan", "low",
                  "{ip} probed {count} ports on {asset} in {window}",
                  "block the source address; no action on the asset"),
    "data_exfiltration": ("Suspected data exfiltration", "critical",
                          "{asset} sent {count} MB to {ip}, 40x its daily baseline",
                          "cut the egress route and preserve logs"),
    "waf_sqli": ("SQL injection attempt", "high",
                 "WAF blocked {count} injection payloads against {asset} from {ip}",
                 "confirm the block held and review the endpoint"),
}

ASSETS = ["vpn-gateway-01", "payments-api", "hr-portal", "build-runner-17", "files-eu-02",
          "crm-db-primary", "laptop-4471", "edge-proxy-03"]
USERS = ["a.jansen", "m.devries", "s.bakker", "l.visser", "k.smit", "j.mulder", "t.bos", "r.dekker"]
CITIES = ["Utrecht", "Lagos", "Singapore", "Lima", "Oslo", "Seoul", "Denver", "Pune"]


def event_id(n: int) -> str:
    return f"evt-{n:04d}"


def make_event(n: int, seed: int = 2026) -> dict:
    rng = random.Random(f"{seed}:evt:{n}")
    kind = rng.choice(list(KINDS))
    return {
        "id": event_id(n),
        "type": kind,
        "ip": f"{rng.choice([185, 45, 91, 203, 77])}.{rng.randint(1, 254)}.{rng.randint(1, 254)}.{rng.randint(1, 254)}",
        "asset": rng.choice(ASSETS),
        "user": rng.choice(USERS),
        "count": rng.choice([3, 12, 48, 120, 640]),
        "window": rng.choice(["2 minutes", "10 minutes", "an hour"]),
        "city": rng.choice(CITIES),
        "far_city": rng.choice(CITIES),
    }


def lookup(item: str, seed: int = 2026) -> dict:
    return make_event(int(item.rsplit("-", 1)[1]), seed)


def iter_events(count: int, seed: int = 2026) -> Iterator[dict]:
    for n in range(1, count + 1):
        yield make_event(n, seed)


def facts(event: dict) -> str:
    title, _, template, _ = KINDS[event["type"]]
    return template.format(**event)


def rule_based_summary(event: dict) -> str:
    """What you can say without an LLM: the SIEM's own template, no analysis."""
    title, _, _, _ = KINDS[event["type"]]
    return f"{title}: {facts(event)}."


def rule_based_severity(event: dict) -> str:
    return KINDS[event["type"]][1]


def action(event: dict) -> str:
    return KINDS[event["type"]][3]
