"""Live board: checkpoints, the job currently executing, and the failure switches.

Served by the vendor process at ``/``. The page polls ``/admin/board``. Checkpoint
reads are query-only so this process never writes the pipeline's sqlite file.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from .events import KINDS
from .pipeline import STATE_DIR
from .pipeline import STEPS as ORDER_STEPS
from .triage import STEPS as TRIAGE_STEPS

PAGE = (Path(__file__).with_name("board.html")).read_text(encoding="utf-8")

PIPELINES = {
    "triage": list(TRIAGE_STEPS.items()),
    "fulfilment": list(ORDER_STEPS.items()),
}

PRIMARY_REGION = "eu-west-1"


def payload(vendors) -> dict:
    state = snapshot_db()
    state["control"] = vendors.control_state()
    _, llm, _ = vendors.security.admin_state()
    state["llm"] = llm
    with vendors.security.lock:
        published = [dict(event) for event in vendors.security.ui.values()]
    published.sort(key=lambda event: event.get("id", ""))
    for event in published:
        text = event.get("summary") or ""
        if len(text) > 180:
            event["summary"] = text[:177] + "..."
    state["published"] = published
    state["server_time"] = time.time()
    return state


def snapshot_db(path: Path | None = None) -> dict:
    path = Path(path) if path is not None else STATE_DIR / "checkpoints.db"
    empty = {"db": False, "runs": [], "activity": [], "providers": [], "latest_event_at": None}
    if not path.exists():
        return empty
    try:
        db = sqlite3.connect(str(path), timeout=1)
        db.execute("PRAGMA query_only=ON")
        db.execute("PRAGMA busy_timeout=1000")
    except sqlite3.Error as exc:
        empty["error"] = str(exc)
        return empty
    try:
        return _read(db)
    except sqlite3.Error as exc:
        empty["error"] = str(exc)
        return empty
    finally:
        db.close()


def _read(db: sqlite3.Connection) -> dict:
    runs = []
    for run_id, workflow, status, params, updated_at in db.execute(
            "SELECT run_id, workflow, status, params, updated_at FROM runs ORDER BY updated_at"):
        try:
            parsed = json.loads(params) if params else {}
        except json.JSONDecodeError:
            parsed = {}
        columns = _columns(db, run_id, workflow)
        items = []
        for item, item_status, degraded, error, retry_at, raw_input, item_updated in db.execute(
                """SELECT item, status, degraded, error, retry_at, input, updated_at
                   FROM items WHERE run_id = ? ORDER BY seq""", (run_id,)):
            try:
                incoming = json.loads(raw_input) if raw_input else {}
            except json.JSONDecodeError:
                incoming = {}
            steps = {}
            for step, seq, step_status, attempts, step_error, started in db.execute(
                    """SELECT step, seq, status, attempts, error, started_at
                       FROM steps WHERE run_id = ? AND item = ? ORDER BY seq""",
                    (run_id, item)):
                steps[step] = {
                    "status": step_status,
                    "seq": seq,
                    "attempts": attempts,
                    "error": step_error,
                    "started_at": started,
                }
            items.append({
                "id": item,
                "status": item_status,
                "degraded": bool(degraded),
                "error": error,
                "retry_at": retry_at,
                "updated_at": item_updated,
                "label": _label(workflow, incoming),
                "steps": steps,
            })
        runs.append({
            "id": run_id,
            "workflow": workflow,
            "status": status,
            "updated_at": updated_at,
            "expected": parsed.get("events") or parsed.get("orders"),
            "columns": [{"id": name, "label": label} for name, label in columns],
            "items": items,
        })

    activity = []
    for row in db.execute(
            """SELECT id, ts, item, step, event, attempt, provider, duration, detail
               FROM audit ORDER BY id DESC LIMIT 40"""):
        ident, ts, item, step, event, attempt, provider, duration, detail = row
        try:
            extra = json.loads(detail) if detail else {}
        except json.JSONDecodeError:
            extra = {}
        activity.append({
            "id": ident, "ts": ts, "item": item, "step": step, "event": event,
            "attempt": attempt, "provider": provider, "duration": duration, "detail": extra,
        })
    activity.reverse()

    latest = db.execute("SELECT MAX(ts) FROM audit").fetchone()[0]
    return {
        "db": True,
        "runs": runs,
        "activity": activity,
        "providers": _providers(db),
        "latest_event_at": latest,
    }


def _columns(db: sqlite3.Connection, run_id: str, workflow: str) -> list[tuple[str, str]]:
    known = PIPELINES.get(workflow)
    if known:
        return known
    rows = db.execute(
        """SELECT step, MIN(seq) FROM steps WHERE run_id = ? GROUP BY step ORDER BY MIN(seq)""",
        (run_id,)).fetchall()
    return [(name, name) for name, _ in rows]


def _label(workflow: str, data: dict) -> str:
    if not isinstance(data, dict):
        return ""
    if workflow == "triage" and data.get("type"):
        title = KINDS.get(data["type"], (data["type"],))[0]
        asset = data.get("asset")
        return f"{title} · {asset}" if asset else title
    if data.get("customer"):
        qty = data.get("qty", "")
        title = data.get("title", "")
        return f"{data['customer']} · {qty}× {title}".strip()
    return ""


def _providers(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute(
        """SELECT name, priority, enabled FROM providers WHERE pool = 'llm' ORDER BY priority, name"""
    ).fetchall()
    breakers = {
        name: (state, failures, retry_at, reason)
        for name, state, failures, retry_at, reason in db.execute(
            "SELECT name, state, failures, retry_at, reason FROM breakers WHERE pool = 'llm'")
    }
    now = time.time()
    out = []
    for name, priority, enabled in rows:
        state, failures, retry_at, reason = breakers.get(name, ("closed", 0, None, None))
        view = "ready"
        if not enabled:
            view = "disabled"
        elif state == "open":
            view = "cooling" if retry_at and retry_at > now else "probe"
        out.append({
            "name": name,
            "priority": priority,
            "state": view,
            "failures": failures,
            "reason": reason,
            "retry_in": max(0.0, retry_at - now) if retry_at else 0.0,
        })
    return out
