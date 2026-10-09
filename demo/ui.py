"""Terminal output for the demo: colours, live step lines, status and ledger tables."""

from __future__ import annotations

import os
import sys
import threading
import time
from collections.abc import Callable

from stitch import Reporter, Stats

_COLOR = (sys.stdout.isatty() or "FORCE_COLOR" in os.environ) and "NO_COLOR" not in os.environ
_print_lock = threading.Lock()


def _c(code: str):
    return (lambda s: f"\033[{code}m{s}\033[0m") if _COLOR else (lambda s: str(s))


bold, dim, red, green, yellow, cyan, magenta = (
    _c("1"), _c("2"), _c("31"), _c("32"), _c("33"), _c("36"), _c("35"))
bold_red = _c("1;31")


def say(*parts: object) -> None:
    with _print_lock:
        print(*parts, flush=True)


def _n(count: int, noun: str) -> str:
    return f"{count} {noun}{'' if count == 1 else 's'}"


def rule(text: str) -> str:
    return bold(f"━━ {text} " + "━" * max(3, 66 - len(text)))


# -- pipeline output -------------------------------------------------------

def _detail(step: str, result: dict | None) -> str:
    if not isinstance(result, dict):
        return ""
    for k in ("charge_id", "reservation_id", "tracking", "message_id"):
        if k in result:
            return result[k]
    if "reputation" in result:
        return f"ip is {result['reputation']}"
    if "version" in result:
        return f"dashboard v{result['version']} ({result['source']})"
    if result.get("source") == "rule-based":
        return "rule-based fallback"
    if result.get("provider"):
        what = result.get("severity") or f"{result.get('tokens', '?')} tokens"
        return f"{what} via {result['provider']}"
    if "score" in result:
        return f"risk {result['score']}"
    return ""


class LiveReporter(Reporter):
    """One line per step. For the stage: concurrency 1, a handful of orders."""

    def __init__(self, describe: Callable[[str], str]):
        self.describe = describe
        self._open: str | None = None  # a "▸ step" line waiting for its result

    def _line(self, text: str) -> None:
        with _print_lock:
            if self._open is not None:
                print(flush=True)
                self._open = None
            print(text, flush=True)

    def _close(self, text: str) -> None:
        with _print_lock:
            if self._open is not None:
                print("\r\033[K" if _COLOR else "\n", end="")
                self._open = None
            print(text, flush=True)

    def on_run_started(self, run_id, durable, concurrency, resume):
        what = "checkpointed" if durable else "naive: no checkpoints, a failure restarts the order"
        say(rule(f"{run_id} · {what}"))
        say(f"   PID {bold(os.getpid())}   {dim('crash me any time:')} kill -9 {os.getpid()}")
        if resume:
            if resume["previous"] == "done":
                say(cyan("   ↻ batch was marked done; only items that need work will run"))
            else:
                where = ", ".join(f"{i} at {s}" for i, s in resume["interrupted"]) or "between steps"
                say(cyan(f"   ↻ resuming: {_n(resume['done'], 'order')} already done; interrupted: {where}"))
        say()

    def on_item_started(self, item, attempt, resumed):
        who = dim(self.describe(item))
        tag = ""
        if attempt > 1:
            tag = bold_red(f"  attempt {attempt}: starting over from step 1")
        elif resumed:
            tag = cyan("  resuming")
        self._line(f"{bold(item)}  {who}{tag}")

    def on_item_skipped(self, item):
        self._line(dim(f"{item}  ↷ already fulfilled (checkpoint)"))

    def on_step_skipped(self, item, step, result):
        self._line(dim(f"   ↷ {step:<20} skipped, checkpointed   {_detail(step, result)}"))

    def on_step_started(self, item, step, interrupted):
        if interrupted:
            self._line(cyan(f"   ↻ {step:<20} was interrupted last time, running it again"))
        with _print_lock:
            print(f"   ▸ {step:<20}", end="", flush=True)
            self._open = step

    def on_step_done(self, item, step, result, attempts, elapsed, degraded=False):
        retried = yellow(f"  after {attempts} attempts") if attempts > 1 else ""
        mark = yellow("↓") if degraded else green("✓")
        self._close(f"   {mark} {step:<20} {elapsed:5.2f}s  {dim(_detail(step, result))}{retried}")

    def on_step_retry(self, item, step, attempt, attempts, delay, error):
        self._line(yellow(f"   ⟳ {step:<20} {error} → retry {attempt + 1}/{attempts} in {delay:.1f}s"))

    def on_step_failed(self, item, step, error, attempts):
        self._line(red(f"   ✗ {step:<20} {error}"))

    def on_item_retry(self, item, attempt, attempts, delay, error):
        self._line(bold_red(f"   ⟲ whole order fails ({error}); retry {attempt + 1}/{attempts} in {delay:.1f}s"))

    def on_item_failed(self, item, error):
        self._line(red(f"   ✗ {item} FAILED: {error}"))

    def on_item_done(self, item, elapsed, degraded=False):
        if degraded:
            self._line(yellow("   ↓ published with fallback results; heal will replace them"))

    def on_provider_failed(self, provider, error, kind):
        self._line(yellow(f"   ! {provider}: {error}"))

    def on_breaker_opened(self, provider, reason, cooldown):
        self._line(bold_red(f"   ⛔ breaker open on {provider} for {cooldown:g}s"))

    def on_breaker_probe(self, provider):
        self._line(cyan(f"   ? {provider}: cooldown over, sending one probe call"))

    def on_breaker_closed(self, provider):
        self._line(green(f"   ✔ {provider} is healthy again, breaker closed"))

    def on_failover(self, from_provider, provider, cause):
        if cause == "failed":
            self._line(cyan(f"   ↪ failed over {from_provider} → {provider}"))

    def on_step_degraded(self, step, reason):
        self._line(yellow(f"   ↓ {step}: {reason}; using the fallback"))

    def on_step_waiting(self, step, reason):
        self._line(yellow(f"   ⏸ {step}: {reason}"))

    def on_item_parked(self, item, until, reason):
        self._line(yellow(f"   ⏸ {item} parked until {time.strftime('%H:%M:%S', time.localtime(until))}; "
                          "carrying on with the others"))

    def on_cooldown_wait(self, count, delay):
        self._line(dim(f"   … {_n(count, 'item')} parked; next retry in {delay:.0f}s"))

    def on_cooldown_over(self, count):
        self._line(cyan(f"   ⏰ cooldown over: retrying {_n(count, 'parked item')}"))

    def on_crash(self, item, step, phase):
        when = "before it did anything" if phase == "before" else "after the vendor did the work, before the checkpoint"
        self._line(bold_red(f"\n   💥 SIGKILL in {step} {when}  (simulated OOM kill: no cleanup, no finally)"))

    def on_run_finished(self, stats: Stats):
        _summary(stats)


class CompactReporter(Reporter):
    """One line per order. For scale: many orders, high concurrency."""

    def __init__(self, total: int):
        self.total = total
        self.width = len(str(total))
        self.count = 0
        self.marks: dict[str, list[str]] = {}
        self.retries: dict[str, int] = {}
        self.failovers = 0

    def _done_line(self, item: str, text: str, counts: bool = True) -> None:
        if counts:
            self.count += 1
            index = f"[{self.count:>{self.width}}/{self.total}]"
        else:  # parked: not finished yet, so don't advance the counter
            index = "[" + "⏸".center(2 * self.width + 1) + "]"
        say(f"{dim(index)} {item}  {text}")

    def on_run_started(self, run_id, durable, concurrency, resume):
        what = "checkpointed" if durable else "naive"
        say(rule(f"{run_id} · {what} · concurrency {concurrency}"))
        say(f"   PID {bold(os.getpid())}   {dim('crash me any time:')} kill -9 {os.getpid()}")
        if resume and resume["previous"] != "done":
            say(cyan(f"   ↻ resuming: {_n(resume['done'], 'order')} already done, "
                     f"{len(resume['interrupted'])} interrupted mid-step"))
        say()

    def on_item_started(self, item, attempt, resumed):
        if attempt == 1:
            self.marks[item] = []

    def on_item_skipped(self, item):
        self._done_line(item, dim("↷ already fulfilled"))

    def on_step_skipped(self, item, step, result):
        self.marks[item].append(dim("↷"))

    def on_step_done(self, item, step, result, attempts, elapsed, degraded=False):
        self.marks[item].append(yellow("↓") if degraded else green("✓"))

    def on_step_waiting(self, item, step, reason):
        self.marks[item].append(yellow("⏸"))

    def on_item_parked(self, item, until, reason):
        self.retries.pop(item, None)
        self._done_line(item, f"{''.join(self.marks.pop(item, []))}  "
                              + yellow(f"parked until {time.strftime('%H:%M:%S', time.localtime(until))}"),
                        counts=False)

    def on_breaker_opened(self, provider, reason, cooldown):
        say(bold_red(f"   ⛔ breaker open on {provider} for {cooldown:g}s: {reason}"))

    def on_breaker_closed(self, provider):
        say(green(f"   ✔ {provider} is healthy again"))

    def on_failover(self, from_provider, provider, cause):
        if cause == "failed":
            self.failovers += 1

    def on_cooldown_wait(self, count, delay):
        say(dim(f"   … {_n(count, 'item')} parked; next retry in {delay:.0f}s"))

    def on_cooldown_over(self, count):
        say(cyan(f"   ⏰ cooldown over: retrying {_n(count, 'parked item')}"))

    def on_step_failed(self, item, step, error, attempts):
        self.marks[item].append(red("✗"))

    def on_step_retry(self, item, step, attempt, attempts, delay, error):
        self.retries[item] = self.retries.get(item, 0) + 1

    def on_item_retry(self, item, attempt, attempts, delay, error):
        self.marks[item].append(bold_red("⟲"))

    def on_item_done(self, item, elapsed, degraded=False):
        r = self.retries.pop(item, 0)
        extra = yellow(f"  ⟳{r}") if r else ""
        extra += yellow("  degraded") if degraded else ""
        self._done_line(item, f"{''.join(self.marks.pop(item))}  {elapsed:4.1f}s{extra}")

    def on_item_failed(self, item, error):
        self.retries.pop(item, None)
        self._done_line(item, f"{''.join(self.marks.pop(item, []))}  {red(error)}")

    def on_crash(self, item, step, phase):
        say(bold_red(f"\n💥 SIGKILL in {item} {step} ({phase})"))

    def on_run_finished(self, stats: Stats):
        _summary(stats)


def _summary(s: Stats) -> None:
    say()
    say(rule(f"finished in {s.elapsed:.1f}s"))
    failed = red(f"failed {s.items_failed}") if s.items_failed else f"failed {s.items_failed}"
    say(f"   items    done {bold(s.items_done)} · already done {s.items_skipped} · {failed}")
    say(f"   steps    ran {s.steps_run} · skipped from checkpoint {s.steps_skipped} · "
        f"step retries {s.retries} · whole-order retries {s.item_retries}")
    if s.steps_degraded or s.items_parked or s.failovers:
        say(f"   failsafe {yellow(f'{s.items_degraded} degraded')} ({s.steps_degraded} fallback steps) · "
            f"parked {s.items_parked}, still parked {s.parked_left} · failovers {s.failovers}")
    say(dim("   what happened to one item?  python -m demo trace <item>"))


# -- status ----------------------------------------------------------------

GLYPH = {"done": green("✓"), "running": bold_red("▶"), "failed": red("✗"),
         "degraded": yellow("↓"), "waiting": yellow("⏸")}


def render_status(run_id: str, run_status: str, items, steps, step_names: dict[str, str], limit: int) -> None:
    """step_names maps step id -> short column header, in pipeline order."""
    by_item: dict[str, dict[str, str]] = {}
    for s in steps:
        by_item.setdefault(s.item, {})[s.step] = s.status
    short = list(step_names.values())
    widths = [max(len(n), 1) for n in short]

    say(rule(f"checkpoints for {run_id} ({run_status})"))
    say("   " + f"{'item':<12}" + "  ".join(f"{n:<{w}}" for n, w in zip(short, widths)))

    shown = 0
    done_hidden = 0
    for item, status in items:
        if status == "done" and len(items) > limit and shown >= limit // 2:
            done_hidden += 1
            continue
        cells = by_item.get(item, {})
        row = []
        for name, w in zip(step_names, widths):
            st = cells.get(name)
            row.append((GLYPH.get(st, dim("·")) if st else dim("·")) + " " * (w - 1))
        note = ""
        if status == "parked":
            note = yellow("  ← parked, will retry after the cooldown")
        elif "running" in cells.values():
            step = next(k for k, v in cells.items() if v == "running")
            note = bold_red(f"  ← killed during {step}")
        elif status == "failed":
            note = red("  ← failed")
        elif "degraded" in cells.values():
            note = yellow("  ← fallback result, healable")
        say("   " + f"{item:<12}" + "  ".join(row) + note)
        shown += 1
    if done_hidden:
        say(dim(f"   … {done_hidden} more fulfilled orders"))

    n_done = sum(1 for _, st in items if st == "done")
    say()
    say(f"   {green('✓')} done  {yellow('↓')} degraded  {yellow('⏸')} waiting  "
        f"{bold_red('▶')} started, never finished  {red('✗')} failed  {dim('·')} not started")
    say(f"   {n_done}/{len(items)} items done · {len(steps)} step records")


# -- ledger ----------------------------------------------------------------

def render_ledger(snapshot: dict) -> None:
    orders = snapshot["orders"]
    say(rule(f"what the vendors did · chaos={snapshot['chaos']} · "
             f"{snapshot['injected_failures']} injected failures"))
    if not orders:
        say(dim("   nothing yet"))
        return
    say("   " + f"{'order':<12}{'charged':>9}{'total':>11}{'stock':>7}{'labels':>8}{'emails':>8}{'replays':>9}")

    over_orders = over_amount = dup_emails = dup_labels = dup_stock = 0
    for order, e in orders.items():
        def cell(n: int, width: int) -> str:
            text = f"{n}x".rjust(width)
            return bold_red(text) if n > 1 else text
        if e["charges"] > 1:
            over_orders += 1
            over_amount += e["charged_total"] * (e["charges"] - 1) / e["charges"]
        dup_emails += max(0, e["emails"] - 1)
        dup_labels += max(0, e["labels"] - 1)
        dup_stock += max(0, e["reservations"] - 1)
        total = f"€{e['charged_total']:.2f}".rjust(11)
        say("   " + f"{order:<12}" + cell(e["charges"], 9)
            + (bold_red(total) if e["charges"] > 1 else total)
            + cell(e["reservations"], 7) + cell(e["labels"], 8) + cell(e["emails"], 8)
            + (cyan(f"{e['replays']:>9}") if e["replays"] else dim(f"{0:>9}")))

    say()
    if over_orders or dup_emails or dup_labels or dup_stock:
        say(bold_red(f"   {over_orders} customers charged more than once (€{over_amount:.2f} too much), "
                     f"{dup_emails} duplicate emails, {dup_labels} extra labels, {dup_stock} extra reservations"))
    else:
        say(green(f"   every order charged exactly once · {len(orders)} orders · no duplicates"))


# -- vendor server log -----------------------------------------------------

VENDOR_TAG = {
    "llm": ("LLM", _c("1;36")), "intel": ("INTEL", magenta), "ui": ("UI", green),
    "fraud": ("FRAUD", magenta), "payments": ("PAY", green), "warehouse": ("STOCK", cyan),
    "carrier": ("LABEL", yellow), "email": ("EMAIL", _c("34")), "vendors": ("----", dim),
}


def vendor_log(vendor: str, order: str, kind: str, message: str) -> None:
    name, colour = VENDOR_TAG.get(vendor, (vendor.upper(), str))
    stamp = dim(time.strftime("%H:%M:%S"))
    if kind == "dup":
        text = bold_red(f"⚠ DUPLICATE  {message}")
    elif kind == "fail":
        text = yellow(f"✗ {message}")
    elif kind == "replay":
        text = cyan(f"↺ {message}")
    else:
        text = message
    say(f"{stamp}  {colour(f'{name:<5}')}  {order:<11} {text}")


def vendor_banner(port: int, chaos: str, seed: int, latency: tuple[float, float]) -> None:
    say(rule(f"fake vendors on 127.0.0.1:{port}"))
    say(f"   chaos {bold(chaos)} · seed {seed} · latency {latency[0]:.2f}-{latency[1]:.2f}s")
    say(dim("   orders · intel · llm · dashboard"))
    say(dim("   other terminal: python -m demo control"))
    say()


# -- providers, LLM meter, dashboard ----------------------------------------

STATE_STYLE = {"ready": green, "cooling": bold_red, "probe": yellow, "disabled": dim}


def render_providers(pool: str, rows: list[dict]) -> None:
    say(rule(f"provider pool '{pool}' (tried in this order)"))
    if not rows:
        say(dim("   no providers registered"))
        return
    for r in rows:
        paint = STATE_STYLE.get(r["state"], str)
        extra = ""
        if r["state"] == "cooling":
            extra = f"  {r['retry_in']:.0f}s left: {r['reason']}"
        elif r["state"] == "probe":
            extra = "  cooldown over, next call is a probe"
        elif r["failures"]:
            extra = f"  {r['failures']} recent failure(s)"
        state = paint(f"{r['state']:<9}")
        say(f"   {r['priority']:>2}  {r['name']:<14} {state}{extra}")


def render_control(state: dict) -> None:
    net = bold_red("PAUSED — calls are waiting") if state["paused"] else green("answering")
    llm = bold_red("STOPPED") if state["llm_stopped"] else green("allowed")
    say(rule("control"))
    say(f"   network  {net}")
    say(f"   llm      {llm}")


def render_llm(state: dict) -> None:
    say(rule("LLM accounts (as the provider sees them)"))
    say("   " + f"{'region':<14}{'credits':>12}{'calls':>8}{'tokens':>9}{'refused':>9}  status")
    for name, r in state["regions"].items():
        credits = "unmetered" if name == "local-small" else f"{max(r['credits'], 0):,}"
        status = bold_red("OUTAGE") if r["outage"] else (
            bold_red("NO CREDITS") if r["credits"] <= 0 else green("ok"))
        say("   " + f"{name:<14}{credits:>12}{r['calls']:>8}{r['tokens']:>9,}{r['refused']:>9}  {status}")


def render_dashboard(events_: list[dict], expected: int | None, limit: int) -> None:
    say(rule("what analysts see on the dashboard"))
    events_ = sorted(events_, key=lambda e: e["id"])
    sev = {"critical": bold_red, "high": red, "medium": yellow, "low": dim}
    say("   " + f"{'event':<10}{'severity':<10}{'analysis':<14}{'via':<13}{'v':>2}  summary")
    for e in events_[:limit]:
        degraded = e["source"] != "llm"
        label = yellow("rule-based ↓") if degraded else green("llm")
        pad = " " * (14 - (12 if degraded else 3))
        text = e["summary"] if len(e["summary"]) <= 54 else e["summary"][:51] + "..."
        say("   " + f"{e['id']:<10}" + sev.get(e["severity"], str)(f"{e['severity']:<10}") + label + pad
            + f"{e.get('provider') or '-':<13}{e['version']:>2}  {dim(text)}")
    if len(events_) > limit:
        say(dim(f"   … {len(events_) - limit} more"))
    llm_n = sum(1 for e in events_ if e["source"] == "llm")
    say()
    say(f"   {len(events_)} events on screen · {green(f'{llm_n} analysed by the LLM')} · "
        f"{yellow(f'{len(events_) - llm_n} rule-based fallback')}"
        + (f" · {red(f'{expected - len(events_)} missing (stale)')}" if expected and expected > len(events_) else ""))
