"""Fire, Forget, Regret: the live demo.

  Hands on: one pipeline, break it yourself
    python -m demo vendors --chaos calm        terminal 1: every external call
    open http://127.0.0.1:8765/                the live board: jobs, steps, failure buttons
    python -m demo live                        terminal 2: Ctrl-C whenever, then: python -m demo resume

  Fake third parties
    python -m demo vendors                     terminal 2: payment, carrier, LLM, dashboard...
    python -m demo ledger | llm | ui           what the vendors / LLM accounts / dashboard show

  Workflow 1: order fulfilment (money moves)
    python -m demo run   [--crash-at order-0007:reserve_inventory]
    python -m demo naive [--crash-at ...]      same steps, no checkpoints

  Workflow 2: security-event triage (LLM with fallbacks)
    python -m demo triage [--events 40] [--crash-at evt-0007:summarise] [--on-outage degrade|park]

  Operate
    python -m demo status [--workflow triage]  what the checkpoints say
    python -m demo trace evt-0007              the audit trail of one item
    python -m demo providers [add|remove|enable|disable NAME]   the LLM provider pool
    python -m demo resume                      continue everything unfinished
    python -m demo heal                        redo steps that used a fallback
    python -m demo reset
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import stitch
from stitch import NO_RETRY, Crash, NullStore, Retry, Store, step

from . import client, events, llm_client, orders, triage as triage_mod, ui, vendors
from .pipeline import STATE_DIR, STEPS, fulfil

DB = STATE_DIR / "checkpoints.db"
OUTPUT = STATE_DIR / "output"
WORKFLOWS = {"fulfilment": STEPS, "triage": triage_mod.STEPS}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m demo", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("vendors", help="start the fake third-party vendors")
    p.add_argument("--port", type=int, default=vendors.DEFAULT_PORT)
    p.add_argument("--chaos", choices=sorted(vendors.PROFILES), default="flaky")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--latency", default="0.25-0.45", help="per-request latency range in seconds")
    p.add_argument("--hang", type=float, default=3.0, help="how long a 'hang' lasts")

    for name, text in (("run", "run order fulfilment, checkpointed"),
                       ("naive", "run order fulfilment with no checkpoints")):
        p = sub.add_parser(name, help=text)
        p.add_argument("--orders", type=int, default=10)
        _common(p)
        if name == "run":
            p.add_argument("--batch", default="demo", help="run id; the same batch resumes")
            p.add_argument("--no-idempotency", action="store_true",
                           help="don't send idempotency keys (shows the checkpoint gap)")
        else:
            p.add_argument("--job-retries", type=int, default=3,
                           help="attempts per order, like a task queue's max_retries")

    p = sub.add_parser("triage", help="run security-event triage with LLM fallbacks")
    _triage_args(p)

    p = sub.add_parser("live", help="one pipeline you can pause, crash, and cut the LLM on")
    _triage_args(p, events=60)

    p = sub.add_parser("control", help="pause the network, or stop outgoing LLM calls")
    p.add_argument("words", nargs="*", help="pause, unpause, llm stop, llm start, status, ui")

    p = sub.add_parser("status", help="show checkpoints")
    p.add_argument("--workflow", choices=sorted(WORKFLOWS), default=None)
    p.add_argument("--batch", default="demo")
    p.add_argument("--limit", type=int, default=24, help="rows to show for big batches")

    p = sub.add_parser("trace", help="the audit trail of one item")
    p.add_argument("item", help="e.g. evt-0007 or order-0007")
    p.add_argument("--all", action="store_true", help="include run-level events (cooldown waits...)")

    p = sub.add_parser("providers", help="the LLM provider pool: list, add, remove, enable, disable")
    p.add_argument("action", nargs="?", choices=["list", "add", "remove", "enable", "disable"], default="list")
    p.add_argument("name", nargs="?")
    p.add_argument("--priority", type=int, default=2, help="lower is tried first")
    p.add_argument("--region", help="which region's endpoint to call (default: the name)")

    p = sub.add_parser("llm", help="the LLM accounts at the vendor: show, top up, outage")
    p.add_argument("action", nargs="?", choices=["show", "topup", "outage"], default="show")
    p.add_argument("region", nargs="?")
    p.add_argument("value", nargs="?", help="tokens for topup; on|off for outage")

    p = sub.add_parser("resume", help="continue every unfinished or parked item from its checkpoints")
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--no-wait", action="store_true")

    p = sub.add_parser("heal", help="redo steps that finished with a fallback result")
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--watch", type=float, metavar="SECONDS",
                   help="keep healing every SECONDS until nothing is degraded")

    p = sub.add_parser("ui", help="show what the dashboard displays")
    p.add_argument("--of", type=int, help="how many events there should be, to show what is missing")
    p.add_argument("--limit", type=int, default=30)

    sub.add_parser("ledger", help="show what the order vendors did")
    sub.add_parser("reset", help="delete checkpoints, labels and all vendor state")

    args = parser.parse_args(argv)
    return COMMANDS[args.command](args)


def _triage_args(p, events: int = 12) -> None:
    p.add_argument("--events", type=int, default=events)
    p.add_argument("--batch", default="demo")
    p.add_argument("--on-outage", choices=["degrade", "park"], default="degrade",
                   help="no LLM available: publish a rule-based fallback, or park until the cooldown ends")
    p.add_argument("--no-wait", action="store_true", help="don't wait for parked events to retry")
    _common(p)


def _common(p) -> None:
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--crash-at", metavar="ITEM:STEP[:after]",
                   help="SIGKILL at this point, e.g. order-0007:reserve_inventory")
    p.add_argument("--compact", action="store_true", help="one line per item")


def describe(item: str) -> str:
    if item.startswith("evt-"):
        e = events.lookup(item)
        return f"{events.KINDS[e['type']][0]} · {e['asset']} · {e['ip']}"
    o = orders.lookup(item)
    return f"{o['customer']} · {o['qty']}x {o['title']} · €{o['amount']:.2f}"


def require_vendors() -> bool:
    try:
        client.get("/health")
        return True
    except client.VendorsDown as exc:
        ui.say(ui.red(f"{exc}. Start them in another terminal:  python -m demo vendors"))
        return False


def reporter_for(args, total: int):
    if args.concurrency == 1 and not args.compact:
        return ui.LiveReporter(describe)
    return ui.CompactReporter(total)


def parse_crash(args, steps) -> Crash | None:
    if not args.crash_at:
        return None
    try:
        crash = Crash.parse(args.crash_at)
    except ValueError as exc:
        sys.exit(f"--crash-at: {exc}")
    if crash.step not in steps:
        sys.exit(f"--crash-at: unknown step {crash.step!r}; choose from {', '.join(steps)}")
    return crash


def run_async(coro_fn, store: Store, args) -> int:
    async def go():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=getattr(args, "concurrency", 1) + 4))
        return await coro_fn()

    try:
        return asyncio.run(go())
    except KeyboardInterrupt:
        ui.say(ui.yellow("\ninterrupted" + (": everything completed so far is checkpointed" if store.durable else "")))
        return 130
    finally:
        store.close()


# -- workflows ---------------------------------------------------------------

def cmd_vendors(args) -> int:
    lo, _, hi = args.latency.partition("-")
    vendors.serve(args.port, args.chaos, args.seed, (float(lo), float(hi or lo)), args.hang)
    return 0


def cmd_run(args) -> int:
    return _orders(args, durable=True)


def cmd_naive(args) -> int:
    return _orders(args, durable=False)


def _orders(args, durable: bool) -> int:
    crash = parse_crash(args, STEPS)
    if not require_vendors():
        return 2
    store = Store(DB) if durable else NullStore()
    reporter = reporter_for(args, args.orders)

    async def go():
        stats = await fulfil.map(
            orders.iter_orders(args.orders), store=store,
            batch=args.batch if durable else "naive", concurrency=args.concurrency,
            item_retry=NO_RETRY if durable else Retry(attempts=args.job_retries, base=0.5, cap=2.0),
            idempotency=durable and not args.no_idempotency, crash=crash, reporter=reporter,
            params={"orders": args.orders})
        return 0 if stats.items_failed == 0 else 1

    return run_async(go, store, args)


def _arm_live_waits() -> None:
    """A paused call sits on the wire until the network returns, instead of timing out."""
    client.TIMEOUT = 3600
    llm_client.TIMEOUT = 3600
    patient = Retry(attempts=8, base=0.4, cap=2.0)
    short = Retry(attempts=2, base=0.2, cap=0.5)
    triage_mod.enrich = step(retry=patient, timeout=None)(triage_mod.enrich.__wrapped__)
    triage_mod.publish = step(retry=patient, timeout=None)(triage_mod.publish.__wrapped__)
    triage_mod.summarise = step(retry=short, timeout=None, degrade=triage_mod.summary_fallback)(
        triage_mod.summarise.__wrapped__)
    triage_mod.classify = step(retry=short, timeout=None, degrade=triage_mod.severity_fallback)(
        triage_mod.classify.__wrapped__)


def cmd_live(args) -> int:
    _arm_live_waits()
    ui.say(ui.rule("one pipeline, break it by hand"))
    ui.say("   Ctrl-C or kill -9 this process whenever you like.")
    ui.say(f"   Finished steps stay checkpointed. Continue with:  {ui.bold('python -m demo resume')}")
    ui.say()
    ui.say("   In the control terminal:")
    ui.say(f"     {ui.bold('pause')} / {ui.bold('unpause')}      the call in flight waits, then finishes")
    ui.say(f"     {ui.bold('llm stop')} / {ui.bold('llm start')}  refuse LLM calls; intel and the dashboard stay up")
    ui.say(f"   After llm stop, new events publish a fallback. Once calls are allowed again:")
    ui.say(f"     {ui.bold('python -m demo heal --watch 5')}")
    ui.say(ui.dim(f"   this batch is {args.events} events. Start the IO server with --chaos calm."))
    ui.say()
    return cmd_triage(args)


def cmd_triage(args) -> int:
    crash = parse_crash(args, triage_mod.STEPS)
    if not require_vendors():
        return 2
    triage_mod.POLICY["on_outage"] = args.on_outage
    store = Store(DB)
    reporter = reporter_for(args, args.events)

    async def go():
        stats = await triage_mod.triage.map(
            events.iter_events(args.events), store=store, batch=args.batch,
            concurrency=args.concurrency, crash=crash, reporter=reporter,
            wait_parked=not args.no_wait, params={"events": args.events})
        return 0 if stats.items_failed == 0 else 1

    return run_async(go, store, args)


def cmd_resume(args) -> int:
    if not require_vendors() or not DB.exists():
        return 2
    store = Store(DB)

    async def go():
        done = await stitch.resume(store=store, reporter=ui.LiveReporter(describe) if args.concurrency == 1
                                 else ui.CompactReporter(0), concurrency=args.concurrency,
                                 wait_parked=not args.no_wait)
        if not done:
            ui.say(ui.dim("nothing to resume: no interrupted or parked items"))
        return 0

    return run_async(go, store, args)


def cmd_heal(args) -> int:
    if not require_vendors() or not DB.exists():
        return 2
    store = Store(DB)

    async def go():
        while True:
            done = await stitch.heal(store=store, reporter=ui.LiveReporter(describe)
                                   if args.concurrency == 1 else ui.CompactReporter(0),
                                   concurrency=args.concurrency)
            left = sum(len(store.degraded_items(r)) for r, _, _ in store.runs())
            if not done:
                ui.say(ui.dim("nothing to heal: no degraded items"))
            if not args.watch or not left:
                if left:
                    ui.say(ui.yellow(f"{left} items are still degraded: providers not back yet"))
                return 0
            ui.say(ui.dim(f"   {left} still degraded; trying again in {args.watch:g}s (Ctrl-C to stop)"))
            await asyncio.sleep(args.watch)

    return run_async(go, store, args)


# -- inspection ---------------------------------------------------------------

def cmd_status(args) -> int:
    if not DB.exists():
        ui.say(ui.dim("no checkpoints yet"))
        return 0
    store = Store(DB)
    try:
        runs = {r[0]: r[2] for r in store.runs()}
        wf = args.workflow
        if wf is None:  # whichever workflow ran last
            wf = next((r.split(":")[0] for r in reversed(list(runs)) if r.split(":")[0] in WORKFLOWS), "fulfilment")
        run_id = f"{wf}:{args.batch}"
        if run_id not in runs:
            ui.say(ui.dim(f"no run {run_id!r}; known runs: {', '.join(runs) or 'none'}"))
            return 0
        ui.render_status(run_id, runs[run_id], store.items(run_id), store.steps(run_id),
                         WORKFLOWS[wf], args.limit)
    finally:
        store.close()
    return 0


def cmd_trace(args) -> int:
    if not DB.exists():
        ui.say(ui.dim("no audit trail yet"))
        return 0
    store = Store(DB)
    try:
        rows = store.audit_rows(item=args.item)
        if args.all and rows:
            lo, hi = rows[0].id, rows[-1].id
            rows = [r for r in store.audit_rows(after_id=lo - 1)
                    if r.id <= hi and (r.item in (None, args.item))]
        if not rows:
            ui.say(ui.dim(f"nothing recorded for {args.item!r}"))
            return 1
        ui.say(ui.rule(f"audit trail for {args.item} · {len(rows)} events"))
        ui.say(stitch.render_trace(rows, color=ui._COLOR))
    finally:
        store.close()
    return 0


def cmd_providers(args) -> int:
    store = Store(DB)
    pool = triage_mod.llm
    try:
        if args.action in ("add", "remove", "enable", "disable") and not args.name:
            sys.exit(f"providers {args.action}: give a provider name")
        pool.seed_defaults(store)
        if args.action == "add":
            pool.register(store, args.name, args.priority, {"region": args.region or args.name})
            ui.say(ui.green(f"registered {args.name} at priority {args.priority}: "
                            "a running pipeline uses it from its next LLM call"))
        elif args.action == "remove":
            ui.say(ui.green(f"removed {args.name}") if store.delete_provider(pool.name, args.name)
                   else ui.red(f"no provider {args.name!r}"))
        elif args.action in ("enable", "disable"):
            ok = store.set_provider_enabled(pool.name, args.name, args.action == "enable")
            ui.say(ui.green(f"{args.action}d {args.name}") if ok else ui.red(f"no provider {args.name!r}"))
        ui.render_providers(pool.name, pool.snapshot(store))
    finally:
        store.close()
    return 0


def cmd_llm(args) -> int:
    try:
        if args.action == "topup":
            if not (args.region and args.value):
                sys.exit("llm topup REGION TOKENS")
            client.post_json("/admin/llm", {"region": args.region, "credits": int(args.value)})
        elif args.action == "outage":
            if not (args.region and args.value in ("on", "off")):
                sys.exit("llm outage REGION on|off")
            client.post_json("/admin/llm", {"region": args.region, "outage": args.value == "on"})
        ui.render_llm(client.get("/admin/llm"))
    except client.VendorsDown as exc:
        ui.say(ui.red(str(exc)))
        return 2
    return 0


def cmd_ui(args) -> int:
    try:
        shown = client.get("/ui/events")["events"]
    except client.VendorsDown as exc:
        ui.say(ui.red(str(exc)))
        return 2
    ui.render_dashboard(shown, args.of, args.limit)
    return 0


def cmd_ledger(args) -> int:
    try:
        ui.render_ledger(client.get("/ledger"))
    except client.VendorsDown as exc:
        ui.say(ui.red(str(exc)))
        return 2
    return 0


_CONTROL = {
    "pause": {"paused": True},
    "unpause": {"paused": False},
    "llm stop": {"llm_stopped": True},
    "llm-stop": {"llm_stopped": True},
    "llm start": {"llm_stopped": False},
    "llm-start": {"llm_stopped": False},
    "status": None,
    "ui": None,
}

_CONTROL_HELP = """\
  pause        hold every pipeline call until you unpause
  unpause      let held calls finish
  llm stop     refuse outgoing LLM calls
  llm start    allow LLM calls again
  status       network, LLM switch, token accounts
  ui           what the dashboard is showing
  quit
"""


def _control_show(include_ui: bool = False) -> int:
    try:
        ui.render_control(client.get("/admin/control"))
        ui.render_llm(client.get("/admin/llm"))
        if include_ui:
            ui.render_dashboard(client.get("/ui/events")["events"], None, 30)
    except client.VendorsDown as exc:
        ui.say(ui.red(f"{exc}. Start the IO server:  python -m demo vendors --chaos calm"))
        return 2
    return 0


def _control_once(line: str) -> int:
    cmd = " ".join(line.split())
    if cmd in ("help", "?"):
        ui.say(_CONTROL_HELP)
        return 0
    if cmd not in _CONTROL:
        ui.say(ui.red(f"unknown command {cmd!r}"))
        ui.say(_CONTROL_HELP)
        return 2
    try:
        if _CONTROL[cmd] is not None:
            client.post_json("/admin/control", _CONTROL[cmd])
    except client.VendorsDown as exc:
        ui.say(ui.red(f"{exc}. Start the IO server:  python -m demo vendors --chaos calm"))
        return 2
    return _control_show(include_ui=cmd == "ui")


def cmd_control(args) -> int:
    if args.words:
        return _control_once(" ".join(args.words))
    ui.say(ui.rule("control"))
    ui.say(ui.dim("   talks to the IO server. The pipeline keeps running while you type."))
    ui.say(_CONTROL_HELP)
    while True:
        try:
            line = input("control> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line in ("quit", "exit"):
            return 0
        _control_once(line)
    return 0


def cmd_reset(args) -> int:
    for path in STATE_DIR.glob("checkpoints.db*"):
        path.unlink()
    shutil.rmtree(OUTPUT, ignore_errors=True)
    try:
        client.post_sync("/reset")
        ledger = "vendor state cleared"
    except client.VendorsDown:
        ledger = "vendors not running, nothing to clear there"
    ui.say(ui.green(f"reset: checkpoints deleted, labels deleted, {ledger}"))
    return 0


COMMANDS = {
    "vendors": cmd_vendors, "run": cmd_run, "naive": cmd_naive, "triage": cmd_triage,
    "live": cmd_live, "control": cmd_control,
    "status": cmd_status, "trace": cmd_trace, "providers": cmd_providers, "llm": cmd_llm,
    "resume": cmd_resume, "heal": cmd_heal, "ui": cmd_ui, "ledger": cmd_ledger, "reset": cmd_reset,
}

if __name__ == "__main__":
    sys.exit(main())
