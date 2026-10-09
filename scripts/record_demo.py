"""Replay the whole live demo, the same way every time, for the backup recording.

    python scripts/record_demo.py                  # ~7 minutes, typed commands with pauses
    python scripts/record_demo.py --manual         # wait for Enter before each command (live narration)
    python scripts/record_demo.py --section triage # one part: hook | orders | triage | memory
    python scripts/record_demo.py --fast           # no typing or pauses (used by the tests)

Screen-record the terminal while it runs, or: asciinema rec -c "python scripts/record_demo.py" demo.cast
It starts its own fake vendors on a spare port with a throwaway state directory, so it never
touches your checkpoints or a vendors process you have running.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

BOLD, DIM, CYAN, RESET = (("", "", "", "") if "NO_COLOR" in os.environ
                         else ("\033[1m", "\033[2m", "\033[36m", "\033[0m"))

# (section, narration shown above the command, command)
SCRIPT: list[tuple[str, str, str]] = [
    ("hook", "Act 1: fire and forget. The caller exits, and the task silently disappears.", "python -m hook 1"),
    ("hook", "Act 2: the loop is alive, nobody holds the task, the garbage collector reclaims it.", "python -m hook 2"),
    ("hook", "Act 3: hold a reference and await it.", "python -m hook 3"),

    ("orders", "Order fulfilment with no checkpoints. Kill it at order 5, then run it again.",
     "python -m demo naive --orders 8 --crash-at order-0005:reserve_inventory"),
    ("orders", "Same command, no special flags.", "python -m demo naive --orders 8"),
    ("orders", "What did the vendors actually do?", "python -m demo ledger"),
    ("orders", "Now the same pipeline, checkpointed. Same crash.",
     "python -m demo reset && python -m demo run --orders 8 --crash-at order-0005:reserve_inventory"),
    ("orders", "Which steps are recorded?", "python -m demo status"),
    ("orders", "The exact same command again: it resumes where it stopped.",
     "python -m demo run --orders 8 --crash-at order-0005:reserve_inventory"),
    ("orders", "Every order charged exactly once.", "python -m demo ledger"),
    ("orders", "The honest caveat: die AFTER the charge but BEFORE the checkpoint, with no idempotency key.",
     "python -m demo reset && python -m demo run --orders 3 --no-idempotency "
     "--crash-at order-0002:capture_payment:after; python -m demo run --orders 3 --no-idempotency "
     "&& python -m demo ledger"),
    ("orders", "With the idempotency key, the provider replays the first charge.",
     "python -m demo reset && python -m demo run --orders 3 --crash-at order-0002:capture_payment:after; "
     "python -m demo run --orders 3 && python -m demo ledger"),

    ("triage", "Security triage. The LLM account is nearly out of credits.",
     "python -m demo reset && python -m demo llm topup eu-west-1 700"),
    ("triage", "Credits run out mid-batch. Events keep flowing with a labelled fallback.",
     "python -m demo triage --events 8"),
    ("triage", "What analysts see: nothing stale, fallbacks marked.", "python -m demo ui --of 8"),
    ("triage", "Why did event 5 degrade? The audit trail.", "python -m demo trace evt-0005"),
    ("triage", "Register a new region in real time.", "python -m demo providers add us-east-1 --priority 2"),
    ("triage", "Heal: redo only the degraded steps.", "python -m demo heal"),
    ("triage", "The dashboard now shows LLM analysis for every event.", "python -m demo ui --of 8"),

    ("memory", "Memory: the same work, four ways.", "python -m demo memory"),
]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def type_out(text: str, fast: bool) -> None:
    for ch in text:
        sys.stdout.write(ch)
        sys.stdout.flush()
        if not fast:
            time.sleep(0.025)
    print(flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--section", choices=["hook", "orders", "triage", "memory"], action="append")
    ap.add_argument("--fast", action="store_true", help="no typing delay or pauses")
    ap.add_argument("--manual", action="store_true", help="wait for Enter before each command")
    ap.add_argument("--pause", type=float, default=2.5, help="seconds to pause after each command")
    ap.add_argument("--chaos", default="flaky", choices=["calm", "flaky", "storm"])
    args = ap.parse_args()

    port = free_port()
    state = tempfile.mkdtemp(prefix="stitch-record-")
    env = {**os.environ, "VENDOR_URL": f"http://127.0.0.1:{port}", "DEMO_STATE_DIR": state,
           "PYTHONUNBUFFERED": "1"}
    vendors = subprocess.Popen(
        [PY, "-m", "demo", "vendors", "--port", str(port), "--chaos", args.chaos,
         "--latency", "0.02-0.05" if args.fast else "0.25-0.45"],
        cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        wanted = set(args.section or ["hook", "orders", "triage", "memory"])
        last = None
        for section, narration, command in SCRIPT:
            if section not in wanted:
                continue
            if section != last:
                print(f"\n{BOLD}{CYAN}══ {section} ══{RESET}\n", flush=True)
                last = section
            print(f"{DIM}# {narration}{RESET}", flush=True)
            if args.manual:
                input(f"{DIM}  [Enter]{RESET} ")
            elif not args.fast:
                time.sleep(1.0)
            sys.stdout.write(f"{BOLD}$ {RESET}")
            type_out(command, args.fast)
            # a crash exits 137 on purpose; keep going whatever a command returns
            subprocess.run(command, shell=True, cwd=ROOT, env=env)
            if not args.fast and not args.manual:
                time.sleep(args.pause)
            print(flush=True)
        return 0
    finally:
        vendors.terminate()
        try:
            vendors.wait(timeout=3)
        except subprocess.TimeoutExpired:
            vendors.kill()


if __name__ == "__main__":
    sys.exit(main())
