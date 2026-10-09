"""The opening hook: three ways a background task lives or dies.

    python -m hook        # all three acts, Enter between them
    python -m hook 2      # just one act

Each act gets its own asyncio.run(), so its own event loop. If they shared
one loop, act 1's orphaned task would not be cancelled; it would finish
quietly during act 2 and ruin the point.
"""

import sys

from . import act1_orphan, act2_gc, act3_fix

ACTS = {
    "1": ("Act 1 · fire and forget, then the caller exits", act1_orphan.main),
    "2": ("Act 2 · no reference held, the loop is alive, the GC runs", act2_gc.main),
    "3": ("Act 3 · hold a reference, await it", act3_fix.main),
}


def main(argv: list[str]) -> None:
    chosen = argv or list(ACTS)
    pause = len(chosen) > 1 and sys.stdin.isatty()
    for i, key in enumerate(chosen):
        title, run = ACTS[key]
        print(f"\n\033[1m━━ {title}\033[0m\n" if sys.stdout.isatty() else f"\n━━ {title}\n", flush=True)
        run()
        if pause and i < len(chosen) - 1:
            input("\n  [Enter for the next act] ")


if __name__ == "__main__":
    main(sys.argv[1:])
