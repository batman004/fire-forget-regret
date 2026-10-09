"""Enqueue orders onto the Celery pipelines.

    python -m comparisons.celery_compare.submit one 10      # one task per order
    python -m comparisons.celery_compare.submit chain 10    # one task per step
"""

import sys

from demo.orders import order_id

from .tasks import fulfil_chain, fulfil_order


def main(style: str, count: int) -> None:
    for n in range(1, count + 1):
        oid = order_id(n)
        if style == "one":
            fulfil_order.delay(oid)
        elif style == "chain":
            fulfil_chain(oid).apply_async()
        else:
            raise SystemExit("style must be 'one' or 'chain'")
    print(f"queued {count} orders ({style}). Watch the worker, then: python -m demo ledger")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "one", int(sys.argv[2]) if len(sys.argv) > 2 else 10)
