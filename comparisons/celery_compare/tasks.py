"""The order-fulfilment pipeline on Celery + RabbitMQ, in the two ways teams write it.

Run from the repository root (so `demo` and `stitch` are importable):

    docker compose -f comparisons/celery_compare/docker-compose.yml up -d
    comparisons/celery_compare/.venv/bin/celery -A comparisons.celery_compare.tasks worker -l info -c 1
    comparisons/celery_compare/.venv/bin/python -m comparisons.celery_compare.submit one 10     # style 1
    comparisons/celery_compare/.venv/bin/python -m comparisons.celery_compare.submit chain 10   # style 2

Style 1, "one task per order". The job is the unit of retry. A 429 from the carrier
in step 5 retries the whole task, which charges the card again.

Style 2, "a chain of tasks". Every step is its own message, so a retry or a killed worker
only repeats that step. That is a real improvement, and it is checkpointing of a kind: the
queue remembers where each order is. What it still lacks, and what the stitch side has:

  * no record of what happened (the broker forgets a message once it is acked)
  * no skip-if-done: a step redelivered after its side effect ran does the work again
    (we send no idempotency key here, to show it; add one and it is safe)
  * no provider fallback, breaker, or degraded result; you build those yourself
  * results travel inside the messages, and the result backend is a second system
  * no way to say "continue everything that was unfinished" except what is left in queues

Both styles call the SAME fake vendors as the stitch demo, so `python -m demo ledger`
shows exactly what Celery did to the vendors.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from celery import Celery, chain
from celery.exceptions import Reject  # noqa: F401  (documented escape hatch)

from demo import client, orders
from demo.pipeline import STEPS as _STEPS  # noqa: F401
from stitch import PermanentError, TransientError

BROKER = os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@127.0.0.1:5672//")

app = Celery("fulfilment", broker=BROKER)
app.conf.update(
    task_acks_late=True,                 # ack after the task finishes, so a killed worker's task is redelivered
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_default_queue="fulfilment",
)

CRASH_MARKER = Path(os.environ.get("CELERY_CRASH_MARKER", "/tmp/celery-crash-marker"))


def maybe_crash(order_id: str, step: str, phase: str = "before") -> None:
    """Same idea as stitch's --crash-at: SIGKILL the worker once at CRASH_AT=order:step[:after].

    "after" means the vendor did the work but the message was not acknowledged yet.
    """
    spec = os.environ.get("CRASH_AT", "").split(":")
    wanted = (spec + ["before"])[:3] if len(spec) == 2 else spec
    if wanted == [order_id, step, phase] and not CRASH_MARKER.exists():
        CRASH_MARKER.write_text("crashed")
        sys.stdout.flush()
        os.kill(os.getpid(), 9)


def call(path: str, payload: dict) -> dict:
    """Synchronous vendor call, errors mapped like the stitch demo does."""
    try:
        return client._post(path, payload, {"Content-Type": "application/json"}, client.TIMEOUT)
    except (TransientError, PermanentError):
        raise


# -- the six steps as plain functions (no framework) ---------------------------------------

def validate(order):  return {"valid": True}
def fraud(order):     return call("/fraud/check", {"order_id": order["id"], "email": order["email"], "amount": order["amount"]})
def pay(order):       return call("/payments/charges", {"order_id": order["id"], "amount": order["amount"], "currency": "EUR"})
def reserve(order):   return call("/warehouse/reservations", {"order_id": order["id"], "sku": order["sku"], "qty": order["qty"]})
def label(order):     return call("/carrier/labels", {"order_id": order["id"], "name": order["customer"], "city": order["city"]})
def email(order):     return call("/email/messages", {"order_id": order["id"], "to": order["email"], "template": "order-shipped"})


# -- style 1: one task per order -----------------------------------------------------------

@app.task(bind=True, autoretry_for=(TransientError,), retry_backoff=1, retry_backoff_max=4,
          retry_jitter=True, max_retries=3)
def fulfil_order(self, order_id: str) -> str:
    order = orders.lookup(order_id)
    for name, fn in [("validate_order", validate), ("fraud_check", fraud), ("capture_payment", pay),
                     ("reserve_inventory", reserve), ("buy_shipping_label", label),
                     ("send_confirmation", email)]:
        maybe_crash(order_id, name)
        fn(order)
        maybe_crash(order_id, name, "after")
        print(f"{order_id} {name} ok (attempt {self.request.retries + 1})", flush=True)
    return order_id


# -- style 2: a chain, one task per step ---------------------------------------------------

def step_task(name, fn):
    @app.task(name=f"fulfilment.{name}", bind=True, autoretry_for=(TransientError,),
              retry_backoff=1, retry_backoff_max=4, retry_jitter=True, max_retries=5)
    def run(self, order_id: str) -> str:
        maybe_crash(order_id, name)
        fn(orders.lookup(order_id))
        maybe_crash(order_id, name, "after")
        print(f"{order_id} {name} ok (attempt {self.request.retries + 1})", flush=True)
        return order_id          # the next link receives only the id; state lives in the vendors
    return run


STEP_TASKS = [step_task(n, f) for n, f in [
    ("validate_order", validate), ("fraud_check", fraud), ("capture_payment", pay),
    ("reserve_inventory", reserve), ("buy_shipping_label", label), ("send_confirmation", email)]]


def fulfil_chain(order_id: str):
    return chain(*[t.s(order_id) if i == 0 else t.s() for i, t in enumerate(STEP_TASKS)])
