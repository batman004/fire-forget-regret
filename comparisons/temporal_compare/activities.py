"""The six order-fulfilment steps as Temporal activities, calling the same fake vendors.

An activity is Temporal's name for a step with side effects. Temporal records each
activity's result in the workflow's event history; when the worker dies, the workflow
resumes by replaying that history and skipping what already completed.

Activities are still at-least-once: if the worker dies after the vendor charged but before
Temporal recorded the result, the activity runs again. So even here you need idempotency
keys. Set IDEMPOTENT=1 to send one (workflow id + activity id).
"""

import asyncio
import os
import sys
from pathlib import Path

from temporalio import activity

from demo import client, orders

CRASH_MARKER = Path(os.environ.get("TEMPORAL_CRASH_MARKER", "/tmp/temporal-crash-marker"))


def maybe_crash(order_id: str, step: str, phase: str) -> None:
    spec = os.environ.get("CRASH_AT", "").split(":")
    wanted = (spec + ["before"])[:3] if len(spec) == 2 else spec
    if wanted == [order_id, step, phase] and not CRASH_MARKER.exists():
        CRASH_MARKER.write_text("crashed")
        sys.stdout.flush()
        os.kill(os.getpid(), 9)


async def call(path: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    if os.environ.get("IDEMPOTENT") == "1":
        info = activity.info()
        headers["Idempotency-Key"] = f"{info.workflow_id}/{info.activity_id}"
    return await asyncio.to_thread(client._post, path, payload, headers, client.TIMEOUT)


async def run_step(name: str, order_id: str, path: str, build) -> str:
    maybe_crash(order_id, name, "before")
    await call(path, build(orders.lookup(order_id)))
    maybe_crash(order_id, name, "after")
    activity.logger.info("%s %s ok (attempt %s)", order_id, name, activity.info().attempt)
    return order_id


@activity.defn
async def validate_order(oid: str) -> str:
    await asyncio.sleep(0.15)
    return oid


@activity.defn
async def fraud_check(oid: str) -> str:
    return await run_step("fraud_check", oid, "/fraud/check",
                          lambda o: {"order_id": o["id"], "email": o["email"], "amount": o["amount"]})


@activity.defn
async def capture_payment(oid: str) -> str:
    return await run_step("capture_payment", oid, "/payments/charges",
                          lambda o: {"order_id": o["id"], "amount": o["amount"], "currency": "EUR"})


@activity.defn
async def reserve_inventory(oid: str) -> str:
    return await run_step("reserve_inventory", oid, "/warehouse/reservations",
                          lambda o: {"order_id": o["id"], "sku": o["sku"], "qty": o["qty"]})


@activity.defn
async def buy_shipping_label(oid: str) -> str:
    return await run_step("buy_shipping_label", oid, "/carrier/labels",
                          lambda o: {"order_id": o["id"], "name": o["customer"], "city": o["city"]})


@activity.defn
async def send_confirmation(oid: str) -> str:
    return await run_step("send_confirmation", oid, "/email/messages",
                          lambda o: {"order_id": o["id"], "to": o["email"], "template": "order-shipped"})


ALL = [validate_order, fraud_check, capture_payment, reserve_inventory,
       buy_shipping_label, send_confirmation]
