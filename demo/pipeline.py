"""Order fulfilment: six steps, five third-party vendors.

    validate_order      local          cheap, safe to repeat
    fraud_check         risk API       slow, flaky
    capture_payment     payment API    moves money: repeating it is the regret
    reserve_inventory   warehouse API  503s under load
    buy_shipping_label  carrier API    rate limited, big payload, costs money
    send_confirmation   email API      customers notice duplicates

The same `fulfil` function runs in both modes. With a Store, every step is
checkpointed and retried on its own. With a NullStore, nothing is remembered
and a failure restarts the whole order, like a job on a task queue.
"""

from __future__ import annotations

import asyncio
import base64
import os
from pathlib import Path

from stitch import PermanentError, Retry, step, workflow

from . import client

# Checkpoints and labels live here. Tests point it at a temp dir.
STATE_DIR = Path(os.environ.get("DEMO_STATE_DIR", Path(__file__).parent))
LABEL_DIR = STATE_DIR / "output" / "labels"

# step name -> short label for tables, in pipeline order
STEPS = {
    "validate_order": "valid",
    "fraud_check": "fraud",
    "capture_payment": "pay",
    "reserve_inventory": "stock",
    "buy_shipping_label": "label",
    "send_confirmation": "email",
}

VENDOR_CALL = Retry(attempts=5, base=0.3, cap=3.0)


@step(retry=Retry(attempts=1))
async def validate_order(order: dict) -> dict:
    await asyncio.sleep(0.15)  # schema checks, address normalisation, tax lookup...
    if order["amount"] <= 0:
        raise PermanentError(f"invalid amount {order['amount']}")
    return {"valid": True}


@step(retry=VENDOR_CALL, timeout=8)
async def fraud_check(order: dict) -> dict:
    risk = await client.post("/fraud/check", {
        "order_id": order["id"], "email": order["email"], "amount": order["amount"],
    })
    if risk["decision"] != "approve":
        raise PermanentError(f"declined by fraud check (score {risk['score']})")
    return {"score": risk["score"]}


@step(retry=VENDOR_CALL, timeout=8)
async def capture_payment(order: dict) -> dict:
    charge = await client.post("/payments/charges", {
        "order_id": order["id"], "amount": order["amount"], "currency": "EUR",
    })
    return {"charge_id": charge["charge_id"]}


@step(retry=VENDOR_CALL, timeout=8)
async def reserve_inventory(order: dict) -> dict:
    reservation = await client.post("/warehouse/reservations", {
        "order_id": order["id"], "sku": order["sku"], "qty": order["qty"],
    })
    return {"reservation_id": reservation["reservation_id"]}


@step(retry=Retry(attempts=6, base=0.3, cap=3.0), timeout=12)
async def buy_shipping_label(order: dict) -> dict:
    label = await client.post("/carrier/labels", {
        "order_id": order["id"], "name": order["customer"], "city": order["city"],
    })
    path = save_label(order["id"], base64.b64decode(label["label_pdf"]))
    # Checkpoint the path, not the 48 KB PDF: keep checkpoints small.
    return {"tracking": label["tracking"], "label": str(path)}


@step(retry=VENDOR_CALL, timeout=8)
async def send_confirmation(order: dict, charge: dict, label: dict) -> dict:
    message = await client.post("/email/messages", {
        "order_id": order["id"], "to": order["email"], "template": "order-shipped",
        "charge_id": charge["charge_id"], "tracking": label["tracking"],
    })
    return {"message_id": message["message_id"]}


@workflow(name="fulfilment", key=lambda order: order["id"])
async def fulfil(order: dict) -> None:
    await validate_order(order)
    await fraud_check(order)
    charge = await capture_payment(order)
    await reserve_inventory(order)
    label = await buy_shipping_label(order)
    await send_confirmation(order, charge, label)


def save_label(order_id: str, pdf: bytes) -> Path:
    """Write-then-rename: a crash leaves either the old file or the new one, never half."""
    LABEL_DIR.mkdir(parents=True, exist_ok=True)
    final = LABEL_DIR / f"{order_id}.pdf"
    tmp = final.with_suffix(".pdf.tmp")
    tmp.write_bytes(pdf)
    os.replace(tmp, final)
    return final
