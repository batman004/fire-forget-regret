"""The workflow: plain-looking code that Temporal makes durable by replaying its history."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from . import activities as a

OPTIONS = dict(
    start_to_close_timeout=timedelta(seconds=6),   # also how long Temporal waits before it notices a dead worker
    retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=500), maximum_attempts=5),
)


@workflow.defn
class FulfilOrder:
    @workflow.run
    async def run(self, order_id: str) -> str:
        for step in (a.validate_order, a.fraud_check, a.capture_payment,
                     a.reserve_inventory, a.buy_shipping_label, a.send_confirmation):
            await workflow.execute_activity(step, order_id, **OPTIONS)
        return order_id
