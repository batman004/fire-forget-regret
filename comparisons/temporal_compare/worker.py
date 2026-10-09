"""Run a Temporal worker:  python -m comparisons.temporal_compare.worker"""

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from .activities import ALL
from .workflows import FulfilOrder

TASK_QUEUE = "fulfilment"


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect("127.0.0.1:7233")
    # One activity at a time, like `celery -c 1` and `stitch --concurrency 1`, so a kill
    # interrupts one step rather than every order that happens to be mid-call.
    await Worker(client, task_queue=TASK_QUEUE, workflows=[FulfilOrder], activities=ALL,
                 max_concurrent_activities=int(os.environ.get("ACTIVITY_CONCURRENCY", "1"))).run()


if __name__ == "__main__":
    asyncio.run(main())
