"""Start N order workflows and wait for them: python -m comparisons.temporal_compare.start 6"""

import asyncio
import sys

from temporalio.client import Client

from demo.orders import order_id

from .workflows import FulfilOrder


async def main(count: int) -> None:
    client = await Client.connect("127.0.0.1:7233")
    handles = [await client.start_workflow(FulfilOrder.run, order_id(n), id=order_id(n),
                                           task_queue="fulfilment") for n in range(1, count + 1)]
    print(f"started {count} workflows (the workflow id is the order id, so starting twice is a no-op)")
    for h in handles:
        await h.result()
    print("all workflows completed")


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]) if len(sys.argv) > 1 else 6))
