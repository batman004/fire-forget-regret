"""Act 3: the fix. Hold a strong reference, and await the task before exiting.

Same code as act 2, plus a set of background tasks (the pattern from the
asyncio docs). The garbage collector runs at the same moment, and the task
survives it. On shutdown we wait for what is still running.

For structured code, asyncio.TaskGroup does both for you.
"""

import asyncio
import gc
import weakref

pending_replies: weakref.WeakValueDictionary = weakref.WeakValueDictionary()
background_tasks: set[asyncio.Task] = set()


async def send_welcome_email(user: str) -> None:
    reply = asyncio.get_running_loop().create_future()
    pending_replies["smtp-1"] = reply
    print(f"  task:    sent email for {user}, waiting for SMTP '250 OK'...", flush=True)
    await reply
    print(f"  task:    ✓ welcome email delivered to {user}", flush=True)


def smtp_reply_arrives() -> None:
    waiter = pending_replies.get("smtp-1")
    if waiter is None:
        print("  network: '250 OK' arrived for smtp-1... nobody is waiting for it", flush=True)
    else:
        waiter.set_result("250 OK")


async def server() -> None:
    task = asyncio.create_task(send_welcome_email("grace@example.com"))
    background_tasks.add(task)                        # strong reference
    task.add_done_callback(background_tasks.discard)  # dropped when finished
    await asyncio.sleep(0)
    print(f"  server:  tasks alive: {len(asyncio.all_tasks())}", flush=True)

    print("  server:  ...memory pressure, the garbage collector runs...", flush=True)
    gc.collect()
    await asyncio.sleep(0.05)

    print(f"  server:  tasks alive: {len(asyncio.all_tasks())}", flush=True)
    asyncio.get_running_loop().call_later(0.3, smtp_reply_arrives)

    await asyncio.gather(*background_tasks)  # graceful shutdown: wait for them
    print("  server:  all background work finished, shutting down", flush=True)


def main() -> None:
    asyncio.run(server())


if __name__ == "__main__":
    main()
