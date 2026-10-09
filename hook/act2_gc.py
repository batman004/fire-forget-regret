"""Act 2: the documented bug. The event loop is alive, the task still vanishes.

The asyncio docs: "Save a reference to the result of this function, to avoid a
task disappearing mid-execution. The event loop only keeps weak references to
tasks."

A sleeping task is safe (the loop's timer holds it). A task waiting on a
future that nothing else holds strongly is not: the task and its future form
a cycle that nobody points at, and the garbage collector reclaims both.

Here the future sits in a WeakValueDictionary of pending replies, the way an
RPC client might track in-flight requests. In production nothing forces the
collection; it happens whenever allocation pressure triggers a GC pass.
"""

import asyncio
import gc
import weakref

pending_replies: weakref.WeakValueDictionary = weakref.WeakValueDictionary()


async def send_welcome_email(user: str) -> None:
    reply = asyncio.get_running_loop().create_future()
    pending_replies["smtp-1"] = reply  # only a weak reference
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
    asyncio.create_task(send_welcome_email("grace@example.com"))  # no reference kept
    await asyncio.sleep(0)  # let the task start and block on its reply
    print(f"  server:  tasks alive: {len(asyncio.all_tasks())}", flush=True)

    print("  server:  ...memory pressure, the garbage collector runs...", flush=True)
    gc.collect()
    await asyncio.sleep(0.05)  # let the warning reach stderr

    print(f"  server:  tasks alive: {len(asyncio.all_tasks())}", flush=True)
    asyncio.get_running_loop().call_later(0.3, smtp_reply_arrives)
    await asyncio.sleep(0.5)
    print("  server:  still up and serving. The task is just gone.", flush=True)


def main() -> None:
    asyncio.run(server())


if __name__ == "__main__":
    main()
