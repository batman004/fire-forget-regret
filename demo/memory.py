"""Memory management for long-running pipelines (the 2-minute segment).

    python -m demo memory                       # 5,000 items of 20 KB
    python -m demo memory --items 20000 --kb 50

The same work (look at every item once) done four ways. Peak memory is measured
with tracemalloc, so the numbers are real Python allocations, not a guess:

  1. build a list of everything first        memory grows with the batch
  2. unbounded queue, fast producer          the queue IS the list, just hidden
  3. stream through a bounded queue          memory stays flat (this is what stitch does)
  4. bounded queue, but a global keeps results   flat pipeline, leaking state
"""

from __future__ import annotations

import asyncio
import gc
import time
import tracemalloc
from collections.abc import Iterator

import stitch

from . import ui


def blobs(count: int, kb: int) -> Iterator[dict]:
    """A generator: one item exists at a time unless someone holds on to it."""
    for i in range(count):
        yield {"id": f"item-{i}", "blob": bytes([i % 251]) * (kb * 1024)}


async def work(item: dict) -> int:
    await asyncio.sleep(0)  # stand-in for I/O
    return len(item["blob"])


async def eager_list(count, kb):
    items = list(blobs(count, kb))           # everything in memory before the first one is processed
    return [await work(i) for i in items]


async def unbounded_queue(count, kb):
    queue: asyncio.Queue = asyncio.Queue()   # no maxsize: the producer never has to wait

    async def produce():
        for item in blobs(count, kb):
            queue.put_nowait(item)           # runs to completion before the consumer gets a turn
        queue.put_nowait(None)

    producer = asyncio.create_task(produce())
    total = 0
    while (item := await queue.get()) is not None:
        total += await work(item)
    await producer
    return total


async def bounded_stream(count, kb):
    stats = await stitch.run_batch(
        "memory-demo", blobs(count, kb), work, key=lambda i: i["id"],
        store=stitch.NullStore(), concurrency=8)
    return stats.items_done


KEPT: list = []   # a "harmless" module-level list: a cache, a metrics buffer, a debug log


async def bounded_but_leaking(count, kb):
    async def handler(item):
        KEPT.append(item)                    # keeps every item alive after it was processed
        return await work(item)

    stats = await stitch.run_batch(
        "memory-demo-leak", blobs(count, kb), handler, key=lambda i: i["id"],
        store=stitch.NullStore(), concurrency=8)
    return stats.items_done


SCENARIOS = [
    ("1  list everything first", eager_list),
    ("2  unbounded queue", unbounded_queue),
    ("3  bounded stream (stitch)", bounded_stream),
    ("4  bounded, but a global keeps results", bounded_but_leaking),
]


def measure(fn, count: int, kb: int) -> tuple[float, float]:
    KEPT.clear()
    gc.collect()
    tracemalloc.start()
    tracemalloc.reset_peak()
    t0 = time.perf_counter()
    asyncio.run(fn(count, kb))
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    KEPT.clear()
    return peak / 2**20, elapsed


def run(count: int, kb: int) -> list[tuple[str, float, float]]:
    results = []
    ui.say(ui.rule(f"peak memory to process {count:,} items of {kb} KB "
                   f"({count * kb / 1024:,.0f} MB of payload in total)"))
    for name, fn in SCENARIOS:
        peak, elapsed = measure(fn, count, kb)
        results.append((name, peak, elapsed))
        ui.say(f"   {name:<42} {peak:8.1f} MiB   {elapsed:5.1f}s")
    ceiling = max(p for _, p, _ in results)
    ui.say()
    for name, peak, _ in results:
        bar = "█" * max(1, round(40 * peak / ceiling))
        paint = ui.green if peak < ceiling * 0.1 else ui.bold_red
        ui.say(f"   {name[:2]} {paint(bar)} {peak:.0f}")
    ui.say()
    ui.say(ui.dim("   Lesson: stream rather than build lists, bound every queue, and "
                  "don't let a cache or log quietly keep what you finished."))
    return results
