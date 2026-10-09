import asyncio
import tempfile
import unittest
from pathlib import Path

from stitch import (
    NullStore, PermanentError, Retry, Store, TransientError, idempotency_key, run_batch, step,
)


class Calls:
    def __init__(self):
        self.log: list[tuple[str, str]] = []
        self.fail: dict[tuple[str, str], list[Exception]] = {}

    def hit(self, name: str, item: str):
        self.log.append((name, item))
        errors = self.fail.get((name, item))
        if errors:
            raise errors.pop(0)


calls = Calls()
fast = Retry(attempts=3, base=0.001, cap=0.001)


@step(retry=fast)
async def first(item):
    calls.hit("first", item)
    return {"first": item}


@step(retry=fast)
async def second(item):
    calls.hit("second", item)
    return {"key": idempotency_key()}


async def handler(item):
    await first(item)
    await second(item)


def run(store, items=("a", "b"), **kw):
    return asyncio.run(run_batch("t", list(items), handler, key=str, store=store, **kw))


class EngineTest(unittest.TestCase):
    def setUp(self):
        calls.log.clear()
        calls.fail.clear()
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "c.db"

    def tearDown(self):
        self.tmp.cleanup()

    def test_resume_skips_completed_steps(self):
        calls.fail[("second", "b")] = [PermanentError("boom")]
        store = Store(self.db)
        stats = run(store)
        self.assertEqual(stats.items_failed, 1)
        calls.log.clear()

        stats = run(Store(self.db))
        self.assertEqual(calls.log, [("second", "b")])  # a skipped, b resumed at step 2
        self.assertEqual((stats.items_skipped, stats.items_done, stats.steps_skipped), (1, 1, 1))

    def test_finished_run_does_nothing_again(self):
        run(Store(self.db))
        calls.log.clear()
        stats = run(Store(self.db))
        self.assertEqual(calls.log, [])
        self.assertEqual(stats.items_skipped, 2)

    def test_step_retries_transient_errors(self):
        calls.fail[("second", "a")] = [TransientError("503"), TransientError("503")]
        stats = run(Store(self.db), items=["a"])
        self.assertEqual(stats.retries, 2)
        self.assertEqual(calls.log.count(("first", "a")), 1)  # only the failing step repeats

    def test_idempotency_key_is_stable_across_runs(self):
        run(Store(self.db), items=["a"])
        key = Store(self.db).get_step("t:default", "a", "second").result["key"]
        self.assertEqual(key, "t:default/a/second")

    def test_naive_mode_restarts_whole_item(self):
        calls.fail[("second", "a")] = [TransientError("503")]
        stats = run(NullStore(), items=["a"], item_retry=fast)
        self.assertEqual(calls.log, [("first", "a"), ("second", "a"), ("first", "a"), ("second", "a")])
        self.assertEqual(stats.item_retries, 1)

    def test_naive_mode_sends_no_idempotency_key(self):
        captured = []

        @step
        async def grab(item):
            captured.append(idempotency_key())

        asyncio.run(run_batch("t", ["a"], grab, key=str, store=NullStore()))
        self.assertEqual(captured, [None])

    def test_repeated_step_gets_distinct_ids(self):
        async def twice(item):
            await first(item)
            await first(item)

        store = Store(self.db)
        asyncio.run(run_batch("t", ["a"], twice, key=str, store=store))
        self.assertEqual([s.step for s in store.steps("t:default")], ["first", "first#2"])

    def test_concurrency_processes_everything(self):
        stats = run(Store(self.db), items=[str(i) for i in range(50)], concurrency=8)
        self.assertEqual(stats.items_done, 50)
        self.assertEqual(len(calls.log), 100)


if __name__ == "__main__":
    unittest.main()
