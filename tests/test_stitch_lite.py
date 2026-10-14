import asyncio
import ast
import tempfile
import tokenize
import unittest
from pathlib import Path
from unittest import mock

import stitch_lite
from stitch_lite import Halted, Store, heal, run, step

LITE_DIRECTORY = Path(__file__).resolve().parents[1] / "stitch-lite"
MAXIMUM_CODE_LINES = 185


def count_code_lines(path: Path) -> int:
    source = path.read_text()
    docstring_lines = set()
    for node in ast.walk(ast.parse(source)):
        body = getattr(node, "body", None)
        if isinstance(body, list) and body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
            docstring_lines.update(range(body[0].lineno, body[0].end_lineno + 1))
    skipped_tokens = {tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER}
    with path.open() as file:
        code_lines = {token.start[0] for token in tokenize.generate_tokens(file.readline) if token.type not in skipped_tokens}
    return len(code_lines - docstring_lines)
FAST = {"retries": 3, "backoff": 0.001}


class ProcessKilled(BaseException):
    pass


class RetryableError(OSError):
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


class Recorder:
    def __init__(self):
        self.calls = []
        self.failures = {}
        self.resource_down = False
        self.alerts = []

    def record(self, step_name, item):
        self.calls.append((step_name, item))
        pending = self.failures.get((step_name, item))
        if pending:
            raise pending.pop(0)

    def check_resource(self):
        if self.resource_down:
            raise ConnectionError("still down")
        return True

    def alert(self, kind, message):
        self.alerts.append(kind)


recorder = Recorder()


def quick_fallback(item):
    return {"source": "fallback"}


@step(**FAST)
async def first_step(item):
    recorder.record("first", item)
    return {"first": item}


@step(**FAST)
async def second_step(item):
    recorder.record("second", item)
    return "second-result"


@step(**FAST, fallback=quick_fallback)
async def risky_step(item):
    recorder.record("risky", item)
    return {"source": "model"}


@step(**FAST, critical=recorder.check_resource)
async def critical_step(item):
    recorder.record("critical", item)
    if recorder.resource_down:
        raise ConnectionError("down")


async def two_steps(item):
    await first_step(item)
    await second_step(item)


async def with_fallback(item):
    await first_step(item)
    await risky_step(item)


async def with_critical(item):
    await first_step(item)
    await critical_step(item)


class LiteTestCase(unittest.TestCase):
    def setUp(self):
        recorder.calls.clear()
        recorder.failures.clear()
        recorder.alerts.clear()
        recorder.resource_down = False
        self.directory = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.directory.name) / "checkpoints.db")

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def run_items(self, items, handler=two_steps, workers=1):
        asyncio.run(run(handler, items, self.store, key=str, workers=workers, alert=recorder.alert))


class CheckpointTest(LiteTestCase):
    def test_a_killed_run_resumes_without_repeating_finished_steps(self):
        recorder.failures[("second", "b")] = [ProcessKilled()]
        with self.assertRaises(BaseException):
            self.run_items(["a", "b"])
        recorder.calls.clear()
        self.run_items(["a", "b"])
        self.assertEqual(recorder.calls, [("second", "b")])

    def test_a_finished_run_does_nothing_again(self):
        self.run_items(["a", "b"])
        recorder.calls.clear()
        self.run_items(["a", "b"])
        self.assertEqual(recorder.calls, [])

    def test_a_step_called_twice_in_one_item_is_stored_twice(self):
        async def handler(item):
            await first_step(item)
            await first_step(item)

        self.run_items(["a"], handler=handler)
        self.assertEqual(recorder.calls.count(("first", "a")), 2)

    def test_five_workers_finish_every_item_exactly_once(self):
        keys = [str(number) for number in range(50)]
        self.run_items(keys, workers=5)
        self.assertEqual(len(recorder.calls), 100)

    def test_a_plain_call_outside_a_run_just_runs(self):
        self.assertEqual(asyncio.run(first_step("x")), {"first": "x"})


class RetryTest(LiteTestCase):
    def test_a_transient_error_retries_only_that_step(self):
        recorder.failures[("second", "a")] = [RetryableError("503"), RetryableError("503")]
        self.run_items(["a"])
        self.assertEqual(recorder.calls.count(("first", "a")), 1)
        self.assertEqual(recorder.calls.count(("second", "a")), 3)

    def test_the_server_retry_after_hint_sets_the_wait(self):
        recorder.failures[("second", "a")] = [RetryableError("429", retry_after=7)]
        with mock.patch.object(stitch_lite.asyncio, "sleep", mock.AsyncMock()) as sleep:
            self.run_items(["a"])
        sleep.assert_awaited_once_with(7)

    def test_a_permanent_error_stops_only_its_item(self):
        recorder.failures[("first", "a")] = [ValueError("bad input")]
        self.run_items(["a", "b"])
        self.assertTrue(self.store.is_done("b"))
        self.assertFalse(self.store.is_done("a"))


class FallbackTest(LiteTestCase):
    def test_exhausted_retries_store_a_flagged_fallback_and_heal_replaces_it(self):
        recorder.failures[("risky", "a")] = [RetryableError("down")] * 3
        self.run_items(["a"], handler=with_fallback)
        self.assertEqual(self.store.items_with_fallback(), ["a"])
        recorder.calls.clear()
        asyncio.run(heal(with_fallback, self.store, key=str, alert=recorder.alert))
        self.assertEqual(recorder.calls, [("risky", "a")])
        self.assertEqual(self.store.items_with_fallback(), [])
        self.assertTrue(self.store.is_done("a"))


class CriticalTest(LiteTestCase):
    def halt_the_run(self):
        recorder.resource_down = True
        with self.assertRaises(Halted):
            self.run_items(["a", "b", "c"], handler=with_critical, workers=3)

    def test_a_critical_step_halts_the_run_and_alerts_once(self):
        self.halt_the_run()
        self.assertEqual(recorder.alerts, ["halted"])

    def test_the_run_stays_blocked_while_the_resource_is_down(self):
        self.halt_the_run()
        recorder.calls.clear()
        with self.assertRaises(Halted):
            self.run_items(["a", "b", "c"], handler=with_critical)
        self.assertEqual(recorder.calls, [])
        self.assertEqual(recorder.alerts, ["halted"])

    def test_the_run_resumes_when_healthy_without_repeating_finished_steps(self):
        self.halt_the_run()
        recorder.resource_down = False
        recorder.calls.clear()
        self.run_items(["a", "b", "c"], handler=with_critical)
        self.assertEqual(sorted(recorder.calls), [("critical", "a"), ("critical", "b"), ("critical", "c")])
        self.assertEqual(recorder.alerts, ["halted", "recovered"])


class SizeTest(unittest.TestCase):
    def test_the_core_stays_small(self):
        code_lines = count_code_lines(LITE_DIRECTORY / "stitch_lite.py")
        print(f"\nstitch_lite.py: {code_lines} code lines")
        self.assertLessEqual(code_lines, MAXIMUM_CODE_LINES)


if __name__ == "__main__":
    unittest.main()
