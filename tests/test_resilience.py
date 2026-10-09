import asyncio
import tempfile
import time
import unittest
from pathlib import Path

import stitch
from stitch.flow import _registry
from stitch import (
    Defaults, Pool, ProviderDown, QuotaExhausted, RateLimited, Retry, Store, step, workflow,
)

TUNING = Defaults(failure_threshold=2, cooldown=0.2, max_cooldown=0.4,
                  quota_cooldown=0.3, rate_limit_cooldown=0.1)


class Client:
    """A fake provider whose behaviour the test controls."""

    def __init__(self, name, config):
        self.name = name
        self.calls = 0
        self.mode = "ok"  # ok | quota | down | limited

    async def complete(self, text):
        self.calls += 1
        if self.mode == "quota":
            raise QuotaExhausted("insufficient credits")
        if self.mode == "down":
            raise ProviderDown("region unavailable")
        if self.mode == "limited":
            raise RateLimited("slow down", retry_after=0.1)
        return f"{self.name}:{text}"


class ResilienceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / "s.db")
        self.clients: dict[str, Client] = {}

        def factory(name, config):
            return self.clients.setdefault(name, Client(name, config))

        self.pool = Pool("llm", factory, tuning=TUNING,
                         defaults=[("primary", 1, {}), ("secondary", 2, {})])
        self.pool.seed_defaults(self.store)
        self.clients["primary"] = Client("primary", {})
        self.clients["secondary"] = Client("secondary", {})

        stitch.engine._detached_store = self.store  # pool called outside a workflow

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()
        stitch.engine._detached_store = stitch.NullStore()

    def call(self, text="x"):
        return asyncio.run(self.pool.call(lambda c: c.complete(text)))

    def test_uses_primary_when_healthy(self):
        self.assertEqual(self.call(), "primary:x")

    def test_quota_exhaustion_fails_over_and_opens_breaker(self):
        self.clients["primary"].mode = "quota"
        self.assertEqual(self.call(), "secondary:x")
        self.assertEqual(self.store.get_breaker("llm", "primary").state, "open")
        calls = self.clients["primary"].calls
        self.assertEqual(self.call(), "secondary:x")
        self.assertEqual(self.clients["primary"].calls, calls)  # not even tried: it is cooling

    def test_breaker_probes_after_cooldown_and_closes(self):
        self.clients["primary"].mode = "quota"
        self.call()
        self.clients["primary"].mode = "ok"  # credits topped up
        time.sleep(0.35)
        self.assertEqual(self.call(), "primary:x")
        self.assertEqual(self.store.get_breaker("llm", "primary").state, "closed")

    def test_failed_probe_reopens_with_longer_cooldown(self):
        self.clients["primary"].mode = "down"
        for _ in range(TUNING.failure_threshold):
            self.call()
        first = self.store.get_breaker("llm", "primary")
        self.assertEqual(first.state, "open")
        time.sleep(0.25)
        self.call()  # probe fails
        second = self.store.get_breaker("llm", "primary")
        self.assertEqual(second.opens, first.opens + 1)
        self.assertGreater(second.retry_at - time.time(), 0.25)

    def test_no_provider_available_reports_when_to_retry(self):
        self.clients["primary"].mode = "quota"
        self.clients["secondary"].mode = "quota"
        with self.assertRaises(stitch.NoProviderAvailable) as ctx:
            self.call()
        self.assertGreater(ctx.exception.retry_at, time.time())

    def test_registering_a_provider_at_runtime(self):
        self.clients["primary"].mode = "quota"
        self.clients["secondary"].mode = "quota"
        with self.assertRaises(stitch.NoProviderAvailable):
            self.call()
        self.pool.register(self.store, "us-west", 2, {"region": "us-west"})
        self.assertEqual(self.call(), "us-west:x")

    def test_rate_limit_is_a_short_cooldown(self):
        self.clients["primary"].mode = "limited"
        self.assertEqual(self.call(), "secondary:x")
        self.clients["primary"].mode = "ok"
        time.sleep(0.15)
        self.assertEqual(self.call(), "primary:x")


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "s.db"
        self.healthy = False
        self.published: list[tuple[str, str]] = []
        test = self

        def factory(name, config):
            return Client(name, config)

        self.pool = Pool("llm", factory, tuning=TUNING, defaults=[("only", 1, {})])

        async def summarise_call(c, text):
            if not test.healthy:
                raise QuotaExhausted("insufficient credits")
            return await c.complete(text)

        @step(degrade=lambda event: {"summary": "rule-based", "source": "degraded"})
        async def summarise(event):
            text = await test.pool.call(lambda c: summarise_call(c, event["id"]))
            return {"summary": text, "source": "llm"}

        @step
        async def publish(event, summary):
            test.published.append((event["id"], summary["source"]))
            return {"ok": True}

        @workflow(name="triage-test", key=lambda e: e["id"])
        async def triage(event):
            summary = await summarise(event)
            await publish(event, summary)
            return summary["source"]

        self.triage = triage

    def tearDown(self):
        self.tmp.cleanup()
        _registry.pop("triage-test", None)

    def test_degrade_publishes_fallback_then_heal_replaces_it(self):
        store = Store(self.db)
        stats = asyncio.run(self.triage.map([{"id": "e1"}, {"id": "e2"}], store=store))
        self.assertEqual((stats.items_done, stats.items_degraded, stats.steps_degraded), (2, 2, 2))
        self.assertEqual(self.published, [("e1", "degraded"), ("e2", "degraded")])

        self.healthy = True
        time.sleep(0.35)  # let the breaker's cooldown pass
        healed = asyncio.run(stitch.heal(store=store))
        (stats,) = healed.values()
        self.assertEqual((stats.items_done, stats.items_degraded), (2, 0))
        self.assertEqual(self.published[2:], [("e1", "llm"), ("e2", "llm")])  # upsert, in order
        self.assertEqual(store.degraded_items("triage-test:default"), [])

    def test_without_degrade_the_item_parks_and_resumes_after_cooldown(self):
        @step
        async def strict(event):
            return await self.pool.call(lambda c: c.complete(event["id"]))

        @workflow(name="strict-test", key=lambda e: e["id"])
        async def strict_flow(event):
            return await strict(event)

        self.pool.tuning = TUNING

        async def go():
            store = Store(self.db)
            client_ok = {"v": False}
            orig = Client.complete

            async def flaky(self_, text):
                if not client_ok["v"]:
                    raise QuotaExhausted("insufficient credits")
                return await orig(self_, text)

            Client.complete = flaky
            try:
                async def recover():
                    await asyncio.sleep(0.2)
                    client_ok["v"] = True

                recovery = asyncio.create_task(recover())
                stats = await strict_flow.map([{"id": "a"}], store=store, wait_parked=True)
                await recovery
                return stats, store
            finally:
                Client.complete = orig

        try:
            stats, store = asyncio.run(go())
        finally:
            _registry.pop("strict-test", None)
        self.assertEqual((stats.items_parked, stats.items_done, stats.parked_left), (1, 1, 0))
        events = [r.event for r in store.audit_rows(item="a")]
        self.assertIn("item_parked", events)
        self.assertIn("breaker_opened", events)
        self.assertIn("cooldown_over", [r.event for r in store.audit_rows()])  # run-level event

    def test_resume_restarts_unfinished_items_from_stored_input(self):
        

        store = Store(self.db)
        self.healthy = True
        # simulate a crash: item started, summarise done, publish never ran
        store.open_run("triage-test:default", "triage-test", {})
        store.set_item("triage-test:default", "e9", "running", input='{"id": "e9"}')
        store.step_started("triage-test:default", "e9", "summarise", 1)
        store.step_done("triage-test:default", "e9", "summarise",
                        {"summary": "kept", "source": "llm"}, 1)
        results = asyncio.run(stitch.resume(store=store))
        (stats,) = results.values()
        self.assertEqual((stats.items_done, stats.steps_skipped, stats.steps_run), (1, 1, 1))
        self.assertEqual(self.published, [("e9", "llm")])

    def test_workflow_call_returns_checkpointed_result(self):
        self.healthy = True
        store = Store(self.db)
        stitch.configure(store)
        first = asyncio.run(self.triage({"id": "z"}))
        second = asyncio.run(self.triage({"id": "z"}))
        self.assertEqual((first, second), ("llm", "llm"))
        self.assertEqual(len(self.published), 1)  # the second call did nothing

    def test_audit_log_tells_the_story(self):
        store = Store(self.db)
        asyncio.run(self.triage.map([{"id": "q"}], store=store))
        rows = store.audit_rows(item="q")
        events = [r.event for r in rows]
        for expected in ("item_started", "step_started", "provider_failed", "breaker_opened",
                         "step_degraded", "step_done", "item_done"):
            self.assertIn(expected, events)
        text = stitch.render_trace(rows)
        self.assertIn("breaker OPEN", text)
        self.assertIn("used its fallback", text)


if __name__ == "__main__":
    unittest.main()
