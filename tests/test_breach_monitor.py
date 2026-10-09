"""Offline tests for examples/breach_monitor/pipeline.py: no network, no Mongo, no RabbitMQ, no LLM."""

import asyncio
import importlib.util
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import stitch
from stitch import PermanentError, QuotaExhausted, TransientError

PATH = Path(__file__).resolve().parents[1] / "examples" / "breach_monitor" / "pipeline.py"
spec = importlib.util.spec_from_file_location("breach_pipeline", PATH)
pl = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = pl   # dataclasses resolve annotations through sys.modules
spec.loader.exec_module(pl)

RSS = b"""<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Acme Health confirms data breach exposing 2.1 million patients</title>
<link>https://news.example/acme?utm_source=x</link><pubDate>Thu, 08 Oct 2026 10:00:00 GMT</pubDate>
<description>&lt;p&gt;Ransomware group stole &lt;b&gt;patient records&lt;/b&gt;, including health data.&lt;/p&gt;</description></item>
<item><title>Ten tips for better meetings</title><link>https://news.example/meetings</link></item>
</channel></rss>"""

ATOM = b"""<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>Leak on r/netsec</title><link href="https://reddit.example/p/1"/>
<updated>2026-10-08T09:00:00+00:00</updated><content type="html">&lt;p&gt;credentials leaked&lt;/p&gt;</content></entry></feed>"""

EVIL = b"""<?xml version="1.0"?><!DOCTYPE lolz [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;&a;">]><rss><channel>
<item><title>&b;</title><link>https://x.example/1</link></item></channel></rss>"""


class ParsingTest(unittest.TestCase):
    def test_rss_and_atom(self):
        rss = pl.parse_feed(RSS, "t")
        self.assertEqual(len(rss), 2)
        self.assertEqual(rss[0]["published"][:10], "2026-10-08")
        self.assertNotIn("<", rss[0]["summary"])
        atom = pl.parse_feed(ATOM, "t")
        self.assertEqual(atom[0]["url"], "https://reddit.example/p/1")

    def test_feed_declaring_entities_is_refused(self):
        with self.assertRaises(PermanentError):
            pl.parse_feed(EVIL, "t")

    def test_ids_ignore_tracking_params_and_fragments(self):
        self.assertEqual(pl.item_id("https://A.example/x/?utm_source=a#top"), pl.item_id("https://a.example/x"))
        self.assertNotEqual(pl.item_id("https://a.example/x"), pl.item_id("https://a.example/y"))

    def test_relevance_filter_respects_window_and_keywords(self):
        now = datetime.now(timezone.utc)
        fresh = {"title": "Retailer data breach", "summary": "", "published": now.isoformat()}
        stale = {**fresh, "published": (now - timedelta(days=9)).isoformat()}
        dull = {"title": "Meeting tips", "summary": "", "published": now.isoformat()}
        since = now - timedelta(days=2)
        self.assertTrue(pl.relevant(fresh, since))
        self.assertFalse(pl.relevant(stale, since))
        self.assertFalse(pl.relevant(dull, since))

    def test_html_to_text_drops_chrome_and_scripts(self):
        page = ("<html><nav>menu menu menu menu menu menu menu menu menu menu</nav><script>evil()</script>"
                "<article><p>" + "The company disclosed unauthorized access to its systems. " * 12 + "</p></article>"
                "<footer>copyright copyright copyright copyright copyright copyright</footer></html>")
        text = pl.html_to_text(page)
        self.assertIn("unauthorized access", text)
        self.assertNotIn("evil", text)
        self.assertNotIn("menu", text)


class AnalysisTest(unittest.TestCase):
    def test_heuristic_rejects_roundups_and_scores_real_breaches(self):
        roundup = {"title": "In Other News: Bank Breaches, Botnet", "summary": "exposed GPU monitors leak telemetry"}
        real = {"title": "Acme Health confirms data breach exposing 2.1 million patients",
                "summary": "Ransomware group stole patient records, including health data and Social Security numbers."}
        self.assertFalse(pl.heuristic_analysis(roundup, "")["is_breach_report"])
        a = pl.heuristic_analysis(real, "")
        self.assertTrue(a["is_breach_report"])
        self.assertEqual((a["severity"], a["records_affected"]), ("critical", 2_100_000))

    def test_unusable_model_output_is_retryable_not_stored(self):
        with self.assertRaises(TransientError):
            pl.clean_analysis({"is_breach_report": True, "severity": "catastrophic"})
        with self.assertRaises(TransientError):
            pl.clean_analysis({"severity": "low"})  # missing fields

    def test_graph_cleaning_drops_dangling_edges_and_bad_types(self):
        g = pl.clean_graph({"nodes": [{"id": "a", "label": "Acme", "type": "organization"},
                                      {"id": "a", "label": "dup", "type": "organization"},
                                      {"id": "b", "label": "X", "type": "nonsense"}],
                            "edges": [{"source": "a", "target": "b", "label": "hit"},
                                      {"source": "a", "target": "zzz", "label": "ghost"}]})
        self.assertEqual([n["id"] for n in g["nodes"]], ["a", "b"])
        self.assertEqual(g["nodes"][1]["type"], "system")
        self.assertEqual(len(g["edges"]), 1)

    def test_prompt_treats_article_as_untrusted(self):
        self.assertIn("untrusted", pl.SYSTEM)
        self.assertIn("<article>", pl.article_block({"title": "t", "source": "s", "url": "u"}, {"text": "hi"}))


class RobotsTest(unittest.TestCase):
    def setUp(self):
        pl._robots.clear()
        self._get = pl.http_get

    def tearDown(self):
        pl.http_get = self._get

    def allowed(self, robots):
        async def fake(url, max_bytes=0, accept=""):
            if isinstance(robots, Exception):
                raise robots
            return robots
        pl.http_get = fake
        return asyncio.run(pl.robots_allows("https://site.example/news/story"))

    def test_disallow_is_honoured(self):
        self.assertFalse(self.allowed(b"User-agent: *\nDisallow: /news/\n"))

    def test_missing_robots_means_allowed_but_unreachable_means_no(self):
        self.assertTrue(self.allowed(PermanentError("HTTP 404")))
        pl._robots.clear()
        self.assertFalse(self.allowed(TransientError("HTTP 503")))


# --- the workflow, with fakes for the LLM, MongoDB and RabbitMQ ------------------------------------

class FakeLLM:
    calls = 0
    mode = "ok"            # ok | quota

    def __init__(self, name, config):
        self.name = name

    async def call_tool(self, system, user, tool):
        FakeLLM.calls += 1
        if FakeLLM.mode == "quota":
            raise QuotaExhausted("Anthropic credit balance is too low")
        if tool["name"] == "report_breach_analysis":
            data = {"is_breach_report": True, "organization": "Acme", "sector": "health", "breach_date": None,
                    "disclosure_date": None, "records_affected": 2100000, "data_types": ["health data"],
                    "attack_vector": "ransomware", "threat_actor": "LockGroup", "severity": "critical",
                    "severity_rationale": "Sensitive data of millions.", "confidence": 0.9, "evidence": "q"}
        else:
            data = {"plain_summary": "Hackers stole patient records.", "timeline": ["Oct 1: attack"],
                    "who_is_affected": "Patients", "what_to_do": ["Watch statements"],
                    "graph": {"nodes": [{"id": "o", "label": "Acme", "type": "organization"},
                                        {"id": "a", "label": "LockGroup", "type": "threat_actor"}],
                              "edges": [{"source": "a", "target": "o", "label": "breached"}]}}
        return {"input": data, "model": "fake-model", "tokens": 100}


class FakeMongo:
    def __init__(self):
        self.docs = {}

    def upsert_breach(self, doc):
        old = self.docs.get(doc["id"])
        if old and old["revision"] > doc["revision"]:
            return "kept-newer"
        self.docs[doc["id"]] = doc
        return "stored"


class FakeAmqp:
    def __init__(self):
        self.messages, self.down = [], False

    def publish(self, message):
        if self.down:
            raise ConnectionError("broker unreachable")
        self.messages.append(message)


ITEM = {"id": "abc123", "source": "t", "title": "Acme Health confirms data breach exposing 2.1 million patients",
        "url": "https://news.example/acme", "published": "2026-10-08T10:00:00+00:00",
        "summary": "Ransomware stole patient records.",
        "inline_text": "Acme Health disclosed a ransomware attack that exposed health data of 2.1 million patients."}


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = stitch.Store(Path(self.tmp.name) / "s.db")
        self.mongo, self.amqp = FakeMongo(), FakeAmqp()
        pl.RT.sinks = pl.Sinks(self.mongo, self.amqp)
        pl.RT.llm, pl.RT.on_outage, pl.RT.budget_usd, pl.RT.spent_usd = True, "degrade", 5.0, 0.0
        FakeLLM.calls, FakeLLM.mode = 0, "ok"
        self.old_factory = pl.llm.factory
        pl.llm.factory, pl.llm._clients = FakeLLM, {}
        pl.llm.tuning = stitch.Defaults(failure_threshold=2, cooldown=0.3, max_cooldown=0.5,
                                        quota_cooldown=0.4, rate_limit_cooldown=0.1)
        self._sleep = None

    def tearDown(self):
        pl.llm.factory = self.old_factory
        self.store.close()
        self.tmp.cleanup()

    def run_item(self, **kw):
        return asyncio.run(pl.process_article.map([ITEM], store=self.store, batch="t", **kw))

    def test_happy_path_stores_and_publishes_once(self):
        stats = self.run_item()
        self.assertEqual((stats.items_done, stats.items_degraded), (1, 0))
        doc = self.mongo.docs["abc123"]
        self.assertEqual((doc["severity"], doc["analysis_source"], doc["revision"]), ("critical", "llm", 1))
        self.assertEqual(len(doc["graph"]["nodes"]), 2)
        (message,) = self.amqp.messages
        self.assertEqual((message["id"], message["revision"], message["event"]), ("abc123", 1, "breach.upserted"))

    def test_rerun_does_no_work_and_publishes_nothing_more(self):
        self.run_item()
        calls = FakeLLM.calls
        self.run_item()
        self.assertEqual((FakeLLM.calls, len(self.amqp.messages)), (calls, 1))

    def test_llm_out_of_credits_publishes_flagged_fallback_then_heal_supersedes_it(self):
        FakeLLM.mode = "quota"
        stats = self.run_item()
        self.assertEqual(stats.items_degraded, 1)
        first = self.mongo.docs["abc123"]
        self.assertEqual((first["analysis_source"], first["revision"]), ("rule-based", 0))   # downstream not stale
        self.assertEqual(len(self.amqp.messages), 1)

        FakeLLM.mode = "ok"
        time.sleep(0.5)                                   # the breaker's cooldown
        healed = asyncio.run(stitch.heal(store=self.store))
        self.assertTrue(healed)
        final = self.mongo.docs["abc123"]
        self.assertEqual((final["analysis_source"], final["revision"]), ("llm", 1))
        self.assertEqual([m["revision"] for m in self.amqp.messages], [0, 1])       # the consumer sees the upgrade

    def test_rule_based_revision_never_overwrites_an_llm_one(self):
        self.run_item()
        late = {**self.mongo.docs["abc123"], "revision": 0, "analysis_source": "rule-based"}
        self.assertEqual(self.mongo.upsert_breach(late), "kept-newer")

    def test_spend_cap_acts_like_exhausted_credits(self):
        pl.RT.budget_usd, pl.RT.spent_usd = 0.01, 0.01
        client = pl.ClaudeClient("p", {"model": "claude-haiku-4-5"})
        with self.assertRaises(QuotaExhausted):
            asyncio.run(client.call_tool("s", "u", pl.ANALYSE_TOOL))

    def test_queue_outage_parks_the_article_and_resume_publishes_it_exactly_once(self):
        self.amqp.down = True
        stats = self.run_item()
        self.assertEqual((stats.items_parked, stats.items_done), (1, 0))
        self.assertEqual(self.amqp.messages, [])
        self.assertEqual(self.store.parked("breach-monitor:t")[0][0], "abc123")
        self.assertIn("abc123", self.mongo.docs)           # Mongo write still happened

        self.amqp.down = False
        time.sleep(0.05)
        for _, _, _ in self.store.parked("breach-monitor:t"):
            pass
        self.store._db.execute("UPDATE items SET retry_at = 0 WHERE status = 'parked'")
        asyncio.run(stitch.resume(store=self.store))
        self.assertEqual(len(self.amqp.messages), 1)

    def test_non_breach_article_is_dropped_without_storing_or_publishing(self):
        class NotBreach(FakeLLM):
            async def call_tool(self, system, user, tool):
                out = await super().call_tool(system, user, tool)
                out["input"]["is_breach_report"] = False
                return out
        pl.llm.factory, pl.llm._clients = NotBreach, {}
        self.run_item()
        self.assertEqual((self.mongo.docs, self.amqp.messages), ({}, []))


if __name__ == "__main__":
    unittest.main()
