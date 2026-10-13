import asyncio
import contextlib
import io
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from openserp import SERPError
from pymongo.errors import ConnectionFailure
from stitch import PipelineHalted

os.environ["TALK_RETRY_SECONDS"] = "0.001"

try:
    from talk import ai_news_pipeline as plain_pipeline
    from talk import talk_alerter
    from talk import ai_news_pipeline_durable as durable_pipeline
except ImportError as error:
    raise unittest.SkipTest(f"needs the demo dependencies: {error}")

ARTICLE_COUNT = 4
ARTICLE_PAGE = ("<html><p>" + "The company confirmed a breach of customer data. " * 5 + "</p></html>").encode()
MODEL_REPLY = json.dumps({"message": {"content": '{"label": "breakthrough", "reason": "short reason"}'}}).encode()


def news_articles(article_count: int) -> list[dict]:
    return [
        {
            "id": str(number),
            "title": f"News number {number}",
            "url": f"https://news.example/a{number}",
            "summary": f"summary {number}",
            "domain": "news.example",
        }
        for number in range(1, article_count + 1)
    ]


class ProcessKilled(BaseException):
    pass


class FakeCollection:
    def __init__(self):
        self.documents = {}
        self.failures_left = 0
        self.down = False

    def find_one(self, query):
        if self.down:
            raise ConnectionFailure("mongo is down")
        return self.documents.get(query["_id"])

    def replace_one(self, query, document, upsert=False):
        if self.down:
            raise ConnectionFailure("mongo is down")
        if self.failures_left:
            self.failures_left -= 1
            raise ConnectionFailure("mongo is down")
        self.documents[query["_id"]] = document

    def count_documents(self, query):
        return len(self.documents)

    def drop(self):
        self.documents.clear()


class FakeInternet:
    def __init__(self):
        self.article_count = ARTICLE_COUNT
        self.searches = []
        self.offline = False
        self.model_refuses = False
        self.cooldown_responses = 0
        self.kill_on_model_call = None
        self.model_requests = 0
        self.downloads = []
        self.downloads_in_flight = 0
        self.most_downloads_in_flight = 0
        self.lock = threading.Lock()

    def __call__(self, url, headers=None, body=None, timeout_seconds=30):
        if self.offline:
            raise urllib.error.URLError("no internet")
        if url == plain_pipeline.OLLAMA_URL:
            return self._model_answer(url)
        return self._page(url)

    async def search(self, query, engine):
        self.searches.append((query, engine))
        return news_articles(self.article_count)

    def _page(self, url):
        with self.lock:
            self.downloads.append(url)
            self.downloads_in_flight += 1
            self.most_downloads_in_flight = max(self.most_downloads_in_flight, self.downloads_in_flight)
        time.sleep(0.002)
        with self.lock:
            self.downloads_in_flight -= 1
        return ARTICLE_PAGE

    def _model_answer(self, url):
        with self.lock:
            self.model_requests += 1
            request_number = self.model_requests
        if request_number == self.kill_on_model_call:
            raise ProcessKilled()
        if self.cooldown_responses >= request_number:
            raise urllib.error.HTTPError(url, 429, "cooling down", {"Retry-After": "0"}, None)
        if self.model_refuses:
            raise urllib.error.HTTPError(url, 429, "limit reached", {}, None)
        return MODEL_REPLY


class TalkPipelineTestCase(unittest.TestCase):
    module = plain_pipeline

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        output = Path(self.directory.name)
        self.internet = FakeInternet()
        self.collection = FakeCollection()
        self.patches = [
            mock.patch.object(self.module, "http_get", self.internet),
            mock.patch.object(self.module, "OUTPUT_DIRECTORY", output),
            mock.patch.object(self.module, "PUBLISHED_FILE", output / "published.jsonl"),
            mock.patch.object(self.module, "MODEL_CALLS_FILE", output / "model_calls.log"),
            mock.patch.object(self.module, "mongo_collection", return_value=self.collection),
            mock.patch.object(self.module, "WORKER_COUNT", 1),
            mock.patch.object(talk_alerter, "ALERTS_FILE", output / "alerts.jsonl"),
            mock.patch.object(self.module, "search_news", self.internet.search),
            mock.patch.object(self.module, "EMPTY_ROUND_PAUSE_SECONDS", 0.001),
            mock.patch.object(self.module, "ARTICLES_LIMIT", ARTICLE_COUNT),
        ]
        if hasattr(self.module, "CHECKPOINT_FILE"):
            self.patches.append(mock.patch.object(self.module, "CHECKPOINT_FILE", output / "checkpoints.db"))
        for patch in self.patches:
            patch.start()

    def tearDown(self):
        for patch in self.patches:
            patch.stop()
        self.directory.cleanup()

    def run_quietly(self, coroutine):
        with contextlib.redirect_stdout(io.StringIO()):
            return asyncio.run(coroutine)

    def published(self) -> list[dict]:
        return [json.loads(line) for line in self.module.read_lines(self.module.PUBLISHED_FILE)]

    def model_calls(self) -> int:
        return len(self.module.read_lines(self.module.MODEL_CALLS_FILE))


class PlainPipelineTest(TalkPipelineTestCase):
    def test_a_crash_makes_the_rerun_repeat_work_and_publish_duplicates(self):
        self.internet.kill_on_model_call = 3
        with self.assertRaises(BaseException):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len(self.published()), 2)
        self.run_quietly(self.module.run_pipeline())
        published = self.published()
        self.assertEqual((len(published), len({record["id"] for record in published})), (ARTICLE_COUNT + 2, ARTICLE_COUNT))
        self.assertEqual(self.model_calls(), ARTICLE_COUNT + 2)

    def test_the_model_refusing_stops_the_whole_run(self):
        self.internet.model_refuses = True
        with self.assertRaises(ExceptionGroup):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual(self.published(), [])

    def test_mongo_holds_one_document_per_article_even_after_a_crash(self):
        self.internet.kill_on_model_call = 3
        with self.assertRaises(BaseException):
            self.run_quietly(self.module.run_pipeline())
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len(self.collection.documents), ARTICLE_COUNT)


class DurablePipelineTest(TalkPipelineTestCase):
    module = durable_pipeline

    def test_a_crash_resumes_without_repeating_work_or_duplicating(self):
        self.internet.kill_on_model_call = 3
        with self.assertRaises(BaseException):
            self.run_quietly(self.module.run_pipeline())
        self.run_quietly(self.module.run_pipeline())
        published = self.published()
        self.assertEqual((len(published), len({record["id"] for record in published})), (ARTICLE_COUNT, ARTICLE_COUNT))
        self.assertEqual(self.model_calls(), ARTICLE_COUNT)
        self.assertEqual(self.internet.downloads.count("https://news.example/a3"), 1)

    def test_the_model_refusing_publishes_flagged_fallbacks_and_heal_replaces_them(self):
        self.internet.model_refuses = True
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual({record["source"] for record in self.published()}, {"fallback"})
        self.assertEqual(self.model_calls(), 0)
        self.internet.model_refuses = False
        self.run_quietly(self.module.heal_pipeline())
        latest = {record["id"]: record for record in self.published()}
        self.assertEqual({record.get("source") for record in latest.values()}, {None})
        self.assertEqual(self.model_calls(), ARTICLE_COUNT)
        self.assertEqual(len(self.internet.downloads), ARTICLE_COUNT)

    def test_an_internet_outage_stops_articles_cleanly_and_a_rerun_finishes_them(self):
        self.internet.offline = True
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(self.published(), [])
        self.internet.offline = False
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len({record["id"] for record in self.published()}), ARTICLE_COUNT)
        self.assertEqual(self.model_calls(), ARTICLE_COUNT)

    def test_a_cooldown_with_retry_after_is_waited_out_not_degraded(self):
        self.internet.cooldown_responses = 3
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual({record.get("source") for record in self.published()}, {None})
        self.assertEqual(self.model_calls(), ARTICLE_COUNT)
        self.assertEqual(len(self.collection.documents), ARTICLE_COUNT)

    def test_mongo_being_down_is_retried_without_asking_the_model_again(self):
        self.collection.failures_left = 2
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len(self.collection.documents), ARTICLE_COUNT)
        self.assertEqual(self.model_calls(), ARTICLE_COUNT)
        self.assertEqual(len(self.published()), ARTICLE_COUNT)

    def test_heal_replaces_the_fallback_documents_in_mongo(self):
        self.internet.model_refuses = True
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual({document["source"] for document in self.collection.documents.values()}, {"fallback"})
        self.internet.model_refuses = False
        self.run_quietly(self.module.heal_pipeline())
        self.assertEqual({document.get("source") for document in self.collection.documents.values()}, {None})

    def alerts(self) -> list[dict]:
        return [json.loads(line) for line in self.module.read_lines(talk_alerter.ALERTS_FILE)]

    def test_mongo_staying_down_halts_the_pipeline_alerts_and_resumes_only_when_it_is_back(self):
        self.collection.down = True
        with self.assertRaises(PipelineHalted):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual({(alert["channel"], alert["severity"]) for alert in self.alerts()}, {("email", "critical"), ("slack", "critical")})
        with self.assertRaises(PipelineHalted):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len(self.alerts()), 2)
        self.assertEqual(self.published(), [])
        self.collection.down = False
        self.run_quietly(self.module.run_pipeline())
        self.assertEqual(len({record["id"] for record in self.published()}), ARTICLE_COUNT)
        self.assertEqual(len(self.collection.documents), ARTICLE_COUNT)
        self.assertEqual(self.model_calls(), ARTICLE_COUNT)
        self.assertEqual([alert["severity"] for alert in self.alerts()][-2:], ["recovered", "recovered"])

    def test_five_workers_finish_every_article_exactly_once(self):
        self.internet.article_count = 40
        with mock.patch.object(self.module, "ARTICLES_LIMIT", 40), mock.patch.object(self.module, "WORKER_COUNT", 5):
            self.run_quietly(self.module.run_pipeline())
        self.assertEqual(sorted(record["id"] for record in self.published()), sorted(str(number) for number in range(1, 41)))
        self.assertEqual(len(self.collection.documents), 40)
        self.assertEqual(self.model_calls(), 40)
        self.assertTrue(1 < self.internet.most_downloads_in_flight <= 5)

    def test_the_stream_skips_urls_it_has_already_seen_and_stops_at_the_limit(self):
        async def collect():
            return [article["url"] async for article in self.module.stream_news()]

        urls = self.run_quietly(collect())
        self.assertEqual(urls, [f"https://news.example/a{number}" for number in range(1, ARTICLE_COUNT + 1)])
        self.assertGreater(len(self.internet.searches), 0)

    def test_snippet_ages_are_read_from_relative_and_absolute_dates(self):
        age = plain_pipeline.snippet_age_days
        self.assertEqual([age("Today It is"), age("1 day ago13 models"), age("5 days agoAI"), age("18 hours ago · x")][:3], [0, 1, 5])
        self.assertAlmostEqual(age("18 hours ago · x"), 0.75)
        self.assertEqual(age("2 weeks ago x"), 14)
        self.assertIsNone(age("Track every major AI model"))
        self.assertGreater(age("Aug 6, 2020 old"), 365)

    def test_only_recent_articles_are_kept_when_a_maximum_age_is_set(self):
        class Result:
            def __init__(self, snippet):
                self.snippet = snippet

        with mock.patch.object(plain_pipeline, "MAX_AGE_DAYS", 7):
            kept = [snippet for snippet in ["Today a", "6 days ago b", "9 days ago c", "no date d"] if plain_pipeline.is_recent_enough(Result(snippet))]
        self.assertEqual(kept, ["Today a", "6 days ago b"])

    def test_a_failed_search_is_skipped_and_the_stream_carries_on(self):
        class BrokenClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *arguments):
                return False

            async def search(self, **parameters):
                raise self.error

        BrokenClient.error = SERPError("captcha detected")
        with mock.patch.object(plain_pipeline, "AsyncOpenSERP", return_value=BrokenClient()):
            found = self.run_quietly(plain_pipeline.search_news("ai", "google"))
        self.assertEqual(found, [])

    def test_the_two_scripts_differ_only_by_the_durability_lines(self):
        plain = set(Path(plain_pipeline.__file__).read_text().splitlines())
        durable = set(Path(durable_pipeline.__file__).read_text().splitlines())
        removed = sorted(line for line in plain - durable if line.strip())
        self.assertEqual(removed, [
            '            await limit.acquire()',
            '            tasks.create_task(process_and_release(article, limit))',
            '        async for article in stream_news():',
            '        asyncio.run(run_pipeline())',
            '        await process_article(article)',
            '        limit.release()',
            '        print("usage: python -m talk.ai_news_pipeline [run|report|reset]")',
            '    async with asyncio.TaskGroup() as tasks:',
            '    finally:',
            '    limit = asyncio.Semaphore(WORKER_COUNT)',
            '    logger.success("finished")',
            'async def process_and_release(article: dict, limit: asyncio.Semaphore) -> None:',
            'from pymongo.errors import PyMongoError',
        ])


if __name__ == "__main__":
    unittest.main()
